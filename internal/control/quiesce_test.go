package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type rejectedCheckpointBackend struct {
	*LocalBackend
	failure error
	calls   atomic.Int32
}

func (b *rejectedCheckpointBackend) Checkpoint(context.Context, *Session, string) (Actor, string, error) {
	b.calls.Add(1)
	return Actor{}, "", b.failure
}

func TestQuiesceTimeoutAbortsWithoutManifestAndPersistsCooldown(t *testing.T) {
	cfg := testConfig(t)
	b := &rejectedCheckpointBackend{LocalBackend: cfg.Backend.(*LocalBackend), failure: &quiesceRejected{code: "DRAIN_TIMEOUT"}}
	cfg.Backend = b
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "drain")
	input := LifecycleRequest{OperationID: "timeout", ExpectedEpoch: s.Epoch}
	_, m, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", input)
	assertCode(t, err, "DRAIN_TIMEOUT")
	if m != nil {
		t.Fatal("timeout fabricated a safe point")
	}
	got, _ := c.Get("tenant-a", s.SessionID)
	if got.State != "RUNNING" || got.Gate != "OPEN" || got.Epoch != s.Epoch || got.Manifest != nil || got.QuiesceTimeouts != 1 || !got.QuiesceRetryAfter.After(time.Now()) || got.Lifecycle["timeout"].Status != "ABORTED" {
		t.Fatalf("unsafe timeout state: %+v", got)
	}
	response := httptest.NewRecorder()
	failure(response, err)
	if response.Code != 504 || !strings.Contains(response.Body.String(), "retry_after") {
		t.Fatal(response.Body.String())
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", input)
	assertCode(t, err, "DRAIN_TIMEOUT")
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "too-soon", ExpectedEpoch: s.Epoch})
	assertCode(t, err, "quiesce_cooldown")
	if b.calls.Load() != 1 || boundarySafe(got) {
		t.Fatal("cooldown did not stop another drain")
	}
	metrics := httptest.NewRecorder()
	c.telemetry.Handler().ServeHTTP(metrics, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), "quiesce_timeout_total 1") {
		t.Fatal("timeout metric missing")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	got, _ = c.Get("tenant-a", s.SessionID)
	if got.State != "RUNNING" || got.Gate != "OPEN" || got.QuiesceTimeouts != 1 || got.Lifecycle["timeout"].Status != "ABORTED" || !got.QuiesceRetryAfter.After(time.Now()) {
		t.Fatalf("restart lost abort: %+v", got)
	}
	if _, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "new-write", Epoch: s.Epoch, Kind: "file_write", Path: "safe.txt", Content: "still running"}); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorDrainDeadlineKeepsAdmittedToolRunning(t *testing.T) {
	cfg := testConfig(t)
	cfg.QuiesceDrainTimeout = 15 * time.Millisecond
	b := &blockedTools{LocalBackend: cfg.Backend.(*LocalBackend), entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Backend = b
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "long-tool")
	done := make(chan error, 1)
	go func() {
		_, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "running", Epoch: s.Epoch, Kind: "shell", Command: "controlled", TimeoutMS: 2000})
		done <- err
	}()
	<-b.entered
	_, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "drain", ExpectedEpoch: s.Epoch})
	assertCode(t, err, "DRAIN_TIMEOUT")
	got, _ := c.Get("tenant-a", s.SessionID)
	if got.ActiveTools != 1 || got.Tools["running"].Status != "RUNNING" || got.State != "RUNNING" || got.Gate != "OPEN" || got.Manifest != nil {
		t.Fatalf("tool was cancelled or snapshot fabricated: %+v", got)
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestUnknownToolReturnsReadOnlyRecoveryAndBlocksSafePoint(t *testing.T) {
	cfg := testConfig(t)
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "unknown")
	if err := os.WriteFile(filepath.Join(cfg.Backend.(*LocalBackend).workspace(s), "effect.txt"), []byte("effect already happened"), 0600); err != nil {
		t.Fatal(err)
	}
	c.lock()
	live := c.state.Sessions[key("tenant-a", s.SessionID)]
	live.State, live.Gate = "RECOVERY_REQUIRED", "CLOSED"
	live.Tools["original-write"] = &ToolReceipt{ID: "original-write", Status: "OUTCOME_UNKNOWN", Epoch: s.Epoch, Sequence: 1}
	live.NextReceipt = 1
	c.markAll()
	if err := c.persist(); err != nil {
		t.Fatal(err)
	}
	c.mu.Unlock()
	_, m, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "unsafe", ExpectedEpoch: s.Epoch})
	assertCode(t, err, "OUTCOME_UNKNOWN")
	if m != nil {
		t.Fatal("unknown result accepted as safe")
	}
	response := httptest.NewRecorder()
	failure(response, err)
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Body.String(), "inspect_workspace_read_only") || !strings.Contains(response.Body.String(), "original-write") {
		t.Fatal(body)
	}
	unknownReceipt, statusErr := c.ToolStatus("tenant-a", s.SessionID, "original-write")
	if statusErr != nil || unknownReceipt.Recovery == nil || unknownReceipt.Recovery.UnknownOperations[0] != "original-write" {
		t.Fatalf("status lost recovery guidance: %+v %v", unknownReceipt, statusErr)
	}
	_, err = c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "different-write-id", Epoch: s.Epoch, Kind: "file_write", Path: "effect.txt", Content: "replayed"})
	assertCode(t, err, "OUTCOME_UNKNOWN")
	read, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "inspect", Epoch: s.Epoch, Kind: "file_read", Path: "effect.txt"})
	if err != nil || read.Result.Content != "effect already happened" {
		t.Fatalf("read-only inspection failed: %+v %v", read, err)
	}
	got, _ := c.Get("tenant-a", s.SessionID)
	if got.State != "RECOVERY_REQUIRED" || got.Gate != "CLOSED" || got.Tools["original-write"].Status != "OUTCOME_UNKNOWN" || got.ReceiptWatermark != 0 {
		t.Fatal("inspection cleared the unresolved effect")
	}
	c.lock()
	live.Lifecycle["uncertain-suspend"] = &LifecycleReceipt{Status: "OUTCOME_UNKNOWN"}
	c.mu.Unlock()
	_, err = c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "unsafe-inspect", Epoch: s.Epoch, Kind: "file_read", Path: "effect.txt"})
	assertCode(t, err, "admission_closed")
}

func TestRuntimeQuiesceCancellationProofAndIndependentContext(t *testing.T) {
	for _, mode := range []string{"timeout", "unknown", "wrong-identity", "cancel-failure", "transport"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var cancels atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/runtime/v1/quiesce/cancel" {
					t.Errorf("unexpected path %s", r.URL.Path)
					return
				}
				cancels.Add(1)
				if mode == "cancel-failure" {
					w.WriteHeader(409)
					return
				}
				var request map[string]any
				_ = json.NewDecoder(r.Body).Decode(&request)
				if request["operation_id"] != "drain" || request["expected_epoch"] != float64(3) || request["barrier_generation"] != float64(7) || request["actor_uid"] != "actor-uid" {
					t.Error("cancellation identity changed")
				}
				uid := "actor-uid"
				if mode == "wrong-identity" {
					uid = "replacement"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": "drain", "epoch": 3, "barrier_generation": 7, "state": "CANCELLED", "gate": "OPEN", "actor_uid": uid, "identity_attested": true})
			}))
			defer server.Close()
			b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: server.URL}, http: server.Client()}
			cause := error(&runtimeHTTPError{status: 504, code: "DRAIN_TIMEOUT"})
			if mode == "unknown" {
				cause = &runtimeHTTPError{status: 409, code: "OUTCOME_UNKNOWN", details: json.RawMessage(`{"unknown_operations":[{"tool_op_id":"original-write"}]}`)}
			}
			if mode == "transport" {
				cause = errors.New("connection lost before terminal reply")
			}
			cancel()
			err := b.rejectQuiesce(ctx, &Session{Epoch: 3, BarrierGeneration: 7, Actor: Actor{Name: "actor", UID: "actor-uid"}}, "drain", cause)
			rejected := asQuiesceRejection(err)
			if mode == "timeout" || mode == "unknown" {
				if rejected == nil || cancels.Load() != 1 {
					t.Fatalf("definite rejection lost: %v", err)
				}
				if mode == "unknown" && (rejected.recovery == nil || len(rejected.recovery.UnknownOperations) != 1) {
					t.Fatal("unknown operation identity missing")
				}
			} else if rejected != nil {
				t.Fatal("unverified cancellation reopened admission")
			}
			if mode == "transport" && cancels.Load() != 0 {
				t.Fatal("ambiguous transport was treated as a safe abort")
			}
		})
	}
}

func TestBoundaryTimeoutIsAbortedAcrossRestart(t *testing.T) {
	cfg := testConfig(t)
	cfg.Scheduling.SnapshotCostMS = 1
	cfg.Scheduling.ResumeCostMS = 1
	cfg.Scheduling.JournalCostMS = 1
	b := &rejectedCheckpointBackend{LocalBackend: cfg.Backend.(*LocalBackend), failure: &quiesceRejected{code: "DRAIN_TIMEOUT"}}
	cfg.Backend = b
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "boundary")
	zero := int64(0)
	input := ModelBoundaryRequest{OperationID: "timeout", ExpectedEpoch: s.Epoch, ExpectedWaitMS: 60000, WorkerPressure: 1, MinResidenceMS: &zero}
	r, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, input)
	assertCode(t, err, "DRAIN_TIMEOUT")
	if r == nil || r.Status != "ABORTED" || r.SessionState != "RUNNING" {
		t.Fatalf("boundary retained an unknown outcome: %+v", r)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	got, _ := c.Get("tenant-a", s.SessionID)
	if got.State != "RUNNING" || got.Gate != "OPEN" || got.Scheduling["timeout"].Status != "ABORTED" {
		t.Fatalf("restart blocked an aborted drain: %+v", got)
	}
	r, err = c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, input)
	assertCode(t, err, "DRAIN_TIMEOUT")
	if r.RetryAfter.IsZero() || err.(*APIError).Details["retry_after"] != r.RetryAfter {
		t.Fatal("boundary replay lost its durable cooldown")
	}
	if b.calls.Load() != 1 {
		t.Fatal("replay repeated a failed drain")
	}
}

func TestQuiesceAbortAtomicallyCommitsBoundaryReceipt(t *testing.T) {
	cfg := testConfig(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "atomic-abort")
	ctx := c.operationContext(context.Background(), "tenant-a", s.SessionID, "same-op")
	c.lock()
	live := c.state.Sessions[key("tenant-a", s.SessionID)]
	live.State, live.Gate = "QUIESCING", "CLOSED"
	receipt := &LifecycleReceipt{OperationID: "same-op", Action: "suspend", Epoch: s.Epoch, Status: "RUNNING"}
	live.Lifecycle["same-op"] = receipt
	live.Scheduling = map[string]*BoundaryDecision{"same-op": {OperationID: "same-op", Status: "RUNNING", ExpectedEpoch: s.Epoch}}
	err = c.abortQuiesceLocked(ctx, live, receipt, &quiesceRejected{code: "DRAIN_TIMEOUT"})
	c.mu.Unlock()
	assertCode(t, err, "DRAIN_TIMEOUT")
	// Read the published journal before ModelBoundary performs its second update.
	var durable State
	if err := c.store.Load(&durable); err != nil {
		t.Fatal(err)
	}
	restored := durable.Sessions[key("tenant-a", s.SessionID)]
	ReconcileScheduling(restored)
	if restored.State != "RUNNING" || restored.Gate != "OPEN" || restored.Scheduling["same-op"].Status != "ABORTED" {
		t.Fatalf("partial receipt commit after safe cancellation: %+v", restored)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQuiesceRPCAttemptAllowsRuntimeDrainReply(t *testing.T) {
	err := retryRuntimeRPC(context.Background(), "/runtime/v1/quiesce", 40*time.Second, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < 34*time.Second {
			t.Fatal("RPC timeout undercuts the 30s runtime drain")
		}
		return &runtimeHTTPError{status: 504, code: "DRAIN_TIMEOUT"}
	})
	var response *runtimeHTTPError
	if !errors.As(err, &response) || response.code != "DRAIN_TIMEOUT" {
		t.Fatal(err)
	}
}

func TestUnknownBoundaryReplayPreservesRecoveryIdentity(t *testing.T) {
	cfg := testConfig(t)
	cfg.Scheduling.SnapshotCostMS, cfg.Scheduling.ResumeCostMS, cfg.Scheduling.JournalCostMS = 1, 1, 1
	cfg.Backend = &rejectedCheckpointBackend{LocalBackend: cfg.Backend.(*LocalBackend),
		failure: &quiesceRejected{code: "OUTCOME_UNKNOWN", recovery: toolRecoveryHint([]string{"runtime-original-write"})}}
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "unknown-boundary")
	zero := int64(0)
	input := ModelBoundaryRequest{OperationID: "unknown", ExpectedEpoch: s.Epoch, ExpectedWaitMS: 60000, WorkerPressure: 1, MinResidenceMS: &zero}
	first, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, input)
	assertCode(t, err, "OUTCOME_UNKNOWN")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	replayed, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, input)
	assertCode(t, err, "OUTCOME_UNKNOWN")
	api := err.(*APIError)
	hint, ok := api.Details["recovery"].(*ToolRecoveryHint)
	if replayed.Status != "ABORTED" || first.RetryAfter != replayed.RetryAfter || !ok || hint == nil || len(hint.UnknownOperations) != 1 || hint.UnknownOperations[0] != "runtime-original-write" || hint.AutomaticRetry {
		t.Fatalf("unknown replay changed recovery contract: %+v %+v", replayed, api.Details)
	}
}
