package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func boundaryTestRequest(id string, epoch uint64) ModelBoundaryRequest {
	zero := int64(0)
	return ModelBoundaryRequest{OperationID: id, ExpectedEpoch: epoch, ExpectedWaitMS: 10_000, MinResidenceMS: &zero, WorkerPressure: 1}
}

func TestModelBoundarySuspendsAtSafePointAndRestoresFiles(t *testing.T) {
	c := newTestController(t, testConfig(t))
	s := createTestSession(t, c, "tenant-a", "boundary")
	if _, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "write", Epoch: s.Epoch, Kind: "file_write", Path: "result.txt", Content: "committed"}); err != nil {
		t.Fatal(err)
	}
	request := boundaryTestRequest("boundary-op", s.Epoch)
	d, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || d.Status != "COMPLETED" || d.Decision.Action != "SUSPEND" || d.ActualLifecycleElapsedMS == nil || d.Manifest == nil || d.Manifest.ReceiptWatermark != 1 {
		t.Fatalf("decision=%+v err=%v", d, err)
	}
	suspended, _ := c.Get("tenant-a", s.SessionID)
	if suspended.State != "SUSPENDED" || suspended.Gate != "CLOSED" || suspended.ActiveTools != 0 {
		t.Fatalf("unsafe suspension: %+v", suspended)
	}
	resumed, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "resume", LifecycleRequest{OperationID: "resume", ExpectedEpoch: s.Epoch})
	if err != nil || resumed.Epoch != s.Epoch+1 || resumed.LastResumedAt.IsZero() {
		t.Fatalf("resume=%+v err=%v", resumed, err)
	}
	r, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "read", Epoch: resumed.Epoch, Kind: "file_read", Path: "result.txt"})
	if err != nil || r.Result.Content != "committed" {
		t.Fatalf("restore=%+v err=%v", r, err)
	}
	replay, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || replay.StartedAt != d.StartedAt || replay.ResultEpoch != d.ResultEpoch {
		t.Fatalf("durable replay=%+v err=%v", replay, err)
	}
	_, err = c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, boundaryTestRequest("stale", s.Epoch))
	assertCode(t, err, "stale_epoch")
	cooldown, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, boundaryTestRequest("cooldown", resumed.Epoch))
	if err != nil || cooldown.Decision.Action != "KEEP" || cooldown.Decision.Reason != "resume_cooldown" {
		t.Fatalf("cooldown=%+v err=%v", cooldown, err)
	}
	request.ExpectedWaitMS++
	_, err = c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	assertCode(t, err, "payload_conflict")
}

func TestModelBoundaryActiveAndUnknownToolsKeepAdmission(t *testing.T) {
	cfg := testConfig(t)
	b := &blockedTools{LocalBackend: cfg.Backend.(*LocalBackend), entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Backend = b
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "active")
	done := make(chan error, 1)
	go func() {
		_, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "active-tool", Epoch: s.Epoch, Kind: "shell", Command: "controlled", TimeoutMS: 1000})
		done <- err
	}()
	<-b.entered
	d, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, boundaryTestRequest("active-hold", s.Epoch))
	close(b.release)
	if toolErr := <-done; toolErr != nil {
		t.Fatal(toolErr)
	}
	if err != nil || d.Decision.Action != "KEEP" || d.Decision.Reason != "unsafe_point" {
		t.Fatalf("active decision=%+v err=%v", d, err)
	}
	state, _ := c.Get("tenant-a", s.SessionID)
	if state.Gate != "OPEN" || state.State != "RUNNING" || len(state.Lifecycle) != 0 {
		t.Fatalf("active tool mutated lifecycle: %+v", state)
	}
	c.mu.Lock()
	c.state.Sessions[key("tenant-a", s.SessionID)].Tools["active-tool"].Status = "OUTCOME_UNKNOWN"
	c.mu.Unlock()
	d, err = c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, boundaryTestRequest("unknown-hold", s.Epoch))
	if err != nil || d.Decision.Action != "KEEP" || d.Decision.Reason != "unsafe_point" {
		t.Fatalf("unknown decision=%+v err=%v", d, err)
	}
}

type observedQueue struct {
	waiting uint64
	err     error
	age     time.Duration
	calls   atomic.Int32
}

func (q *observedQueue) Waiting(ctx context.Context, s *Session) (QueueObservation, error) {
	q.calls.Add(1)
	return QueueObservation{Source: "native_dynamo", Scope: "classifier", WaitingRequests: q.waiting, ObservedAt: time.Now().UTC().Add(-q.age)}, q.err
}

func TestModelBoundaryQueueEvidenceAndFailureFallback(t *testing.T) {
	for _, test := range []struct {
		name           string
		queue          *observedQueue
		pressure       float64
		action, reason string
	}{
		{"native_pressure", &observedQueue{waiting: 3}, 1, "SUSPEND", "estimated_wait_exceeds_cost"},
		{"empty_classifier", &observedQueue{}, 0, "KEEP", "observed_queue_empty"},
		{"worker_pressure_without_model_queue", &observedQueue{}, 0.8, "SUSPEND", "estimated_wait_exceeds_cost"},
		{"unavailable", &observedQueue{err: errors.New("router unavailable")}, 1, "KEEP", "queue_observation_failed"},
		{"stale", &observedQueue{waiting: 3, age: time.Minute}, 1, "KEEP", "queue_observation_stale"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t)
			q := &observedQueue{waiting: test.queue.waiting, err: test.queue.err, age: test.queue.age}
			cfg.QueueObserver = q
			c := newTestController(t, cfg)
			s := createTestSession(t, c, "tenant-a", "observed")
			request := boundaryTestRequest("observed-op", s.Epoch)
			request.WorkerPressure = test.pressure
			d, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
			if err != nil || d.Decision.Action != test.action || d.Decision.Reason != test.reason || d.EstimateSource != "caller_estimate" {
				t.Fatalf("decision=%+v err=%v", d, err)
			}
			if q.err == nil && (d.QueueObservation == nil || d.QueueObservation.Scope != "classifier") {
				t.Fatalf("queue provenance missing: %+v", d)
			}
			if _, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request); err != nil || q.calls.Load() != 1 {
				t.Fatalf("replay contacted upstream: calls=%d err=%v", q.calls.Load(), err)
			}
		})
	}
}

func TestModelBoundaryPersistenceAndAuthenticatedEndpoint(t *testing.T) {
	cfg := testConfig(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "persisted")
	request := boundaryTestRequest("short-wait", s.Epoch)
	request.ExpectedWaitMS = 1
	d, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || d.Decision.Action != "KEEP" || d.ActualLifecycleElapsedMS != nil {
		t.Fatalf("decision=%+v err=%v", d, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newTestController(t, cfg)
	replay, err := reopened.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || replay.StartedAt != d.StartedAt {
		t.Fatalf("restart replay=%+v err=%v", replay, err)
	}
	for _, test := range []struct {
		token, body string
		status      int
	}{
		{"", `{"operation_id":"http","expected_epoch":1}`, 401},
		{"token-b", `{"operation_id":"http","expected_epoch":1}`, 404},
		{"token-a", `{"operation_id":"http","expected_epoch":1,"worker_pressure":2}`, 400},
		{"token-a", `{"operation_id":"http","expected_epoch":1,"expected_wait_ms":1}`, 200},
	} {
		r := httptest.NewRequest(http.MethodPost, "/v1/sessions/persisted/model-boundary", strings.NewReader(test.body))
		if test.token != "" {
			r.Header.Set("Authorization", "Bearer "+test.token)
		}
		w := httptest.NewRecorder()
		reopened.Handler().ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("status=%d want=%d body=%s", w.Code, test.status, w.Body.String())
		}
	}
}

func TestInterruptedBoundaryRequiresReconciliation(t *testing.T) {
	cfg := testConfig(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "interrupted")
	c.mu.Lock()
	live := c.state.Sessions[key("tenant-a", s.SessionID)]
	live.Scheduling = map[string]*BoundaryDecision{"interrupted": {OperationID: "interrupted", Status: "RUNNING"}}
	live.Gate = "CLOSED"
	err = c.persist()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newTestController(t, cfg)
	state, _ := reopened.Get("tenant-a", s.SessionID)
	if state.State != "RECOVERY_REQUIRED" || state.Gate != "CLOSED" || state.Scheduling["interrupted"].Status != "OUTCOME_UNKNOWN" {
		t.Fatalf("interrupted intent reopened admission: %+v", state)
	}
}

type boundaryCheckpoint struct {
	*LocalBackend
	entered     chan struct{}
	release     chan struct{}
	checkpoints atomic.Int32
}

func (b *boundaryCheckpoint) Checkpoint(ctx context.Context, s *Session, id string) (Actor, string, error) {
	b.checkpoints.Add(1)
	close(b.entered)
	select {
	case <-ctx.Done():
		return Actor{}, "", ctx.Err()
	case <-b.release:
		return b.LocalBackend.Checkpoint(ctx, s, id)
	}
}

func TestModelBoundaryConcurrentReplayClosesToolAdmission(t *testing.T) {
	cfg := testConfig(t)
	b := &boundaryCheckpoint{LocalBackend: cfg.Backend.(*LocalBackend), entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Backend = b
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "concurrent-boundary")
	request := boundaryTestRequest("single-op", s.Epoch)
	done := make(chan error, 1)
	go func() {
		_, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
		done <- err
	}()
	<-b.entered
	_, replayErr := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	_, toolErr := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "late", Epoch: s.Epoch, Kind: "file_write", Path: "late", Content: "late"})
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertCode(t, replayErr, "scheduling_unknown")
	assertCode(t, toolErr, "admission_closed")
	if b.checkpoints.Load() != 1 {
		t.Fatalf("duplicate snapshots: %d", b.checkpoints.Load())
	}
	if _, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request); err != nil {
		t.Fatal(err)
	}
}

func TestModelBoundaryOperationIDRejectsPublicLifecycleReuse(t *testing.T) {
	c := newTestController(t, testConfig(t))
	s := createTestSession(t, c, "tenant-a", "shared-operation")
	request := boundaryTestRequest("shared", s.Epoch)
	request.ExpectedWaitMS = 0
	decision, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || decision.Decision.Action != "KEEP" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: request.OperationID, ExpectedEpoch: s.Epoch})
	assertCode(t, err, "payload_conflict")
	state, err := c.Get("tenant-a", s.SessionID)
	if err != nil || state.State != "RUNNING" || state.Gate != "OPEN" || len(state.Lifecycle) != 0 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	replay, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, request)
	if err != nil || replay.StartedAt != decision.StartedAt || replay.Decision.Action != "KEEP" {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func TestModelBoundaryReservationFencesPublicLifecycleBeforeActuation(t *testing.T) {
	c := newTestController(t, testConfig(t))
	s := createTestSession(t, c, "tenant-a", "reserved-boundary")
	// Recreate the persisted reservation while the boundary caller is between
	// releasing its decision lock and entering the lifecycle actuator.
	c.mu.Lock()
	live := c.state.Sessions[key("tenant-a", s.SessionID)]
	live.Scheduling = map[string]*BoundaryDecision{"reserved": {OperationID: "reserved", Status: "RUNNING"}}
	live.Gate = "CLOSED"
	err := c.persist()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"checkpoint", "suspend"} {
		_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, action, LifecycleRequest{OperationID: "manual-" + action, ExpectedEpoch: s.Epoch})
		assertCode(t, err, "lifecycle_busy")
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "reserved", ExpectedEpoch: s.Epoch, CheckpointID: "another-payload"})
	assertCode(t, err, "payload_conflict")
	state, err := c.Get("tenant-a", s.SessionID)
	if err != nil || state.State != "RUNNING" || state.Scheduling["reserved"].Status != "RUNNING" || len(state.Lifecycle) != 0 {
		t.Fatalf("reservation=%+v err=%v", state, err)
	}
	state, manifest, err := c.lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "reserved", ExpectedEpoch: s.Epoch}, true)
	if err != nil || state.State != "SUSPENDED" || manifest == nil {
		t.Fatalf("reserved actuation=%+v manifest=%+v err=%v", state, manifest, err)
	}
}

func TestModelBoundaryConcurrentManualSuspendKeepsKnownOutcome(t *testing.T) {
	for attempt := 0; attempt < 32; attempt++ {
		t.Run("attempt", func(t *testing.T) {
			c := newTestController(t, testConfig(t))
			s := createTestSession(t, c, "tenant-a", "concurrent-manual")
			start := make(chan struct{})
			boundaryDone := make(chan error, 1)
			manualDone := make(chan error, 1)
			go func() {
				<-start
				_, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, boundaryTestRequest("cost", s.Epoch))
				boundaryDone <- err
			}()
			go func() {
				<-start
				_, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "manual", ExpectedEpoch: s.Epoch})
				manualDone <- err
			}()
			close(start)
			boundaryErr, manualErr := <-boundaryDone, <-manualDone
			if boundaryErr != nil {
				t.Fatalf("boundary error=%v manual error=%v", boundaryErr, manualErr)
			}
			state, err := c.Get("tenant-a", s.SessionID)
			if err != nil || state.State != "SUSPENDED" || state.Scheduling["cost"].Status != "COMPLETED" {
				t.Fatalf("state=%+v manual error=%v err=%v", state, manualErr, err)
			}
		})
	}
}
