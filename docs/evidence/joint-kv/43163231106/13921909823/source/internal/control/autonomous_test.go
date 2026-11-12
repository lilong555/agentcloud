package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

func TestAutonomousObservedWaitSafeSuspendReturnDemandAndCooldown(t *testing.T) {
	var requests atomic.Int32
	var queued atomic.Uint64
	var delayedObservation atomic.Bool
	releaseWaiting := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWaiting) }) }
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 2 {
			// Keep the observed upstream queue present until safe suspension
			// completes. Runner load then cannot consume a short response-time
			// window before the scheduler confirms stable waiting.
			queued.Store(1)
			select {
			case <-releaseWaiting:
			case <-r.Context().Done():
				return
			}
			queued.Store(0)
		} else {
			time.Sleep(200 * time.Millisecond)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer model.Close()
	defer release()
	cfg := testConfig(t)
	cfg.EnableAutonomousScheduler = true
	cfg.SchedulerInterval = 10 * time.Millisecond
	cfg.SchedulerStableFor = 20 * time.Millisecond
	cfg.ModelURL = model.URL
	cfg.ModelTimeout = 2 * time.Second
	cfg.QueueObserver = costQueueFunc(func(context.Context, *Session) (QueueObservation, error) {
		waiting := queued.Load()
		if waiting > 0 && delayedObservation.CompareAndSwap(false, true) {
			// Exercise an observation that outlasts the 200ms calibration.
			time.Sleep(250 * time.Millisecond)
		}
		return QueueObservation{Source: "observed_test_model_queue", Scope: "session", WaitingRequests: waiting, SessionWaitingRequests: waiting, ObservedAt: time.Now().UTC()}, nil
	})
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5, JournalCostMS: 1, CooldownMS: 500}
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "workspace")
	payload := json.RawMessage(`{"messages":[{"role":"user","content":"task"}]}`)
	if _, err := c.Submit("tenant-a", s.SessionID, "calibration", payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		r, _ := c.Request("tenant-a", s.SessionID, "calibration", 0)
		return r.Status == "COMPLETED"
	})
	if _, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "write", Epoch: 1, Kind: "file_write", Path: "state.txt", Content: "persistent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit("tenant-a", s.SessionID, "waiting", payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { state, _ := c.Get("tenant-a", s.SessionID); return state.State == "SUSPENDED" })
	suspended, _ := c.Get("tenant-a", s.SessionID)
	if suspended.Manifest == nil || suspended.Autonomous == nil || suspended.Autonomous.Evidence.ModelSamples < 1 {
		t.Fatalf("missing safety/evidence %+v", suspended)
	}
	if !delayedObservation.Load() {
		t.Fatal("delayed queue observation was not exercised")
	}
	for _, receipt := range suspended.Scheduling {
		if receipt.EstimateSource != "server_observed" || receipt.ServerEvidence == nil {
			t.Fatalf("caller hints supplied %+v", receipt)
		}
		if receipt.Decision.Action == "SUSPEND" && (receipt.QueueObservation == nil || receipt.QueueObservation.SessionWaitingRequests != 1) {
			t.Fatalf("missing pending upstream queue evidence %+v", receipt)
		}
	}
	release()
	waitFor(t, func() bool { r, _ := c.Request("tenant-a", s.SessionID, "waiting", 0); return r.Status == "COMPLETED" })
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", s.SessionID, LifecycleRequest{OperationID: "return", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		state, _ := c.Get("tenant-a", s.SessionID)
		return state.State == "RUNNING" && state.Epoch == 2
	})
	result, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "read", Epoch: 2, Kind: "file_read", Path: "state.txt"})
	if err != nil || result.Result.Content != "persistent" {
		t.Fatalf("continuity %+v %v", result, err)
	}
	if _, err := c.Submit("tenant-a", s.SessionID, "cooldown", payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { r, _ := c.Request("tenant-a", s.SessionID, "cooldown", 0); return r.Status == "COMPLETED" })
	state, _ := c.Get("tenant-a", s.SessionID)
	if state.State != "RUNNING" || state.Epoch != 2 || requests.Load() != 3 {
		t.Fatalf("cooldown caused another sleep %+v", state)
	}
}

func TestIdleWindowClosesAcrossResumeWithOriginEpoch(t *testing.T) {
	c := newTestController(t, testConfig(t))
	createTestSession(t, c, "tenant-a", "idle")
	_, err := c.ObserveIdle(context.Background(), "tenant-a", "idle", IdleWindowRequest{OperationID: "idle-1", ExpectedEpoch: 1, Phase: "START", TTLMS: 10000})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Lifecycle(context.Background(), "tenant-a", "idle", "suspend", LifecycleRequest{OperationID: "sleep", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Lifecycle(context.Background(), "tenant-a", "idle", "resume", LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	end := IdleWindowRequest{OperationID: "idle-1", ExpectedEpoch: 1, Phase: "END"}
	window, err := c.ObserveIdle(context.Background(), "tenant-a", "idle", end)
	if err != nil || window.FinishedAt == nil {
		t.Fatalf("window retained across epoch: %+v %v", window, err)
	}
	replay, err := c.ObserveIdle(context.Background(), "tenant-a", "idle", end)
	if err != nil || !replay.FinishedAt.Equal(*window.FinishedAt) {
		t.Fatal("idle completion replay changed the observation")
	}
	end.ExpectedEpoch = 2
	_, err = c.ObserveIdle(context.Background(), "tenant-a", "idle", end)
	assertCode(t, err, "stale_epoch")
}
