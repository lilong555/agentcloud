package control

import (
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

func TestActorLocalComputationKeepsResourcesUntilAnObservedWait(t *testing.T) {
	for _, strategy := range []string{"observed", "cost-aware"} {
		t.Run(strategy, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.SchedulerPolicy = strategy
			cfg.SchedulerIdlePrior = time.Minute
			c := newTestController(t, cfg)
			created := createTestSession(t, c, "tenant-a", "computing")
			c.lock()
			defer c.mu.Unlock()
			s := c.state.Sessions[key("tenant-a", created.SessionID)]
			s.Mode = "in-actor"
			now := time.Now().UTC()
			s.IdleSince = now.Add(-10 * time.Second)
			config := policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5, JournalCostMS: 1}
			input, evidence := c.serverEstimateLocked(s, ModelBoundaryRequest{ExpectedEpoch: s.Epoch}, nil, nil, config, now)
			if input.ExpectedWaitMS != 0 || !evidence.WaitStartedAt.IsZero() {
				t.Fatalf("local computation was inferred as idle: %+v %+v", input, evidence)
			}
			// The same actor can explicitly announce an idle window.
			s.IdleWindows = map[string]*IdleWindow{"idle": {OperationID: "idle", Epoch: s.Epoch,
				StartedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute)}}
			input, evidence = c.serverEstimateLocked(s, ModelBoundaryRequest{ExpectedEpoch: s.Epoch}, nil, nil, config, now)
			if strategy == "observed" && input.ExpectedWaitMS == 0 || strategy == "cost-aware" && evidence.WaitStartedAt.IsZero() {
				t.Fatalf("explicit Actor idle window was lost: %+v %+v", input, evidence)
			}
			// Model registration supplies a separate, durable waiting phase.
			s.IdleWindows = nil
			c.state.Requests[key("tenant-a", "model")] = &ModelRequest{ID: "model", TenantID: "tenant-a",
				SessionID: s.SessionID, Status: "PENDING", CreatedAt: now.Add(-time.Second)}
			phase, started, _ := c.waitHistoryLocked(s, now)
			if phase != "model" || started.IsZero() {
				t.Fatal("registered model did not expose the Actor waiting phase")
			}
		})
	}
}
