package control

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

func TestPostgresCostDeadlineSurvivesRestartAndToolReset(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	cfg.SchedulerPolicy = "cost-aware"
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "cost-deadline")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "cost-deadline")]
	now := time.Now().UTC()
	_, err := c.costDecisionLocked(s, ModelBoundaryRequest{}, predictionEvidence(now, -1), policy.DefaultLifecycleConfig(), now)
	plan := copyValue(s.Autonomous.ReleasePlan)
	if err == nil {
		err = c.persist()
	}
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := newTestController(t, cfg)
	state, err := reopened.Get("tenant-a", "cost-deadline")
	if err != nil || state.Autonomous == nil || !reflect.DeepEqual(state.Autonomous.ReleasePlan, plan) {
		t.Fatalf("PostgreSQL release budget changed on restart: %+v %v", state, err)
	}
	reopened.lock()
	s = reopened.state.Sessions[key("tenant-a", "cost-deadline")]
	_, err = reopened.costDecisionLocked(s, ModelBoundaryRequest{}, predictionEvidence(now.Add(time.Second), -1), policy.DefaultLifecycleConfig(), now.Add(time.Second))
	renewed := !s.Autonomous.ReleasePlan.PlannedReleaseAt.Equal(plan.PlannedReleaseAt)
	reopened.mu.Unlock()
	if err != nil || renewed {
		t.Fatalf("PostgreSQL recovered wait renewed its deadline: renewed=%v error=%v", renewed, err)
	}
	_, err = reopened.Tool(context.Background(), "tenant-a", "cost-deadline", ToolRequest{ID: "work", Epoch: 1, Kind: "file_write", Path: "durable.txt", Content: "returned"})
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	returned := newTestController(t, cfg)
	state, err = returned.Get("tenant-a", "cost-deadline")
	if err != nil || state.Autonomous == nil || state.Autonomous.ReleasePlan != nil || !state.Autonomous.CandidateSince.IsZero() {
		t.Fatalf("PostgreSQL retained a completed wait after tool admission: %+v %v", state, err)
	}
}
