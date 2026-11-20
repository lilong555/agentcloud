package control

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestConditionalReturnOrdersWithModelRegistrationAndSurvivesRestart(t *testing.T) {
	cfg := testConfig(t)
	cfg.EnableAutonomousScheduler = true
	cfg.SchedulerInterval = time.Hour
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "conditional")
	c.lock()
	c.state.Requests[key("tenant-a", "done")] = &ModelRequest{ID: "done", TenantID: "tenant-a", SessionID: "conditional", Status: "COMPLETED"}
	c.markEntity("requests", key("tenant-a", "done"))
	c.mu.Unlock()
	input := LifecycleRequest{OperationID: "continue", ExpectedEpoch: 1, AfterModelRequest: "done"}
	accepted, err := c.ReturnDemand(context.Background(), "tenant-a", "conditional", input)
	if err != nil || accepted.Status != "COMPLETED" || accepted.AfterModelRequest != "done" {
		t.Fatalf("conditional return: %+v %v", accepted, err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	replayed, err := c.ReturnDemand(context.Background(), "tenant-a", "conditional", input)
	if err != nil || !reflect.DeepEqual(accepted, replayed) {
		t.Fatalf("conditional receipt was not durable: %+v %v", replayed, err)
	}
	conflict := input
	conflict.AfterModelRequest = "different"
	_, err = c.ReturnDemand(context.Background(), "tenant-a", "conditional", conflict)
	assertCode(t, err, "payload_conflict")
	// Registration after an accepted return consumes the old demand.
	_, err = c.Submit("tenant-a", "conditional", "next", json.RawMessage(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	s, _ := c.Get("tenant-a", "conditional")
	if s.Autonomous.ReturnDemand {
		t.Fatal("old return pinned a new model wait")
	}
	// Replaying the old operation preserves its result and does not reapply it.
	_, err = c.ReturnDemand(context.Background(), "tenant-a", "conditional", input)
	if err != nil {
		t.Fatal(err)
	}
	s, _ = c.Get("tenant-a", "conditional")
	if s.Autonomous.ReturnDemand {
		t.Fatal("receipt replay reactivated an old demand")
	}
}

func TestConditionalReturnRejectsUnresolvedAndForeignModelFacts(t *testing.T) {
	for _, status := range []string{"PENDING", "DISPATCHING", "OUTCOME_UNKNOWN", "FOREIGN_SESSION", "FOREIGN_TENANT", "MISSING"} {
		t.Run(status, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.EnableAutonomousScheduler = true
			cfg.SchedulerInterval = time.Hour
			c := newTestController(t, cfg)
			createTestSession(t, c, "tenant-a", "conditional")
			c.lock()
			r := &ModelRequest{ID: "done", TenantID: "tenant-a", SessionID: "conditional", Status: "COMPLETED"}
			if status == "FOREIGN_SESSION" {
				r.SessionID = "another"
			}
			if status == "FOREIGN_TENANT" {
				r.TenantID = "tenant-b"
			}
			if status != "MISSING" {
				c.state.Requests[key(r.TenantID, r.ID)] = r
			}
			if status == "PENDING" || status == "DISPATCHING" || status == "OUTCOME_UNKNOWN" {
				c.state.Requests[key("tenant-a", "newer")] = &ModelRequest{ID: "newer", TenantID: "tenant-a", SessionID: "conditional", Status: status}
			}
			c.mu.Unlock()
			_, err := c.ReturnDemand(context.Background(), "tenant-a", "conditional", LifecycleRequest{OperationID: "continue", ExpectedEpoch: 1, AfterModelRequest: "done"})
			assertCode(t, err, "return_condition_unmet")
			s, _ := c.Get("tenant-a", "conditional")
			if s.Autonomous != nil && s.Autonomous.ReturnDemand || len(s.ReturnDemands) != 0 {
				t.Fatal("unmet condition changed return admission")
			}
		})
	}
}
