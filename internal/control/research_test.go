package control

import (
	"context"
	"encoding/json"
	"testing"
)

func TestResearchCaptureRequiresTransactionalStore(t *testing.T) {
	cfg := testConfig(t)
	cfg.ResearchRunID = "research-test"
	if c, err := New(context.Background(), cfg); err == nil {
		c.Close()
		t.Fatal("nontransactional research store accepted")
	}
}

func TestResearchPostgresCapturesEventClockEpochAndDecision(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	cfg.ResearchRunID = "research-test"
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "research")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "research")]
	s.Autonomous = &AutonomousState{Evidence: &ServerEstimateEvidence{ModelSamples: 3}}
	c.event(s, "autonomous.evaluation", "")
	s.Autonomous.Evidence.ModelSamples = 100
	if err := c.persist(); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	c.mu.Unlock()
	db := postgresInspector(t, cfg.DatabaseURL)
	var raw []byte
	if err := db.QueryRow(context.Background(), `SELECT payload FROM agentcloud.research_records
WHERE payload->>'kind'='autonomous.evaluation'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	if event.EvidenceClock == nil || event.EvidenceClock.SourceInstanceID == "" || event.ExecutionEpochID == "" || event.ActorUID == "" {
		t.Fatalf("identity incomplete: %s", raw)
	}
	if event.AutonomousSnapshot.Evidence.ModelSamples != 3 {
		t.Fatalf("decision mutated after capture: %s", raw)
	}
}
