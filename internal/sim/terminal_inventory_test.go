package sim

import (
	"strings"
	"testing"
)

func TestTerminalSnapshotProtocolNeedsHeadroomBeforeCompletionGC(t *testing.T) {
	trace := fixture(node("m0", "model_wait", 5000), node("a", "active", 1, "m0"), node("m1", "model_wait", 1, "a"))
	cfg := nativeConfig()
	cfg.DiskBudgetBytes = 150
	cfg.MaxEvents = 1000
	cfg.ParkInactiveTicks = true
	_, err := Run(trace, costs(), cfg, NewV2(500, 1000), nil)
	if err == nil || !strings.Contains(err.Error(), "event budget exceeded") {
		t.Fatalf("expected explicit terminal-snapshot inventory stall, got %v", err)
	}
	cfg.TerminalCheckpoint = false
	r, err := Run(trace, costs(), cfg, NewV2(500, 1000), nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.TerminalCheckpoints != 0 || r.Checkpoints != 1 || r.Restores != 1 {
		t.Fatalf("trace-end release accounting %+v", r)
	}
}
