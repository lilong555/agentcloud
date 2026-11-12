package policy

import (
	"math"
	"testing"
)

func lifecycleTestInput() LifecycleInput {
	return LifecycleInput{ExpectedWaitMS: 2000, SnapshotCostMS: 500, ResumeCostMS: 500, JournalCostMS: 50, MinimumResidenceMS: 1000, ResidenceMS: 10_000, SafePoint: true}
}

func TestLifecycleThresholdAndPressure(t *testing.T) {
	p, err := NewLifecycle(LifecycleConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name           string
		wait           int64
		pressure       float64
		residence      int64
		action, reason string
	}{
		{"short_wait", 100, 0, 10_000, "KEEP", "below_cost_threshold"},
		{"exact_threshold", 1550, 0, 10_000, "KEEP", "below_cost_threshold"},
		{"above_threshold", 1551, 0, 10_000, "SUSPEND", "estimated_wait_exceeds_cost"},
		{"pressure_tightens_window", 1300, 1, 300, "SUSPEND", "estimated_wait_exceeds_cost"},
		{"low_pressure_preserves_residence", 1300, 0, 300, "KEEP", "minimum_residence"},
		{"pressure_preserves_positive_cost", 1050, 1, 10_000, "KEEP", "below_cost_threshold"},
		{"no_wait", 0, 1, 10_000, "KEEP", "no_expected_wait"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := lifecycleTestInput()
			input.ExpectedWaitMS, input.WorkerPressure, input.ResidenceMS = test.wait, test.pressure, test.residence
			d, err := p.Decide(input)
			if err != nil || d.Action != test.action || d.Reason != test.reason || d.ThresholdMS <= d.BreakEvenMS {
				t.Fatalf("decision=%+v error=%v", d, err)
			}
		})
	}
}

func TestLifecycleSafetyCooldownAndEstimateValidation(t *testing.T) {
	p, _ := NewLifecycle(LifecycleConfig{})
	input := lifecycleTestInput()
	input.SafePoint = false
	d, _ := p.Decide(input)
	if d.Action != "KEEP" || d.Reason != "unsafe_point" {
		t.Fatalf("unsafe point: %+v", d)
	}
	input.SafePoint, input.WorkerPressure = true, 1
	elapsed := int64(4999)
	input.SinceResumeMS = &elapsed
	d, _ = p.Decide(input)
	if d.Action != "KEEP" || d.Reason != "resume_cooldown" {
		t.Fatalf("cooldown: %+v", d)
	}
	elapsed = 5000
	d, _ = p.Decide(input)
	if d.Action != "SUSPEND" {
		t.Fatalf("cooldown boundary: %+v", d)
	}
	for _, pressure := range []float64{-1, 1.1, math.NaN(), math.Inf(1)} {
		input.WorkerPressure = pressure
		if _, err := p.Decide(input); err == nil {
			t.Fatalf("accepted pressure %v", pressure)
		}
	}
	input = lifecycleTestInput()
	input.ResumeCostMS = -1
	if _, err := p.Decide(input); err == nil {
		t.Fatal("accepted negative cost")
	}
	input = lifecycleTestInput()
	input.ExpectedWaitMS = 86_400_001
	if _, err := p.Decide(input); err == nil {
		t.Fatal("accepted excessive wait")
	}
}
