// Package policy computes logical admission budgets from verified feedback.
package policy

import (
	"fmt"
	"math"
	"time"

	"github.com/lilong555/agentcloud/internal/signals"
)

type ProgramStatus string

const (
	Reasoning ProgramStatus = "Reasoning"
	Acting    ProgramStatus = "Acting"
)

type Config struct {
	ParkedMultiplier    float64       `json:"parked_multiplier"`
	SuspendedMultiplier float64       `json:"suspended_multiplier"`
	HeavyToolMultiplier float64       `json:"heavy_tool_multiplier"`
	HeavyToolMinimumMS  int64         `json:"heavy_tool_minimum_ms"`
	MinimumResidence    time.Duration `json:"-"`
}

func DefaultConfig() Config {
	return Config{ParkedMultiplier: 0.1, SuspendedMultiplier: 0, HeavyToolMultiplier: 0.25, HeavyToolMinimumMS: 1_000, MinimumResidence: 250 * time.Millisecond}
}

type Decision struct {
	Identity             signals.Binding `json:"identity"`
	ProgramStatus        ProgramStatus   `json:"program_status"`
	SignalState          signals.State   `json:"signal_state"`
	SignalFresh          bool            `json:"signal_fresh"`
	BaselineTokens       uint64          `json:"baseline_tokens"`
	BaselineWeight       float64         `json:"baseline_weight"`
	ActingBaselineWeight float64         `json:"acting_baseline_weight"`
	AppliedMultiplier    float64         `json:"applied_multiplier"`
	EffectiveWeight      float64         `json:"effective_weight"`
	LogicalBudgetTokens  uint64          `json:"logical_budget_tokens"`
	Reason               string          `json:"reason"`
	PhysicalKVAction     string          `json:"physical_kv_action"`
}

type Policy struct{ config Config }

func New(config Config) (*Policy, error) {
	for _, n := range []float64{config.ParkedMultiplier, config.SuspendedMultiplier, config.HeavyToolMultiplier} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1 {
			return nil, fmt.Errorf("multipliers must be finite and within [0,1]")
		}
	}
	if config.MinimumResidence < 0 || config.MinimumResidence > time.Minute || config.HeavyToolMinimumMS < 0 || config.HeavyToolMinimumMS > 600_000 {
		return nil, fmt.Errorf("policy duration limits are invalid")
	}
	return &Policy{config: config}, nil
}

// Decide scales only Acting's logical admission weight. baselineTokens is the
// upstream unweighted token accounting input. baselineWeight is the upstream
// static Acting weight; Reasoning always retains all baselineTokens at weight 1.
// A cached View is rechecked against now before its signal affects the budget.
func (p *Policy) Decide(view signals.View, now time.Time, status ProgramStatus, baselineTokens uint64, baselineWeight float64) (Decision, error) {
	if status != Reasoning && status != Acting {
		return Decision{}, fmt.Errorf("program_status must be Reasoning or Acting")
	}
	if math.IsNaN(baselineWeight) || math.IsInf(baselineWeight, 0) || baselineWeight < 0 || baselineWeight > 1 || baselineTokens > 1<<53 {
		return Decision{}, fmt.Errorf("baseline weight must be within [0,1] and tokens <= 2^53")
	}
	if view.Fresh && (view.ExpiresAt.IsZero() || !now.Before(view.ExpiresAt)) {
		view.Fresh, view.State, view.FallbackReason = false, signals.RunningFast, "ttl_expired"
	}
	d := Decision{Identity: view.Binding, ProgramStatus: status, SignalState: view.State, SignalFresh: view.Fresh, BaselineTokens: baselineTokens, BaselineWeight: baselineWeight, ActingBaselineWeight: baselineWeight, AppliedMultiplier: 1, Reason: "baseline", PhysicalKVAction: "none"}
	switch {
	case status == Reasoning:
		d.Reason = "reasoning_preserved"
		d.BaselineWeight = 1
	case !view.Fresh:
		d.Reason = view.FallbackReason
	case view.State == signals.Resuming:
		d.Reason = "resuming_baseline"
	case view.State == signals.RunningFast:
		d.Reason = "running_baseline"
	case now.Before(view.RecoveryUntil):
		d.Reason = "recovery_hold"
	case now.Sub(view.StateSince) < p.config.MinimumResidence:
		d.Reason = "state_debounce"
	case view.State == signals.Parked:
		d.AppliedMultiplier, d.Reason = p.config.ParkedMultiplier, "parked_acting"
	case view.State == signals.Suspended:
		d.AppliedMultiplier, d.Reason = p.config.SuspendedMultiplier, "suspended_acting"
	case view.State == signals.HeavyTool && view.EstimatedReturnMS >= p.config.HeavyToolMinimumMS:
		d.AppliedMultiplier, d.Reason = p.config.HeavyToolMultiplier, "heavy_tool_acting"
	}
	d.EffectiveWeight = d.BaselineWeight * d.AppliedMultiplier
	d.LogicalBudgetTokens = uint64(math.Floor(float64(baselineTokens) * d.EffectiveWeight))
	return d, nil
}
