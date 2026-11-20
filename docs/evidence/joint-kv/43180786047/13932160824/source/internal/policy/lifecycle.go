package policy

import (
	"fmt"
	"math"
)

// LifecycleConfig contains millisecond estimates and conservative switching limits.
type LifecycleConfig struct {
	SnapshotCostMS     int64 `json:"snapshot_cost_ms"`
	ResumeCostMS       int64 `json:"resume_cost_ms"`
	JournalCostMS      int64 `json:"journal_cost_ms"`
	MarginMS           int64 `json:"margin_ms"`
	MinimumResidenceMS int64 `json:"min_residence_ms"`
	CooldownMS         int64 `json:"cooldown_ms"`
}

func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{SnapshotCostMS: 500, ResumeCostMS: 500, JournalCostMS: 50, MarginMS: 500, MinimumResidenceMS: 1000, CooldownMS: 5000}
}

// NewLifecycle applies defaults to an entirely empty configuration.
func NewLifecycle(config LifecycleConfig) (*LifecyclePolicy, error) {
	if config == (LifecycleConfig{}) {
		config = DefaultLifecycleConfig()
	}
	for _, value := range []int64{config.SnapshotCostMS, config.ResumeCostMS, config.JournalCostMS, config.MarginMS, config.MinimumResidenceMS, config.CooldownMS} {
		if value < 0 || value > 600_000 {
			return nil, fmt.Errorf("lifecycle duration estimates must be within 0..600000 ms")
		}
	}
	return &LifecyclePolicy{config: config}, nil
}

type LifecyclePolicy struct{ config LifecycleConfig }

func (p *LifecyclePolicy) Config() LifecycleConfig { return p.config }

type LifecycleInput struct {
	ExpectedWaitMS     int64   `json:"expected_wait_ms"`
	SnapshotCostMS     int64   `json:"snapshot_cost_ms"`
	ResumeCostMS       int64   `json:"resume_cost_ms"`
	JournalCostMS      int64   `json:"journal_cost_ms"`
	MinimumResidenceMS int64   `json:"min_residence_ms"`
	WorkerPressure     float64 `json:"worker_pressure"`
	ResidenceMS        int64   `json:"residence_ms"`
	SinceResumeMS      *int64  `json:"since_resume_ms,omitempty"`
	SafePoint          bool    `json:"safe_point"`
}

type LifecycleDecision struct {
	Action                  string         `json:"action"`
	Reason                  string         `json:"reason"`
	Estimates               LifecycleInput `json:"estimates"`
	BreakEvenMS             int64          `json:"break_even_ms"`
	ThresholdMS             int64          `json:"threshold_ms"`
	EffectiveMarginMS       int64          `json:"effective_margin_ms"`
	EffectiveMinResidenceMS int64          `json:"effective_min_residence_ms"`
	EstimatedReleasedMS     int64          `json:"estimated_released_ms"`
	CooldownMS              int64          `json:"cooldown_ms"`
}

// Decide uses a serial cost heuristic. All wait and cost inputs remain estimates;
// the actuator records its observed completion duration separately.
func (p *LifecyclePolicy) Decide(input LifecycleInput) (LifecycleDecision, error) {
	if input.ExpectedWaitMS < 0 || input.ExpectedWaitMS > 86_400_000 {
		return LifecycleDecision{}, fmt.Errorf("expected_wait_ms must be within 0..86400000")
	}
	for _, value := range []int64{input.SnapshotCostMS, input.ResumeCostMS, input.JournalCostMS, input.MinimumResidenceMS} {
		if value < 0 || value > 600_000 {
			return LifecycleDecision{}, fmt.Errorf("cost and residence estimates must be within 0..600000 ms")
		}
	}
	if math.IsNaN(input.WorkerPressure) || math.IsInf(input.WorkerPressure, 0) || input.WorkerPressure < 0 || input.WorkerPressure > 1 {
		return LifecycleDecision{}, fmt.Errorf("worker_pressure must be finite and within [0,1]")
	}
	if input.ResidenceMS < 0 || input.SinceResumeMS != nil && *input.SinceResumeMS < 0 {
		return LifecycleDecision{}, fmt.Errorf("observed residence durations must be nonnegative")
	}
	pressureFactor := 1 - 0.75*input.WorkerPressure
	margin := int64(math.Ceil(float64(p.config.MarginMS) * pressureFactor))
	residence := int64(math.Ceil(float64(input.MinimumResidenceMS) * pressureFactor))
	cost := input.SnapshotCostMS + input.ResumeCostMS + input.JournalCostMS
	d := LifecycleDecision{Action: "KEEP", Estimates: input, BreakEvenMS: cost, ThresholdMS: cost + margin, EffectiveMarginMS: margin, EffectiveMinResidenceMS: residence, EstimatedReleasedMS: input.ExpectedWaitMS - cost, CooldownMS: p.config.CooldownMS}
	switch {
	case !input.SafePoint:
		d.Reason = "unsafe_point"
	case input.SinceResumeMS != nil && *input.SinceResumeMS < p.config.CooldownMS:
		d.Reason = "resume_cooldown"
	case input.ResidenceMS < residence:
		d.Reason = "minimum_residence"
	case input.ExpectedWaitMS == 0:
		d.Reason = "no_expected_wait"
	case input.ExpectedWaitMS <= d.ThresholdMS:
		d.Reason = "below_cost_threshold"
	default:
		d.Action, d.Reason = "SUSPEND", "estimated_wait_exceeds_cost"
	}
	return d, nil
}
