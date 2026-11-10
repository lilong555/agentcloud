package control

import (
	"math"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

// ReleasePlan fixes a resource budget for one uninterrupted safe wait. New
// observations may advance PlannedReleaseAt; LatestReleaseAt never moves later.
// Time costs are a measured heuristic, and physical resource/SLO gains are
// evaluated independently.
type ReleasePlan struct {
	Epoch             uint64    `json:"epoch"`
	StartedAt         time.Time `json:"started_at"`
	ObservedAt        time.Time `json:"observed_at"`
	EarliestReleaseAt time.Time `json:"earliest_release_at"`
	PlannedReleaseAt  time.Time `json:"planned_release_at"`
	LatestReleaseAt   time.Time `json:"latest_release_at"`
	SwitchCostMS      int64     `json:"switch_cost_ms"`
	BreakEvenWaitMS   int64     `json:"break_even_wait_ms"`
	HoldingRate       float64   `json:"holding_rate"`
	ReadyReturns      int       `json:"ready_returns"`
	PredictionPhase   string    `json:"prediction_phase"`
	Selection         string    `json:"selection"`
	PredictedCostMS   *float64  `json:"predicted_cost_ms,omitempty"`
}

func resetReleasePlan(s *Session) {
	if s.Autonomous != nil {
		s.Autonomous.ReleasePlan = nil
		s.Autonomous.CandidateSince = time.Time{}
	}
}

func (c *Controller) readyReturnCountLocked() int {
	count := 0
	for _, s := range c.state.Sessions {
		if (s.State == "SUSPENDED" || s.State == "CHECKPOINTED") && s.Autonomous != nil && s.Autonomous.ReturnDemand {
			count++
		}
	}
	return count
}

func (c *Controller) costDecisionLocked(s *Session, input ModelBoundaryRequest, evidence *ServerEstimateEvidence, config policy.LifecycleConfig, now time.Time) (policy.LifecycleDecision, error) {
	p, err := policy.NewLifecycle(config)
	if err != nil {
		return policy.LifecycleDecision{}, err
	}
	initial := boundaryInput(s, input, config, now)
	decision, err := p.Decide(initial)
	if err != nil {
		return decision, err
	}
	if evidence == nil || evidence.WaitStartedAt.IsZero() || evidence.WaitStartedAt.After(now) {
		decision.Action, decision.Reason = "KEEP", "no_active_wait"
		return decision, nil
	}
	if !initial.SafePoint || s.Autonomous != nil && s.Autonomous.ReturnDemand {
		decision.Action, decision.Reason = "KEEP", "unsafe_point"
		if s.Autonomous != nil && s.Autonomous.ReturnDemand {
			decision.Reason = "return_demand"
		}
		return decision, nil
	}
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	plan := s.Autonomous.ReleasePlan
	if plan == nil || plan.Epoch != s.Epoch {
		started := evidence.WaitStartedAt
		if s.LastResumedAt.After(started) {
			started = s.LastResumedAt
		}
		ready := c.readyReturnCountLocked()
		slots := 1
		if evidence.WorkerCapacity != nil && evidence.WorkerCapacity.CapacityActors > 0 {
			slots = int(evidence.WorkerCapacity.CapacityActors)
		}
		rate := 1 + min(3.0, float64(ready)/float64(slots))
		switchCost := max(int64(1), initial.SnapshotCostMS+initial.ResumeCostMS+initial.JournalCostMS)
		budget := max(int64(1), int64(math.Ceil(float64(switchCost)/rate)))
		plan = &ReleasePlan{Epoch: s.Epoch, StartedAt: started, ObservedAt: now,
			SwitchCostMS: switchCost, BreakEvenWaitMS: budget, HoldingRate: rate, ReadyReturns: ready,
			EarliestReleaseAt: started.Add(time.Duration(max(int64(1), (budget+1)/2)) * time.Millisecond),
			PlannedReleaseAt:  started.Add(time.Duration(budget) * time.Millisecond),
			LatestReleaseAt:   started.Add(time.Duration(2*budget) * time.Millisecond),
			Selection:         "observed_break_even_fallback"}
		c.selectReleaseDeadline(plan, evidence, now, true)
		s.Autonomous.ReleasePlan = plan
		c.markSession(s)
	} else {
		before := plan.PlannedReleaseAt
		c.selectReleaseDeadline(plan, evidence, now, false)
		if !plan.PlannedReleaseAt.Equal(before) {
			c.markSession(s)
		}
	}
	evidence.ReleasePlan = copyValue(plan)
	// The episode budget supplies the churn bound. Its earliest release replaces
	// a workload-independent five-second cooldown in this opt-in policy.
	config.CooldownMS = min(config.CooldownMS, max(int64(1), (plan.BreakEvenWaitMS+1)/2))
	p, _ = policy.NewLifecycle(config)
	decision, err = p.Decide(boundaryInput(s, input, config, now))
	if err != nil {
		return decision, err
	}
	switch decision.Reason {
	case "unsafe_point", "minimum_residence", "resume_cooldown":
		return decision, nil
	}
	decision.Action, decision.Reason = "KEEP", "observed_cost_wait"
	if !now.Before(plan.PlannedReleaseAt) {
		decision.Action, decision.Reason = "SUSPEND", "observed_cost_deadline"
	}
	return decision, nil
}

func (c *Controller) selectReleaseDeadline(plan *ReleasePlan, evidence *ServerEstimateEvidence, now time.Time, initial bool) {
	if evidence.WaitPrediction == nil || !evidence.WaitPrediction.Known {
		return
	}
	candidates := []time.Time{
		plan.EarliestReleaseAt,
		plan.StartedAt.Add(time.Duration(plan.BreakEvenWaitMS) * time.Millisecond),
		plan.LatestReleaseAt,
	}
	bestCost := math.Inf(1)
	var selected time.Time
	for _, deadline := range candidates {
		remaining := max(int64(0), deadline.Sub(now).Milliseconds())
		cost, known := policy.ConditionalKeepCost(evidence.waitSamples, evidence.WaitAgeMS, remaining, plan.BreakEvenWaitMS, waitMinimumSupport)
		if known && (cost < bestCost || cost == bestCost && deadline.After(selected)) {
			bestCost, selected = cost, deadline
		}
	}
	if selected.IsZero() || !initial && !selected.Before(plan.PlannedReleaseAt) {
		return
	}
	plan.PlannedReleaseAt, plan.PredictionPhase = selected, evidence.WaitPhase
	plan.Selection, plan.PredictedCostMS = "conditional_cost_minimum", &bestCost
}

// schedulingSnapshot copies identity and bounded observer inputs without
// serializing accumulated tool results, model decisions or lifecycle receipts.
func schedulingSnapshot(s *Session) *Session {
	snapshot := *s
	snapshot.Tools, snapshot.Lifecycle, snapshot.Scheduling = nil, nil, nil
	snapshot.IdleWindows, snapshot.ReturnDemands = nil, nil
	if s.Autonomous != nil {
		snapshot.Autonomous = &AutonomousState{Epoch: s.Autonomous.Epoch, LastEvaluatedAt: s.Autonomous.LastEvaluatedAt,
			ReturnDemand: s.Autonomous.ReturnDemand, ReturnRequestedAt: s.Autonomous.ReturnRequestedAt}
	}
	snapshot.Manifest = copyValue(s.Manifest)
	snapshot.DynamoFinalizedSnapshot = copyValue(s.DynamoFinalizedSnapshot)
	return &snapshot
}
