package control

import (
	"math"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

const v2UnknownWait = 2 * time.Second
const schedulerDiagnosticLimit = 32

// WaitEpisode follows a continuous safe wait across model completion and idle.
// Its identity and unknown deadline survive controller restarts.
type WaitEpisode struct {
	ID                string    `json:"id"`
	Epoch             uint64    `json:"epoch"`
	Origin            string    `json:"origin"`
	StartedAt         time.Time `json:"started_at"`
	ObservedAt        time.Time `json:"observed_at"`
	ValidUntil        time.Time `json:"valid_until,omitempty"`
	EarliestReleaseAt time.Time `json:"earliest_release_at"`
	UnknownReleaseAt  time.Time `json:"unknown_release_at"`
	PlannedReleaseAt  time.Time `json:"planned_release_at,omitempty"`
	Selection         string    `json:"selection"`
	PredictionKnown   bool      `json:"prediction_known"`
	PredictionReason  string    `json:"prediction_reason,omitempty"`
	SwitchCostMS      int64     `json:"switch_cost_ms"`
	HoldingRate       float64   `json:"holding_rate"`
	PredictedCostMS   *float64  `json:"predicted_cost_ms,omitempty"`
	KeepCostMS        *float64  `json:"keep_cost_ms,omitempty"`
}

// SchedulerDiagnostic retains a fixed-size decision trace, including Shadow's
// computed intent separately from the action allowed through its actuator gate.
type SchedulerDiagnostic struct {
	At               time.Time `json:"at"`
	Policy           string    `json:"policy"`
	Shadow           bool      `json:"shadow"`
	EpisodeID        string    `json:"episode_id,omitempty"`
	WaitPhase        string    `json:"wait_phase,omitempty"`
	PredictionKnown  bool      `json:"prediction_known"`
	PredictionReason string    `json:"prediction_reason,omitempty"`
	ComputedAction   string    `json:"computed_action"`
	ComputedReason   string    `json:"computed_reason"`
	Action           string    `json:"action"`
	Reason           string    `json:"reason"`
	ObservationMS    float64   `json:"observation_ms"`
	DecisionMS       float64   `json:"decision_ms"`
}

func (c *Controller) schedulerPolicy() string {
	if c.cfg.SchedulerShadow {
		return "cost-aware"
	}
	return c.cfg.SchedulerPolicy
}

func (c *Controller) conditionalScheduler() bool {
	return c.schedulerPolicy() == "cost-aware" || c.schedulerPolicy() == "agentcloud-v2"
}

func (c *Controller) schedulerDecisionLocked(s *Session, input ModelBoundaryRequest, evidence *ServerEstimateEvidence, config policy.LifecycleConfig, now time.Time) (policy.LifecycleDecision, error) {
	switch c.schedulerPolicy() {
	case "cost-aware":
		return c.costDecisionLocked(s, input, evidence, config, now)
	case "agentcloud-v2":
		return c.v2DecisionLocked(s, input, evidence, config, now)
	default:
		p, err := policy.NewLifecycle(config)
		if err != nil {
			return policy.LifecycleDecision{}, err
		}
		return p.Decide(boundaryInput(s, input, p.Config(), now))
	}
}

func (c *Controller) beginWaitEpisodeLocked(s *Session, origin string, started, now time.Time) *WaitEpisode {
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	a := s.Autonomous
	if a.Episode != nil && a.Episode.Epoch == s.Epoch {
		if a.Episode.ValidUntil.IsZero() || started.Before(a.Episode.ValidUntil) {
			return a.Episode
		}
		// Expired explicit waits end the continuity proof even when no observer
		// tick happened between the expiry and the next registered wait.
		a.Episode, a.CandidateSince = nil, time.Time{}
	}
	started = laterTime(started, s.LastResumedAt)
	started = laterTime(started, a.WaitResetAt)
	// A request can overlap tool execution. Its original registration then
	// predates the safe wait; completed tool receipts supply the later boundary.
	for _, receipt := range s.Tools {
		if receipt.Epoch == s.Epoch && receipt.FinishedAt != nil {
			started = laterTime(started, *receipt.FinishedAt)
		}
	}
	a.Epoch = s.Epoch
	a.Episode = &WaitEpisode{ID: newID(), Epoch: s.Epoch, Origin: origin,
		StartedAt: started, ObservedAt: now, UnknownReleaseAt: started.Add(v2UnknownWait)}
	// Registration/idle facts begin stability and the fallback clock together.
	// Tool admission, return and epoch transitions invalidate both clocks.
	a.CandidateSince = started
	c.markSession(s)
	return a.Episode
}

func laterTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (c *Controller) v2DecisionLocked(s *Session, input ModelBoundaryRequest, evidence *ServerEstimateEvidence, config policy.LifecycleConfig, now time.Time) (policy.LifecycleDecision, error) {
	// V2 uses the continuous-wait stability interval as its cooldown. The old
	// policies retain their existing cooldown and cost-derived deadlines.
	config.CooldownMS = c.cfg.SchedulerStableFor.Milliseconds()
	p, err := policy.NewLifecycle(config)
	if err != nil {
		return policy.LifecycleDecision{}, err
	}
	initial := boundaryInput(s, input, p.Config(), now)
	d, err := p.Decide(initial)
	if err != nil {
		return d, err
	}
	if !initial.SafePoint || s.Autonomous != nil && s.Autonomous.ReturnDemand {
		resetReleasePlan(s)
		if s.Autonomous != nil {
			s.Autonomous.WaitResetAt = now
			c.markSession(s)
		}
		d.Action, d.Reason = "KEEP", "unsafe_point"
		if s.Autonomous != nil && s.Autonomous.ReturnDemand {
			d.Reason = "return_demand"
		}
		return d, nil
	}
	if evidence == nil || evidence.WaitStartedAt.IsZero() || evidence.WaitStartedAt.After(now) {
		resetReleasePlan(s)
		if s.Autonomous != nil {
			s.Autonomous.WaitResetAt = now
			c.markSession(s)
		}
		d.Action, d.Reason = "KEEP", "no_active_wait"
		return d, nil
	}
	episode := c.beginWaitEpisodeLocked(s, evidence.WaitPhase, evidence.WaitStartedAt, now)
	episode.ObservedAt = now
	if evidence.WaitPhase == "explicit_idle" {
		for _, window := range s.IdleWindows {
			if window.Epoch == s.Epoch && window.FinishedAt == nil && window.StartedAt.Equal(evidence.WaitStartedAt) {
				episode.ValidUntil = window.ExpiresAt
				break
			}
		}
	} else {
		episode.ValidUntil = time.Time{}
	}
	if s.Autonomous.CandidateSince.IsZero() {
		s.Autonomous.CandidateSince = now
	}
	episode.EarliestReleaseAt = episode.StartedAt.Add(c.cfg.SchedulerStableFor)
	episode.EarliestReleaseAt = laterTime(episode.EarliestReleaseAt, s.Autonomous.CandidateSince.Add(c.cfg.SchedulerStableFor))
	running := s.RunningSince
	if running.IsZero() {
		running = s.CreatedAt
	}
	episode.EarliestReleaseAt = laterTime(episode.EarliestReleaseAt, running.Add(time.Duration(d.EffectiveMinResidenceMS)*time.Millisecond))
	if !s.LastResumedAt.IsZero() {
		episode.EarliestReleaseAt = laterTime(episode.EarliestReleaseAt, s.LastResumedAt.Add(c.cfg.SchedulerStableFor))
	}
	episode.SwitchCostMS = max(int64(1), initial.SnapshotCostMS+initial.ResumeCostMS+initial.JournalCostMS)
	slots := int64(1)
	if capacity := evidence.WorkerCapacity; capacity != nil {
		if capacity.CapacityActors > 0 {
			slots = capacity.CapacityActors
		}
		if capacity.CapacityMemoryBytes > 0 && s.Actor.AssignedMemoryBytes > 0 {
			memorySlots := max(int64(1), capacity.CapacityMemoryBytes/s.Actor.AssignedMemoryBytes)
			if capacity.CapacityActors > 0 {
				slots = min(slots, memorySlots)
			} else {
				slots = memorySlots
			}
		}
	}
	episode.HoldingRate = 1 + min(3.0, float64(c.readyReturnCountLocked())/float64(slots))
	episode.PredictedCostMS, episode.KeepCostMS = nil, nil
	episode.PredictionKnown = evidence.WaitPrediction != nil && evidence.WaitPrediction.Known
	if evidence.WaitPrediction != nil {
		episode.PredictionReason = evidence.WaitPrediction.Reason
	}
	episode.Selection = "unknown_two_second_deadline"
	episode.PlannedReleaseAt = laterTime(episode.UnknownReleaseAt, episode.EarliestReleaseAt)
	if episode.PredictionKnown {
		c.selectV2ReleaseLocked(episode, evidence, now)
	}
	evidence.Episode = copyValue(episode)
	c.markSession(s)
	d.Action, d.Reason = "KEEP", "v2_"+episode.Selection
	if now.Before(episode.EarliestReleaseAt) {
		d.Reason = "v2_safe_wait_confirmation"
	} else if !episode.PlannedReleaseAt.IsZero() && !now.Before(episode.PlannedReleaseAt) {
		d.Action, d.Reason = "SUSPEND", "v2_"+episode.Selection
	}
	return d, nil
}

func (c *Controller) selectV2ReleaseLocked(episode *WaitEpisode, evidence *ServerEstimateEvidence, now time.Time) {
	budget := max(int64(1), int64(math.Ceil(float64(episode.SwitchCostMS)/episode.HoldingRate)))
	earliest := laterTime(now, episode.EarliestReleaseAt)
	candidates := []time.Time{earliest, laterTime(episode.UnknownReleaseAt, earliest),
		laterTime(episode.StartedAt.Add(time.Duration(budget)*time.Millisecond), earliest),
		laterTime(episode.StartedAt.Add(time.Duration(2*budget)*time.Millisecond), earliest)}
	bestCost := math.Inf(1)
	var selected time.Time
	for _, deadline := range candidates {
		remaining := max(int64(0), deadline.Sub(now).Milliseconds())
		cost, known := policy.ConditionalKeepCost(evidence.waitSamples, evidence.WaitAgeMS, remaining, budget, waitMinimumSupport)
		if known && (cost < bestCost || cost == bestCost && deadline.After(selected)) {
			bestCost, selected = cost, deadline
		}
	}
	// A complete conditional tail identifies the cost of waiting for return.
	// Requiring a known tail preserves KEEP when recovery outweighs the wait.
	horizon := max(int64(0), evidence.WaitPrediction.SupportMS-evidence.WaitAgeMS) + 1
	keepCost, known := policy.ConditionalKeepCost(evidence.waitSamples, evidence.WaitAgeMS, horizon, budget, waitMinimumSupport)
	if known {
		episode.KeepCostMS = &keepCost
		if keepCost <= bestCost {
			episode.Selection, episode.PlannedReleaseAt = "conditional_keep", time.Time{}
			episode.PredictedCostMS = &keepCost
			return
		}
	}
	if !selected.IsZero() {
		episode.Selection, episode.PlannedReleaseAt = "conditional_cost_minimum", selected
		episode.PredictedCostMS = &bestCost
	}
}

func (c *Controller) recordSchedulerDiagnosticLocked(s *Session, evidence *ServerEstimateEvidence, computedAction, computedReason, action, reason string, observationMS, decisionMS float64, now time.Time) {
	if !c.cfg.SchedulerShadow && c.schedulerPolicy() != "agentcloud-v2" {
		return
	}
	a := s.Autonomous
	row := SchedulerDiagnostic{At: now, Policy: c.schedulerPolicy(), Shadow: c.cfg.SchedulerShadow,
		ComputedAction: computedAction, ComputedReason: computedReason, Action: action, Reason: reason,
		ObservationMS: observationMS, DecisionMS: decisionMS}
	if evidence != nil {
		row.WaitPhase = evidence.WaitPhase
		if evidence.WaitPrediction != nil {
			row.PredictionKnown, row.PredictionReason = evidence.WaitPrediction.Known, evidence.WaitPrediction.Reason
		}
		if evidence.Episode != nil {
			row.EpisodeID = evidence.Episode.ID
		}
	}
	if len(a.Diagnostics) >= schedulerDiagnosticLimit {
		copy(a.Diagnostics, a.Diagnostics[len(a.Diagnostics)-schedulerDiagnosticLimit+1:])
		a.Diagnostics = a.Diagnostics[:schedulerDiagnosticLimit-1]
	}
	a.Diagnostics = append(a.Diagnostics, row)
	c.markSession(s)
}
