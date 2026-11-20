package control

import (
	"math"
	"sort"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

const waitHistoryLimit = 64
const waitMinimumSupport = 4

type startedWait struct {
	id      string
	started time.Time
	sample  policy.SurvivalSample
}

// waitCohort retains the most recently started waits. In-flight observations
// occupy the same bounded cohort as completed observations.
type waitCohort struct{ rows []startedWait }

func (h *waitCohort) add(id string, started, until, now time.Time, complete bool) {
	if started.IsZero() || started.After(now) || until.Before(started) {
		return
	}
	if until.After(now) {
		until, complete = now, false
	}
	row := startedWait{id, started, policy.SurvivalSample{DurationMS: until.Sub(started).Milliseconds(), Completed: complete}}
	for i, prior := range h.rows {
		if prior.id == id {
			if prior.started.After(started) {
				return
			}
			copy(h.rows[i:], h.rows[i+1:])
			h.rows = h.rows[:len(h.rows)-1]
			break
		}
	}
	index := sort.Search(len(h.rows), func(i int) bool {
		return h.rows[i].started.Before(started) || h.rows[i].started.Equal(started) && h.rows[i].id > id
	})
	if index >= waitHistoryLimit {
		return
	}
	if len(h.rows) < waitHistoryLimit {
		h.rows = append(h.rows, startedWait{})
	}
	copy(h.rows[index+1:], h.rows[index:len(h.rows)-1])
	h.rows[index] = row
}

func (h *waitCohort) samples() []policy.SurvivalSample {
	result := make([]policy.SurvivalSample, len(h.rows))
	for i, row := range h.rows {
		result[i] = row.sample
	}
	return result
}

func earlierRequest(a, b *ModelRequest) bool {
	return b == nil || a.CreatedAt.Before(b.CreatedAt) || a.CreatedAt.Equal(b.CreatedAt) && a.ID < b.ID
}

func (c *Controller) sameWaitingProfile(s *Session, tenant, session string) bool {
	peer := c.state.Sessions[key(tenant, session)]
	return tenant == s.TenantID && peer != nil && peer.Profile == s.Profile
}

// waitHistoryLocked uses only facts visible at now. A cancelled or unknown model
// outcome contributes exposure through its last observation, never a completion.
func (c *Controller) waitHistoryLocked(s *Session, now time.Time) (string, time.Time, []policy.SurvivalSample) {
	var current *ModelRequest
	for _, request := range c.state.Requests {
		if request.TenantID == s.TenantID && request.SessionID == s.SessionID &&
			(request.Status == "PENDING" || request.Status == "DISPATCHING") && earlierRequest(request, current) {
			current = request
		}
	}
	history := waitCohort{rows: make([]startedWait, 0, waitHistoryLimit)}
	if current != nil {
		for _, request := range c.state.Requests {
			if request == current || !c.sameWaitingProfile(s, request.TenantID, request.SessionID) {
				continue
			}
			until, complete := now, false
			switch request.Status {
			case "COMPLETED":
				until, complete = request.UpdatedAt, true
			case "OUTCOME_UNKNOWN":
				until = request.UpdatedAt
			case "PENDING", "DISPATCHING":
			default:
				continue
			}
			history.add(key(request.TenantID, request.ID), request.CreatedAt, until, now, complete)
		}
		return "model", current.CreatedAt, history.samples()
	}

	started := s.IdleSince
	var active *IdleWindow
	for _, window := range s.IdleWindows {
		if window.FinishedAt == nil && window.Epoch == s.Epoch && now.Before(window.ExpiresAt) &&
			(active == nil || window.StartedAt.Before(active.StartedAt)) {
			active = window
		}
	}
	if active == nil {
		// The Actor can be computing between HTTP tools. Only registered model
		// waits and explicit idle windows identify its safe waiting phase.
		if s.Mode == "in-actor" {
			return "in_actor_execution", time.Time{}, nil
		}
		for _, window := range s.IdleWindows {
			if window.FinishedAt == nil && window.StartedAt.Equal(started) && !now.Before(window.ExpiresAt) {
				return "expired_idle", time.Time{}, nil
			}
		}
	}
	if active != nil {
		started = active.StartedAt
		for _, peer := range c.state.Sessions {
			if !c.sameWaitingProfile(s, peer.TenantID, peer.SessionID) {
				continue
			}
			for _, window := range peer.IdleWindows {
				if window == active {
					continue
				}
				until, complete := now, false
				if window.FinishedAt != nil {
					until, complete = *window.FinishedAt, true
				}
				if until.After(window.ExpiresAt) {
					until, complete = window.ExpiresAt, false
				}
				history.add(key(peer.SessionID, window.OperationID), window.StartedAt, until, now, complete)
			}
		}
		return "explicit_idle", started, history.samples()
	}

	// Implicit idle periods have durable terminal observations and live session
	// start times. Explicit windows are kept in their own prediction cohort.
	for _, item := range c.state.Observations {
		if item.Metric != "idle_until_tool_ms" || item.At.After(now) ||
			math.IsNaN(item.Value) || math.IsInf(item.Value, 0) || item.Value < 0 || item.Value > 86_400_000 ||
			!c.sameWaitingProfile(s, item.TenantID, item.SessionID) {
			continue
		}
		begin := item.At.Add(-time.Duration(item.Value * float64(time.Millisecond)))
		history.add(key(item.SessionID, item.OperationID), begin, item.At, now, true)
	}
	for _, peer := range c.state.Sessions {
		if peer == s || peer.IdleSince.IsZero() || peer.ActiveTools != 0 ||
			!c.sameWaitingProfile(s, peer.TenantID, peer.SessionID) {
			continue
		}
		explicit := false
		for _, window := range peer.IdleWindows {
			if window.FinishedAt == nil && now.Before(window.ExpiresAt) {
				explicit = true
				break
			}
		}
		if !explicit {
			history.add(key(peer.SessionID, "active"), peer.IdleSince, now, now, false)
		}
	}
	return "implicit_idle", started, history.samples()
}

func (c *Controller) conditionalEstimateLocked(s *Session, input ModelBoundaryRequest, evidence *ServerEstimateEvidence, now time.Time) ModelBoundaryRequest {
	phase, started, samples := c.waitHistoryLocked(s, now)
	age := max(int64(0), now.Sub(started).Milliseconds())
	if started.IsZero() || started.After(now) {
		age = 0
	}
	prediction := policy.ConditionalRemaining(samples, age, .75, waitMinimumSupport)
	evidence.WaitPhase, evidence.WaitStartedAt, evidence.WaitAgeMS = phase, started, age
	evidence.WaitPrediction, evidence.waitSamples = &prediction, samples
	evidence.Sources = append(evidence.Sources, "bounded_started_cohort_conditional_survival")
	input.ExpectedWaitMS = 0
	if prediction.Known {
		input.ExpectedWaitMS = min(int64(86_400_000), prediction.RemainingMS)
	}
	return input
}
