package control

import (
	"fmt"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

// V2Replay observes caller-owned copies and executes the full production decision
// method. It has no Backend, journal, goroutines or actuator. Episode state lives
// on Session, including across caller-simulated restart/epoch boundaries.
type V2Replay struct{ controller Controller }

type V2Observed struct {
	Now           time.Time
	WaitStartedAt time.Time
	WaitPhase     string
	WaitAgeMS     int64
	Samples       []policy.SurvivalSample
	ReadyReturns  int
	WorkerSlots   int64
	Boundary      ModelBoundaryRequest
}

func NewV2Replay(stable time.Duration) *V2Replay {
	return &V2Replay{controller: Controller{cfg: Config{SchedulerPolicy: "agentcloud-v2", SchedulerStableFor: stable, ModelConcurrency: 4}}}
}

// V2NativeObserved contains durable facts disclosed by the replay clock. The
// production estimator owns cohort selection, censoring and recent-cost P75.
type V2NativeObserved struct {
	Now          time.Time
	Sessions     []*Session
	Requests     []*ModelRequest
	Measurements []RecordedMeasurement
	Capacity     *CapacityObservation
}

// BeginWait applies the production registration boundary to a replay Session.
// Explicit idle expiry is a caller-selected registration TTL, independent of
// the eventual idle duration.
func (r *V2Replay) BeginWait(s *Session, origin string, started, expires time.Time) {
	if boundarySafe(s) {
		episode := r.controller.beginWaitEpisodeLocked(s, origin, started, started)
		episode.ValidUntil = expires
	}
}

func (r *V2Replay) EndIdle(s *Session, now time.Time) {
	resetReleasePlan(s)
	if s.Autonomous != nil {
		s.Autonomous.WaitResetAt = now
	}
}

func (r *V2Replay) DecideNative(s *Session, o V2NativeObserved, costs policy.LifecycleConfig) (policy.LifecycleDecision, *ServerEstimateEvidence, error) {
	p, err := policy.NewLifecycle(costs)
	if err != nil {
		return policy.LifecycleDecision{}, nil, err
	}
	costs = p.Config()
	c := &r.controller
	c.state = State{Sessions: map[string]*Session{}, Requests: map[string]*ModelRequest{}}
	for _, peer := range o.Sessions {
		if !peer.CreatedAt.After(o.Now) {
			c.state.Sessions[key(peer.TenantID, peer.SessionID)] = peer
		}
	}
	if c.state.Sessions[key(s.TenantID, s.SessionID)] != s {
		return policy.LifecycleDecision{}, nil, fmt.Errorf("native replay subject missing from observed sessions")
	}
	for _, request := range o.Requests {
		if request.CreatedAt.After(o.Now) {
			continue
		}
		observed := *request
		if observed.UpdatedAt.After(o.Now) {
			observed.Status, observed.UpdatedAt = "DISPATCHING", o.Now
			observed.Result, observed.FirstToken, observed.FirstTokenAt = nil, "", nil
		}
		c.state.Requests[key(observed.TenantID, observed.ID)] = &observed
	}
	for _, item := range o.Measurements {
		if !item.At.After(o.Now) {
			c.state.Observations = append(c.state.Observations, item)
		}
	}
	input, evidence := c.serverEstimateLocked(s, ModelBoundaryRequest{ExpectedEpoch: s.Epoch}, nil, o.Capacity, costs, o.Now)
	d, err := c.v2DecisionLocked(s, input, evidence, costs, o.Now)
	return d, evidence, err
}

func (r *V2Replay) Decide(s *Session, o V2Observed, costs policy.LifecycleConfig) (policy.LifecycleDecision, error) {
	c := &r.controller
	c.state.Sessions = map[string]*Session{"observed": s}
	// These peers represent already observed ready-return demands, preserving
	// readyReturnCountLocked's production semantics without a synthetic slot gate.
	for i := 0; i < o.ReadyReturns; i++ {
		c.state.Sessions[string(rune(i))+"/return"] = &Session{State: "SUSPENDED", Autonomous: &AutonomousState{ReturnDemand: true}}
	}
	prediction := policy.ConditionalRemaining(o.Samples, o.WaitAgeMS, .75, waitMinimumSupport)
	evidence := &ServerEstimateEvidence{WaitPhase: o.WaitPhase, WaitStartedAt: o.WaitStartedAt,
		WaitAgeMS: o.WaitAgeMS, waitSamples: append([]policy.SurvivalSample(nil), o.Samples...), WaitPrediction: &prediction,
		WorkerCapacity: &CapacityObservation{CapacityActors: o.WorkerSlots}}
	return c.v2DecisionLocked(s, o.Boundary, evidence, costs, o.Now)
}
