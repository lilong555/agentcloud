package control

import (
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
	return &V2Replay{controller: Controller{cfg: Config{SchedulerPolicy: "agentcloud-v2", SchedulerStableFor: stable}}}
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
