package sim

import (
	"github.com/lilong555/agentcloud/internal/control"
	"github.com/lilong555/agentcloud/internal/policy"
	"math"
	"time"
)

// Observation contains facts disclosed at Now, excluding future durations/DAG.
type Observation struct {
	Now, WaitStarted, Earliest, Age               float64
	PhaseStarted, PhaseAge                        float64
	SessionID, EventID, Phase                     string
	InitialQueue, ResumeQueue, Slots              int
	InitialOldestMS, ResumeOldestMS               float64
	SwitchCostMS, SnapshotMS, ResumeMS, JournalMS float64
	Samples                                       []policy.SurvivalSample
}
type Action struct {
	Suspend       bool
	RevisitAt     float64
	Reason, Error string
	Episode       *control.WaitEpisode
}
type OnlinePolicy interface{ Decide(Observation) Action }

// Axes: proactive release, pressure eviction score, and FIFO admission.
type Baseline struct {
	Name     string
	TTL      float64
	Eviction string
}

func (p Baseline) Decide(o Observation) Action {
	deadline := math.Max(o.WaitStarted+p.TTL, o.Earliest)
	if p.Name == "ttl2" {
		deadline = math.Max(o.WaitStarted+2000, o.Earliest)
	}
	if p.Name == "ttl15" {
		deadline = math.Max(o.WaitStarted+15000, o.Earliest)
	}
	if p.Name == "break-even-gdsf" {
		deadline = math.Max(o.WaitStarted+math.Max(1, o.SwitchCostMS), o.Earliest)
	}
	if o.Now >= deadline {
		return Action{Suspend: true, Reason: p.Name}
	}
	return Action{RevisitAt: deadline, Reason: p.Name}
}

type ActorState struct {
	ID                                 string
	Now, Created, LastReady            float64
	Epoch                              uint64
	Safe, Waiting, Ready, ReturnDemand bool
}
type StateObserver interface{ Observe(ActorState) }

// V2ControllerPolicy invokes the complete, unmodified production decision method.
// It retains Session/Autonomous/Episode state and has no live Backend or actuator.
type V2ControllerPolicy struct {
	replay   *control.V2Replay
	sessions map[string]*control.Session
	minimum  float64
}

func NewV2(stable, minimum float64) *V2ControllerPolicy {
	return &V2ControllerPolicy{replay: control.NewV2Replay(time.Duration(stable * 1e6)), sessions: map[string]*control.Session{}, minimum: minimum}
}
func simTime(ms float64) time.Time { return time.Unix(1_000_000, int64(ms*1e6)).UTC() }
func fromSimTime(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.Sub(simTime(0))) / 1e6
}
func (p *V2ControllerPolicy) Observe(o ActorState) {
	s := p.sessions[o.ID]
	if s == nil {
		s = &control.Session{TenantID: "synthetic-profile", SessionID: o.ID, Profile: "L0", CreatedAt: simTime(o.Created), State: "RUNNING", Gate: "OPEN"}
		p.sessions[o.ID] = s
	}
	s.Epoch = o.Epoch
	s.RunningSince = simTime(o.LastReady)
	if o.Epoch > 1 {
		s.LastResumedAt = simTime(o.LastReady)
	}
	if s.Autonomous == nil {
		s.Autonomous = &control.AutonomousState{}
	}
	s.Autonomous.ReturnDemand = o.ReturnDemand
	s.ActiveTools = 0
	if !o.Safe {
		s.ActiveTools = 1
	}
	if !o.Safe || !o.Waiting {
		_, _ = p.replay.Decide(s, control.V2Observed{Now: simTime(o.Now), WorkerSlots: 1}, policy.DefaultLifecycleConfig())
	}
}
func (p *V2ControllerPolicy) Decide(o Observation) Action {
	s := p.sessions[o.SessionID]
	if s == nil {
		return Action{Error: "v2 state observation missing"}
	}
	costs := policy.LifecycleConfig{SnapshotCostMS: int64(math.Ceil(o.SnapshotMS)), ResumeCostMS: int64(math.Ceil(o.ResumeMS)), JournalCostMS: int64(math.Ceil(o.JournalMS)), MinimumResidenceMS: int64(p.minimum)}
	phase := "model"
	if o.Phase == "user_idle" {
		phase = "implicit_idle"
	}
	d, err := p.replay.Decide(s, control.V2Observed{Now: simTime(o.Now), WaitStartedAt: simTime(o.PhaseStarted), WaitPhase: phase, WaitAgeMS: int64(o.PhaseAge), Samples: o.Samples, ReadyReturns: o.ResumeQueue, WorkerSlots: int64(o.Slots)}, costs)
	if err != nil {
		return Action{Error: err.Error()}
	}
	var episode *control.WaitEpisode
	if s.Autonomous != nil && s.Autonomous.Episode != nil {
		copy := *s.Autonomous.Episode
		copy.ID = "opaque-episode"
		episode = &copy
	}
	next := 0.0
	if episode != nil {
		next = fromSimTime(episode.PlannedReleaseAt)
		if next != 0 && next <= o.Now && d.Action != "SUSPEND" {
			next = fromSimTime(episode.EarliestReleaseAt)
		}
	}
	return Action{Suspend: d.Action == "SUSPEND", RevisitAt: next, Reason: d.Reason, Episode: episode}
}
