package sim

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lilong555/agentcloud/internal/control"
	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/policy"
)

// Observation contains facts disclosed at Now, excluding future durations/DAG.
type Observation struct {
	Now, WaitStarted, Earliest, Age               float64
	PhaseStarted, PhaseAge                        float64
	SessionID, EventID, Phase                     string
	InitialQueue, ResumeQueue, Slots              int
	Occupied                                      int
	InitialOldestMS, ResumeOldestMS               float64
	SwitchCostMS, SnapshotMS, ResumeMS, JournalMS float64
	Samples                                       []policy.SurvivalSample
}
type Action struct {
	Suspend       bool
	RevisitAt     float64
	Reason, Error string
	Episode       *control.WaitEpisode
	Evidence      *control.ServerEstimateEvidence
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
	ID, Tenant, Profile                string
	Now, Created, LastReady            float64
	Epoch                              uint64
	Safe, Waiting, Ready, ReturnDemand bool
	Resident, Checkpointing, Finished  bool
}
type StateObserver interface{ Observe(ActorState) }

type EventObservation struct {
	Actor       ActorState
	ID, Phase   string
	Started, At float64
	Completed   bool
}
type EventObserver interface{ ObserveEvent(EventObservation) }
type MeasurementObserver interface {
	ObserveMeasurement(ActorState, string, float64)
}

// V2ControllerPolicy invokes the complete, unmodified production decision method.
// It retains Session/Autonomous/Episode state and has no live Backend or actuator.
type V2ControllerPolicy struct {
	replay       *control.V2Replay
	sessions     map[string]*control.Session
	minimum      float64
	cfg          Config
	requests     map[string]*control.ModelRequest
	measurements []control.RecordedMeasurement
	episodeIDs   map[string]string
}

func NewV2(stable, minimum float64) *V2ControllerPolicy {
	return &V2ControllerPolicy{replay: control.NewV2Replay(time.Duration(stable * 1e6)), sessions: map[string]*control.Session{}, requests: map[string]*control.ModelRequest{}, episodeIDs: map[string]string{}, minimum: minimum, cfg: Config{HistoryMode: "synthetic_shared", IdleMode: "implicit"}}
}

func (p *V2ControllerPolicy) configure(cfg Config, history []WaitSample) {
	p.cfg, p.minimum = cfg, cfg.MinimumResidenceMS
	p.replay = control.NewV2Replay(time.Duration(cfg.StableMS * 1e6))
	if cfg.ObservationMode != "native" || !cfg.TrainingHistory {
		return
	}
	for _, row := range history {
		state := ActorState{ID: row.ID, Tenant: row.Tenant, Profile: row.Profile, Created: row.Started, Epoch: 1, Safe: true}
		s := p.session(state)
		if row.Phase == "model_wait" {
			status := "OUTCOME_UNKNOWN"
			if row.Completed {
				status = "COMPLETED"
			}
			p.requests[row.ID] = &control.ModelRequest{ID: row.ID, TenantID: s.TenantID, SessionID: s.SessionID, CreatedAt: simTime(row.Started), UpdatedAt: simTime(row.Started + row.Duration), Status: status}
		} else if row.Phase == "user_idle" {
			finished := simTime(row.Started + row.Duration)
			s.IdleWindows[row.ID] = &control.IdleWindow{OperationID: row.ID, Epoch: 1, StartedAt: simTime(row.Started), ExpiresAt: simTime(row.Started + cfg.IdleTTLMS), FinishedAt: &finished}
		}
	}
}
func simTime(ms float64) time.Time { return time.Unix(1_000_000, int64(ms*1e6)).UTC() }
func fromSimTime(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.Sub(simTime(0))) / 1e6
}
func (p *V2ControllerPolicy) session(o ActorState) *control.Session {
	s := p.sessions[o.ID]
	if s == nil {
		tenant, actorProfile := o.Tenant, profile(o.Profile)
		if p.cfg.HistoryMode == "synthetic_shared" {
			tenant, actorProfile = "synthetic-profile", "L0"
		}
		s = &control.Session{TenantID: tenant, SessionID: o.ID, Profile: actorProfile, Mode: "external", CreatedAt: simTime(o.Created), State: "RUNNING", Gate: "OPEN", IdleWindows: map[string]*control.IdleWindow{}}
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
	if p.cfg.ObservationMode == "native" {
		s.Actor.AssignedMemoryBytes = p.cfg.ReservationBytes
		s.State, s.Gate = "RUNNING", "OPEN"
		if !o.Resident {
			s.State, s.Gate = "SUSPENDED", "CLOSED"
		}
		if o.Resident && !o.Ready && !o.Checkpointing {
			s.State, s.Gate = "RESUMING", "CLOSED"
		}
		if o.Checkpointing {
			s.Gate = "CLOSED"
		}
		if o.Finished {
			s.State, s.Gate = "FINISHED", "CLOSED"
		}
	}
	return s
}
func (p *V2ControllerPolicy) Observe(o ActorState) {
	s := p.session(o)
	if !o.Safe || !o.Waiting {
		_, _ = p.replay.Decide(s, control.V2Observed{Now: simTime(o.Now), WorkerSlots: 1}, policy.DefaultLifecycleConfig())
	}
}

func (p *V2ControllerPolicy) ObserveEvent(o EventObservation) {
	if p.cfg.ObservationMode != "native" {
		return
	}
	s := p.session(o.Actor)
	id := o.Actor.ID + "/" + o.ID
	now := simTime(o.At)
	switch o.Phase {
	case "model_wait":
		if !o.Completed {
			p.requests[id] = &control.ModelRequest{ID: id, TenantID: s.TenantID, SessionID: s.SessionID, CreatedAt: now, UpdatedAt: now, Status: "DISPATCHING"}
			s.IdleSince = time.Time{}
			if s.Autonomous.Episode != nil && !strings.HasPrefix(s.Autonomous.Episode.Origin, "model") {
				p.replay.EndIdle(s, now)
			}
			p.replay.BeginWait(s, "model:"+id, now, time.Time{})
		} else {
			r := p.requests[id]
			r.Status, r.UpdatedAt = "COMPLETED", now
			s.IdleSince = now
		}
	case "user_idle":
		if !o.Completed {
			until := simTime(o.At + p.cfg.IdleTTLMS)
			s.IdleWindows[id] = &control.IdleWindow{OperationID: id, Epoch: s.Epoch, StartedAt: now, ExpiresAt: until}
			s.IdleSince = now
			p.replay.BeginWait(s, "explicit_idle:"+id, now, until)
		} else {
			s.IdleWindows[id].FinishedAt = &now
			s.IdleSince = time.Time{}
			p.replay.EndIdle(s, now)
		}
	case "active":
		s.IdleSince = time.Time{}
		p.replay.EndIdle(s, now)
	}
}

func (p *V2ControllerPolicy) ObserveMeasurement(o ActorState, metric string, value float64) {
	if p.cfg.ObservationMode != "native" {
		return
	}
	s := p.sessions[o.ID]
	if s == nil {
		s = p.session(o)
	}
	p.measurements = append(p.measurements, control.RecordedMeasurement{TenantID: s.TenantID, SessionID: s.SessionID, Measurement: observability.Measurement{At: simTime(o.Now), Metric: metric, Value: value, Source: "simulated_completed_service"}})
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
		if p.cfg.IdleMode == "explicit" {
			phase = "explicit_idle"
			if _, exists := s.IdleWindows[o.EventID]; !exists && p.cfg.ObservationMode != "native" {
				s.IdleWindows[o.EventID] = &control.IdleWindow{OperationID: o.EventID, Epoch: s.Epoch, StartedAt: simTime(o.PhaseStarted), ExpiresAt: simTime(o.PhaseStarted + p.cfg.IdleTTLMS)}
			}
		}
	}
	var d policy.LifecycleDecision
	var evidence *control.ServerEstimateEvidence
	var err error
	if p.cfg.ObservationMode == "native" {
		costs = policy.DefaultLifecycleConfig()
		costs.MinimumResidenceMS = int64(p.minimum)
		observed := control.V2NativeObserved{Now: simTime(o.Now), Measurements: p.measurements,
			Capacity: &control.CapacityObservation{CapacityActors: int64(o.Slots), AllocatedActors: int64(o.Occupied), Source: "simulated_slot_capacity", ObservedAt: simTime(o.Now)}}
		for _, peer := range p.sessions {
			observed.Sessions = append(observed.Sessions, peer)
		}
		for _, request := range p.requests {
			observed.Requests = append(observed.Requests, request)
		}
		d, evidence, err = p.replay.DecideNative(s, observed, costs)
	} else {
		started := simTime(o.PhaseStarted)
		if phase == "explicit_idle" && o.PhaseAge >= p.cfg.IdleTTLMS {
			started, phase = time.Time{}, "expired_idle"
		}
		d, err = p.replay.Decide(s, control.V2Observed{Now: simTime(o.Now), WaitStartedAt: started, WaitPhase: phase, WaitAgeMS: int64(o.PhaseAge), Samples: o.Samples, ReadyReturns: o.ResumeQueue, WorkerSlots: int64(o.Slots)}, costs)
	}
	if err != nil {
		return Action{Error: err.Error()}
	}
	var episode *control.WaitEpisode
	if s.Autonomous != nil && s.Autonomous.Episode != nil {
		copy := *s.Autonomous.Episode
		copy.ID = p.episodeID(copy.ID)
		episode = &copy
	}
	if evidence != nil && evidence.Episode != nil {
		copy := *evidence.Episode
		copy.ID = p.episodeID(copy.ID)
		evidence.Episode = &copy
	}
	next := 0.0
	if episode != nil {
		next = fromSimTime(episode.PlannedReleaseAt)
		if next != 0 && next <= o.Now && d.Action != "SUSPEND" {
			next = fromSimTime(episode.EarliestReleaseAt)
		}
	}
	return Action{Suspend: d.Action == "SUSPEND", RevisitAt: next, Reason: d.Reason, Episode: episode, Evidence: evidence}
}

func (p *V2ControllerPolicy) episodeID(id string) string {
	if p.cfg.ObservationMode != "native" {
		return "opaque-episode"
	}
	if value := p.episodeIDs[id]; value != "" {
		return value
	}
	value := fmt.Sprintf("episode-%d", len(p.episodeIDs)+1)
	p.episodeIDs[id] = value
	return value
}
