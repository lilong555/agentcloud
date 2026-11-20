package control

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/policy"
)

func TestV2ReplayDifferentialStatefulController(t *testing.T) {
	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	for _, known := range []bool{false, true} {
		c, s, cfg := costFixture(start)
		c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
		r := NewV2Replay(500 * time.Millisecond)
		simulated := copyValue(s)
		history := []policy.SurvivalSample{}
		if known {
			for i := 0; i < 8; i++ {
				history = append(history, policy.SurvivalSample{DurationMS: 10000, Completed: true})
			}
		}
		previousID, previousSimID := "", ""
		for i, ms := range []int64{0, 499, 500, 1999, 2000, 2100, 2200, 2500, 2600, 3000, 3500, 5000} {
			now := start.Add(time.Duration(ms) * time.Millisecond)
			phase := "model"
			if i >= 2 {
				phase = "implicit_idle"
			}
			// Exercise unsafe tool reset, return demand, epoch change and restart.
			active := 0
			if i == 5 {
				active = 1
			}
			s.ActiveTools, simulated.ActiveTools = active, active
			if i == 7 {
				s.Epoch++
				simulated.Epoch++
				s.LastResumedAt = now
				simulated.LastResumedAt = now
			}
			if i == 8 {
				s.Autonomous.ReturnDemand = true
				simulated.Autonomous.ReturnDemand = true
			}
			if i == 9 {
				s.Autonomous.ReturnDemand = false
				simulated.Autonomous.ReturnDemand = false
			}
			if i == 10 {
				data, _ := json.Marshal(simulated)
				simulated = &Session{}
				if err := json.Unmarshal(data, simulated); err != nil {
					t.Fatal(err)
				}
				r = NewV2Replay(500 * time.Millisecond)
			}
			o := V2Observed{Now: now, WaitStartedAt: start, WaitPhase: phase, WaitAgeMS: ms, Samples: history, WorkerSlots: 4}
			prediction := policy.ConditionalRemaining(history, ms, .75, 4)
			e := &ServerEstimateEvidence{WaitPhase: phase, WaitStartedAt: start, WaitAgeMS: ms, waitSamples: history, WaitPrediction: &prediction, WorkerCapacity: &CapacityObservation{CapacityActors: 4}}
			a, ea := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now)
			b, eb := r.Decide(simulated, o, cfg)
			if ea != nil || eb != nil || !reflect.DeepEqual(a, b) {
				t.Fatalf("known=%v step=%d decisions differ: %+v %+v errors %v %v", known, i, a, b, ea, eb)
			}
			x, y := copyValue(s.Autonomous), copyValue(simulated.Autonomous)
			if (x.Episode == nil) != (y.Episode == nil) {
				t.Fatal("episode presence differs")
			}
			if x.Episode != nil {
				if (x.Episode.ID == previousID) != (y.Episode.ID == previousSimID) {
					t.Fatal("episode continuity differs")
				}
				previousID, previousSimID = x.Episode.ID, y.Episode.ID
				x.Episode.ID, y.Episode.ID = "episode-id", "episode-id"
			}
			if !reflect.DeepEqual(x, y) {
				t.Fatalf("known=%v step=%d episode/deadline differs:\n%+v\n%+v", known, i, x, y)
			}
		}
	}
}

func TestV2NativeReplayMatchesProductionCohortsCostsAndIdle(t *testing.T) {
	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	cfg := policy.DefaultLifecycleConfig()
	// Native's gateway limit differs from replay's default. V2's conditional
	// estimate replaces the legacy gateway-ahead estimate for pending requests.
	c := &Controller{cfg: Config{SchedulerPolicy: "agentcloud-v2", SchedulerStableFor: 500 * time.Millisecond, ModelConcurrency: 64}, state: State{Sessions: map[string]*Session{}, Requests: map[string]*ModelRequest{}}}
	s := &Session{TenantID: "tenant-a", SessionID: "subject", Profile: "L0", Mode: "external", State: "RUNNING", Gate: "OPEN", Epoch: 1, CreatedAt: start, RunningSince: start, Autonomous: &AutonomousState{}, IdleWindows: map[string]*IdleWindow{}}
	c.state.Sessions[key(s.TenantID, s.SessionID)] = s
	for _, peer := range []*Session{
		{TenantID: "tenant-a", SessionID: "same", Profile: "L0", CreatedAt: start.Add(-time.Hour)},
		{TenantID: "tenant-b", SessionID: "other-tenant", Profile: "L0", CreatedAt: start.Add(-time.Hour)},
		{TenantID: "tenant-a", SessionID: "other-profile", Profile: "L1", CreatedAt: start.Add(-time.Hour)},
	} {
		c.state.Sessions[key(peer.TenantID, peer.SessionID)] = peer
		for i := 0; i < 4; i++ {
			id := peer.SessionID + string(rune('a'+i))
			c.state.Requests[key(peer.TenantID, id)] = &ModelRequest{ID: id, TenantID: peer.TenantID, SessionID: peer.SessionID, Status: "COMPLETED", CreatedAt: start.Add(-10 * time.Second), UpdatedAt: start.Add(-2 * time.Second)}
		}
	}
	current := &ModelRequest{ID: "current", TenantID: s.TenantID, SessionID: s.SessionID, Status: "PENDING", CreatedAt: start, UpdatedAt: start}
	c.state.Requests[key(s.TenantID, current.ID)] = current
	c.state.Observations = append(c.state.Observations, RecordedMeasurement{TenantID: s.TenantID, SessionID: s.SessionID, Measurement: observability.Measurement{At: start.Add(-time.Millisecond), Metric: "model_http_ms", Value: 250}})
	for i := 1; i <= 40; i++ {
		c.state.Observations = append(c.state.Observations, RecordedMeasurement{Measurement: observability.Measurement{At: start.Add(-time.Millisecond), Metric: "checkpoint_total_ms", Value: float64(i)}})
	}
	shadow := copyValue(s)
	r := NewV2Replay(500 * time.Millisecond)
	for step, ms := range []int64{0, 500, 1000, 1500, 1999, 2000, 2250} {
		now := start.Add(time.Duration(ms) * time.Millisecond)
		if step == 2 {
			current.Status, current.UpdatedAt = "COMPLETED", now
			for _, subject := range []*Session{s, shadow} {
				if subject.IdleWindows == nil {
					subject.IdleWindows = map[string]*IdleWindow{}
				}
				subject.IdleSince = now
				subject.IdleWindows["idle"] = &IdleWindow{OperationID: "idle", Epoch: 1, StartedAt: now, ExpiresAt: start.Add(2 * time.Second)}
			}
			finished := start.Add(-time.Second)
			peer := c.state.Sessions[key("tenant-a", "same")]
			peer.IdleWindows = map[string]*IdleWindow{}
			for i := 0; i < 4; i++ {
				id := string(rune('a' + i))
				peer.IdleWindows[id] = &IdleWindow{OperationID: id, Epoch: 1, StartedAt: start.Add(-4 * time.Second), ExpiresAt: start.Add(time.Minute), FinishedAt: &finished}
			}
		}
		if step == 3 {
			c.state.Observations = append(c.state.Observations, RecordedMeasurement{Measurement: observability.Measurement{At: now, Metric: "resume_ready_ms", Value: 123}})
		}
		capacity := &CapacityObservation{CapacityActors: 4, AllocatedActors: 2, ObservedAt: now}
		o := V2NativeObserved{Now: now, Capacity: capacity, Measurements: append([]RecordedMeasurement(nil), c.state.Observations...)}
		for _, peer := range c.state.Sessions {
			if peer == s {
				o.Sessions = append(o.Sessions, shadow)
			} else {
				o.Sessions = append(o.Sessions, copyValue(peer))
			}
		}
		for _, request := range c.state.Requests {
			o.Requests = append(o.Requests, copyValue(request))
		}
		// Future costs and terminal payloads have no observed value at this tick.
		o.Measurements = append(o.Measurements, RecordedMeasurement{Measurement: observability.Measurement{At: now.Add(time.Hour), Metric: "checkpoint_total_ms", Value: 99999}})
		input, evidence := c.serverEstimateLocked(s, ModelBoundaryRequest{ExpectedEpoch: s.Epoch}, nil, capacity, cfg, now)
		want, ew := c.v2DecisionLocked(s, input, evidence, cfg, now)
		got, seen, eg := r.DecideNative(shadow, o, cfg)
		if ew != nil || eg != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("step %d decision got=%+v want=%+v errors=%v/%v", step, got, want, eg, ew)
		}
		if got.Estimates.SnapshotCostMS != 32 {
			t.Fatalf("recent 32 P75 was %d", got.Estimates.SnapshotCostMS)
		}
		if step < 2 && seen.WaitPrediction.Samples != 4 {
			t.Fatalf("tenant/profile cohort=%+v", seen.WaitPrediction)
		}
		if step == 2 && (seen.WaitPhase != "explicit_idle" || seen.WaitPrediction.Samples != 4 || !shadow.Autonomous.Episode.ValidUntil.Equal(start.Add(2*time.Second))) {
			t.Fatalf("idle window evidence=%+v episode=%+v", seen, shadow.Autonomous.Episode)
		}
		if step >= 5 && (seen.WaitPhase != "expired_idle" || shadow.Autonomous.Episode != nil) {
			t.Fatalf("expired idle retained episode: %+v", shadow.Autonomous)
		}
		x, y := copyValue(s.Autonomous), copyValue(shadow.Autonomous)
		if x.Episode != nil {
			x.Episode.ID = "episode"
		}
		if y.Episode != nil {
			y.Episode.ID = "episode"
		}
		if !reflect.DeepEqual(x, y) {
			t.Fatalf("step %d full episode/deadline differs: %+v / %+v", step, x, y)
		}
	}
}

func TestV2NativeReplayHidesFutureTerminalRequests(t *testing.T) {
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	s := &Session{TenantID: "t", SessionID: "s", Profile: "L0", State: "RUNNING", Gate: "OPEN", Epoch: 1, CreatedAt: now.Add(-time.Second)}
	future := &ModelRequest{ID: "r", TenantID: "t", SessionID: "s", CreatedAt: now.Add(-500 * time.Millisecond), UpdatedAt: now.Add(time.Second), Status: "COMPLETED", Result: json.RawMessage(`{"hidden":true}`)}
	o := V2NativeObserved{Now: now, Sessions: []*Session{s}, Requests: []*ModelRequest{future, {ID: "unstarted", TenantID: "t", SessionID: "s", CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(2 * time.Second), Status: "COMPLETED"}}}
	r := NewV2Replay(500 * time.Millisecond)
	_, evidence, err := r.DecideNative(s, o, policy.DefaultLifecycleConfig())
	if err != nil || evidence.WaitPhase != "model" || evidence.WaitAgeMS != 500 || len(r.controller.state.Requests) != 1 || r.controller.state.Requests[key("t", "r")].Status != "DISPATCHING" || future.Status != "COMPLETED" {
		t.Fatalf("future fact exposed or caller mutated: evidence=%+v error=%v requests=%+v", evidence, err, r.controller.state.Requests)
	}
}
