package sim

import (
	"github.com/lilong555/agentcloud/internal/control"
	"github.com/lilong555/agentcloud/internal/policy"
	"reflect"
	"testing"
)

func TestV2DESAdapterMatchesControllerReplay(t *testing.T) {
	p := NewV2(200, 1000)
	r := control.NewV2Replay(200_000_000)
	s := &control.Session{TenantID: "synthetic-profile", SessionID: "a", Profile: "L0", State: "RUNNING", Gate: "OPEN", CreatedAt: simTime(0), RunningSince: simTime(0), Epoch: 1, Autonomous: &control.AutonomousState{}}
	history := []policy.SurvivalSample{{DurationMS: 5000, Completed: true}, {DurationMS: 5000, Completed: true}, {DurationMS: 5000, Completed: true}, {DurationMS: 5000, Completed: true}}
	for i, now := range []float64{0, 200, 1000, 2000, 2200, 2400, 3000} {
		epoch := uint64(1)
		ready := 0.0
		safe := i != 3
		if i >= 4 {
			epoch = 2
			ready = 2200
		}
		state := ActorState{ID: "a", Now: now, Created: 0, LastReady: ready, Epoch: epoch, Safe: safe, Waiting: safe, Ready: true}
		p.Observe(state)
		s.Epoch = epoch
		s.RunningSince = simTime(ready)
		if epoch > 1 {
			s.LastResumedAt = simTime(ready)
		}
		s.ActiveTools = 0
		if !safe {
			s.ActiveTools = 1
			_, _ = r.Decide(s, control.V2Observed{Now: simTime(now), WorkerSlots: 1}, policy.DefaultLifecycleConfig())
			continue
		}
		phase := "model_wait"
		controlPhase := "model"
		phaseStart := ready
		if i == 6 {
			phase = "user_idle"
			controlPhase = "implicit_idle"
			phaseStart = 2900
		}
		o := Observation{Now: now, SessionID: "a", Phase: phase, PhaseStarted: phaseStart, PhaseAge: now - phaseStart, Slots: 4, ResumeQueue: 2, SnapshotMS: 150, ResumeMS: 200, JournalMS: 25, Samples: history}
		d := p.Decide(o)
		want, err := r.Decide(s, control.V2Observed{Now: simTime(now), WaitStartedAt: simTime(phaseStart), WaitPhase: controlPhase, WaitAgeMS: int64(now - phaseStart), Samples: history, ReadyReturns: 2, WorkerSlots: 4}, policy.LifecycleConfig{SnapshotCostMS: 150, ResumeCostMS: 200, JournalCostMS: 25, MinimumResidenceMS: 1000})
		if err != nil || d.Error != "" || d.Suspend != (want.Action == "SUSPEND") || d.Reason != want.Reason {
			t.Fatalf("step=%d decision=%+v want=%+v err=%v", i, d, want, err)
		}
		episode := *s.Autonomous.Episode
		episode.ID = "opaque-episode"
		if !reflect.DeepEqual(d.Episode, &episode) || d.RevisitAt != fromSimTime(episode.PlannedReleaseAt) {
			t.Fatalf("step=%d deadline/episode mismatch", i)
		}
	}
}

func TestGDSFAndLRUOnlyChangePressureScore(t *testing.T) {
	choices := []victim{{index: 0, id: "a", frequency: 10, lastAccess: 1, costMS: 100, bytes: 100}, {index: 1, id: "b", frequency: 1, lastAccess: 2, costMS: 100, bytes: 100}}
	g, l := pressureEvictor{mode: "gdsf"}, pressureEvictor{mode: "lru"}
	if g.choose(choices) != 1 || l.choose(choices) != 0 || g.age != 0 {
		t.Fatal("GDSF frequency/aging or LRU order incorrect")
	}
	g.evict("b", true)
	if g.age != 1 {
		t.Fatal("aging must advance on committed pressure eviction")
	}
	choices[0].frequency++
	g.touch(choices[0])
	if g.choose(choices[:1]) != 0 || g.items["a"].key != 12 {
		t.Fatal("GDSF hit must recompute key with aging")
	}
	g.evict("a", false)
	if g.age != 1 {
		t.Fatal("proactive release must not advance pressure aging")
	}
	a := Baseline{Name: "break-even-gdsf", Eviction: "gdsf"}
	b := a
	b.Eviction = "lru"
	o := Observation{Now: 100, WaitStarted: 0, SwitchCostMS: 300}
	if !reflect.DeepEqual(a.Decide(o), b.Decide(o)) || a.Decide(o).RevisitAt != 300 {
		t.Fatal("eviction switch changed break-even release")
	}
}

func TestHistoryIsPhaseBoundedAndExcludesCurrentRequest(t *testing.T) {
	a := &actor{source: Session{ID: "a", Events: []Event{node("m", "model_wait", 100)}}, states: []int{2}, startTimes: []float64{0}}
	e := &engine{now: 20, actors: []*actor{a}}
	for i := 0; i < 80; i++ {
		e.samples = append(e.samples, WaitSample{ID: string(rune(i)), Phase: "model_wait", Started: -float64(i + 1), Duration: float64(i), Completed: true})
	}
	e.samples = append(e.samples, WaitSample{ID: "idle", Phase: "user_idle", Started: 0, Duration: 999, Completed: true})
	e.retainHistory()
	samples := e.waitSamples(a, "model_wait", "m")
	if len(samples) != 64 || samples[0].DurationMS != 0 || samples[63].DurationMS != 63 {
		t.Fatal("history phase/recency changed")
	}
	for _, s := range samples {
		if !s.Completed {
			t.Fatal("current request leaked into its own cohort")
		}
	}
}
