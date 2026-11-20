package control

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

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
