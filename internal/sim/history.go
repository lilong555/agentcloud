package sim

import (
	"github.com/lilong555/agentcloud/internal/policy"
	"sort"
)

// WaitSample is disclosed only after completion (or a known censor boundary).
// Histories are phase-specific and bounded by the 64 most recently started waits.
type WaitSample struct {
	ID, Phase         string
	Started, Duration float64
	Completed         bool
}

func (e *engine) retainHistory() {
	sort.Slice(e.samples, func(i, j int) bool {
		if e.samples[i].Started == e.samples[j].Started {
			return e.samples[i].ID < e.samples[j].ID
		}
		return e.samples[i].Started > e.samples[j].Started
	})
	counts := map[string]int{}
	kept := e.samples[:0]
	for _, r := range e.samples {
		if counts[r.Phase] < 64 {
			kept = append(kept, r)
			counts[r.Phase]++
		}
	}
	e.samples = kept
}

// TrainingHistory consumes only the frozen training population. All selected
// timestamps precede replay time zero. A published model completion is observed;
// an unobserved session continuation supplies no invented censoring sample.
func TrainingHistory(t Trace) []WaitSample {
	var rows []WaitSample
	latest := 0.0
	for _, s := range t.Sessions {
		if s.Split != "train" {
			continue
		}
		ends := map[string]float64{}
		for _, n := range s.Events {
			start := s.Arrival
			for _, p := range n.Parents {
				start = max(start, ends[p])
			}
			end := start + *n.Duration
			ends[n.ID] = end
			latest = max(latest, end)
			if n.Kind == "model_wait" || n.Kind == "user_idle" {
				rows = append(rows, WaitSample{"train/" + s.ID + "/" + n.ID, n.Kind, start, *n.Duration, true})
			}
		}
	}
	for i := range rows {
		rows[i].Started -= latest + 1
	}
	return rows
}

func (e *engine) waitSamples(subject *actor, phase, event string) []policy.SurvivalSample {
	rows := make([]WaitSample, 0, len(e.samples)+len(e.actors))
	for _, r := range e.samples {
		if r.Phase == phase && r.Started <= e.now {
			rows = append(rows, r)
		}
	}
	for _, a := range e.actors {
		for j, n := range a.source.Events {
			if a.states[j] != 2 || n.Kind != phase || a == subject && n.ID == event {
				continue
			}
			rows = append(rows, WaitSample{a.source.ID + "/" + n.ID, phase, a.startTimes[j], e.now - a.startTimes[j], false})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Started == rows[j].Started {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Started > rows[j].Started
	})
	rows = rows[:min(len(rows), 64)]
	result := make([]policy.SurvivalSample, len(rows))
	for i, r := range rows {
		result[i] = policy.SurvivalSample{DurationMS: int64(r.Duration), Completed: r.Completed}
	}
	return result
}
