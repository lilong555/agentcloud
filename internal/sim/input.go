// Package sim implements offline, causal L0 replay. It has no runtime backend.
package sim

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
)

type Trace struct {
	SchemaVersion int       `json:"schema_version"`
	TraceID       string    `json:"trace_id"`
	Sessions      []Session `json:"sessions"`
}
type Session struct {
	ID       string  `json:"session_id"`
	Family   string  `json:"family_id"`
	Tenant   string  `json:"tenant_id"`
	Harness  string  `json:"harness"`
	Split    string  `json:"split"`
	Arrival  float64 `json:"arrival_ms"`
	Workload *int64  `json:"workload_bytes"`
	Events   []Event `json:"events"`
}
type Event struct {
	ID       string   `json:"id"`
	Kind     string   `json:"kind"`
	Parents  []string `json:"parents"`
	Duration *float64 `json:"duration_ms"`
	Safe     *bool    `json:"safe_to_suspend"`
}
type Metric struct {
	P50     *float64 `json:"p50"`
	P95     *float64 `json:"p95"`
	Samples int      `json:"samples"`
	Unit    string   `json:"unit"`
	Source  string   `json:"source"`
	Scope   string   `json:"scope"`
}
type CostEntry struct {
	Workload    int64             `json:"workload_bytes"`
	Concurrency int               `json:"concurrency"`
	Metrics     map[string]Metric `json:"metrics"`
}
type Costs struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"cost_id"`
	Kind          string         `json:"kind"`
	Environment   map[string]any `json:"environment"`
	Entries       []CostEntry    `json:"entries"`
}
type LifecycleCost struct {
	Checkpoint, SnapshotReady, Resume, Journal, Bytes, SnapshotCost float64
}

func ReadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (t Trace) Validate() error {
	if t.SchemaVersion != 1 || t.TraceID == "" {
		return fmt.Errorf("invalid trace identity")
	}
	ids, families := map[string]bool{}, map[string]string{}
	starts, finishes := map[string]float64{}, map[string]float64{}
	for _, s := range t.Sessions {
		if s.ID == "" || s.Family == "" || ids[s.ID] || !valid(s.Arrival) || s.Arrival < 0 {
			return fmt.Errorf("invalid session %q", s.ID)
		}
		ids[s.ID] = true
		if s.Split != "excluded" {
			if prev, ok := families[s.Family]; ok && prev != s.Split {
				return fmt.Errorf("family split leakage: %s", s.Family)
			}
			families[s.Family] = s.Split
		}
		seen := map[string]bool{}
		ends := map[string]float64{}
		finish := s.Arrival
		if len(s.Events) == 0 {
			return fmt.Errorf("empty session %s", s.ID)
		}
		for _, e := range s.Events {
			if e.ID == "" || seen[e.ID] || e.Duration == nil || !valid(*e.Duration) || *e.Duration < 0 {
				return fmt.Errorf("missing/invalid event duration: %s/%s", s.ID, e.ID)
			}
			if e.Kind != "model_wait" && e.Kind != "active" && e.Kind != "user_idle" {
				return fmt.Errorf("unknown event kind")
			}
			for _, p := range e.Parents {
				if !seen[p] {
					return fmt.Errorf("non-topological parent %s", p)
				}
			}
			start := s.Arrival
			for _, p := range e.Parents {
				start = math.Max(start, ends[p])
			}
			ends[e.ID] = start + *e.Duration
			finish = math.Max(finish, ends[e.ID])
			seen[e.ID] = true
		}
		if s.Split != "excluded" {
			if old, ok := starts[s.Split]; !ok || s.Arrival < old {
				starts[s.Split] = s.Arrival
			}
			finishes[s.Split] = math.Max(finishes[s.Split], finish)
		}
	}
	// Validate source chronology before the CLI remaps arrivals into a scenario.
	// Both training labels and validation-selected parameters precede evaluation.
	for _, pair := range [][2]string{{"train", "validation"}, {"train", "holdout"}, {"train", "generalization"}, {"validation", "holdout"}, {"validation", "generalization"}} {
		end, hasEnd := finishes[pair[0]]
		start, hasStart := starts[pair[1]]
		if hasEnd && hasStart && end > start {
			return fmt.Errorf("temporal split leakage: %s overlaps %s", pair[0], pair[1])
		}
	}
	return nil
}

func valid(n float64) bool { return !math.IsNaN(n) && !math.IsInf(n, 0) }

// Lookup requires an exact coordinate. A caller may explicitly request a band
// scenario, but the table itself never interpolates physical observations.
func (c Costs) Lookup(bytes int64, concurrency int) (LifecycleCost, error) {
	if c.SchemaVersion != 1 || c.ID == "" || (c.Kind != "prior" && c.Kind != "measured") {
		return LifecycleCost{}, fmt.Errorf("invalid cost table")
	}
	if c.Kind == "measured" && (c.Environment["architecture"] != "x86_64" || c.Environment["runtime"] != "gvisor_MEMORY") {
		return LifecycleCost{}, fmt.Errorf("measured table requires x86 gVisor MEMORY")
	}
	var selected *CostEntry
	for i := range c.Entries {
		e := &c.Entries[i]
		if e.Workload == bytes && e.Concurrency == concurrency {
			if selected != nil {
				return LifecycleCost{}, fmt.Errorf("duplicate cost coordinate")
			}
			selected = e
		}
	}
	if selected == nil {
		return LifecycleCost{}, fmt.Errorf("unmeasured coordinate workload=%d concurrency=%d", bytes, concurrency)
	}
	keys := []string{"checkpoint_slot_ms", "snapshot_ready_ms", "resume_ready_ms", "journal_commit_ms", "snapshot_allocated_bytes", "backend_checkpoint_total_ms"}
	v := make([]float64, len(keys))
	for i, k := range keys {
		m, ok := selected.Metrics[k]
		unit := "ms"
		if i == 4 {
			unit = "bytes"
		}
		if !ok || m.P50 == nil || !valid(*m.P50) || *m.P50 < 0 || m.Unit != unit || m.Source == "" {
			return LifecycleCost{}, fmt.Errorf("missing/invalid required cost %s", k)
		}
		v[i] = *m.P50
	}
	return LifecycleCost{v[0], v[1], v[2], v[3], v[4], v[5]}, nil
}
