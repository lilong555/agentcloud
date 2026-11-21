package sim

import (
	"encoding/json"
	"fmt"
)

// Environment describes measured service phases. It is engine-owned; online
// policies receive only completed observations, never this document or its order.
type Environment struct {
	SchemaVersion int                 `json:"schema_version"`
	ID            string              `json:"model_id"`
	Source        string              `json:"source"`
	InitialMS     float64             `json:"initial_ready_ms"`
	Harness       HarnessOverhead     `json:"harness_overhead"`
	InitialOrder  map[string][]string `json:"conditioned_initial_order,omitempty"`
}

type HarnessOverhead struct {
	SetupMS       float64 `json:"setup_ms"`
	ModelProofMS  float64 `json:"model_proof_ms"`
	ActiveExtraMS float64 `json:"active_extra_ms"`
}

func (m *Environment) UnmarshalJSON(data []byte) error {
	type document Environment
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var harness map[string]json.RawMessage
	if err := json.Unmarshal(fields["harness_overhead"], &harness); err != nil {
		return fmt.Errorf("missing Harness phase measurements: %w", err)
	}
	for key, raw := range map[string]json.RawMessage{"initial_ready_ms": fields["initial_ready_ms"],
		"setup_ms": harness["setup_ms"], "model_proof_ms": harness["model_proof_ms"], "active_extra_ms": harness["active_extra_ms"]} {
		var value *float64
		if len(raw) == 0 || json.Unmarshal(raw, &value) != nil || value == nil {
			return fmt.Errorf("missing phase measurement %s", key)
		}
	}
	var parsed document
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*m = Environment(parsed)
	return m.Validate()
}

func (m Environment) Validate() error {
	if m.SchemaVersion != 1 || m.ID == "" || m.Source == "" {
		return fmt.Errorf("environment requires version, identity and measurement source")
	}
	for _, v := range []float64{m.InitialMS, m.Harness.SetupMS, m.Harness.ModelProofMS, m.Harness.ActiveExtraMS} {
		if !valid(v) || v < 0 {
			return fmt.Errorf("environment durations must be finite nonnegative milliseconds")
		}
	}
	return nil
}

// Interpolate uses the measured endpoints verbatim and discloses a derived
// concurrency model. Extrapolation and interpolation over missing values fail.
func (c Costs) Interpolate(bytes int64, n int) (LifecycleCost, error) {
	if n != 2 && n != 3 {
		return c.Lookup(bytes, n)
	}
	a, err := c.Lookup(bytes, 1)
	if err != nil {
		return LifecycleCost{}, err
	}
	b, err := c.Lookup(bytes, 4)
	if err != nil {
		return LifecycleCost{}, err
	}
	f := float64(n-1) / 3
	lerp := func(x, y float64) float64 { return x + f*(y-x) }
	return LifecycleCost{lerp(a.Checkpoint, b.Checkpoint), lerp(a.SnapshotReady, b.SnapshotReady),
		lerp(a.Resume, b.Resume), lerp(a.Journal, b.Journal), lerp(a.Bytes, b.Bytes), lerp(a.SnapshotCost, b.SnapshotCost)}, nil
}
