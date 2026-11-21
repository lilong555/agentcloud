package sim

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestEnvironmentRejectsMissingPhaseMeasurements(t *testing.T) {
	for _, raw := range []string{
		`{"schema_version":1,"model_id":"x","source":"test","initial_ready_ms":0}`,
		`{"schema_version":1,"model_id":"x","source":"test","initial_ready_ms":0,"harness_overhead":{"setup_ms":0,"model_proof_ms":null,"active_extra_ms":0}}`,
	} {
		var model Environment
		if json.Unmarshal([]byte(raw), &model) == nil {
			t.Fatal("missing measurement was accepted")
		}
	}
}

func testEnvironment() *Environment {
	return &Environment{SchemaVersion: 1, ID: "fixture", Source: "test", InitialMS: 10,
		Harness: HarnessOverhead{SetupMS: 20, ModelProofMS: 30, ActiveExtraMS: 40}}
}

func TestHarnessPhasesAreCausalOccupancyAndDefaultOff(t *testing.T) {
	trace := fixture(node("m", "model_wait", 100), node("a", "active", 50, "m"), node("m2", "model_wait", 100, "a"))
	cfg := config()
	cfg.Environment = testEnvironment()
	off, err := Run(trace, costs(), cfg, Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.HarnessEnabled = true
	on, err := Run(trace, costs(), cfg, Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if getEvent(t, off, "m").Start != 10 || getEvent(t, on, "m").Start != 60 || getEvent(t, on, "m2").Start != 280 {
		t.Fatalf("Harness dependency timing: off=%+v on=%+v", off.Events, on.Events)
	}
	extra := 20.0 + 2*30 + 40
	if math.Abs(on.ReservedGBSeconds-off.ReservedGBSeconds-extra*float64(cfg.ReservationBytes)/1e12) > 1e-9 {
		t.Fatal("Harness occupancy was not integrated exactly once")
	}
	if getEvent(t, on, "m").End-getEvent(t, on, "m").Start != 100 {
		t.Fatal("proof polluted source model duration")
	}
	for _, stage := range off.ServiceStages {
		if stage.Kind != "initial_activation" {
			t.Fatalf("disabled Harness emitted %+v", stage)
		}
	}
}

func TestHarnessDoesNotIntroduceSuspendSafetyOrFutureFeatures(t *testing.T) {
	trace := fixture(node("m", "model_wait", 100))
	cfg := config()
	cfg.HarnessEnabled = true
	cfg.Environment = testEnvironment()
	p := &spy{}
	r, err := Run(trace, costs(), cfg, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range p.observations {
		if o.Now < 60 {
			t.Fatal("policy evaluated during unsafe setup/proof")
		}
	}
	trace.Sessions[0].Events[0].Duration = ptr(1000.0)
	q := &spy{}
	_, err = Run(trace, costs(), cfg, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.observations) == 0 || !reflect.DeepEqual(p.observations[0], q.observations[0]) {
		t.Fatal("future model duration entered initial decision")
	}
	if r.Completed != 1 {
		t.Fatal("incomplete")
	}
}

func TestCapacityQueueExcludesSnapshotReadiness(t *testing.T) {
	trace := fixture(node("m", "model_wait", 2100), node("tool", "active", 1, "m"))
	c := costs()
	for i := range c.Entries {
		c.Entries[i].Metrics["snapshot_ready_ms"] = Metric{P50: ptr(500.0), Unit: "ms", Source: "fixture"}
	}
	cfg := nativeConfig()
	cfg.QueueScope = "capacity_wait"
	r, err := Run(trace, c, cfg, NewV2(500, 1000), nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sessions[0].ResumeQueues[0] != 0 || r.Sessions[0].SnapshotWaits[0] != 400 || getEvent(t, r, "tool").Start != 2530 {
		t.Fatalf("snapshot wait must affect start but stay outside capacity queue: %+v", r.Sessions)
	}
	last := r.Admissions[len(r.Admissions)-1]
	if last.Eligible-last.Requested != 400 || last.CapacityWait != 0 {
		t.Fatal(last)
	}
}

func TestConditionedOrderPreservesArrivalsAndInput(t *testing.T) {
	trace := fixture(node("work", "active", 100))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "b", Split: "holdout", Events: []Event{node("work", "active", 100)}})
	cfg := config()
	cfg.InitialOrder = []string{"b", "a"}
	r, err := Run(trace, costs(), cfg, Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Admissions[0].Session != "b" || trace.Sessions[0].ID != "a" {
		t.Fatal("conditioned order or input immutability")
	}
	trace.Sessions[1].Arrival = 1
	r, err = Run(trace, costs(), cfg, Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Admissions[0].Session != "a" {
		t.Fatal("tie-break crossed arrival boundary")
	}
	cfg.InitialOrder = []string{"b", "b"}
	if _, err = Run(trace, costs(), cfg, Baseline{Name: "ttl15"}, nil); err == nil {
		t.Fatal("duplicate order accepted")
	}
}

func TestLinearCostsPreserveMeasuredEndpointsAndMissingValues(t *testing.T) {
	c := costs()
	c.Entries[1].Metrics["checkpoint_slot_ms"] = Metric{P50: ptr(40.0), Unit: "ms", Source: "fixture"}
	for n, want := range map[int]float64{1: 10, 2: 20, 3: 30, 4: 40} {
		got, err := c.Interpolate(16<<20, n)
		if err != nil || got.Checkpoint != want {
			t.Fatalf("n=%d cost=%+v err=%v", n, got, err)
		}
	}
	if _, err := c.Interpolate(16<<20, 5); err == nil {
		t.Fatal("extrapolation accepted")
	}
	delete(c.Entries[1].Metrics, "resume_ready_ms")
	if _, err := c.Interpolate(16<<20, 2); err == nil {
		t.Fatal("missing observation filled")
	}
	m := testEnvironment()
	m.Harness.ModelProofMS = math.NaN()
	if m.Validate() == nil {
		t.Fatal("invalid phase accepted")
	}
}
