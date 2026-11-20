package sim

import (
	"reflect"
	"testing"
)

func ptr[T any](v T) *T { return &v }
func node(id, kind string, d float64, parents ...string) Event {
	return Event{ID: id, Kind: kind, Duration: ptr(d), Parents: parents, Safe: ptr(kind != "active")}
}
func fixture(events ...Event) Trace {
	return Trace{SchemaVersion: 1, TraceID: "test", Sessions: []Session{{ID: "a", Family: "a", Split: "holdout", Events: events}}}
}
func config() Config {
	return Config{Slots: 1, WorkloadBytes: 16 << 20, ReservationBytes: 512 << 20, DiskBudgetBytes: 1024, StableMS: 0, MinimumResidenceMS: 0, SnapshotKeep: 2, AllowConcurrencyBands: true}
}
func costs() Costs {
	c := Costs{SchemaVersion: 1, ID: "explicit-test-prior", Kind: "prior"}
	for _, n := range []int{1, 4} {
		m := map[string]Metric{}
		for k, v := range map[string]float64{"checkpoint_slot_ms": 10, "snapshot_ready_ms": 20, "resume_ready_ms": 30, "journal_commit_ms": 0, "snapshot_allocated_bytes": 100, "backend_checkpoint_total_ms": 20} {
			unit := "ms"
			if k == "snapshot_allocated_bytes" {
				unit = "bytes"
			}
			m[k] = Metric{P50: ptr(v), Unit: unit, Source: "test_fixture"}
		}
		c.Entries = append(c.Entries, CostEntry{16 << 20, n, m})
	}
	return c
}
func getEvent(t *testing.T, r Result, id string) EventResult {
	t.Helper()
	for _, e := range r.Events {
		if e.Event == id && e.Session == "a" {
			return e
		}
	}
	t.Fatalf("event %s absent", id)
	return EventResult{}
}

func TestClosedLoopParentInterventionAndJoin(t *testing.T) {
	trace := fixture(node("m", "model_wait", 100), node("x", "active", 20, "m"), node("y", "active", 30, "m"), node("m2", "model_wait", 10, "x", "y"))
	r, err := Run(trace, costs(), config(), Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if getEvent(t, r, "m2").Start != 130 {
		t.Fatal("all-parent join violated")
	}
	trace.Sessions[0].Events[0].Duration = ptr(150.0)
	r2, err := Run(trace, costs(), config(), Baseline{Name: "ttl15"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if getEvent(t, r2, "x").Start-getEvent(t, r, "x").Start != 50 || getEvent(t, r2, "m2").Start-getEvent(t, r, "m2").Start != 50 {
		t.Fatal("parent delay did not causally propagate")
	}
}

func TestCheckpointSlotAndResumeQueuePropagate(t *testing.T) {
	trace := fixture(node("m", "model_wait", 100), node("tool", "active", 20, "m"))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "b", Split: "holdout", Arrival: 1, Events: []Event{node("work", "active", 200)}})
	r, err := Run(trace, costs(), config(), Baseline{Name: "break-even-gdsf"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.MaxOccupied != 1 || r.Checkpoints != 1 || r.Restores != 1 {
		t.Fatalf("capacity accounting: %+v", r)
	}
	if r.Sessions[1].InitialQueue != 10 {
		t.Fatalf("checkpoint released slot too early: %+v", r.Sessions)
	}
	if getEvent(t, r, "tool").Start != 241 {
		t.Fatalf("restore wait failed to move tool: %+v", r.Events)
	}
}

type spy struct{ observations []Observation }

func (s *spy) Decide(o Observation) Action {
	s.observations = append(s.observations, o)
	return Baseline{Name: "ttl2"}.Decide(o)
}
func TestIdenticalObservedPrefixesHaveIdenticalDecisions(t *testing.T) {
	a := fixture(node("m", "model_wait", 3000), node("tool", "active", 10, "m"))
	b := fixture(node("m", "model_wait", 9000), node("tool", "active", 99, "m"))
	p, q := &spy{}, &spy{}
	if _, err := Run(a, costs(), config(), p, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(b, costs(), config(), q, nil); err != nil {
		t.Fatal(err)
	}
	cut := func(rows []Observation) []Observation {
		var x []Observation
		for _, o := range rows {
			if o.Now < 3000 {
				x = append(x, o)
			}
		}
		return x
	}
	if !reflect.DeepEqual(cut(p.observations), cut(q.observations)) {
		t.Fatal("future duration reached online policy")
	}
}

func TestMissingCostAndDiskGuard(t *testing.T) {
	trace := fixture(node("m", "model_wait", 3000), node("tool", "active", 10, "m"))
	c := costs()
	c.Entries[0].Metrics["snapshot_allocated_bytes"] = Metric{Unit: "bytes"}
	if _, err := Run(trace, c, config(), Baseline{Name: "ttl2"}, nil); err == nil {
		t.Fatal("missing cost silently zero-filled")
	}
	cfg := config()
	cfg.DiskBudgetBytes = 50
	r, err := Run(trace, costs(), cfg, Baseline{Name: "ttl2"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Checkpoints != 0 || r.DiskDenied == 0 || r.DiskPeak > 50 {
		t.Fatal("disk guard failed")
	}
}

func TestOracleBracketsRestrictedFeasiblePolicy(t *testing.T) {
	trace := fixture(node("m", "model_wait", 3000), node("tool", "active", 10, "m"))
	r, err := EnumerateOracle(trace, costs(), config(), 4, .1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Evaluated != 2 || r.Feasible != 2 || r.BestRestrictedFeasible == nil || r.RelaxedLowerBound > *r.BestRestrictedFeasible {
		t.Fatalf("invalid oracle bracket: %+v", r)
	}
}

func TestTrainingFamilyLeakageRejected(t *testing.T) {
	trace := fixture(node("m", "model_wait", 10))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "a", Split: "train", Events: []Event{node("n", "active", 10)}})
	if err := trace.Validate(); err == nil {
		t.Fatal("family leakage accepted")
	}
}

func TestTemporalTrainingAndTuningLeakageRejected(t *testing.T) {
	trace := Trace{SchemaVersion: 1, TraceID: "time-split", Sessions: []Session{
		{ID: "train", Family: "train", Split: "train", Events: []Event{node("m", "model_wait", 10)}},
		{ID: "tune", Family: "tune", Split: "validation", Arrival: 10, Events: []Event{node("m", "model_wait", 10)}},
		{ID: "eval", Family: "eval", Split: "generalization", Arrival: 20, Events: []Event{node("m", "model_wait", 10)}},
	}}
	if err := trace.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, arrival := range []float64{5, 15} {
		trace.Sessions[2].Arrival = arrival
		if err := trace.Validate(); err == nil {
			t.Fatalf("evaluation at %g accepted future training/tuning labels", arrival)
		}
	}
}

func TestSimultaneousCheckpointsUseOneContentionCoordinate(t *testing.T) {
	trace := fixture(node("m", "model_wait", 100), node("tool", "active", 10, "m"))
	b := trace.Sessions[0]
	b.ID = "b"
	b.Family = "b"
	trace.Sessions = append(trace.Sessions, b)
	cfg := config()
	cfg.Slots = 2
	c := costs()
	c.Entries[1].Metrics["checkpoint_slot_ms"] = Metric{P50: ptr(40.0), Unit: "ms", Source: "fixture"}
	r, err := Run(trace, c, cfg, Baseline{Name: "ttl", TTL: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Two slots held for 40ms each, plus two restores (30ms) and tools (10ms).
	want := 160 * float64(cfg.ReservationBytes) / 1e12
	if r.ReservedGBSeconds != want {
		t.Fatalf("mixed cost coordinates in one wave: %g != %g", r.ReservedGBSeconds, want)
	}
}

func TestSlotReleasePrecedesDurabilityAndRestoration(t *testing.T) {
	trace := fixture(node("m", "model_wait", 15), node("tool", "active", 1, "m"))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "b", Split: "holdout", Arrival: 1, Events: []Event{node("work", "active", 20)}})
	c := costs()
	c.Entries[0].Metrics["snapshot_ready_ms"] = Metric{P50: ptr(100.0), Unit: "ms", Source: "fixture"}
	r, err := Run(trace, c, config(), Baseline{Name: "fixed-ttl", TTL: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sessions[1].Ready != 10 || getEvent(t, r, "tool").Start != 130 {
		t.Fatalf("release/durable boundaries merged: %+v", r.Sessions)
	}
}

func TestPressureWakeUsesEarliestSafeTime(t *testing.T) {
	trace := fixture(node("m", "model_wait", 1000), node("tool", "active", 1, "m"))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "b", Split: "holdout", Arrival: 1, Events: []Event{node("work", "active", 1)}})
	c := costs()
	c.Entries[0].Metrics["backend_checkpoint_total_ms"] = Metric{P50: ptr(200.0), Unit: "ms", Source: "fixture"}
	cfg := config()
	cfg.MinimumResidenceMS = 20
	r, err := Run(trace, c, cfg, Baseline{Name: "break-even-gdsf"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Decisions) == 0 || r.Decisions[0].At != 20 || r.Decisions[0].Reason != "pressure_gdsf" {
		t.Fatalf("pressure waited for break-even instead of safety: %+v", r.Decisions)
	}
}

func nativeConfig() Config {
	cfg := config()
	cfg.ObservationMode, cfg.HistoryMode, cfg.IdleMode = "native", "tenant_profile", "explicit"
	cfg.StableMS, cfg.MinimumResidenceMS, cfg.SchedulerIntervalMS, cfg.IdleTTLMS = 500, 1000, 250, 600000
	cfg.TerminalCheckpoint = true
	return cfg
}

func TestNativeTicksCostUpdatesAndTerminalRelease(t *testing.T) {
	trace := fixture(node("m", "model_wait", 3000), node("tool", "active", 10, "m"), node("m2", "model_wait", 3000, "tool"))
	trace.Sessions[0].Tenant = "tenant-a"
	cfg := nativeConfig()
	p := NewV2(cfg.StableMS, cfg.MinimumResidenceMS)
	r, err := Run(trace, costs(), cfg, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Completed != 1 || r.Checkpoints != 2 || r.Restores != 1 || r.TerminalCheckpoints != 0 {
		t.Fatalf("unexpected lifecycle %+v", r)
	}
	var first, second *Decision
	for i := range r.Evaluations {
		d := &r.Evaluations[i]
		if int(d.At)%250 != 0 {
			t.Fatalf("off-tick decision at %g", d.At)
		}
		if d.Event == "m" && first == nil {
			first = d
		}
		if d.Event == "m2" && second == nil {
			second = d
		}
	}
	if first == nil || second == nil || first.Episode.SwitchCostMS != 1000 || second.Episode.SwitchCostMS != 50 || second.Evidence.CostSources["snapshot"] != "observed_checkpoint_total_ms_p75" {
		t.Fatalf("cost observations first=%+v second=%+v", first, second)
	}
	if p.sessions["a"].TenantID != "tenant-a" || p.sessions["a"].Profile != "L0" {
		t.Fatal("actor cohort identity lost")
	}
	short := fixture(node("work", "active", 100))
	r, err = Run(short, costs(), cfg, Baseline{Name: "ttl2"}, nil)
	if err != nil || r.TerminalCheckpoints != 1 || r.Sessions[0].Finish != 120 || r.ReservedGBSeconds != 110*float64(cfg.ReservationBytes)/1e12 {
		t.Fatalf("terminal release accounting %+v %v", r, err)
	}
}

func TestNativeExplicitIdleExpiryPreservesModelEpisodeUntilTTL(t *testing.T) {
	trace := fixture(node("m", "model_wait", 1000), node("idle", "user_idle", 4000, "m"))
	cfg := nativeConfig()
	cfg.IdleTTLMS = 750
	r, err := Run(trace, costs(), cfg, NewV2(cfg.StableMS, cfg.MinimumResidenceMS), nil)
	if err != nil {
		t.Fatal(err)
	}
	var active, expired bool
	for _, d := range r.Evaluations {
		if d.At == 1000 {
			active = d.Episode != nil && d.Episode.Origin == "model:a/m" && fromSimTime(d.Episode.StartedAt) == 0 && fromSimTime(d.Episode.UnknownReleaseAt) == 2000 && fromSimTime(d.Episode.ValidUntil) == 1750
		}
		if d.At == 1750 {
			expired = d.Episode == nil && d.Reason == "no_active_wait"
		}
	}
	if !active || !expired || r.TerminalCheckpoints != 1 || r.Checkpoints != 1 {
		t.Fatalf("idle continuity/expiry wrong: %+v", r)
	}
}

func TestNativeObservedPrefixHidesFutureCompletionAndColdTraining(t *testing.T) {
	a := fixture(node("m", "model_wait", 3000), node("tool", "active", 10, "m"))
	b := fixture(node("m", "model_wait", 9000), node("tool", "active", 99, "m"))
	cfg := nativeConfig()
	history := []WaitSample{{ID: "old", Tenant: "t", Profile: "L0", Phase: "model_wait", Started: -10000, Duration: 5, Completed: true}}
	r, err := Run(a, costs(), cfg, NewV2(500, 1000), history)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Run(b, costs(), cfg, NewV2(500, 1000), nil)
	if err != nil {
		t.Fatal(err)
	}
	cut := func(rows []Decision) []Decision {
		var out []Decision
		for _, row := range rows {
			if row.At < 3000 {
				out = append(out, row)
			}
		}
		return out
	}
	if !reflect.DeepEqual(cut(r.Evaluations), cut(s.Evaluations)) {
		t.Fatal("future duration or disabled training affected prefix")
	}
}

func TestNativeResumeQueueIncludesDurabilityWait(t *testing.T) {
	trace := fixture(node("m", "model_wait", 2100), node("tool", "active", 1, "m"))
	c := costs()
	for i := range c.Entries {
		c.Entries[i].Metrics["snapshot_ready_ms"] = Metric{P50: ptr(500.0), Unit: "ms", Source: "fixture"}
	}
	cfg := nativeConfig()
	r, err := Run(trace, c, cfg, NewV2(500, 1000), nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sessions[0].ResumeQueues[0] != 400 || getEvent(t, r, "tool").Start != 2530 {
		t.Fatalf("durability demand wait missing %+v", r.Sessions)
	}
}

func TestNativePendingCheckpointKeepsCostPrior(t *testing.T) {
	trace := fixture(node("m", "model_wait", 5000))
	trace.Sessions = append(trace.Sessions, Session{ID: "b", Family: "b", Split: "holdout", Arrival: 1000, Events: []Event{node("m", "model_wait", 5000)}})
	c := costs()
	for i := range c.Entries {
		c.Entries[i].Metrics["snapshot_ready_ms"] = Metric{P50: ptr(800.0), Unit: "ms", Source: "fixture"}
	}
	cfg := nativeConfig()
	cfg.Slots = 2
	r, err := Run(trace, c, cfg, NewV2(500, 1000), nil)
	if err != nil {
		t.Fatal(err)
	}
	before, after := "", ""
	for _, d := range r.Evaluations {
		if d.Session == "b" && d.At == 2500 {
			before = d.Evidence.CostSources["snapshot"]
		}
		if d.Session == "b" && d.At == 3000 {
			after = d.Evidence.CostSources["snapshot"]
		}
	}
	if before != "startup_config_prior" || after != "observed_checkpoint_total_ms_p75" {
		t.Fatalf("checkpoint observation crossed unfinished boundary: before=%s after=%s", before, after)
	}
}
