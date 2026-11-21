// agentcloud-sim consumes offline contracts. It never creates a live Actor.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lilong555/agentcloud/internal/sim"
)

func main() {
	tracePath := flag.String("trace", "", "contract v1 trace")
	costPath := flag.String("costs", "", "contract v1 measured or explicit prior costs")
	out := flag.String("output", "", "output directory")
	split := flag.String("split", "holdout", "frozen evaluation split")
	limit := flag.Int("limit", 64, "earliest sessions in selected split")
	slots := flag.Int("slots", 4, "synthetic capacity slots")
	workload := flag.Int64("workload-mib", 64, "explicit unobserved working set scenario")
	arrival := flag.Float64("arrival-spacing-ms", -1, "synthetic regular spacing; -1 preserves observed subset arrivals")
	arrivalFile := flag.String("arrival-configs", "", "explicit synthetic cross-source pressure config")
	scenarioID := flag.String("scenario", "", "scenario id from arrival-configs")
	disk := flag.Int64("disk-budget-mib", 4096, "synthetic snapshot inventory budget")
	oracle := flag.Int("oracle-sessions", 2, "small earliest-session oracle slice")
	oracleBits := flag.Int("oracle-max-bits", 12, "bounded binary enumeration budget")
	oracleEvents := flag.Int("oracle-events", 5, "topological prefix events per oracle session")
	policies := flag.String("policies", "ttl2,ttl15,v2,break-even-gdsf", "baseline set")
	ttl := flag.Float64("ttl-ms", 2000, "FixedTTL tau in ms, chosen on validation only")
	eviction := flag.String("eviction-score", "gdsf", "pressure score: gdsf or lru")
	observationMode := flag.String("observation-mode", "c1", "c1 or native production observation reconstruction")
	historyMode := flag.String("history-mode", "", "tenant_profile or synthetic_shared; mode default when empty")
	idleMode := flag.String("idle-mode", "", "explicit or implicit; mode default when empty")
	idleTTL := flag.Float64("idle-ttl-ms", 600000, "explicit idle registration TTL, independent of event duration")
	stable := flag.Float64("stable-ms", -1, "stable wait; c1=200, native=500 ms when omitted")
	minimum := flag.Float64("minimum-residence-ms", 1000, "configured minimum residence before pressure scaling")
	interval := flag.Float64("scheduler-interval-ms", -1, "c1=event-driven, native=250 ms when omitted")
	ttlPoll := flag.Float64("ttl-poll-ms", 100, "native FixedTTL driver polling interval")
	reservation := flag.Int64("reservation-mib", 512, "Actor reserved memory per occupied slot")
	initialReady := flag.Float64("initial-ready-ms", 500, "initial provisioning service time")
	environmentPath := flag.String("environment-model", "", "sourced system and optional Harness phase timings")
	harnessEnabled := flag.Bool("harness-overhead", false, "enable measured setup/proof/active Harness phases for runtime calibration")
	conditionOrder := flag.Bool("condition-initial-order", false, "retrospective calibration only: use observed initial order from environment model")
	queueScope := flag.String("queue-scope", "request_to_admit", "capacity_wait excludes snapshot readiness; request_to_admit preserves legacy DES scope")
	concurrencyModel := flag.String("concurrency-model", "band", "exact, band, or derived linear interpolation for concurrency 2/3")
	parkInactive := flag.Bool("park-inactive-ticks", false, "park periodic ticks while every Actor is ineligible for a policy decision")
	discardEvaluations := flag.Bool("discard-evaluations", false, "retain evaluation count and actions without serializing every KEEP evaluation")
	maxEvents := flag.Int("max-events", 2000000, "maximum processed events per run")
	gc := flag.Float64("gc-ms", -1, "snapshot GC delay; c1=1000, native=30000 ms when omitted")
	training := flag.Bool("training-history", true, "use frozen training history; native defaults to cold history")
	terminal := flag.Bool("terminal-checkpoint", false, "checkpoint resident Actors at trace termination; native defaults true")
	flag.Parse()
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if *observationMode == "native" {
		if !explicit["training-history"] {
			*training = false
		}
		if !explicit["terminal-checkpoint"] {
			*terminal = true
		}
	}
	if *stable == -1 {
		*stable = 200
		if *observationMode == "native" {
			*stable = 500
		}
	}
	if *interval == -1 {
		*interval = 0
		if *observationMode == "native" {
			*interval = 250
		}
	}
	if *gc == -1 {
		*gc = 1000
		if *observationMode == "native" {
			*gc = 30000
		}
	}
	if *ttl < 0 || (*eviction != "gdsf" && *eviction != "lru") {
		fail(fmt.Errorf("invalid TTL/eviction config"))
	}
	if *tracePath == "" || *costPath == "" || *out == "" || *limit < 1 || *oracle < 0 {
		fail(fmt.Errorf("trace, costs, output and positive limit required"))
	}
	var full sim.Trace
	var costs sim.Costs
	if err := sim.ReadJSON(*tracePath, &full); err != nil {
		fail(err)
	}
	if err := full.Validate(); err != nil {
		fail(err)
	}
	if err := sim.ReadJSON(*costPath, &costs); err != nil {
		fail(err)
	}
	var environment *sim.Environment
	if *environmentPath != "" {
		environment = &sim.Environment{}
		if err := sim.ReadJSON(*environmentPath, environment); err != nil {
			fail(err)
		}
		if err := environment.Validate(); err != nil {
			fail(err)
		}
	}
	if *conditionOrder && (environment == nil || len(environment.InitialOrder) == 0) {
		fail(fmt.Errorf("conditioned replay requires observed initial orders"))
	}
	history := sim.TrainingHistory(full)
	if !*training {
		history = nil
	}
	trace := sim.Trace{SchemaVersion: 1, TraceID: full.TraceID}
	for _, s := range full.Sessions {
		if s.Split == *split {
			trace.Sessions = append(trace.Sessions, s)
		}
	}
	sort.Slice(trace.Sessions, func(i, j int) bool {
		a, b := trace.Sessions[i], trace.Sessions[j]
		if a.Arrival == b.Arrival {
			return a.ID < b.ID
		}
		return a.Arrival < b.Arrival
	})
	if len(trace.Sessions) == 0 {
		fail(fmt.Errorf("empty evaluation split %s", *split))
	}
	if len(trace.Sessions) > *limit {
		trace.Sessions = trace.Sessions[:*limit]
	}
	origin := trace.Sessions[0].Arrival
	for i := range trace.Sessions {
		trace.Sessions[i].Arrival -= origin
		if *arrival >= 0 {
			trace.Sessions[i].Arrival = float64(i) * *arrival
		}
	}
	if *arrivalFile != "" {
		var scenarios struct {
			Scenarios []struct {
				ID      string    `json:"id"`
				Offsets []float64 `json:"arrival_offsets_ms"`
			} `json:"scenarios"`
		}
		if err := sim.ReadJSON(*arrivalFile, &scenarios); err != nil {
			fail(err)
		}
		found := false
		for _, s := range scenarios.Scenarios {
			if s.ID == *scenarioID {
				found = true
				if len(s.Offsets) < len(trace.Sessions) {
					fail(fmt.Errorf("insufficient arrival offsets"))
				}
				for i := range trace.Sessions {
					trace.Sessions[i].Arrival = s.Offsets[i]
				}
			}
		}
		if !found {
			fail(fmt.Errorf("arrival scenario absent"))
		}
	}
	cfg := sim.Config{ObservationMode: *observationMode, HistoryMode: *historyMode, IdleMode: *idleMode, IdleTTLMS: *idleTTL, SchedulerIntervalMS: *interval,
		ParkInactiveTicks: *parkInactive, DiscardEvaluations: *discardEvaluations, MaxEvents: *maxEvents,
		Environment: environment, HarnessEnabled: *harnessEnabled, QueueScope: *queueScope, ConcurrencyModel: *concurrencyModel,
		TrainingHistory: *training, TerminalCheckpoint: *terminal, Slots: *slots, WorkloadBytes: *workload << 20, ReservationBytes: *reservation << 20,
		DiskBudgetBytes: float64(*disk << 20), StableMS: *stable, MinimumResidenceMS: *minimum, InitialReadyMS: *initialReady, GCMS: *gc, SnapshotKeep: 2, AllowConcurrencyBands: true}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fail(err)
	}
	reports := map[string]any{"research_status": "exploratory", "phase": "C1", "trace_id": trace.TraceID, "cost_id": costs.ID, "cost_kind": costs.Kind, "split": *split, "sessions": len(trace.Sessions), "training_samples": len(history), "arrival_spacing_ms": *arrival, "calibration_status": "pending_native_paired_calibration"}
	if *observationMode == "native" {
		reports["phase"] = "C2"
	}
	reports["observation_mode"] = *observationMode
	reports["arrival_scenario"] = *scenarioID
	failed := false
	for _, name := range strings.Split(*policies, ",") {
		switch name {
		case "ttl2", "ttl15", "fixed-ttl", "v2", "break-even-gdsf":
		default:
			fail(fmt.Errorf("unknown baseline %s", name))
		}
		var online sim.OnlinePolicy = sim.Baseline{Name: name, TTL: *ttl, Eviction: *eviction}
		policyConfig := cfg
		if *conditionOrder {
			policyConfig.InitialOrder = environment.InitialOrder[name]
			if len(policyConfig.InitialOrder) == 0 {
				fail(fmt.Errorf("missing observed initial order for %s", name))
			}
		}
		if *observationMode == "native" && (name == "ttl2" || name == "ttl15" || name == "fixed-ttl") {
			if *ttlPoll <= 0 {
				fail(fmt.Errorf("native TTL polling interval must be positive"))
			}
			policyConfig.SchedulerIntervalMS = *ttlPoll
		}
		if name == "v2" {
			online = sim.NewV2(cfg.StableMS, cfg.MinimumResidenceMS)
		}
		r, err := sim.Run(trace, costs, policyConfig, online, history)
		write(filepath.Join(*out, name+".json"), r)
		row := map[string]any{"status": r.Status, "completed": r.Completed, "offered": r.Offered, "actor_reserved_gb_seconds": r.ReservedGBSeconds, "checkpoints": r.Checkpoints, "terminal_checkpoints": r.TerminalCheckpoints, "restores": r.Restores, "initial_queue_p95_ms": r.InitialP95, "resume_queue_p95_ms": r.ResumeP95, "session_e2e_p95_ms": r.E2EP95, "snapshot_allocated_peak_bytes": r.DiskPeak, "objective_gbs_plus_0_1_e2e_s": sim.Objective(r, .1)}
		if err != nil {
			row["error"] = err.Error()
			failed = true
		}
		reports[name] = row
	}
	if *oracle > 0 {
		slice := trace
		slice.Sessions = trace.Sessions[:min(*oracle, len(trace.Sessions))]
		slice.Sessions = append([]sim.Session(nil), slice.Sessions...)
		for i := range slice.Sessions {
			if *oracleEvents > 0 && len(slice.Sessions[i].Events) > *oracleEvents {
				slice.Sessions[i].Events = slice.Sessions[i].Events[:*oracleEvents]
			}
		}
		r, err := sim.EnumerateOracle(slice, costs, cfg, *oracleBits, .1)
		if err != nil {
			fail(err)
		}
		write(filepath.Join(*out, "oracle.json"), r)
		reports["oracle"] = r
		reports["oracle_scope"] = map[string]any{"sessions": len(slice.Sessions), "topological_prefix_events": *oracleEvents, "termination": "synthetic end at prefix boundary"}
	}
	write(filepath.Join(*out, "summary.json"), reports)
	if failed {
		os.Exit(1)
	}
}
func write(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fail(err)
	}
	if err = os.WriteFile(path, append(b, '\n'), 0600); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
