package sim

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/lilong555/agentcloud/internal/control"
)

type Config struct {
	ParkInactiveTicks     bool         `json:"park_inactive_ticks"`
	DiscardEvaluations    bool         `json:"discard_evaluations"`
	QueueScope            string       `json:"queue_scope"`
	ConcurrencyModel      string       `json:"concurrency_model"`
	Environment           *Environment `json:"environment,omitempty"`
	HarnessEnabled        bool         `json:"harness_overhead_enabled"`
	InitialOrder          []string     `json:"conditioned_initial_order,omitempty"`
	ObservationMode       string       `json:"observation_mode"`
	HistoryMode           string       `json:"history_mode"`
	IdleMode              string       `json:"idle_mode"`
	IdleTTLMS             float64      `json:"idle_ttl_ms"`
	SchedulerIntervalMS   float64      `json:"scheduler_interval_ms"`
	TrainingHistory       bool         `json:"training_history"`
	TerminalCheckpoint    bool         `json:"terminal_checkpoint"`
	Slots                 int          `json:"slots"`
	WorkloadBytes         int64        `json:"workload_bytes"`
	ReservationBytes      int64        `json:"reservation_bytes"`
	DiskBudgetBytes       float64      `json:"disk_budget_bytes"`
	StableMS              float64      `json:"stable_ms"`
	MinimumResidenceMS    float64      `json:"minimum_residence_ms"`
	InitialReadyMS        float64      `json:"initial_ready_ms"`
	GCMS                  float64      `json:"gc_ms"`
	SnapshotKeep          int          `json:"snapshot_keep"`
	AllowConcurrencyBands bool         `json:"allow_concurrency_bands"`
	MaxEvents             int          `json:"max_events"`
}
type Decision struct {
	At       float64                         `json:"at_ms"`
	Session  string                          `json:"session_id"`
	Event    string                          `json:"event_id"`
	Age      float64                         `json:"wait_age_ms"`
	Action   string                          `json:"action"`
	Reason   string                          `json:"reason"`
	Deadline float64                         `json:"deadline_ms,omitempty"`
	Episode  *control.WaitEpisode            `json:"episode,omitempty"`
	Evidence *control.ServerEstimateEvidence `json:"evidence,omitempty"`
}
type EventResult struct {
	Session string  `json:"session_id"`
	Event   string  `json:"event_id"`
	Kind    string  `json:"kind"`
	Ready   float64 `json:"ready_ms"`
	Start   float64 `json:"start_ms"`
	End     float64 `json:"end_ms"`
}
type SessionResult struct {
	ID            string    `json:"session_id"`
	Arrival       float64   `json:"arrival_ms"`
	Ready         float64   `json:"initial_ready_ms"`
	Finish        float64   `json:"finish_ms"`
	InitialQueue  float64   `json:"initial_queue_ms"`
	ResumeQueues  []float64 `json:"resume_queue_ms"`
	SnapshotWaits []float64 `json:"snapshot_readiness_wait_ms"`
}
type AdmissionResult struct {
	Session      string  `json:"session_id"`
	Kind         string  `json:"kind"`
	Requested    float64 `json:"requested_ms"`
	Eligible     float64 `json:"snapshot_ready_for_admission_ms"`
	Attempt      float64 `json:"successful_attempt_ms"`
	Ready        float64 `json:"actor_ready_ms"`
	CapacityWait float64 `json:"capacity_wait_ms"`
	SnapshotWait float64 `json:"snapshot_readiness_wait_ms"`
}
type ServiceStage struct {
	Session string  `json:"session_id"`
	Event   string  `json:"event_id,omitempty"`
	Kind    string  `json:"kind"`
	Start   float64 `json:"start_ms"`
	End     float64 `json:"end_ms"`
}
type Result struct {
	ProcessedEvents        int               `json:"processed_events"`
	EvaluationCount        int               `json:"policy_evaluations"`
	Admissions             []AdmissionResult `json:"admissions"`
	ServiceStages          []ServiceStage    `json:"service_stages"`
	ResearchStatus         string            `json:"research_status"`
	StrategyAxes           map[string]string `json:"strategy_axes"`
	Status                 string            `json:"status"`
	Policy                 string            `json:"policy"`
	TraceID                string            `json:"trace_id"`
	CostID                 string            `json:"cost_id"`
	CostKind               string            `json:"cost_kind"`
	Config                 Config            `json:"config"`
	Offered                int               `json:"offered"`
	Completed              int               `json:"completed"`
	Restores               int               `json:"restores"`
	Checkpoints            int               `json:"checkpoints"`
	TerminalCheckpoints    int               `json:"terminal_checkpoints"`
	DiskDenied             int               `json:"disk_denied"`
	MaxOccupied            int               `json:"max_occupied_slots"`
	ReservedGBSeconds      float64           `json:"actor_reserved_gb_seconds"`
	DiskPeak               float64           `json:"snapshot_allocated_peak_bytes"`
	AllocatedWritten       float64           `json:"snapshot_allocated_bytes_created"`
	Makespan               float64           `json:"makespan_ms"`
	InitialP95             float64           `json:"initial_queue_p95_ms"`
	ResumeP95              float64           `json:"resume_queue_p95_ms"`
	E2EP95                 float64           `json:"session_e2e_p95_ms"`
	Sessions               []SessionResult   `json:"sessions"`
	Events                 []EventResult     `json:"events"`
	Decisions              []Decision        `json:"decisions,omitempty"`
	Evaluations            []Decision        `json:"evaluations,omitempty"`
	Assumptions            []string          `json:"assumptions"`
	ConcurrencyCoordinates map[int]int       `json:"concurrency_coordinate_uses"`
}
type scheduled struct {
	at                         float64
	sequence                   int
	kind                       string
	session, event, generation int
	value                      float64
}
type eventHeap []scheduled

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].sequence < h[j].sequence
	}
	return h[i].at < h[j].at
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(scheduled)) }
func (h *eventHeap) Pop() any     { a := *h; v := a[len(a)-1]; *h = a[:len(a)-1]; return v }

type actor struct {
	setupDone, harnessBusy                                           bool
	proofDone                                                        []bool
	eligibleAt                                                       float64
	epoch                                                            uint64
	accesses                                                         int
	lastAccess                                                       float64
	initialReady                                                     bool
	source                                                           Session
	arrived, resident, ready, checkpointing, snapshotReady, finished bool
	queued                                                           string
	queueAt, waitStarted, lastReady, deadline                        float64
	waiting                                                          bool
	terminalPending                                                  bool
	returnDemand                                                     bool
	returnAt                                                         float64
	currentWait                                                      string
	generation                                                       int
	states                                                           []int
	readyTimes, startTimes, endTimes                                 []float64
	index                                                            map[string]int
	tags                                                             []float64
	result                                                           SessionResult
}
type engine struct {
	eviction                        pressureEvictor
	cfg                             Config
	costs                           Costs
	policy                          OnlinePolicy
	actors                          []*actor
	queue                           []int
	heap                            eventHeap
	now, previous, occupiedMS, disk float64
	occupied, io, sequence          int
	samples                         []WaitSample
	result                          Result
	schedulerTick                   bool
	schedulerPending                bool
}

func Run(t Trace, costs Costs, cfg Config, p OnlinePolicy, history []WaitSample) (Result, error) {
	r, err := run(t, costs, cfg, p, history)
	if err != nil {
		r.Status = "failed"
		r.ResearchStatus = "exploratory"
	}
	return r, err
}

func run(t Trace, costs Costs, cfg Config, p OnlinePolicy, history []WaitSample) (Result, error) {
	if err := t.Validate(); err != nil {
		return Result{}, err
	}
	if cfg.QueueScope == "" {
		cfg.QueueScope = "request_to_admit"
	}
	if cfg.ConcurrencyModel == "" {
		cfg.ConcurrencyModel = "exact"
		if cfg.AllowConcurrencyBands {
			cfg.ConcurrencyModel = "band"
		}
	}
	if cfg.QueueScope != "request_to_admit" && cfg.QueueScope != "capacity_wait" {
		return Result{}, fmt.Errorf("invalid queue scope")
	}
	if cfg.ConcurrencyModel != "exact" && cfg.ConcurrencyModel != "band" && cfg.ConcurrencyModel != "linear" {
		return Result{}, fmt.Errorf("invalid concurrency model")
	}
	if cfg.Environment != nil {
		if err := cfg.Environment.Validate(); err != nil {
			return Result{}, err
		}
		cfg.InitialReadyMS = cfg.Environment.InitialMS
	} else if cfg.HarnessEnabled {
		return Result{}, fmt.Errorf("harness overhead requires a sourced environment model")
	}
	if len(cfg.InitialOrder) > 0 {
		if len(cfg.InitialOrder) != len(t.Sessions) {
			return Result{}, fmt.Errorf("conditioned order requires every session")
		}
		positions := map[string]int{}
		for i, id := range cfg.InitialOrder {
			if _, exists := positions[id]; exists {
				return Result{}, fmt.Errorf("duplicate conditioned order identity")
			}
			positions[id] = i
		}
		t.Sessions = append([]Session(nil), t.Sessions...)
		for _, s := range t.Sessions {
			if _, exists := positions[s.ID]; !exists {
				return Result{}, fmt.Errorf("conditioned order identity missing: %s", s.ID)
			}
		}
		sort.SliceStable(t.Sessions, func(i, j int) bool {
			a, b := t.Sessions[i], t.Sessions[j]
			if a.Arrival == b.Arrival {
				return positions[a.ID] < positions[b.ID]
			}
			return a.Arrival < b.Arrival
		})
	}
	if cfg.ObservationMode == "" {
		cfg.ObservationMode = "c1"
	}
	if cfg.HistoryMode == "" {
		cfg.HistoryMode = "synthetic_shared"
		if cfg.ObservationMode == "native" {
			cfg.HistoryMode = "tenant_profile"
		}
	}
	if cfg.IdleMode == "" {
		cfg.IdleMode = "implicit"
		if cfg.ObservationMode == "native" {
			cfg.IdleMode = "explicit"
		}
	}
	if cfg.IdleTTLMS == 0 {
		cfg.IdleTTLMS = 600000
	}
	if cfg.ObservationMode == "native" && cfg.SchedulerIntervalMS == 0 {
		cfg.SchedulerIntervalMS = 250
	}
	if cfg.ObservationMode == "native" && !cfg.TrainingHistory {
		history = nil
	}
	if cfg.Slots < 1 || cfg.WorkloadBytes < 1 || cfg.ReservationBytes < 1 || cfg.DiskBudgetBytes < 0 || cfg.StableMS < 0 || cfg.MinimumResidenceMS < 0 || cfg.InitialReadyMS < 0 || cfg.GCMS < 0 || cfg.SnapshotKeep < 1 ||
		(cfg.ObservationMode != "c1" && cfg.ObservationMode != "native") ||
		(cfg.HistoryMode != "synthetic_shared" && cfg.HistoryMode != "tenant_profile") ||
		(cfg.IdleMode != "implicit" && cfg.IdleMode != "explicit") || cfg.IdleTTLMS < 1 || cfg.IdleTTLMS > 600000 || cfg.SchedulerIntervalMS < 0 ||
		(cfg.ObservationMode == "native" && cfg.IdleMode != "explicit") {
		return Result{}, fmt.Errorf("invalid simulation config")
	}
	for _, value := range []float64{cfg.DiskBudgetBytes, cfg.StableMS, cfg.MinimumResidenceMS, cfg.InitialReadyMS, cfg.GCMS, cfg.IdleTTLMS, cfg.SchedulerIntervalMS} {
		if !valid(value) {
			return Result{}, fmt.Errorf("non-finite simulation config")
		}
	}
	if cfg.MaxEvents == 0 {
		cfg.MaxEvents = 2_000_000
	}
	e := &engine{cfg: cfg, costs: costs, policy: p, samples: append([]WaitSample(nil), history...)}
	e.retainHistory()
	if v2, ok := p.(*V2ControllerPolicy); ok {
		v2.configure(cfg, e.samples)
	}
	e.result = Result{ResearchStatus: "exploratory", Status: "complete", TraceID: t.TraceID, CostID: costs.ID, CostKind: costs.Kind, Config: cfg, Offered: len(t.Sessions), ConcurrencyCoordinates: map[int]int{},
		Assumptions: []string{"L0 external API durations include historical service/queue; no additional inference queue is added",
			"stateful v2 calls the unmodified Controller; RPC, placement and simulated service timing remain subject to paired native calibration",
			"phase history uses the 64 most recently started waits, excludes the current request, and discloses only past completions/current peer exposure; grouping is configured explicitly",
			"v2-joint is an uncalibrated extension excluded from the primary matrix",
			"initial service time, stable/minimum residence and GC delay are explicit scenario parameters",
			"checkpoint occupies a slot until release; snapshot readiness can occur later; journal overlaps measured stages",
			"costs are point estimates of batch means; device writes are not inferred from snapshot bytes",
			"snapshot inventory retains configured generations and uses explicit delayed GC",
			"concurrent operations use cost coordinates at dispatch; mixed checkpoint/restore contention awaits calibration"}}
	if cfg.ObservationMode == "native" {
		e.result.Assumptions = append(e.result.Assumptions,
			"native observation uses production tenant/profile cohorts, explicit idle TTL and recent 32-observation P75; cold startup costs are 500/500/50 ms",
			"periodic scheduler ticks start at the configured interval after replay time zero; simulated service durations remain cost-table P50",
			"journal measurements are emitted at simulated initial/lifecycle commits; coordinator per-request commit frequency and RPC overhead require conditioned replay")
	}
	if cfg.ConcurrencyModel == "band" {
		e.result.Assumptions = append(e.result.Assumptions, "explicit band model: concurrency 2/3 uses measured coordinate 4; concurrency above 4 is rejected")
	}
	if cfg.ConcurrencyModel == "linear" {
		e.result.Assumptions = append(e.result.Assumptions, "concurrency 2/3 is linear interpolation between measured 1/4 endpoints; interpolation is derived, not directly measured; no extrapolation")
	}
	if len(cfg.InitialOrder) > 0 {
		e.result.Assumptions = append(e.result.Assumptions, "retrospective initial dispatch order conditions simultaneous arrivals on observed placement; this run is calibration, not an independent forecast")
	}
	if b, ok := p.(Baseline); ok {
		e.result.Policy = b.Name
		e.result.StrategyAxes = map[string]string{"active_release": b.Name, "pressure_eviction": "none", "admission": "fifo"}
		if b.Name == "break-even-gdsf" {
			e.eviction.mode = b.Eviction
			if e.eviction.mode == "" {
				e.eviction.mode = "gdsf"
			}
			e.result.StrategyAxes["pressure_eviction"] = e.eviction.mode
		}
	} else {
		e.result.Policy = "custom"
		if _, ok := p.(*V2ControllerPolicy); ok {
			e.result.Policy = "v2"
			e.result.StrategyAxes = map[string]string{"active_release": "stateful_controller_v2", "pressure_eviction": "none", "admission": "fifo"}
		}
	}
	for _, s := range t.Sessions {
		a := &actor{epoch: 1, source: s, states: make([]int, len(s.Events)), proofDone: make([]bool, len(s.Events)), readyTimes: make([]float64, len(s.Events)), startTimes: make([]float64, len(s.Events)), endTimes: make([]float64, len(s.Events)), index: map[string]int{}}
		a.result = SessionResult{ID: s.ID, Arrival: s.Arrival}
		for i, n := range s.Events {
			a.index[n.ID] = i
		}
		e.actors = append(e.actors, a)
		e.push(s.Arrival, "arrival", len(e.actors)-1, 0, 0, 0)
	}
	if cfg.SchedulerIntervalMS > 0 {
		e.push(cfg.SchedulerIntervalMS, "scheduler", 0, 0, 0, 0)
		e.schedulerPending = true
	}
	steps := 0
	for len(e.heap) > 0 {
		at := e.heap[0].at
		e.occupiedMS += (at - e.previous) * float64(e.occupied)
		e.previous = at
		e.now = at
		e.schedulerTick = false
		// Drain simultaneous completions before any decisions at this timestamp.
		for len(e.heap) > 0 && e.heap[0].at == at {
			item := heap.Pop(&e.heap).(scheduled)
			steps++
			if steps > cfg.MaxEvents {
				return e.result, fmt.Errorf("event budget exceeded")
			}
			if err := e.handle(item); err != nil {
				return e.result, err
			}
		}
		if err := e.advance(); err != nil {
			return e.result, err
		}
		if err := e.admit(); err != nil {
			return e.result, err
		}
		// Parked ticks retain the original global polling grid, including work
		// that becomes eligible exactly on a previously idle grid boundary.
		if cfg.ParkInactiveTicks && cfg.SchedulerIntervalMS > 0 && e.now >= cfg.SchedulerIntervalMS &&
			e.now == math.Round(e.now/cfg.SchedulerIntervalMS)*cfg.SchedulerIntervalMS {
			e.schedulerTick = true
		}
		if err := e.decide(); err != nil {
			return e.result, err
		}
		if cfg.ParkInactiveTicks && cfg.SchedulerIntervalMS > 0 {
			if !e.schedulerPending {
				for _, a := range e.actors {
					if a.waiting && a.ready && a.resident && !a.checkpointing && !a.finished {
						e.push((math.Floor(e.now/cfg.SchedulerIntervalMS)+1)*cfg.SchedulerIntervalMS, "scheduler", 0, 0, 0, 0)
						e.schedulerPending = true
						break
					}
				}
			}
		} else if e.schedulerTick {
			for _, a := range e.actors {
				if !a.finished {
					e.push(e.now+cfg.SchedulerIntervalMS, "scheduler", 0, 0, 0, 0)
					break
				}
			}
		}
		if e.occupied < 0 || e.occupied > cfg.Slots || e.disk < -0.01 || e.disk > cfg.DiskBudgetBytes+.01 {
			return e.result, fmt.Errorf("capacity conservation violated")
		}
	}
	e.result.ProcessedEvents = steps
	e.result.ReservedGBSeconds = e.occupiedMS * float64(cfg.ReservationBytes) / 1e12
	var initial, resume, e2e []float64
	for _, a := range e.actors {
		e.result.Sessions = append(e.result.Sessions, a.result)
		if a.finished {
			e.result.Completed++
			initial = append(initial, a.result.InitialQueue)
			e2e = append(e2e, a.result.Finish-a.result.Arrival)
		}
		resume = append(resume, a.result.ResumeQueues...)
		for j, n := range a.source.Events {
			if a.states[j] == 3 {
				e.result.Events = append(e.result.Events, EventResult{a.source.ID, n.ID, n.Kind, a.readyTimes[j], a.startTimes[j], a.endTimes[j]})
			}
		}
	}
	for _, a := range e.actors {
		e.result.Makespan = math.Max(e.result.Makespan, a.result.Finish)
	}
	e.result.InitialP95 = Quantile(initial, .95)
	e.result.ResumeP95 = Quantile(resume, .95)
	e.result.E2EP95 = Quantile(e2e, .95)
	if e.result.Completed != e.result.Offered {
		e.result.Status = "deadlock"
		return e.result, fmt.Errorf("incomplete replay: %d/%d", e.result.Completed, e.result.Offered)
	}
	return e.result, nil
}

func (e *engine) push(at float64, kind string, s, n, g int, value float64) {
	e.sequence++
	heap.Push(&e.heap, scheduled{at, e.sequence, kind, s, n, g, value})
}
func (e *engine) handle(x scheduled) error {
	if x.kind == "scheduler" {
		e.schedulerTick = true
		e.schedulerPending = false
		return nil
	}
	if x.kind == "gc" {
		e.disk -= x.value
		return nil
	}
	a := e.actors[x.session]
	if strings.HasPrefix(x.kind, "metric:") {
		if observer, ok := e.policy.(MeasurementObserver); ok {
			observer.ObserveMeasurement(e.actorState(a), strings.TrimPrefix(x.kind, "metric:"), x.value)
		}
		return nil
	}
	switch x.kind {
	case "arrival":
		a.arrived = true
		e.enqueue(x.session, "initial")
	case "ready":
		e.io -= x.event
		if x.event == 1 {
			a.epoch++
		}
		a.ready = true
		a.returnDemand = false
		a.lastReady = e.now
		if x.event == 0 {
			a.initialReady = true
			a.result.Ready = e.now
			a.setupDone = true
			if h := e.harness(); h.SetupMS > 0 {
				a.setupDone, a.harnessBusy = false, true
				e.stage(a, "", "harness_setup", h.SetupMS)
				e.push(e.now+h.SetupMS, "setup_done", x.session, 0, 0, 0)
			}
		}
	case "setup_done":
		a.setupDone, a.harnessBusy = true, false
	case "proof_done":
		a.proofDone[x.event], a.harnessBusy = true, false
	case "node":
		a.states[x.event] = 3
		a.endTimes[x.event] = e.now
		n := a.source.Events[x.event]
		e.observeEvent(a, x.event, true)
		if n.Kind == "model_wait" || n.Kind == "user_idle" {
			duration, completed := e.now-a.startTimes[x.event], true
			if n.Kind == "user_idle" && e.cfg.IdleMode == "explicit" && duration >= e.cfg.IdleTTLMS {
				duration, completed = e.cfg.IdleTTLMS, false
			}
			e.samples = append(e.samples, WaitSample{ID: a.source.ID + "/" + n.ID, Phase: n.Kind, Tenant: a.source.Tenant, Profile: profile(a.source.Profile), Started: a.startTimes[x.event], Duration: duration, Completed: completed})
			e.retainHistory()
		}
	case "release":
		e.result.ServiceStages = append(e.result.ServiceStages, ServiceStage{Session: a.source.ID, Kind: "slot_release", Start: e.now, End: e.now})
		if a.resident {
			a.resident = false
			a.ready = false
			e.occupied--
		}
	case "durable":
		e.result.ServiceStages = append(e.result.ServiceStages, ServiceStage{Session: a.source.ID, Kind: "snapshot_ready", Start: e.now, End: e.now})
		a.snapshotReady = true
	case "checkpoint_done":
		a.checkpointing = false
		e.io--
	case "tick":
		if x.generation != a.generation {
			return nil
		}
		a.deadline = 0
	}
	return nil
}

func (e *engine) enqueue(i int, kind string) {
	a := e.actors[i]
	if a.queued != "" || a.finished {
		return
	}
	a.queued = kind
	a.queueAt = e.now
	a.eligibleAt = e.now
	if kind == "resume" && e.cfg.ObservationMode == "native" && a.returnDemand {
		a.queueAt = a.returnAt
	}
	e.queue = append(e.queue, i)
}

func (e *engine) actorState(a *actor) ActorState {
	safe, running := !a.harnessBusy, false
	for j, n := range a.source.Events {
		if a.states[j] == 2 {
			running = true
			if n.Kind == "active" || n.Safe == nil || !*n.Safe {
				safe = false
			}
		}
	}
	return ActorState{ID: a.source.ID, Tenant: a.source.Tenant, Profile: profile(a.source.Profile), Now: e.now, Created: a.source.Arrival,
		LastReady: a.lastReady, Epoch: a.epoch, Safe: safe && running, Waiting: a.waiting, Ready: a.ready,
		Resident: a.resident, Checkpointing: a.checkpointing, Finished: a.finished, ReturnDemand: a.queued == "resume" || a.returnDemand}
}

func (e *engine) observeEvent(a *actor, j int, completed bool) {
	if observer, ok := e.policy.(EventObserver); ok {
		n := a.source.Events[j]
		observer.ObserveEvent(EventObservation{Actor: e.actorState(a), ID: n.ID, Phase: n.Kind, Started: a.startTimes[j], At: e.now, Completed: completed})
	}
}

func (e *engine) advance() error {
	for i, a := range e.actors {
		if !a.arrived || a.finished {
			continue
		}
		if a.harnessBusy || a.initialReady && !a.setupDone {
			if a.waiting {
				e.finishWait(a, true)
			}
			if observer, ok := e.policy.(StateObserver); ok {
				observer.Observe(e.actorState(a))
			}
			continue
		}
		need, running, allDone := false, false, true
		for j, n := range a.source.Events {
			if a.states[j] == 3 {
				continue
			}
			allDone = false
			if a.states[j] == 2 {
				running = true
				continue
			}
			parentsDone := true
			for _, p := range n.Parents {
				if a.states[a.index[p]] != 3 {
					parentsDone = false
				}
			}
			if !parentsDone {
				continue
			}
			if a.states[j] == 0 {
				a.states[j] = 1
				a.readyTimes[j] = e.now
			}
			if (n.Kind != "user_idle" || e.cfg.ObservationMode == "native" && !a.initialReady) && (!a.ready || a.checkpointing) {
				need = true
				continue
			}
			if n.Kind == "model_wait" && !a.proofDone[j] && e.harness().ModelProofMS > 0 {
				a.harnessBusy = true
				if a.waiting {
					e.finishWait(a, true)
				}
				e.stage(a, n.ID, "harness_model_proof", e.harness().ModelProofMS)
				e.push(e.now+e.harness().ModelProofMS, "proof_done", i, j, 0, 0)
				break
			}
			a.states[j] = 2
			if n.Kind != "user_idle" {
				a.accesses++
				a.lastAccess = e.now
				if e.eviction.mode != "" {
					c, err := e.cost(a, 1)
					if err != nil {
						return err
					}
					e.eviction.touch(victim{i, a.source.ID, a.accesses, a.lastAccess, c.SnapshotCost + c.Resume + c.Journal, float64(e.cfg.ReservationBytes)})
				}
			}
			a.startTimes[j] = e.now
			e.observeEvent(a, j, false)
			running = true
			duration := *n.Duration
			if n.Kind == "active" && e.harness().ActiveExtraMS > 0 {
				e.result.ServiceStages = append(e.result.ServiceStages, ServiceStage{a.source.ID, n.ID, "harness_active_extra", e.now + duration, e.now + duration + e.harness().ActiveExtraMS})
				duration += e.harness().ActiveExtraMS
			}
			e.push(e.now+duration, "node", i, j, 0, 0)
		}
		if allDone && !a.checkpointing {
			if e.cfg.TerminalCheckpoint && a.resident {
				a.terminalPending = true
				continue
			}
			a.finished = true
			e.eviction.evict(a.source.ID, false)
			a.result.Finish = e.now
			if a.resident {
				e.occupied--
				a.resident = false
				a.ready = false
			}
			if a.waiting {
				e.finishWait(a, false)
			}
			for _, size := range a.tags {
				e.push(e.now+e.cfg.GCMS, "gc", i, 0, 0, size)
			}
			a.tags = nil
			if e.cfg.ObservationMode == "native" {
				if observer, ok := e.policy.(StateObserver); ok {
					observer.Observe(e.actorState(a))
				}
			}
			continue
		}
		if e.cfg.ObservationMode == "native" && need && !a.ready && a.initialReady && !a.returnDemand {
			a.returnDemand, a.returnAt = true, e.now
		}
		if need && !a.resident && !a.checkpointing && a.snapshotReady {
			e.enqueue(i, "resume")
		}
		safe := running && !need && !a.harnessBusy
		event := ""
		for j, n := range a.source.Events {
			if a.states[j] == 2 {
				if n.Kind == "active" || n.Safe == nil || !*n.Safe {
					safe = false
				}
				if event == "" {
					event = n.ID
				}
			}
		}
		if safe && !a.waiting {
			a.waiting = true
			a.waitStarted = e.now
			a.currentWait = event
			a.generation++
			a.deadline = 0
		}
		if !safe && a.waiting {
			e.finishWait(a, true)
		}
		if observer, ok := e.policy.(StateObserver); ok {
			state := e.actorState(a)
			state.Safe = safe
			observer.Observe(state)
		}
	}
	return nil
}

func (e *engine) finishWait(a *actor, completed bool) {
	a.waiting = false
	a.generation++
	a.deadline = 0
}

func (e *engine) cost(a *actor, concurrency int) (LifecycleCost, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	if e.cfg.ConcurrencyModel == "band" && (concurrency == 2 || concurrency == 3) {
		concurrency = 4
	}
	bytes := e.cfg.WorkloadBytes
	if a.source.Workload != nil {
		bytes = *a.source.Workload
	}
	c, err := e.costs.Lookup(bytes, concurrency)
	if e.cfg.ConcurrencyModel == "linear" {
		c, err = e.costs.Interpolate(bytes, concurrency)
	}
	if err == nil {
		e.result.ConcurrencyCoordinates[concurrency]++
	}
	return c, err
}

func (e *engine) admit() error {
	// All dispatches in this timestamp share the observed concurrent wave.
	waveIO := e.io
	for _, i := range e.queue[:min(len(e.queue), e.cfg.Slots-e.occupied)] {
		if e.actors[i].queued == "resume" {
			waveIO++
		}
	}
	waveIO = min(4, waveIO)
	// FIFO spans initial and resume demands; readiness is owned by this engine.
	for len(e.queue) > 0 && e.occupied < e.cfg.Slots {
		i := e.queue[0]
		a := e.actors[i]
		if a.queued == "resume" && e.io >= 4 {
			break
		}
		e.queue = e.queue[1:]
		kind := a.queued
		a.queued = ""
		a.resident = true
		e.occupied++
		e.result.MaxOccupied = max(e.result.MaxOccupied, e.occupied)
		if kind == "initial" {
			a.result.InitialQueue = e.now - a.queueAt
			e.result.Admissions = append(e.result.Admissions, AdmissionResult{a.source.ID, kind, a.queueAt, a.eligibleAt, e.now, e.now + e.cfg.InitialReadyMS, e.now - a.eligibleAt, 0})
			e.stage(a, "", "initial_activation", e.cfg.InitialReadyMS)
			e.push(e.now+e.cfg.InitialReadyMS, "ready", i, 0, 0, 0)
			if e.cfg.ObservationMode == "native" {
				c, err := e.cost(a, 1)
				if err != nil {
					return err
				}
				e.push(e.now+e.cfg.InitialReadyMS+c.Journal, "metric:journal_commit_ms", i, 0, 0, c.Journal)
			}
		} else {
			wait := e.now - a.queueAt
			if e.cfg.QueueScope == "capacity_wait" {
				wait = e.now - a.eligibleAt
			}
			a.result.ResumeQueues = append(a.result.ResumeQueues, wait)
			a.result.SnapshotWaits = append(a.result.SnapshotWaits, a.eligibleAt-a.queueAt)
			c, err := e.cost(a, waveIO)
			if err != nil {
				return err
			}
			e.io++
			e.result.Restores++
			e.result.Admissions = append(e.result.Admissions, AdmissionResult{a.source.ID, kind, a.queueAt, a.eligibleAt, e.now, e.now + c.Resume, e.now - a.eligibleAt, a.eligibleAt - a.queueAt})
			e.stage(a, "", "resume", c.Resume)
			e.push(e.now+c.Resume, "ready", i, 1, 0, 0)
			if e.cfg.ObservationMode == "native" {
				e.push(e.now+c.Resume, "metric:resume_ready_ms", i, 0, 0, c.Resume)
				e.push(e.now+c.Resume+c.Journal, "metric:journal_commit_ms", i, 0, 0, c.Journal)
			}
		}
		if e.cfg.ObservationMode == "native" {
			if observer, ok := e.policy.(StateObserver); ok {
				observer.Observe(e.actorState(a))
			}
		}
	}
	return nil
}

func (e *engine) harness() HarnessOverhead {
	if e.cfg.HarnessEnabled && e.cfg.Environment != nil {
		return e.cfg.Environment.Harness
	}
	return HarnessOverhead{}
}

func (e *engine) stage(a *actor, event, kind string, duration float64) {
	e.result.ServiceStages = append(e.result.ServiceStages, ServiceStage{a.source.ID, event, kind, e.now, e.now + duration})
}

func (e *engine) decide() error {
	initN, resumeN := 0, 0
	initAge, resumeAge := 0.0, 0.0
	for _, i := range e.queue {
		a := e.actors[i]
		if a.queued == "initial" {
			initN++
			initAge = math.Max(initAge, e.now-a.queueAt)
		} else {
			resumeN++
			resumeAge = math.Max(resumeAge, e.now-a.queueAt)
		}
	}
	type pending struct {
		index  int
		action Action
		age    float64
	}
	var suspends []pending
	for i, a := range e.actors {
		if a.terminalPending && !a.checkpointing {
			suspends = append(suspends, pending{i, Action{Suspend: true, Reason: "terminal_checkpoint"}, 0})
			continue
		}
		if e.cfg.SchedulerIntervalMS > 0 && !e.schedulerTick {
			continue
		}
		if !a.waiting || !a.ready || !a.resident || a.checkpointing || a.finished {
			continue
		}
		c, err := e.cost(a, 1)
		if err != nil {
			return err
		}
		phase := "model_wait"
		phaseStart, event := a.waitStarted, a.currentWait
		for j, n := range a.source.Events {
			if a.states[j] == 2 {
				phase = n.Kind
				phaseStart, event = a.startTimes[j], n.ID
				break
			}
		}
		if phase == "user_idle" && e.cfg.IdleMode == "explicit" && e.now-phaseStart >= e.cfg.IdleTTLMS {
			if _, isV2 := e.policy.(*V2ControllerPolicy); !isV2 {
				continue
			}
		}
		samples := e.waitSamples(a, phase, event)
		observedEvent := a.currentWait
		if e.cfg.ObservationMode == "native" || e.cfg.IdleMode == "explicit" {
			observedEvent = event
		}
		o := Observation{Now: e.now, WaitStarted: a.waitStarted, Earliest: math.Max(a.waitStarted+e.cfg.StableMS, a.lastReady+e.cfg.MinimumResidenceMS), Age: e.now - a.waitStarted, SessionID: a.source.ID, EventID: observedEvent, Phase: phase,
			PhaseStarted: phaseStart, PhaseAge: e.now - phaseStart,
			InitialQueue: initN, ResumeQueue: resumeN, InitialOldestMS: initAge, ResumeOldestMS: resumeAge, Slots: e.cfg.Slots, Occupied: e.occupied,
			SwitchCostMS: c.SnapshotCost + c.Resume + c.Journal, SnapshotMS: c.SnapshotCost, ResumeMS: c.Resume, JournalMS: c.Journal, Samples: samples}
		d := e.policy.Decide(o)
		e.result.EvaluationCount++
		if d.Error != "" {
			return fmt.Errorf("policy: %s", d.Error)
		}
		if e.cfg.ObservationMode == "native" && !e.cfg.DiscardEvaluations {
			action := "KEEP"
			if d.Suspend {
				action = "SUSPEND"
			}
			e.result.Evaluations = append(e.result.Evaluations, Decision{At: e.now, Session: a.source.ID, Event: event, Age: o.Age, Action: action, Reason: d.Reason, Deadline: d.RevisitAt, Episode: d.Episode, Evidence: d.Evidence})
		}
		if d.Suspend {
			suspends = append(suspends, pending{i, d, o.Age})
		} else if e.cfg.SchedulerIntervalMS == 0 && d.RevisitAt > e.now && (a.deadline == 0 || d.RevisitAt < a.deadline) {
			a.deadline = d.RevisitAt
			e.push(d.RevisitAt, "tick", i, 0, a.generation, 0)
		}
	}
	if e.eviction.mode != "" && e.io < 4 && len(e.queue) > 0 && e.occupied >= e.cfg.Slots && len(suspends) == 0 {
		var choices []victim
		for i, a := range e.actors {
			if !a.waiting || !a.ready || !a.resident || a.checkpointing || a.finished {
				continue
			}
			eligible := math.Max(a.waitStarted+e.cfg.StableMS, a.lastReady+e.cfg.MinimumResidenceMS)
			if e.now < eligible {
				if a.deadline == 0 || eligible < a.deadline {
					a.deadline = eligible
					e.push(eligible, "tick", i, 0, a.generation, 0)
				}
				continue
			}
			c, err := e.cost(a, 1)
			if err != nil {
				return err
			}
			if e.disk+c.Bytes > e.cfg.DiskBudgetBytes {
				continue
			}
			choices = append(choices, victim{i, a.source.ID, a.accesses, a.lastAccess, c.SnapshotCost + c.Resume + c.Journal, float64(e.cfg.ReservationBytes)})
		}
		if i := e.eviction.choose(choices); i >= 0 {
			a := e.actors[i]
			suspends = append(suspends, pending{i, Action{Suspend: true, Reason: "pressure_" + e.eviction.mode}, e.now - a.waitStarted})
		}
	}
	waveIO := min(4, e.io+len(suspends))
	for _, item := range suspends {
		i := item.index
		a := e.actors[i]

		if e.io >= 4 {
			break
		}
		c, err := e.cost(a, waveIO)
		if err != nil {
			return err
		}
		if e.disk+c.Bytes > e.cfg.DiskBudgetBytes {
			e.result.DiskDenied++
			continue
		}
		e.result.Decisions = append(e.result.Decisions, Decision{At: e.now, Session: a.source.ID, Event: a.currentWait, Age: item.age, Action: "SUSPEND", Reason: item.action.Reason, Deadline: item.action.RevisitAt, Episode: item.action.Episode})
		if a.terminalPending {
			e.result.TerminalCheckpoints++
			a.terminalPending = false
		}
		e.eviction.evict(a.source.ID, item.action.Reason == "pressure_gdsf")
		a.ready = false
		a.checkpointing = true
		a.snapshotReady = false
		a.generation++
		a.deadline = 0
		e.io++
		e.result.Checkpoints++
		e.disk += c.Bytes
		e.result.AllocatedWritten += c.Bytes
		e.result.DiskPeak = math.Max(e.result.DiskPeak, e.disk)
		a.tags = append(a.tags, c.Bytes)
		if len(a.tags) > e.cfg.SnapshotKeep {
			size := a.tags[0]
			a.tags = a.tags[1:]
			e.push(e.now+math.Max(c.Checkpoint, c.SnapshotReady)+e.cfg.GCMS, "gc", i, 0, 0, size)
		}
		e.push(e.now+c.Checkpoint, "release", i, 0, 0, 0)
		e.push(e.now+c.SnapshotReady, "durable", i, 0, 0, 0)
		completion := math.Max(c.Checkpoint, c.SnapshotReady)
		if e.cfg.ObservationMode == "native" {
			completion = math.Max(completion, c.SnapshotCost)
			e.push(e.now+completion, "metric:checkpoint_total_ms", i, 0, 0, completion)
			e.push(e.now+completion+c.Journal, "metric:journal_commit_ms", i, 0, 0, c.Journal)
		}
		e.push(e.now+completion, "checkpoint_done", i, 0, 0, 0)
	}
	return nil
}

func Quantile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	v = append([]float64(nil), v...)
	sort.Float64s(v)
	pos := float64(len(v)-1) * p
	lo := int(pos)
	hi := min(lo+1, len(v)-1)
	return v[lo] + (v[hi]-v[lo])*(pos-float64(lo))
}
