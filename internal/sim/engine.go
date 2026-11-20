package sim

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
)

type Config struct {
	Slots                 int     `json:"slots"`
	WorkloadBytes         int64   `json:"workload_bytes"`
	ReservationBytes      int64   `json:"reservation_bytes"`
	DiskBudgetBytes       float64 `json:"disk_budget_bytes"`
	StableMS              float64 `json:"stable_ms"`
	MinimumResidenceMS    float64 `json:"minimum_residence_ms"`
	InitialReadyMS        float64 `json:"initial_ready_ms"`
	GCMS                  float64 `json:"gc_ms"`
	SnapshotKeep          int     `json:"snapshot_keep"`
	AllowConcurrencyBands bool    `json:"allow_concurrency_bands"`
	MaxEvents             int     `json:"max_events"`
}
type Decision struct {
	At      float64 `json:"at_ms"`
	Session string  `json:"session_id"`
	Event   string  `json:"event_id"`
	Age     float64 `json:"wait_age_ms"`
	Action  string  `json:"action"`
	Reason  string  `json:"reason"`
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
	ID           string    `json:"session_id"`
	Arrival      float64   `json:"arrival_ms"`
	Ready        float64   `json:"initial_ready_ms"`
	Finish       float64   `json:"finish_ms"`
	InitialQueue float64   `json:"initial_queue_ms"`
	ResumeQueues []float64 `json:"resume_queue_ms"`
}
type Result struct {
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
	epoch                                                            uint64
	accesses                                                         int
	lastAccess                                                       float64
	source                                                           Session
	arrived, resident, ready, checkpointing, snapshotReady, finished bool
	queued                                                           string
	queueAt, waitStarted, lastReady, deadline                        float64
	waiting                                                          bool
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
	if cfg.Slots < 1 || cfg.WorkloadBytes < 1 || cfg.ReservationBytes < 1 || cfg.DiskBudgetBytes < 0 || cfg.StableMS < 0 || cfg.MinimumResidenceMS < 0 || cfg.InitialReadyMS < 0 || cfg.GCMS < 0 || cfg.SnapshotKeep < 1 {
		return Result{}, fmt.Errorf("invalid simulation config")
	}
	if cfg.MaxEvents == 0 {
		cfg.MaxEvents = 2_000_000
	}
	e := &engine{cfg: cfg, costs: costs, policy: p, samples: append([]WaitSample(nil), history...)}
	e.retainHistory()
	e.result = Result{ResearchStatus: "exploratory", Status: "complete", TraceID: t.TraceID, CostID: costs.ID, CostKind: costs.Kind, Config: cfg, Offered: len(t.Sessions), ConcurrencyCoordinates: map[int]int{},
		Assumptions: []string{"L0 external API durations include historical service/queue; no additional inference queue is added",
			"stateful v2 calls the unmodified Controller; observation reconstruction, native interval, RPC and placement await C2 calibration",
			"phase-specific history uses the 64 most recently started waits, excludes the current request, and discloses only past completions/current peer exposure; one synthetic tenant/profile",
			"v2-joint is an uncalibrated extension excluded from the primary matrix",
			"initial service time, stable/minimum residence and GC delay are explicit scenario parameters",
			"checkpoint occupies a slot until release; snapshot readiness can occur later; journal overlaps measured stages",
			"costs are point estimates of batch means; device writes are not inferred from snapshot bytes",
			"snapshot inventory retains configured generations and uses explicit delayed GC",
			"concurrent operations use cost coordinates at dispatch; mixed checkpoint/restore contention awaits calibration"}}
	if cfg.AllowConcurrencyBands {
		e.result.Assumptions = append(e.result.Assumptions, "explicit band model: concurrency 2/3 uses measured coordinate 4; concurrency above 4 is rejected")
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
		a := &actor{epoch: 1, source: s, states: make([]int, len(s.Events)), readyTimes: make([]float64, len(s.Events)), startTimes: make([]float64, len(s.Events)), endTimes: make([]float64, len(s.Events)), index: map[string]int{}}
		a.result = SessionResult{ID: s.ID, Arrival: s.Arrival}
		for i, n := range s.Events {
			a.index[n.ID] = i
		}
		e.actors = append(e.actors, a)
		e.push(s.Arrival, "arrival", len(e.actors)-1, 0, 0, 0)
	}
	steps := 0
	for len(e.heap) > 0 {
		at := e.heap[0].at
		e.occupiedMS += (at - e.previous) * float64(e.occupied)
		e.previous = at
		e.now = at
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
		if err := e.decide(); err != nil {
			return e.result, err
		}
		if e.occupied < 0 || e.occupied > cfg.Slots || e.disk < -0.01 || e.disk > cfg.DiskBudgetBytes+.01 {
			return e.result, fmt.Errorf("capacity conservation violated")
		}
	}
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
	if x.kind == "gc" {
		e.disk -= x.value
		return nil
	}
	a := e.actors[x.session]
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
		a.lastReady = e.now
		if x.event == 0 {
			a.result.Ready = e.now
		}
	case "node":
		a.states[x.event] = 3
		a.endTimes[x.event] = e.now
		n := a.source.Events[x.event]
		if n.Kind == "model_wait" || n.Kind == "user_idle" {
			e.samples = append(e.samples, WaitSample{a.source.ID + "/" + n.ID, n.Kind, a.startTimes[x.event], e.now - a.startTimes[x.event], true})
			e.retainHistory()
		}
	case "release":
		if a.resident {
			a.resident = false
			a.ready = false
			e.occupied--
		}
	case "durable":
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
	e.queue = append(e.queue, i)
}

func (e *engine) advance() error {
	for i, a := range e.actors {
		if !a.arrived || a.finished {
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
			if n.Kind != "user_idle" && (!a.ready || a.checkpointing) {
				need = true
				continue
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
			running = true
			e.push(e.now+*n.Duration, "node", i, j, 0, 0)
		}
		if allDone && !a.checkpointing {
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
			continue
		}
		if need && !a.resident && !a.checkpointing && a.snapshotReady {
			e.enqueue(i, "resume")
		}
		safe := running && !need
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
			observer.Observe(ActorState{ID: a.source.ID, Now: e.now, Created: a.source.Arrival, LastReady: a.lastReady, Epoch: a.epoch, Safe: safe, Waiting: a.waiting, Ready: a.ready, ReturnDemand: a.queued == "resume"})
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
	if e.cfg.AllowConcurrencyBands && (concurrency == 2 || concurrency == 3) {
		concurrency = 4
	}
	bytes := e.cfg.WorkloadBytes
	if a.source.Workload != nil {
		bytes = *a.source.Workload
	}
	c, err := e.costs.Lookup(bytes, concurrency)
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
			e.push(e.now+e.cfg.InitialReadyMS, "ready", i, 0, 0, 0)
		} else {
			a.result.ResumeQueues = append(a.result.ResumeQueues, e.now-a.queueAt)
			c, err := e.cost(a, waveIO)
			if err != nil {
				return err
			}
			e.io++
			e.result.Restores++
			e.push(e.now+c.Resume, "ready", i, 1, 0, 0)
		}
	}
	return nil
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
		samples := e.waitSamples(a, phase, event)
		o := Observation{Now: e.now, WaitStarted: a.waitStarted, Earliest: math.Max(a.waitStarted+e.cfg.StableMS, a.lastReady+e.cfg.MinimumResidenceMS), Age: e.now - a.waitStarted, SessionID: a.source.ID, EventID: a.currentWait, Phase: phase,
			PhaseStarted: phaseStart, PhaseAge: e.now - phaseStart,
			InitialQueue: initN, ResumeQueue: resumeN, InitialOldestMS: initAge, ResumeOldestMS: resumeAge, Slots: e.cfg.Slots,
			SwitchCostMS: c.SnapshotCost + c.Resume + c.Journal, SnapshotMS: c.SnapshotCost, ResumeMS: c.Resume, JournalMS: c.Journal, Samples: samples}
		d := e.policy.Decide(o)
		if d.Error != "" {
			return fmt.Errorf("policy: %s", d.Error)
		}
		if d.Suspend {
			suspends = append(suspends, pending{i, d, o.Age})
		} else if d.RevisitAt > e.now && (a.deadline == 0 || d.RevisitAt < a.deadline) {
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
		e.result.Decisions = append(e.result.Decisions, Decision{e.now, a.source.ID, a.currentWait, item.age, "SUSPEND", item.action.Reason})
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
		e.push(e.now+math.Max(c.Checkpoint, c.SnapshotReady), "checkpoint_done", i, 0, 0, 0)
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
