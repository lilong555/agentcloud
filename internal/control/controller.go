package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lilong555/agentcloud/internal/journal"
	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/policy"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	QuiesceDrainTimeout       time.Duration
	QuiesceCooldown           time.Duration
	EnableAutonomousScheduler bool
	SchedulerInterval         time.Duration
	SchedulerStableFor        time.Duration
	SchedulerIdlePrior        time.Duration
	SchedulerPolicy           string
	SchedulerShadow           bool
	SchedulerConcurrency      int
	CapacityObserver          CapacityObserver
	Observability             *observability.Runtime
	DataDir                   string
	DatabaseURL               string
	Backend                   Backend
	Signals                   SignalSink
	ModelURL                  string
	ModelAPIKey               string
	ModelConcurrency          int
	ModelTimeout              time.Duration
	Tokens                    map[string]string
	MaxSessions               int
	MaxRequests               int
	Scheduling                policy.LifecycleConfig
	QueueObserver             QueueObserver
	EnableExperimentControls  bool
	SnapshotGC                SnapshotGCConfig
}
type APIError struct {
	Code    string
	Message string
	Status  int
	Details map[string]any
}

func (e *APIError) Error() string { return e.Message }
func problem(status int, code, message string) error {
	return &APIError{Code: code, Message: message, Status: status}
}

type Controller struct {
	dirty               map[string]map[string]bool
	pendingEvents       map[string]Event
	pendingObservations map[string]RecordedMeasurement
	commitCond          *sync.Cond
	committing          bool
	mu                  sync.Mutex
	cfg                 Config
	store               *journal.Store
	state               State
	ctx                 context.Context
	cancel              context.CancelFunc
	workers             sync.WaitGroup
	operations          sync.WaitGroup
	closing             bool
	telemetry           *observability.Runtime
	ownTelemetry        bool
	stranded            map[string]*strandedInterval
	schedulerBusy       map[string]bool
	signalLocks         keyedMutex[string]
	unhealthy           error
	snapshotGCWake      chan struct{}
}

func New(ctx context.Context, cfg Config) (*Controller, error) {
	if cfg.Backend == nil {
		return nil, errors.New("execution backend is required")
	}
	if err := cfg.SnapshotGC.defaults(); err != nil {
		return nil, err
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 256
	}
	if cfg.QuiesceDrainTimeout <= 0 {
		cfg.QuiesceDrainTimeout = 30 * time.Second
	}
	if cfg.QuiesceCooldown <= 0 {
		cfg.QuiesceCooldown = 30 * time.Second
	}
	if cfg.MaxRequests == 0 {
		cfg.MaxRequests = 4096
	}
	if cfg.ModelConcurrency == 0 {
		cfg.ModelConcurrency = 4
	}
	if cfg.SchedulerInterval <= 0 {
		cfg.SchedulerInterval = 250 * time.Millisecond
	}
	if cfg.SchedulerStableFor <= 0 {
		cfg.SchedulerStableFor = 500 * time.Millisecond
	}
	if cfg.SchedulerConcurrency <= 0 {
		cfg.SchedulerConcurrency = 4
	}
	if cfg.ModelTimeout == 0 {
		cfg.ModelTimeout = 120 * time.Second
	}
	if cfg.SchedulerPolicy == "" {
		cfg.SchedulerPolicy = "observed"
	}
	if cfg.SchedulerPolicy != "observed" && cfg.SchedulerPolicy != "cost-aware" && cfg.SchedulerPolicy != "agentcloud-v2" {
		return nil, errors.New("scheduler policy must be observed, cost-aware or agentcloud-v2")
	}
	if cfg.SchedulerShadow && (!cfg.EnableExperimentControls || !cfg.EnableAutonomousScheduler) {
		return nil, errors.New("scheduler shadow requires experiment controls and autonomous scheduler")
	}
	if cfg.ModelConcurrency < 1 || cfg.ModelConcurrency > 64 {
		return nil, errors.New("model concurrency must be 1..64")
	}
	store, err := journal.Open(ctx, cfg.DataDir+"/journal.json", cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	telemetry := cfg.Observability
	ownTelemetry := telemetry == nil
	if telemetry == nil {
		telemetry, err = observability.New(ctx, observability.Config{})
		if err != nil {
			store.Close()
			return nil, err
		}
	}
	c := &Controller{cfg: cfg, store: store, telemetry: telemetry, ownTelemetry: ownTelemetry, stranded: map[string]*strandedInterval{}, schedulerBusy: map[string]bool{}}
	c.commitCond = sync.NewCond(&c.mu)
	c.ctx, c.cancel = context.WithCancel(ctx)
	if err = store.Load(&c.state); err != nil {
		store.Close()
		return nil, err
	}
	if c.state.Sessions == nil {
		c.state.Sessions = map[string]*Session{}
	}
	if c.state.Requests == nil {
		c.state.Requests = map[string]*ModelRequest{}
	}
	c.state.Version = 1
	for _, s := range c.state.Sessions {
		if s.Backend != cfg.Backend.Name() {
			store.Close()
			return nil, errors.New("journal execution backend differs from configuration")
		}
		ReconcileScheduling(s)
		if s.Tools == nil {
			s.Tools = map[string]*ToolReceipt{}
		}
		if s.Lifecycle == nil {
			s.Lifecycle = map[string]*LifecycleReceipt{}
		}
		for _, receipt := range s.Tools {
			if receipt.Status == "RUNNING" {
				receipt.Status = "OUTCOME_UNKNOWN"
				receipt.Error = "executor restarted before a terminal receipt"
				s.State = "RECOVERY_REQUIRED"
				s.Gate = "CLOSED"
			}
		}
		s.ActiveTools = 0
		if s.State == "QUIESCING" || s.State == "SUSPENDING" || s.State == "RESUMING" || s.State == "CREATING" {
			s.State = "RECOVERY_REQUIRED"
			s.Gate = "CLOSED"
		}
		for _, r := range s.Lifecycle {
			if r.Status == "RUNNING" {
				r.Status = "OUTCOME_UNKNOWN"
				r.Error = "lifecycle completion requires upstream reconciliation"
			}
		}
	}
	for _, r := range c.state.Requests {
		if r.Status == "DISPATCHING" {
			r.Status = "OUTCOME_UNKNOWN"
			r.Error = "dispatcher restarted after durable dispatch intent"
			r.UpdatedAt = time.Now().UTC()
		}
	}
	c.lock()
	err = c.persist()
	c.mu.Unlock()
	if err != nil {
		store.Close()
		return nil, err
	}
	c.registerGauges()
	c.startSnapshotGC()
	c.workers.Add(1)
	go c.strandingLoop()
	if cfg.EnableAutonomousScheduler {
		c.workers.Add(1)
		go c.autonomousLoop()
	}
	for i := 0; i < cfg.ModelConcurrency; i++ {
		c.workers.Add(1)
		go c.dispatchLoop()
	}
	if cfg.Signals != nil {
		c.workers.Add(1)
		go c.feedbackLoop()
	}
	return c, nil
}
func (c *Controller) Close() error {
	c.lock()
	c.closing = true
	c.mu.Unlock()
	c.cancel()
	c.workers.Wait()
	c.operations.Wait()
	c.lock()
	for _, s := range c.state.Sessions {
		c.updateStrandingLocked(s, time.Now().UTC())
	}
	_ = c.persist()
	c.mu.Unlock()
	err := c.store.Close()
	if c.ownTelemetry {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := c.telemetry.Close(ctx); err == nil {
			err = e
		}
	}
	return err
}
func (c *Controller) persist() error { c.markAll(); return c.persistContext(c.ctx) }
func (c *Controller) persistContext(parent context.Context) error {
	parent, span := observability.Start(observability.WithRuntime(parent, c.telemetry), "journal.commit", trace.SpanKindInternal)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if c.store.SupportsEntities() {
		changes, prepareErr := c.entityChanges()
		if prepareErr != nil {
			observability.Finish(span, prepareErr)
			return prepareErr
		}
		c.committing = true
		c.mu.Unlock()
		err = c.store.SaveEntities(ctx, changes)
		c.mu.Lock()
		c.committing = false
		c.commitCond.Broadcast()
	} else {
		data, prepareErr := json.Marshal(c.state)
		if prepareErr != nil {
			observability.Finish(span, prepareErr)
			return prepareErr
		}
		c.committing = true
		c.mu.Unlock()
		err = c.store.Save(ctx, json.RawMessage(data))
		c.mu.Lock()
		c.committing = false
		c.commitCond.Broadcast()
	}
	if err == nil {
		c.dirty = map[string]map[string]bool{}
		c.pendingEvents = map[string]Event{}
		c.pendingObservations = map[string]RecordedMeasurement{}
	}
	c.observeLocked(parent, "journal_commit_ms", float64(time.Since(started))/float64(time.Millisecond), "journal_"+c.store.Backend())
	observability.Finish(span, err)
	if err != nil {
		c.unhealthy = err
		return err
	}
	return nil
}
func (c *Controller) ready() error {
	if c.closing {
		return problem(503, "shutting_down", "coordinator is shutting down")
	}
	if c.unhealthy != nil {
		return problem(503, "journal_unavailable", "journal reconciliation requires restart")
	}
	return nil
}
func key(tenant, id string) string { return tenant + "/" + id }
func validID(value string) bool {
	if len(value) < 1 || len(value) > 128 || value == "." || value == ".." {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:-", r)) {
			return false
		}
	}
	return true
}
func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}
func payloadHash(value any) string {
	data, _ := json.Marshal(value)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func copyValue[T any](value T) T {
	data, _ := json.Marshal(value)
	var out T
	_ = json.Unmarshal(data, &out)
	return out
}
func (c *Controller) event(s *Session, kind, detail string) {
	c.updateStrandingLocked(s, time.Now().UTC())
	c.markSession(s)
	sessionKey := key(s.TenantID, s.SessionID)
	if strings.HasPrefix(kind, "model.") && c.state.Requests[key(s.TenantID, detail)] != nil {
		c.markEntity("requests", key(s.TenantID, detail))
	}
	if strings.HasPrefix(kind, "tool.") && s.Tools[detail] != nil {
		c.markEntity("tools", sessionKey+"/"+detail)
	}
	if strings.HasPrefix(kind, "lifecycle.") && s.Lifecycle[detail] != nil {
		c.markEntity("lifecycle", sessionKey+"/"+detail)
	}
	if strings.HasPrefix(kind, "scheduling.") && s.Scheduling[detail] != nil {
		c.markEntity("scheduling", sessionKey+"/"+detail)
	}
	c.state.EventSequence++
	event := Event{Sequence: c.state.EventSequence, At: time.Now().UTC(), TenantID: s.TenantID, SessionID: s.SessionID, Kind: kind, Detail: detail, State: s.State, Gate: s.Gate, Epoch: s.Epoch}
	if s.Manifest != nil {
		event.ManifestID = s.Manifest.ID
	}
	c.state.Events = append(c.state.Events, event)
	eventKey := fmt.Sprintf("%020d", event.Sequence)
	c.markEntity("events", eventKey)
	if c.pendingEvents == nil {
		c.pendingEvents = map[string]Event{}
	}
	c.pendingEvents[eventKey] = event
	if len(c.state.Events) > 8192 {
		c.state.Events = c.state.Events[len(c.state.Events)-8192:]
	}
	s.UpdatedAt = time.Now().UTC()
}
func (c *Controller) find(tenant, id string) (*Session, error) {
	s := c.state.Sessions[key(tenant, id)]
	if s == nil {
		return nil, problem(404, "session_not_found", "session not found")
	}
	return s, nil
}
func (c *Controller) Get(tenant, id string) (*Session, error) {
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	return copyValue(s), nil
}
func (c *Controller) List(tenant string) []*Session {
	c.lock()
	defer c.mu.Unlock()
	if c.ready() != nil {
		return []*Session{}
	}
	out := []*Session{}
	for _, s := range c.state.Sessions {
		if s.TenantID == tenant {
			out = append(out, copyValue(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}
func (c *Controller) Create(ctx context.Context, tenant string, input CreateSession) (*Session, error) {
	ctx = c.operationContext(ctx, tenant, input.SessionID, input.SessionID)
	ctx, span := observability.Start(ctx, "coordinator.create", trace.SpanKindInternal)
	defer span.End()
	if input.AblationGroup != "" {
		if len(input.AblationGroup) != 1 || input.AblationGroup < "A" || input.AblationGroup > "E" {
			return nil, problem(400, "invalid_ablation_group", "ablation_group must be A..E")
		}
		if !c.cfg.EnableExperimentControls {
			return nil, problem(403, "experiment_controls_disabled", "explicit ablation groups require enabled experiment controls")
		}
	}
	if !validID(tenant) || !validID(input.SessionID) || !validID(input.WorkspaceID) {
		return nil, problem(400, "invalid_identity", "session and workspace identifiers are required")
	}
	if input.Mode == "" {
		input.Mode = "external"
	}
	if input.Mode != "external" && input.Mode != "in-actor" {
		return nil, problem(400, "invalid_mode", "mode must be external or in-actor")
	}
	c.lock()
	if err := c.ready(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if existing := c.state.Sessions[key(tenant, input.SessionID)]; existing != nil {
		if existing.WorkspaceID != input.WorkspaceID || existing.Mode != input.Mode || existing.AblationGroup != input.AblationGroup {
			c.mu.Unlock()
			return nil, problem(409, "identity_conflict", "session is bound to another workspace, mode or ablation group")
		}
		out := copyValue(existing)
		c.mu.Unlock()
		return out, nil
	}
	if len(c.state.Sessions) >= c.cfg.MaxSessions {
		c.mu.Unlock()
		return nil, problem(429, "session_quota", "session capacity reached")
	}
	for _, s := range c.state.Sessions {
		if s.TenantID == tenant && s.WorkspaceID == input.WorkspaceID {
			c.mu.Unlock()
			return nil, problem(409, "workspace_busy", "workspace already has an execution session")
		}
	}
	now := time.Now().UTC()
	s := &Session{TenantID: tenant, WorkspaceID: input.WorkspaceID, SessionID: input.SessionID, Mode: input.Mode, Profile: input.Profile, AblationGroup: input.AblationGroup, State: "CREATING", Gate: "CLOSED", Epoch: 1, Backend: c.cfg.Backend.Name(), Actor: Actor{Name: input.ActorName}, Tools: map[string]*ToolReceipt{}, Lifecycle: map[string]*LifecycleReceipt{}, CreatedAt: now, UpdatedAt: now}
	c.state.Sessions[key(tenant, input.SessionID)] = s
	c.event(s, "session.creating", "")
	if err := c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	actor, err := c.cfg.Backend.Ensure(ctx, snapshot)
	c.lock()
	defer c.mu.Unlock()
	if err != nil {
		s.State = "RECOVERY_REQUIRED"
		c.event(s, "session.create_unknown", err.Error())
		_ = c.persistContext(ctx)
		return nil, problem(502, "create_unknown", err.Error())
	}
	s.Actor = actor
	c.observeAllocationMissing(s)
	s.ActorUID = actor.UID
	s.ActualFidelity = actor.ActualFidelity
	s.State = "RUNNING"
	s.IdleSince = time.Now().UTC()
	s.RunningSince = time.Now().UTC()
	s.Gate = "OPEN"
	c.event(s, "session.running", "")
	if err = c.persistContext(ctx); err != nil {
		return nil, err
	}
	return copyValue(s), nil
}

func (c *Controller) Tool(ctx context.Context, tenant, id string, input ToolRequest) (*ToolReceipt, error) {
	ctx = c.operationContext(ctx, tenant, id, input.ID)
	ctx, span := observability.Start(ctx, "coordinator.tool", trace.SpanKindInternal)
	defer span.End()
	if !validID(input.ID) {
		return nil, problem(400, "invalid_operation", "tool_op_id is required")
	}
	if input.TimeoutMS == 0 {
		input.TimeoutMS = 30000
	}
	if input.TimeoutMS < 1 || input.TimeoutMS > 300000 {
		return nil, problem(400, "invalid_timeout", "timeout_ms must be 1..300000")
	}
	if input.Kind != "file_read" && input.Kind != "file_write" && input.Kind != "shell" {
		return nil, problem(400, "invalid_tool", "unknown tool kind")
	}
	if input.Kind == "file_read" || input.Kind == "file_write" {
		if !filepath.IsLocal(input.Path) || input.Path == "." || strings.Contains(input.Path, "\\") {
			return nil, problem(400, "invalid_path", "a confined relative file path is required")
		}
		for _, part := range strings.Split(input.Path, "/") {
			if part == ".." || strings.HasPrefix(part, ".agentcloud") {
				return nil, problem(400, "invalid_path", "path component is reserved")
			}
		}
	}
	if len(input.Content) > 1<<20 {
		return nil, problem(413, "tool_too_large", "file content exceeds 1 MiB")
	}
	business := input
	business.Epoch = 0
	hash := payloadHash(business)
	c.lock()
	if err := c.ready(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if receipt := s.Tools[input.ID]; receipt != nil {
		if receipt.PayloadHash != hash {
			c.mu.Unlock()
			return nil, problem(409, "payload_conflict", "operation ID has another payload")
		}
		out := copyValue(receipt)
		if out.Status == "OUTCOME_UNKNOWN" {
			out.Recovery = unknownTools(s)
		}
		c.mu.Unlock()
		return out, nil
	}
	if s.Epoch != input.Epoch {
		c.mu.Unlock()
		return nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	if unknownTools(s) != nil && input.Kind != "file_read" {
		err := unknownToolProblem(s)
		c.mu.Unlock()
		return nil, err
	}
	inspection := input.Kind == "file_read" && toolInspectionAllowed(s)
	if (s.Gate != "OPEN" || s.State != "RUNNING") && !inspection {
		if c.cfg.EnableAutonomousScheduler && s.State != "RECOVERY_REQUIRED" {
			if _, e := c.requestReturnLocked(s, LifecycleRequest{OperationID: input.ID, ExpectedEpoch: input.Epoch}); e == nil {
				_ = c.persistContext(ctx)
			}
		}
		c.mu.Unlock()
		return nil, problem(423, "admission_closed", "workspace admission is closed; explicit return demand can schedule resume")
	}
	if len(s.Tools) >= 4096 {
		c.mu.Unlock()
		return nil, problem(429, "tool_quota", "tool receipt capacity reached")
	}
	s.NextReceipt++
	receipt := &ToolReceipt{ID: input.ID, Epoch: input.Epoch, Status: "RUNNING", PayloadHash: hash, Sequence: s.NextReceipt, StartedAt: time.Now().UTC(), Deadline: time.Now().UTC().Add(time.Duration(input.TimeoutMS) * time.Millisecond)}
	s.Tools[input.ID] = receipt
	if !s.IdleSince.IsZero() {
		explicit := false
		for _, window := range s.IdleWindows {
			if window.FinishedAt == nil && time.Now().Before(window.ExpiresAt) {
				explicit = true
				break
			}
		}
		if !explicit {
			c.observeLocked(ctx, "idle_until_tool_ms", float64(time.Since(s.IdleSince))/float64(time.Millisecond), "model_terminal_or_last_tool_to_next_tool_admission")
		}
	}
	s.IdleSince = time.Time{}
	resetReleasePlan(s)
	if s.Autonomous != nil {
		s.Autonomous.ReturnDemand = false
		for _, demand := range s.ReturnDemands {
			if demand.Status == "PENDING" {
				demand.Status = "COMPLETED"
				demand.ResultEpoch = s.Epoch
			}
		}
	}
	s.ActiveTools++
	c.event(s, "tool.started", input.ID)
	if err = c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	c.signalContext(ctx, snapshot, "HEAVY_TOOL")
	workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(input.TimeoutMS)*time.Millisecond)
	stopShutdown := context.AfterFunc(c.ctx, cancel)
	defer stopShutdown()
	result, toolErr := c.cfg.Backend.Tool(workCtx, snapshot, input)
	cancel()
	c.lock()
	s.ActiveTools--
	if s.ActiveTools == 0 {
		pending := false
		for _, r := range c.state.Requests {
			if r.TenantID == s.TenantID && r.SessionID == s.SessionID && (r.Status == "PENDING" || r.Status == "DISPATCHING") {
				pending = true
				break
			}
		}
		if !pending {
			s.IdleSince = time.Now().UTC()
		}
	}
	receipt.Result = result
	now := time.Now().UTC()
	receipt.FinishedAt = &now
	if toolErr != nil {
		receipt.Status = "OUTCOME_UNKNOWN"
		receipt.Error = toolErr.Error()
		if hint := runtimeUnknownHint(toolErr); hint != nil {
			if s.ToolRecovery == nil {
				s.ToolRecovery = hint
			} else {
				s.ToolRecovery.UnknownOperations = append(s.ToolRecovery.UnknownOperations, hint.UnknownOperations...)
			}
		}
		s.ToolRecovery = unknownTools(s)
		receipt.Recovery = copyValue(s.ToolRecovery)
		s.State = "RECOVERY_REQUIRED"
		s.Gate = "CLOSED"
	} else {
		receipt.Status = "COMPLETED"
	}
	for next := s.ReceiptWatermark + 1; next <= s.NextReceipt; next++ {
		found := false
		for _, r := range s.Tools {
			if r.Sequence == next && r.Status == "COMPLETED" {
				found = true
				break
			}
		}
		if !found {
			break
		}
		s.ReceiptWatermark = next
	}
	c.event(s, "tool."+strings.ToLower(receipt.Status), input.ID)
	err = c.persistContext(ctx)
	out := copyValue(receipt)
	snapshot = copyValue(s)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	c.signalContext(ctx, snapshot, "RUNNING_FAST")
	return out, nil
}

func (c *Controller) Lifecycle(ctx context.Context, tenant, id, action string, input LifecycleRequest) (*Session, *Manifest, error) {
	return c.lifecycle(ctx, tenant, id, action, input, false)
}
func (c *Controller) lifecycle(ctx context.Context, tenant, id, action string, input LifecycleRequest, reservedBoundary bool) (*Session, *Manifest, error) {
	ctx = c.operationContext(ctx, tenant, id, input.OperationID)
	ctx, span := observability.Start(ctx, "coordinator.lifecycle", trace.SpanKindInternal)
	defer span.End()
	if action != "checkpoint" && action != "suspend" && action != "resume" {
		return nil, nil, problem(400, "invalid_action", "unknown lifecycle action")
	}
	if !validID(input.OperationID) {
		return nil, nil, problem(400, "invalid_operation", "operation_id is required")
	}
	hash := payloadHash(struct {
		Action  string
		Request LifecycleRequest
	}{action, input})
	c.lock()
	if err := c.ready(); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	for operation, decision := range s.Scheduling {
		if reservedBoundary && operation == input.OperationID && decision.Status == "RUNNING" {
			continue
		}
		if operation == input.OperationID {
			c.mu.Unlock()
			return nil, nil, problem(409, "payload_conflict", "operation ID belongs to a scheduling decision")
		}
		if decision.Status == "RUNNING" || decision.Status == "OUTCOME_UNKNOWN" {
			c.mu.Unlock()
			return nil, nil, problem(409, "lifecycle_busy", "scheduling operation requires completion or reconciliation")
		}
	}
	if reservedBoundary && (s.Scheduling[input.OperationID] == nil || s.Scheduling[input.OperationID].Status != "RUNNING") {
		c.mu.Unlock()
		return nil, nil, problem(409, "lifecycle_busy", "scheduling reservation is unavailable")
	}
	if prior := s.Lifecycle[input.OperationID]; prior != nil {
		if prior.PayloadHash != hash {
			c.mu.Unlock()
			return nil, nil, problem(409, "payload_conflict", "operation ID has another payload")
		}
		if prior.Status == "COMPLETED" {
			out, manifest := copyValue(s), copyValue(prior.Manifest)
			c.mu.Unlock()
			return out, manifest, nil
		}
		if prior.Status == "ABORTED" {
			err := quiesceProblem(prior.ErrorCode, prior.Error, prior.RetryAfter, s.ToolRecovery)
			c.mu.Unlock()
			return nil, nil, err
		}
		if action != "resume" || prior.Status != "WAITING_CAPACITY" {
			c.mu.Unlock()
			return nil, nil, problem(409, "lifecycle_unknown", "lifecycle operation requires reconciliation")
		}
	}
	if s.Epoch != input.ExpectedEpoch {
		c.mu.Unlock()
		return nil, nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	if action != "resume" && unknownTools(s) != nil {
		err := unknownToolProblem(s)
		c.mu.Unlock()
		return nil, nil, err
	}
	if action != "resume" && time.Now().Before(s.QuiesceRetryAfter) {
		err := quiesceProblem("quiesce_cooldown", "safe point drain is cooling down", s.QuiesceRetryAfter, nil)
		c.mu.Unlock()
		return nil, nil, err
	}
	if s.State == "RECOVERY_REQUIRED" || s.State == "CREATING" || s.State == "QUIESCING" || s.State == "SUSPENDING" || s.State == "RESUMING" {
		c.mu.Unlock()
		return nil, nil, problem(409, "lifecycle_busy", "unresolved lifecycle or tool outcome")
	}
	if action == "checkpoint" && s.State != "RUNNING" || action == "suspend" && s.State != "RUNNING" && s.State != "CHECKPOINTED" {
		c.mu.Unlock()
		return nil, nil, problem(409, "invalid_state", "operation requires a running session")
	}
	if action == "resume" && s.State != "SUSPENDED" && s.State != "CHECKPOINTED" {
		c.mu.Unlock()
		return nil, nil, problem(409, "invalid_state", "resume requires a suspended session")
	}
	if prior := s.Lifecycle[input.OperationID]; prior != nil && time.Now().Before(prior.RetryAfter) {
		c.mu.Unlock()
		return nil, nil, problem(409, "resume_capacity_wait", "resume is waiting for Worker capacity; retry the same operation after retry_after")
	}
	previousState, previousBarrier := s.State, s.BarrierGeneration
	prepared := s.State == "CHECKPOINTED"
	receipt := s.Lifecycle[input.OperationID]
	if receipt == nil {
		receipt = &LifecycleReceipt{OperationID: input.OperationID, Action: action, Epoch: s.Epoch, PayloadHash: hash}
	}
	receipt.Status, receipt.Error, receipt.CapacityWait = "RUNNING", "", nil
	receipt.AttemptCount++
	receipt.LastAttemptAt, receipt.RetryAfter = time.Now().UTC(), time.Time{}
	s.Lifecycle[input.OperationID] = receipt
	if action != "resume" && !prepared {
		c.recordSnapshotIntentLocked(s, input.OperationID)
	}
	s.Gate = "CLOSED"
	if !prepared {
		s.BarrierGeneration++
	}
	if action == "resume" {
		s.State = "RESUMING"
	} else {
		s.State = "QUIESCING"
	}
	c.event(s, "lifecycle."+action+".started", input.OperationID)
	if err = c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	if action == "resume" {
		c.signalContext(ctx, snapshot, "RESUMING")
	}
	drainDeadline := time.Now().Add(c.cfg.QuiesceDrainTimeout)
	waitedForTools := false
	for action != "resume" {
		c.lock()
		active := s.ActiveTools
		unknown := unknownTools(s)
		c.mu.Unlock()
		if unknown != nil {
			err = &quiesceRejected{code: "OUTCOME_UNKNOWN", recovery: unknown}
			break
		}
		waitedForTools = waitedForTools || active > 0
		if waitedForTools && !time.Now().Before(drainDeadline) {
			err = &quiesceRejected{code: "DRAIN_TIMEOUT"}
			break
		}
		if active == 0 {
			break
		}
		select {
		case <-ctx.Done():
			err = &quiesceRejected{code: "DRAIN_CANCELLED"}
		case <-time.After(10 * time.Millisecond):
		}
		if err != nil {
			break
		}
	}
	var actor Actor
	var digest string
	var manifest *Manifest
	if err == nil {
		c.lock()
		snapshot = copyValue(s)
		c.mu.Unlock()
		switch action {
		case "checkpoint":
			started := time.Now()
			actor, digest, err = c.cfg.Backend.Checkpoint(ctx, snapshot, input.OperationID)
			observability.Observe(ctx, "checkpoint_total_ms", float64(time.Since(started))/float64(time.Millisecond), "backend_checkpoint_completion")
		case "suspend":
			if prepared {
				actor = snapshot.Actor
				digest = snapshot.Manifest.WorkspaceDigest
			} else {
				started := time.Now()
				actor, digest, err = c.cfg.Backend.Checkpoint(ctx, snapshot, input.OperationID)
				observability.Observe(ctx, "checkpoint_total_ms", float64(time.Since(started))/float64(time.Millisecond), "backend_checkpoint_completion")
			}
			if err == nil {
				snapshot.Actor = actor
				actor, err = c.cfg.Backend.Suspend(ctx, snapshot)
			}
		case "resume":
			if snapshot.Manifest == nil || snapshot.Manifest.State != "COMMITTED" || snapshot.Manifest.ActorUID != snapshot.Actor.UID || snapshot.Manifest.ReceiptWatermark != snapshot.ReceiptWatermark || snapshot.Manifest.Epoch != snapshot.Epoch || snapshot.Manifest.SnapshotUUID != snapshot.Actor.SnapshotUUID || snapshot.Manifest.SnapshotGeneration != snapshot.Actor.SnapshotGeneration {
				err = errors.New("committed manifest does not match Actor and receipt watermark")
			} else {
				started := time.Now()
				actor, err = c.cfg.Backend.Resume(ctx, snapshot)
				if err == nil {
					observability.Observe(ctx, "resume_ready_ms", float64(time.Since(started))/float64(time.Millisecond), "backend_resume_attach_completion")
				}
			}
		default:
			err = errors.New("unknown lifecycle action")
		}
	}
	c.lock()
	if err != nil {
		if rejected := asQuiesceRejection(err); action != "resume" && !prepared && rejected != nil {
			abortErr := c.abortQuiesceLocked(ctx, s, receipt, rejected)
			c.mu.Unlock()
			return nil, nil, abortErr
		}
		var waiting *resumeCapacityWaitError
		if action == "resume" && errors.As(err, &waiting) && waiting.proof.matches(snapshot) {
			receipt.Status, receipt.Error = "WAITING_CAPACITY", err.Error()
			receipt.CapacityWait = copyValue(&waiting.proof)
			receipt.RetryAfter = time.Now().UTC().Add(resumeCapacityRetryDelay)
			s.State, s.Gate, s.BarrierGeneration = previousState, "CLOSED", previousBarrier
			c.event(s, "lifecycle.resume.waiting_capacity", input.OperationID)
			persistErr := c.persistContext(ctx)
			out := copyValue(s)
			c.mu.Unlock()
			if persistErr != nil {
				return nil, nil, persistErr
			}
			c.signalContext(ctx, out, "SUSPENDED")
			return nil, nil, problem(409, "resume_capacity_wait", "resume was rejected before Actor mutation; retry the same operation after retry_after")
		}
		receipt.Status = "OUTCOME_UNKNOWN"
		c.markEntity("lifecycle", key(tenant, id)+"/"+input.OperationID)
		receipt.Error = err.Error()
		s.State = "RECOVERY_REQUIRED"
		s.Gate = "CLOSED"
		c.event(s, "lifecycle.unknown", err.Error())
		_ = c.persistContext(ctx)
		c.mu.Unlock()
		return nil, nil, problem(409, "recovery_required", err.Error())
	}
	s.Actor = actor
	c.observeAllocationMissing(s)
	s.ActorUID = actor.UID
	s.ActualFidelity = actor.ActualFidelity
	if action == "resume" {
		s.Epoch++
		if c.schedulerPolicy() == "agentcloud-v2" {
			resetReleasePlan(s)
		}
		s.RunningSince = time.Now().UTC()
		s.LastResumedAt = s.RunningSince
		s.IdleSince = time.Time{}
		s.State = "RUNNING"
		s.Gate = "OPEN"
	} else {
		manifest = &Manifest{ID: newID(), State: "COMMITTED", Epoch: s.Epoch, BarrierGeneration: s.BarrierGeneration, ReceiptWatermark: s.ReceiptWatermark, ActorUID: actor.UID, SnapshotUUID: actor.SnapshotUUID, SnapshotGeneration: actor.SnapshotGeneration, WorkspaceDigest: digest, Fidelity: actor.ActualFidelity, CheckpointID: input.CheckpointID, RetainedTag: actor.RetainedTag, RetainedTagUID: actor.RetainedTagUID, CreatedAt: time.Now().UTC()}
		if prepared {
			manifest = copyValue(s.Manifest)
		}
		s.Manifest = manifest
		if action == "suspend" {
			s.State = "SUSPENDED"
			s.Gate = "CLOSED"
		} else {
			s.State = "CHECKPOINTED"
			s.Gate = "CLOSED"
		}
	}
	receipt.Status = "COMPLETED"
	receipt.Manifest = manifest
	c.recordCommittedSnapshotLocked(s, input.OperationID, manifest)
	c.event(s, "lifecycle."+action+".completed", input.OperationID)
	err = c.persistContext(ctx)
	out := copyValue(s)
	manifest = copyValue(manifest)
	c.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	state := "RUNNING_FAST"
	if action == "suspend" {
		state = "SUSPENDED"
	}
	c.signalContext(ctx, out, state)
	c.wakeSnapshotGC()
	return out, manifest, nil
}

func (c *Controller) signal(session *Session, state string) { c.signalContext(c.ctx, session, state) }
func (c *Controller) signalContext(parent context.Context, session *Session, state string) {
	parent = c.operationContext(parent, session.TenantID, session.SessionID, "signal")
	parent, span := observability.Start(parent, "coordinator.execution_feedback", trace.SpanKindInternal)
	defer span.End()
	unlock := c.signalLocks.lock(key(session.TenantID, session.SessionID))
	defer unlock()
	if c.cfg.Signals == nil {
		return
	}
	c.lock()
	live := c.state.Sessions[key(session.TenantID, session.SessionID)]
	if live == nil || live.AblationGroup != "" && live.AblationGroup != "E" {
		c.mu.Unlock()
		return
	}
	// Derive the state under the lock so a delayed completion cannot overwrite a newer lifecycle.
	switch live.State {
	case "SUSPENDED":
		state = "SUSPENDED"
	case "RESUMING":
		state = "RESUMING"
	case "CHECKPOINTED", "QUIESCING", "SUSPENDING", "RECOVERY_REQUIRED":
		state = "PARKED"
	default:
		if live.ActiveTools > 0 {
			state = "HEAVY_TOOL"
		} else {
			state = "RUNNING_FAST"
		}
	}
	live.SignalSequence++
	c.markSession(live)
	snapshot := copyValue(live)
	err := c.persistContext(parent)
	c.mu.Unlock()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	err = c.cfg.Signals.Send(ctx, snapshot, state)
	c.lock()
	defer c.mu.Unlock()
	if err != nil {
		c.event(live, "signal.pending", fmt.Sprintf("%s: %v", state, err))
	} else {
		c.event(live, "signal.applied", state)
	}
	_ = c.persistContext(parent)
}

func (c *Controller) feedbackLoop() {
	defer c.workers.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		c.lock()
		var sessions []*Session
		for _, s := range c.state.Sessions {
			if s.Actor.UID != "" {
				sessions = append(sessions, copyValue(s))
			}
		}
		c.mu.Unlock()
		for _, s := range sessions {
			if c.ctx.Err() != nil {
				return
			}
			c.signal(s, "")
		}
	}
}
