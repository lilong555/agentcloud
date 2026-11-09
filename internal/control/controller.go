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
	"github.com/lilong555/agentcloud/internal/policy"
)

type Config struct {
	DataDir                  string
	DatabaseURL              string
	Backend                  Backend
	Signals                  SignalSink
	ModelURL                 string
	ModelAPIKey              string
	ModelConcurrency         int
	ModelTimeout             time.Duration
	Tokens                   map[string]string
	MaxSessions              int
	MaxRequests              int
	Scheduling               policy.LifecycleConfig
	QueueObserver            QueueObserver
	EnableExperimentControls bool
}
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string                    { return e.Message }
func problem(status int, code, message string) error { return &APIError{code, message, status} }

type Controller struct {
	mu         sync.Mutex
	cfg        Config
	store      *journal.Store
	state      State
	ctx        context.Context
	cancel     context.CancelFunc
	workers    sync.WaitGroup
	operations sync.WaitGroup
	closing    bool
	signalMu   sync.Mutex
	unhealthy  error
}

func New(ctx context.Context, cfg Config) (*Controller, error) {
	if cfg.Backend == nil {
		return nil, errors.New("execution backend is required")
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 256
	}
	if cfg.MaxRequests == 0 {
		cfg.MaxRequests = 4096
	}
	if cfg.ModelConcurrency == 0 {
		cfg.ModelConcurrency = 4
	}
	if cfg.ModelTimeout == 0 {
		cfg.ModelTimeout = 120 * time.Second
	}
	if cfg.ModelConcurrency < 1 || cfg.ModelConcurrency > 64 {
		return nil, errors.New("model concurrency must be 1..64")
	}
	store, err := journal.Open(ctx, cfg.DataDir+"/journal.json", cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	c := &Controller{cfg: cfg, store: store}
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
	if err = c.persist(); err != nil {
		store.Close()
		return nil, err
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
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.cancel()
	c.workers.Wait()
	c.operations.Wait()
	return c.store.Close()
}
func (c *Controller) persist() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.store.Save(ctx, c.state); err != nil {
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
	c.state.EventSequence++
	c.state.Events = append(c.state.Events, Event{c.state.EventSequence, time.Now().UTC(), s.TenantID, s.SessionID, kind, detail})
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
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	return copyValue(s), nil
}
func (c *Controller) List(tenant string) []*Session {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
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
	if err := c.persist(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	actor, err := c.cfg.Backend.Ensure(ctx, snapshot)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		s.State = "RECOVERY_REQUIRED"
		c.event(s, "session.create_unknown", err.Error())
		_ = c.persist()
		return nil, problem(502, "create_unknown", err.Error())
	}
	s.Actor = actor
	s.ActorUID = actor.UID
	s.ActualFidelity = actor.ActualFidelity
	s.State = "RUNNING"
	s.RunningSince = time.Now().UTC()
	s.Gate = "OPEN"
	c.event(s, "session.running", "")
	if err = c.persist(); err != nil {
		return nil, err
	}
	return copyValue(s), nil
}

func (c *Controller) Tool(ctx context.Context, tenant, id string, input ToolRequest) (*ToolReceipt, error) {
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
	c.mu.Lock()
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
		c.mu.Unlock()
		return out, nil
	}
	if s.Epoch != input.Epoch {
		c.mu.Unlock()
		return nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	if s.Gate != "OPEN" || s.State != "RUNNING" {
		c.mu.Unlock()
		return nil, problem(423, "admission_closed", "workspace admission is closed")
	}
	if len(s.Tools) >= 4096 {
		c.mu.Unlock()
		return nil, problem(429, "tool_quota", "tool receipt capacity reached")
	}
	s.NextReceipt++
	receipt := &ToolReceipt{ID: input.ID, Epoch: input.Epoch, Status: "RUNNING", PayloadHash: hash, Sequence: s.NextReceipt, StartedAt: time.Now().UTC(), Deadline: time.Now().UTC().Add(time.Duration(input.TimeoutMS) * time.Millisecond)}
	s.Tools[input.ID] = receipt
	s.ActiveTools++
	c.event(s, "tool.started", input.ID)
	if err = c.persist(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	c.signal(snapshot, "HEAVY_TOOL")
	workCtx, cancel := context.WithTimeout(c.ctx, time.Duration(input.TimeoutMS)*time.Millisecond)
	result, toolErr := c.cfg.Backend.Tool(workCtx, snapshot, input)
	cancel()
	c.mu.Lock()
	s.ActiveTools--
	receipt.Result = result
	now := time.Now().UTC()
	receipt.FinishedAt = &now
	if toolErr != nil {
		receipt.Status = "OUTCOME_UNKNOWN"
		receipt.Error = toolErr.Error()
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
	err = c.persist()
	out := copyValue(receipt)
	snapshot = copyValue(s)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	c.signal(snapshot, "RUNNING_FAST")
	return out, nil
}

func (c *Controller) Lifecycle(ctx context.Context, tenant, id, action string, input LifecycleRequest) (*Session, *Manifest, error) {
	return c.lifecycle(ctx, tenant, id, action, input, false)
}
func (c *Controller) lifecycle(ctx context.Context, tenant, id, action string, input LifecycleRequest, reservedBoundary bool) (*Session, *Manifest, error) {
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
	c.mu.Lock()
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
		if prior.Status != "COMPLETED" {
			c.mu.Unlock()
			return nil, nil, problem(409, "lifecycle_unknown", "lifecycle operation requires reconciliation")
		}
		out, manifest := copyValue(s), copyValue(prior.Manifest)
		c.mu.Unlock()
		return out, manifest, nil
	}
	if s.Epoch != input.ExpectedEpoch {
		c.mu.Unlock()
		return nil, nil, problem(409, "stale_epoch", "execution epoch differs")
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
	prepared := s.State == "CHECKPOINTED"
	receipt := &LifecycleReceipt{OperationID: input.OperationID, Action: action, Epoch: s.Epoch, PayloadHash: hash, Status: "RUNNING"}
	s.Lifecycle[input.OperationID] = receipt
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
	if err = c.persist(); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	if action == "resume" {
		c.signal(snapshot, "RESUMING")
	}
	for action != "resume" {
		c.mu.Lock()
		active := s.ActiveTools
		unknown := false
		for _, r := range s.Tools {
			if r.Status == "OUTCOME_UNKNOWN" {
				unknown = true
			}
		}
		c.mu.Unlock()
		if unknown {
			err = errors.New("unknown tool result prevents a safe point")
			break
		}
		if active == 0 {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
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
		c.mu.Lock()
		snapshot = copyValue(s)
		c.mu.Unlock()
		switch action {
		case "checkpoint":
			actor, digest, err = c.cfg.Backend.Checkpoint(ctx, snapshot, input.OperationID)
		case "suspend":
			if prepared {
				actor = snapshot.Actor
				digest = snapshot.Manifest.WorkspaceDigest
			} else {
				actor, digest, err = c.cfg.Backend.Checkpoint(ctx, snapshot, input.OperationID)
			}
			if err == nil {
				snapshot.Actor = actor
				actor, err = c.cfg.Backend.Suspend(ctx, snapshot)
			}
		case "resume":
			if snapshot.Manifest == nil || snapshot.Manifest.State != "COMMITTED" || snapshot.Manifest.ActorUID != snapshot.Actor.UID || snapshot.Manifest.ReceiptWatermark != snapshot.ReceiptWatermark || snapshot.Manifest.Epoch != snapshot.Epoch || snapshot.Manifest.SnapshotUUID != snapshot.Actor.SnapshotUUID || snapshot.Manifest.SnapshotGeneration != snapshot.Actor.SnapshotGeneration {
				err = errors.New("committed manifest does not match Actor and receipt watermark")
			} else {
				actor, err = c.cfg.Backend.Resume(ctx, snapshot)
			}
		default:
			err = errors.New("unknown lifecycle action")
		}
	}
	c.mu.Lock()
	if err != nil {
		receipt.Status = "OUTCOME_UNKNOWN"
		receipt.Error = err.Error()
		s.State = "RECOVERY_REQUIRED"
		s.Gate = "CLOSED"
		c.event(s, "lifecycle.unknown", err.Error())
		_ = c.persist()
		c.mu.Unlock()
		return nil, nil, problem(409, "recovery_required", err.Error())
	}
	s.Actor = actor
	s.ActorUID = actor.UID
	s.ActualFidelity = actor.ActualFidelity
	if action == "resume" {
		s.Epoch++
		s.RunningSince = time.Now().UTC()
		s.LastResumedAt = s.RunningSince
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
	c.event(s, "lifecycle."+action+".completed", input.OperationID)
	err = c.persist()
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
	c.signal(out, state)
	return out, manifest, nil
}

func (c *Controller) signal(session *Session, state string) {
	c.signalMu.Lock()
	defer c.signalMu.Unlock()
	if c.cfg.Signals == nil {
		return
	}
	c.mu.Lock()
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
	snapshot := copyValue(live)
	err := c.persist()
	c.mu.Unlock()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
	defer cancel()
	err = c.cfg.Signals.Send(ctx, snapshot, state)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.event(live, "signal.pending", fmt.Sprintf("%s: %v", state, err))
	} else {
		c.event(live, "signal.applied", state)
	}
	_ = c.persist()
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
		c.mu.Lock()
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
