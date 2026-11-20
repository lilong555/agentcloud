package control

import (
	"context"
	"errors"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"google.golang.org/grpc/status"
)

// AdmissionDemand belongs to the durable control plane. Backend capacity
// refusals, rather than a configured slot count, determine admission waits.
type AdmissionDemand struct {
	ID              string                `json:"id"`
	Kind            string                `json:"kind"`
	Source          string                `json:"source"`
	TenantID        string                `json:"tenant_id"`
	SessionID       string                `json:"session_id"`
	Epoch           uint64                `json:"epoch"`
	OperationID     string                `json:"operation_id"`
	Status          string                `json:"status"`
	RequestedAt     time.Time             `json:"requested_at"`
	ReceivedAt      time.Time             `json:"received_at"`
	ClientSentAtNS  string                `json:"client_sent_at_ns,omitempty"`
	AttemptCount    uint64                `json:"attempt_count"`
	LastAttemptAt   time.Time             `json:"last_attempt_at,omitempty"`
	RetryAfter      time.Time             `json:"retry_after,omitempty"`
	WakePending     bool                  `json:"wake_pending,omitempty"`
	RetryTrigger    string                `json:"retry_trigger,omitempty"`
	ReleaseSequence uint64                `json:"release_sequence,omitempty"`
	WaitingSince    time.Time             `json:"waiting_since,omitempty"`
	CapacityWaitMS  float64               `json:"capacity_wait_ms"`
	WaitAgeMS       float64               `json:"wait_age_ms"`
	BackendMS       float64               `json:"backend_ms"`
	ReadyAt         *time.Time            `json:"ready_at,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	RPCMethod       string                `json:"rpc_method,omitempty"`
	RPCDurationMS   float64               `json:"rpc_duration_ms"`
	RPCCode         string                `json:"rpc_code,omitempty"`
	InitialActor    *substrateapi.Summary `json:"initial_actor,omitempty"`
}

type arrivalKey struct{}
type requestArrival struct {
	received time.Time
	clientNS string
}

func (c *Controller) newAdmissionLocked(ctx context.Context, s *Session, kind, id, operation string) *AdmissionDemand {
	if s.Admissions == nil {
		s.Admissions = map[string]*AdmissionDemand{}
	}
	if existing := s.Admissions[id]; existing != nil {
		return existing
	}
	now := time.Now().UTC()
	arrival, ok := ctx.Value(arrivalKey{}).(requestArrival)
	if !ok {
		arrival.received = now
	}
	d := &AdmissionDemand{ID: id, Kind: kind, Source: "coordinator_admission", TenantID: s.TenantID,
		SessionID: s.SessionID, Epoch: s.Epoch, OperationID: operation, Status: "PENDING",
		RequestedAt: arrival.received, ReceivedAt: arrival.received, ClientSentAtNS: arrival.clientNS}
	s.Admissions[id] = d
	c.admissionEventLocked(s, d, "request_received")
	c.admissionEventLocked(s, d, "demand_persisted")
	return d
}

func (c *Controller) resumeAdmissionLocked(ctx context.Context, s *Session, operation string) *AdmissionDemand {
	for _, d := range s.Admissions {
		if d.Kind == "resume_admission" && d.Epoch == s.Epoch && d.Status != "READY" && d.Status != "OUTCOME_UNKNOWN" {
			return d
		}
	}
	return c.newAdmissionLocked(ctx, s, "resume_admission", "resume-"+operation, operation)
}

func (c *Controller) admissionEventLocked(s *Session, d *AdmissionDemand, kind string) {
	if d != nil {
		d.WaitAgeMS = float64(time.Since(d.RequestedAt)) / float64(time.Millisecond)
	}
	c.event(s, "admission."+kind, func() string {
		if d != nil {
			return d.ID
		}
		return ""
	}())
}

func (c *Controller) startAdmissionLocked(s *Session, d *AdmissionDemand) {
	now := time.Now().UTC()
	if !d.WaitingSince.IsZero() {
		d.CapacityWaitMS += float64(now.Sub(d.WaitingSince)) / float64(time.Millisecond)
		d.WaitingSince = time.Time{}
		c.admissionEventLocked(s, d, "waiting_capacity_exited")
	}
	d.Status, d.Reason = "ATTEMPTING", ""
	if !d.WakePending && d.RetryTrigger == "slot_release" {
		d.RetryTrigger, d.ReleaseSequence = "direct_retry", 0
	}
	d.WakePending = false
	if d.RetryTrigger == "" {
		d.RetryTrigger = "request"
	}
	d.AttemptCount++
	d.LastAttemptAt, d.RetryAfter = now, time.Time{}
	c.admissionEventLocked(s, d, "attempt_started")
}

func (c *Controller) wakeAdmission() {
	select {
	case c.admissionWake <- struct{}{}:
	default:
	}
}

func activeAdmission(s *Session) *AdmissionDemand {
	if s == nil || !pendingAdmission(s) {
		return nil
	}
	for _, d := range s.Admissions {
		if d.Epoch == s.Epoch && (d.Status == "PENDING" || d.Status == "WAITING_CAPACITY") {
			return d
		}
	}
	return nil
}

// One committed release wakes the oldest durable demand. It is an opportunity
// to ask Substrate, not a reservation or a statement about physical capacity.
func (c *Controller) notifyCapacityReleaseLocked(sequence uint64) {
	var selected *Session
	var oldest *AdmissionDemand
	for _, s := range c.state.Sessions {
		d := activeAdmission(s)
		if d == nil || d.WakePending {
			continue
		}
		if oldest == nil || d.RequestedAt.Before(oldest.RequestedAt) ||
			d.RequestedAt.Equal(oldest.RequestedAt) && key(s.TenantID, s.SessionID) < key(selected.TenantID, selected.SessionID) {
			selected, oldest = s, d
		}
	}
	if oldest == nil {
		return
	}
	oldest.WakePending, oldest.RetryTrigger, oldest.ReleaseSequence = true, "slot_release", sequence
	oldest.RetryAfter = time.Time{}
	for _, r := range selected.Lifecycle {
		if r.Action == "resume" && r.Epoch == selected.Epoch && r.Status == "WAITING_CAPACITY" {
			r.RetryAfter = time.Time{}
			c.markEntity("lifecycle", key(selected.TenantID, selected.SessionID)+"/"+r.OperationID)
		}
	}
	c.admissionEventLocked(selected, oldest, "retry_notified")
}

func (c *Controller) waitAdmissionLocked(s *Session, d *AdmissionDemand, err error) {
	d.Status, d.Reason = "WAITING_CAPACITY", err.Error()
	d.WaitingSince = time.Now().UTC()
	d.RetryAfter = d.WaitingSince.Add(resumeCapacityRetryDelay)
	c.admissionEventLocked(s, d, "capacity_rejected")
	c.admissionEventLocked(s, d, "waiting_capacity_entered")
}

func (c *Controller) readyAdmissionLocked(s *Session, d *AdmissionDemand) {
	now := time.Now().UTC()
	d.Status, d.ReadyAt, d.Reason = "READY", &now, "actor_ready"
	d.RetryAfter = time.Time{}
	if d.Kind == "resume_admission" {
		if s.Autonomous != nil {
			s.Autonomous.ReturnDemand = false
			s.Autonomous.CandidateSince = time.Time{}
		}
		for _, r := range s.ReturnDemands {
			if r.Status == "PENDING" && r.ExpectedEpoch == d.Epoch {
				r.Status, r.ResultEpoch = "COMPLETED", s.Epoch
			}
		}
	}
	c.admissionEventLocked(s, d, "actor_ready")
}

type admissionHooksKey struct{}
type admissionHooks struct {
	rpc     func(string, string, float64, error) error
	created func(substrateapi.Summary) error
}

// Backend hooks commit intent before an RPC and its result afterwards. All
// evidence uses the existing journal transaction; no separate WAL is written.
func (c *Controller) admissionContext(ctx context.Context, s *Session, id string) context.Context {
	h := admissionHooks{}
	h.rpc = func(phase, method string, elapsed float64, rpcErr error) error {
		c.lock()
		defer c.mu.Unlock()
		if err := c.ready(); err != nil {
			return err
		}
		d := s.Admissions[id]
		d.RPCMethod, d.RPCDurationMS = method, elapsed
		d.RPCCode = status.Code(rpcErr).String()
		c.admissionEventLocked(s, d, "rpc_"+phase)
		return c.persistContext(ctx)
	}
	h.created = func(actor substrateapi.Summary) error {
		c.lock()
		defer c.mu.Unlock()
		if err := c.ready(); err != nil {
			return err
		}
		s.Actor, s.ActorUID = fromSubstrate(actor), actor.UID
		s.Admissions[id].InitialActor = copyValue(&actor)
		c.admissionEventLocked(s, s.Admissions[id], "actor_created")
		return c.persistContext(ctx)
	}
	return context.WithValue(ctx, admissionHooksKey{}, h)
}

func admissionRPC(ctx context.Context, method string, call func() (substrateapi.Summary, error)) (substrateapi.Summary, error) {
	h, _ := ctx.Value(admissionHooksKey{}).(admissionHooks)
	if h.rpc != nil {
		if err := h.rpc("started", method, 0, nil); err != nil {
			return substrateapi.Summary{}, err
		}
	}
	started := time.Now()
	actor, err := call()
	if h.rpc != nil {
		if commitErr := h.rpc("completed", method, float64(time.Since(started))/float64(time.Millisecond), err); commitErr != nil {
			return substrateapi.Summary{}, commitErr
		}
	}
	return actor, err
}

func (c *Controller) finishInitialLocked(ctx context.Context, s, snapshot *Session, actor Actor, err error, elapsed time.Duration) (*Session, error) {
	d := s.Admissions["initial"]
	d.BackendMS += float64(elapsed) / float64(time.Millisecond)
	if err != nil {
		var wait *initialCapacityWaitError
		if errors.As(err, &wait) && wait.matches(snapshot) {
			s.Actor, s.ActorUID = wait.actor, wait.actor.UID
			d.InitialActor = copyValue(wait.proof)
			s.State, s.Gate = "WAITING_CAPACITY", "CLOSED"
			c.waitAdmissionLocked(s, d, err)
			if e := c.persistContext(ctx); e != nil {
				return nil, e
			}
			return copyValue(s), nil
		}
		s.State, s.Gate = "RECOVERY_REQUIRED", "CLOSED"
		d.Status, d.Reason = "OUTCOME_UNKNOWN", err.Error()
		c.admissionEventLocked(s, d, "outcome_unknown")
		c.event(s, "session.create_unknown", err.Error())
		if e := c.persistContext(ctx); e != nil {
			return nil, e
		}
		return nil, problem(502, "create_unknown", err.Error())
	}
	s.Actor, s.ActorUID, s.ActualFidelity = actor, actor.UID, actor.ActualFidelity
	c.observeAllocationMissing(s)
	s.State, s.Gate = "RUNNING", "OPEN"
	s.RunningSince, s.IdleSince = time.Now().UTC(), time.Now().UTC()
	c.readyAdmissionLocked(s, d)
	c.event(s, "session.running", "")
	if e := c.persistContext(ctx); e != nil {
		return nil, e
	}
	return copyValue(s), nil
}

func (c *Controller) retryInitial(ctx context.Context, tenant, id string) {
	c.lock()
	s, err := c.find(tenant, id)
	if err != nil || c.ready() != nil || s.State != "WAITING_CAPACITY" {
		c.mu.Unlock()
		return
	}
	d := s.Admissions["initial"]
	if d == nil || time.Now().Before(d.RetryAfter) {
		c.mu.Unlock()
		return
	}
	s.State = "CREATING"
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	s.Autonomous.LastEvaluatedAt = time.Now().UTC()
	c.startAdmissionLocked(s, d)
	if err = c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return
	}
	snapshot := copyValue(s)
	c.mu.Unlock()
	started := time.Now()
	actor, err := c.cfg.Backend.Ensure(c.admissionContext(ctx, s, d.ID), snapshot)
	c.lock()
	_, _ = c.finishInitialLocked(ctx, s, snapshot, actor, err, time.Since(started))
	c.mu.Unlock()
}

// Admission retries run for every policy. SchedulerConcurrency bounds RPC
// work, and carries no assertion about the number of physical Actor slots.
func pendingAdmission(s *Session) bool {
	if s.State == "WAITING_CAPACITY" {
		return true
	}
	if s.State != "SUSPENDED" && s.State != "CHECKPOINTED" {
		return false
	}
	if s.Autonomous != nil && s.Autonomous.ReturnDemand {
		return true
	}
	for _, r := range s.Lifecycle {
		if r.Action == "resume" && r.Epoch == s.Epoch && r.Status == "WAITING_CAPACITY" {
			return true
		}
	}
	return false
}

func (c *Controller) advanceAdmission(ctx context.Context, snapshot *Session) bool {
	if snapshot.State == "WAITING_CAPACITY" {
		c.retryInitial(ctx, snapshot.TenantID, snapshot.SessionID)
		return true
	}
	if c.cfg.SchedulerShadow {
		return false
	}
	c.lock()
	s, err := c.find(snapshot.TenantID, snapshot.SessionID)
	if err != nil || c.ready() != nil || !pendingAdmission(s) {
		c.mu.Unlock()
		return false
	}
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	a := s.Autonomous
	operation := "auto-" + newID()
	for _, r := range s.Lifecycle {
		if r.Action == "resume" && r.Epoch == s.Epoch && r.Status == "WAITING_CAPACITY" {
			if time.Now().Before(r.RetryAfter) {
				c.mu.Unlock()
				return true
			}
			operation = r.OperationID
			break
		}
	}
	a.LastOperationID, a.LastEvaluatedAt = operation, time.Now().UTC()
	a.LastAction, a.Reason = "RESUME", "return_demand_substrate_admission"
	c.event(s, "autonomous.RESUME", operation)
	if err = c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return true
	}
	tenant, id, epoch := s.TenantID, s.SessionID, s.Epoch
	c.mu.Unlock()
	_, _, err = c.Lifecycle(ctx, tenant, id, "resume", LifecycleRequest{OperationID: operation, ExpectedEpoch: epoch})
	// Ready commits matching returns atomically. A later demand may already
	// belong to the next epoch while execution feedback is still being sent.
	if err == nil {
		return true
	}
	c.lock()
	defer c.mu.Unlock()
	if c.ready() != nil {
		return true
	}
	var api *APIError
	if errors.As(err, &api) && api.Code == "resume_capacity_wait" {
		a.LastAction, a.Reason = "KEEP", "await_worker_capacity_retry"
	} else {
		a.Reason = "lifecycle_requires_reconciliation"
	}
	c.markSession(s)
	_ = c.persistContext(ctx)
	return true
}
