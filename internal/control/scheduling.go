package control

import (
	"context"
	"net/http"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/policy"
	"go.opentelemetry.io/otel/trace"
)

// QueueObserver reads a server-side queue snapshot for the bound session.
type QueueObserver interface {
	Waiting(context.Context, *Session) (QueueObservation, error)
}

type QueueObservation struct {
	SessionWaitingRequests uint64                        `json:"session_waiting_requests,omitempty"`
	AdmissionHistory       []dynamo.AdmissionObservation `json:"admission_history,omitempty"`
	Source                 string                        `json:"source"`
	Scope                  string                        `json:"scope"`
	WaitingRequests        uint64                        `json:"waiting_requests"`
	ObservedAt             time.Time                     `json:"observed_at"`
}

type ModelBoundaryRequest struct {
	ServerEvidence *ServerEstimateEvidence `json:"-"`
	OperationID    string                  `json:"operation_id"`
	ExpectedEpoch  uint64                  `json:"expected_epoch"`
	CheckpointID   string                  `json:"checkpoint_id,omitempty"`
	ExpectedWaitMS int64                   `json:"expected_wait_ms"`
	SnapshotCostMS *int64                  `json:"snapshot_cost_ms,omitempty"`
	ResumeCostMS   *int64                  `json:"resume_cost_ms,omitempty"`
	JournalCostMS  *int64                  `json:"journal_cost_ms,omitempty"`
	MinResidenceMS *int64                  `json:"min_residence_ms,omitempty"`
	WorkerPressure float64                 `json:"worker_pressure"`
}

type BoundaryDecision struct {
	ServerEvidence           *ServerEstimateEvidence  `json:"server_evidence,omitempty"`
	OperationID              string                   `json:"operation_id"`
	SessionID                string                   `json:"session_id"`
	ExpectedEpoch            uint64                   `json:"expected_epoch"`
	ResultEpoch              uint64                   `json:"result_epoch"`
	PayloadHash              string                   `json:"payload_hash"`
	Status                   string                   `json:"status"`
	SessionState             string                   `json:"session_state"`
	Decision                 policy.LifecycleDecision `json:"decision"`
	EstimateSource           string                   `json:"estimate_source"`
	QueueObservation         *QueueObservation        `json:"queue_observation,omitempty"`
	QueueObservationError    string                   `json:"queue_observation_error,omitempty"`
	StartedAt                time.Time                `json:"started_at"`
	FinishedAt               *time.Time               `json:"finished_at,omitempty"`
	ActualLifecycleElapsedMS *int64                   `json:"actual_lifecycle_elapsed_ms,omitempty"`
	Manifest                 *Manifest                `json:"manifest,omitempty"`
	ErrorCode                string                   `json:"error_code,omitempty"`
	Error                    string                   `json:"error,omitempty"`
}

func boundaryEstimate(override *int64, fallback int64) int64 {
	if override != nil {
		return *override
	}
	return fallback
}

func boundarySafe(s *Session) bool {
	if s.State != "RUNNING" && s.State != "CHECKPOINTED" || s.ActiveTools != 0 {
		return false
	}
	for _, receipt := range s.Tools {
		if receipt.Status == "RUNNING" || receipt.Status == "OUTCOME_UNKNOWN" {
			return false
		}
	}
	for _, receipt := range s.Lifecycle {
		if receipt.Status == "RUNNING" || receipt.Status == "OUTCOME_UNKNOWN" {
			return false
		}
	}
	for _, receipt := range s.Scheduling {
		if receipt.Status == "RUNNING" || receipt.Status == "OUTCOME_UNKNOWN" {
			return false
		}
	}
	return true
}

// ReconcileScheduling closes admission after an interrupted durable decision.
func ReconcileScheduling(s *Session) {
	for _, receipt := range s.Scheduling {
		if receipt.Status == "RUNNING" {
			receipt.Status, receipt.Error = "OUTCOME_UNKNOWN", "coordinator restarted before scheduling completion"
		}
		if receipt.Status == "OUTCOME_UNKNOWN" {
			s.State, s.Gate = "RECOVERY_REQUIRED", "CLOSED"
		}
	}
}

func boundaryInput(s *Session, request ModelBoundaryRequest, config policy.LifecycleConfig, now time.Time) policy.LifecycleInput {
	since := s.RunningSince
	if since.IsZero() {
		since = s.CreatedAt
	}
	residence := max(int64(0), now.Sub(since).Milliseconds())
	var resume *int64
	if !s.LastResumedAt.IsZero() {
		elapsed := max(int64(0), now.Sub(s.LastResumedAt).Milliseconds())
		resume = &elapsed
	}
	return policy.LifecycleInput{ExpectedWaitMS: request.ExpectedWaitMS, SnapshotCostMS: boundaryEstimate(request.SnapshotCostMS, config.SnapshotCostMS), ResumeCostMS: boundaryEstimate(request.ResumeCostMS, config.ResumeCostMS), JournalCostMS: boundaryEstimate(request.JournalCostMS, config.JournalCostMS), MinimumResidenceMS: boundaryEstimate(request.MinResidenceMS, config.MinimumResidenceMS), WorkerPressure: request.WorkerPressure, ResidenceMS: residence, SinceResumeMS: resume, SafePoint: boundarySafe(s)}
}

// ModelBoundary durably reserves an operation before closing admission and invoking
// the fenced lifecycle actuator. Matching retries return the original receipt.
func (c *Controller) ModelBoundary(ctx context.Context, tenant, id string, input ModelBoundaryRequest) (*BoundaryDecision, error) {
	ctx = c.operationContext(ctx, tenant, id, input.OperationID)
	ctx, span := observability.Start(ctx, "coordinator.model_boundary", trace.SpanKindInternal)
	defer span.End()
	if !validID(input.OperationID) || input.ExpectedEpoch == 0 {
		return nil, problem(400, "invalid_operation", "operation_id and expected_epoch are required")
	}
	p, err := policy.NewLifecycle(c.cfg.Scheduling)
	if err != nil {
		return nil, problem(400, "invalid_scheduling_config", err.Error())
	}
	hash := payloadHash(input)
	c.lock()
	if err = c.ready(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if s.AblationGroup == "A" || s.AblationGroup == "B" || s.AblationGroup == "C" {
		c.mu.Unlock()
		return nil, problem(403, "ablation_policy_conflict", "model-boundary is supported by ablation groups D and E")
	}
	if prior := s.Scheduling[input.OperationID]; prior != nil {
		out, err := replayBoundary(prior, hash)
		c.mu.Unlock()
		return out, err
	}
	if s.Epoch != input.ExpectedEpoch {
		c.mu.Unlock()
		return nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	// Validate supplied estimates before contacting the upstream observer.
	if _, err = p.Decide(boundaryInput(s, input, p.Config(), time.Now().UTC())); err != nil {
		c.mu.Unlock()
		return nil, problem(400, "invalid_cost_estimate", err.Error())
	}
	snapshot := schedulingSnapshot(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	observer := c.cfg.QueueObserver
	if observer == nil {
		observer, _ = c.cfg.Signals.(QueueObserver)
	}
	var observation *QueueObservation
	var observeErr error
	if observer != nil {
		observeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		value, err := observer.Waiting(observeCtx, snapshot)
		cancel()
		observeErr = err
		if err == nil {
			observation = &value
		}
	}
	c.lock()
	if err = c.ready(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	s, err = c.find(tenant, id)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if prior := s.Scheduling[input.OperationID]; prior != nil {
		out, err := replayBoundary(prior, hash)
		c.mu.Unlock()
		return out, err
	}
	if s.Epoch != input.ExpectedEpoch {
		c.mu.Unlock()
		return nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	if s.Lifecycle[input.OperationID] != nil {
		c.mu.Unlock()
		return nil, problem(409, "payload_conflict", "operation ID is already used by a lifecycle operation")
	}
	now := time.Now().UTC()
	serverEvidence := input.ServerEvidence
	estimateSource := "caller_estimate"
	if serverEvidence != nil {
		estimateSource = "server_observed"
	}
	var capacity *CapacityObservation
	if c.schedulerPolicy() == "agentcloud-v2" && serverEvidence != nil {
		capacity = serverEvidence.WorkerCapacity
	}
	if input.ExpectedWaitMS == 0 || c.conditionalScheduler() && input.ServerEvidence != nil || c.schedulerPolicy() == "agentcloud-v2" || c.cfg.SchedulerShadow {
		input, serverEvidence = c.serverEstimateLocked(s, input, observation, capacity, p.Config(), now)
		estimateSource = "server_observed"
	}
	d, err := p.Decide(boundaryInput(s, input, p.Config(), now))
	if c.conditionalScheduler() && estimateSource == "server_observed" {
		d, err = c.schedulerDecisionLocked(s, input, serverEvidence, p.Config(), now)
	}
	if err != nil {
		c.mu.Unlock()
		return nil, problem(400, "invalid_cost_estimate", err.Error())
	}
	if s.Autonomous != nil && s.Autonomous.ReturnDemand {
		d.Action, d.Reason = "KEEP", "return_demand"
	}
	receipt := &BoundaryDecision{OperationID: input.OperationID, SessionID: id, ExpectedEpoch: input.ExpectedEpoch, ResultEpoch: s.Epoch, PayloadHash: hash, Status: "COMPLETED", SessionState: s.State, Decision: d, EstimateSource: estimateSource, ServerEvidence: serverEvidence, QueueObservation: observation, StartedAt: now, FinishedAt: &now}
	if observeErr != nil {
		receipt.QueueObservationError = observeErr.Error()
		receipt.Decision.Action, receipt.Decision.Reason = "KEEP", "queue_observation_failed"
	} else if observation != nil {
		switch {
		case observation.Source == "" || observation.Scope == "" || observation.ObservedAt.IsZero() || observation.ObservedAt.After(now.Add(time.Second)) || now.Sub(observation.ObservedAt) > 5*time.Second:
			receipt.Decision.Action, receipt.Decision.Reason = "KEEP", "queue_observation_stale"
		case observation.WaitingRequests == 0 && input.WorkerPressure == 0 && !(c.conditionalScheduler() && estimateSource == "server_observed"):
			receipt.Decision.Action, receipt.Decision.Reason = "KEEP", "observed_queue_empty"
		}
	}
	if capacity != nil && (capacity.ObservedAt.IsZero() || capacity.ObservedAt.After(now.Add(time.Second)) || now.Sub(capacity.ObservedAt) > 5*time.Second) {
		receipt.Decision.Action, receipt.Decision.Reason = "KEEP", "worker_capacity_stale"
	}
	if c.cfg.SchedulerShadow {
		serverEvidence.Shadow = true
		serverEvidence.ComputedAction, serverEvidence.ComputedReason = receipt.Decision.Action, receipt.Decision.Reason
		receipt.Decision.Action, receipt.Decision.Reason = "KEEP", "shadow_action_suppressed"
	}
	if s.Scheduling == nil {
		s.Scheduling = map[string]*BoundaryDecision{}
	}
	if len(s.Scheduling) >= c.cfg.MaxRequests {
		c.mu.Unlock()
		return nil, problem(429, "scheduling_limit", "session scheduling receipt limit reached")
	}
	s.Scheduling[input.OperationID] = receipt
	if receipt.Decision.Action == "SUSPEND" {
		// The safety test and admission closure share the controller lock.
		s.Gate = "CLOSED"
		receipt.Status, receipt.FinishedAt = "RUNNING", nil
	}
	c.event(s, "scheduling."+receipt.Decision.Action, input.OperationID)
	if err = c.persistContext(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if receipt.Status == "COMPLETED" {
		out := copyValue(receipt)
		c.mu.Unlock()
		return out, nil
	}
	c.mu.Unlock()
	started := time.Now()
	result, manifest, lifecycleErr := c.lifecycle(ctx, tenant, id, "suspend", LifecycleRequest{OperationID: input.OperationID, ExpectedEpoch: input.ExpectedEpoch, CheckpointID: input.CheckpointID}, true)
	elapsed := time.Since(started).Milliseconds()
	c.lock()
	now = time.Now().UTC()
	receipt.FinishedAt, receipt.ActualLifecycleElapsedMS = &now, &elapsed
	if lifecycleErr != nil {
		receipt.Status, receipt.Error = "OUTCOME_UNKNOWN", lifecycleErr.Error()
		if api, ok := lifecycleErr.(*APIError); ok {
			receipt.ErrorCode = api.Code
		}
	} else {
		receipt.Status, receipt.ResultEpoch, receipt.SessionState, receipt.Manifest = "COMPLETED", result.Epoch, result.State, manifest
	}
	c.event(s, "scheduling.completed", input.OperationID)
	err = c.persistContext(ctx)
	out := copyValue(receipt)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return out, lifecycleErr
}

func replayBoundary(prior *BoundaryDecision, hash string) (*BoundaryDecision, error) {
	if prior.PayloadHash != hash {
		return nil, problem(409, "payload_conflict", "operation ID has another payload")
	}
	if prior.Status == "OUTCOME_UNKNOWN" || prior.Status == "RUNNING" {
		return copyValue(prior), problem(409, "scheduling_unknown", "scheduling completion requires reconciliation")
	}
	return copyValue(prior), nil
}

func (c *Controller) ModelBoundaryEndpoint() http.HandlerFunc {
	return c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input ModelBoundaryRequest
		if err := decode(w, r, &input); err != nil {
			return err
		}
		decision, err := c.ModelBoundary(r.Context(), tenant, r.PathValue("id"), input)
		if err != nil {
			return err
		}
		respond(w, http.StatusOK, decision)
		return nil
	})
}
