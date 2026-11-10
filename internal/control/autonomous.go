package control

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"github.com/lilong555/agentcloud/internal/policy"
)

type CapacityObservation = substrateapi.WorkerCapacity
type CapacityObserver interface {
	SchedulingCapacity(context.Context, *Session) (CapacityObservation, error)
}

func (b *SubstrateBackend) SchedulingCapacity(ctx context.Context, s *Session) (CapacityObservation, error) {
	return b.cfg.Client.WorkerPoolCapacity(ctx, s.Actor.AssignedWorker, s.Actor.AssignedMemoryBytes)
}

type ServerEstimateEvidence struct {
	WaitPhase            string                     `json:"wait_phase,omitempty"`
	WaitStartedAt        time.Time                  `json:"wait_started_at,omitempty"`
	WaitAgeMS            int64                      `json:"wait_age_ms,omitempty"`
	WaitPrediction       *policy.ResidualPrediction `json:"wait_prediction,omitempty"`
	ReleasePlan          *ReleasePlan               `json:"release_plan,omitempty"`
	waitSamples          []policy.SurvivalSample
	Sources              []string             `json:"sources"`
	ModelSamples         int                  `json:"model_samples"`
	ModelServiceP75MS    int64                `json:"model_service_p75_ms"`
	ObservedGatewayAhead int                  `json:"observed_gateway_ahead"`
	ElapsedRequestMS     int64                `json:"elapsed_request_ms"`
	CostSources          map[string]string    `json:"cost_sources"`
	WorkerCapacity       *CapacityObservation `json:"worker_capacity,omitempty"`
}
type AutonomousState struct {
	ReleasePlan       *ReleasePlan            `json:"release_plan,omitempty"`
	Epoch             uint64                  `json:"epoch"`
	LastEvaluatedAt   time.Time               `json:"last_evaluated_at"`
	CandidateSince    time.Time               `json:"candidate_since,omitempty"`
	LastAction        string                  `json:"last_action,omitempty"`
	Reason            string                  `json:"reason,omitempty"`
	LastOperationID   string                  `json:"last_operation_id,omitempty"`
	ReturnDemand      bool                    `json:"return_demand"`
	ReturnRequestedAt time.Time               `json:"return_requested_at,omitempty"`
	Evidence          *ServerEstimateEvidence `json:"evidence,omitempty"`
}

func quantile(values []float64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	index := int(math.Ceil(float64(len(values))*q)) - 1
	if index < 0 {
		index = 0
	}
	return int64(math.Ceil(values[index]))
}
func (c *Controller) metricEstimateLocked(metric string, fallback int64) (int64, string, int) {
	return c.scopedMetricEstimateLocked(nil, metric, fallback)
}
func (c *Controller) scopedMetricEstimateLocked(s *Session, metric string, fallback int64) (int64, string, int) {
	values := make([]float64, 0, 32)
	for i := len(c.state.Observations) - 1; i >= 0 && len(values) < 32; i-- {
		item := c.state.Observations[i]
		if s != nil {
			peer := c.state.Sessions[key(item.TenantID, item.SessionID)]
			if item.TenantID != s.TenantID || peer == nil || peer.Profile != s.Profile {
				continue
			}
		}
		if item.Metric == metric {
			values = append(values, item.Value)
		}
	}
	if len(values) == 0 {
		return fallback, "startup_config_prior", 0
	}
	return quantile(values, .75), "observed_" + metric + "_p75", len(values)
}
func (c *Controller) serverEstimateLocked(s *Session, input ModelBoundaryRequest, queue *QueueObservation, capacity *CapacityObservation, config policy.LifecycleConfig, now time.Time) (ModelBoundaryRequest, *ServerEstimateEvidence) {
	evidence := &ServerEstimateEvidence{Sources: []string{"coordinator_durable_request_state"}, CostSources: map[string]string{}, WorkerCapacity: capacity}
	service, source, count := c.scopedMetricEstimateLocked(s, "model_http_ms", 0)
	evidence.ModelServiceP75MS = service
	evidence.ModelSamples = count
	if count > 0 {
		evidence.Sources = append(evidence.Sources, source)
	}
	var current *ModelRequest
	for _, request := range c.state.Requests {
		if request.TenantID == s.TenantID && request.SessionID == s.SessionID && (request.Status == "PENDING" || request.Status == "DISPATCHING") {
			if current == nil || request.CreatedAt.Before(current.CreatedAt) {
				current = request
			}
		}
	}
	expected := int64(0)
	if current != nil {
		elapsed := max(int64(0), now.Sub(current.CreatedAt).Milliseconds())
		evidence.ElapsedRequestMS = elapsed
		if current.Status == "PENDING" {
			for _, request := range c.state.Requests {
				if request.Status == "DISPATCHING" || request.Status == "PENDING" && request.CreatedAt.Before(current.CreatedAt) {
					evidence.ObservedGatewayAhead++
				}
			}
			if service > 0 {
				expected = int64(math.Ceil(float64(evidence.ObservedGatewayAhead+1)/float64(c.cfg.ModelConcurrency))) * service
			}
		} else if service > 0 {
			dispatchElapsed := elapsed
			if current.DispatchedAt != nil {
				dispatchElapsed = max(int64(0), now.Sub(*current.DispatchedAt).Milliseconds())
			}
			expected = max(int64(0), service-dispatchElapsed)
		}
		if queue != nil && queue.WaitingRequests > 0 {
			evidence.Sources = append(evidence.Sources, queue.Source+"_"+queue.Scope+"_queue")
			admission := int64(0)
			for _, item := range queue.AdmissionHistory {
				admission = max(admission, int64(math.Ceil(item.WaitMS)))
			}
			if admission > 0 {
				expected = max(expected, admission)
			}
			if service > 0 {
				expected = max(expected, int64(queue.WaitingRequests)*service)
			}
		}
	}
	idleService, idleSource, idleCount := c.scopedMetricEstimateLocked(s, "idle_window_ms", c.cfg.SchedulerIdlePrior.Milliseconds())
	if idleCount == 0 {
		idleService, idleSource, idleCount = c.scopedMetricEstimateLocked(s, "idle_until_tool_ms", c.cfg.SchedulerIdlePrior.Milliseconds())
	}
	if current == nil && s.Mode != "in-actor" && !s.IdleSince.IsZero() && idleService > 0 {
		expected = max(expected, max(int64(0), idleService-now.Sub(s.IdleSince).Milliseconds()))
		evidence.Sources = append(evidence.Sources, idleSource)
	}
	for _, window := range s.IdleWindows {
		if current == nil && window.FinishedAt == nil && window.Epoch == s.Epoch && now.Before(window.ExpiresAt) && idleService > 0 {
			expected = max(expected, max(int64(0), idleService-now.Sub(window.StartedAt).Milliseconds()))
			evidence.Sources = append(evidence.Sources, idleSource)
		}
	}
	input.ExpectedWaitMS = min(int64(86_400_000), expected)
	if capacity != nil {
		pressure := 0.0
		if capacity.CapacityActors > 0 {
			pressure = float64(capacity.AllocatedActors) / float64(capacity.CapacityActors)
		}
		if capacity.CapacityMemoryBytes > 0 {
			pressure = max(pressure, float64(capacity.AllocatedMemoryBytes)/float64(capacity.CapacityMemoryBytes))
		}
		input.WorkerPressure = min(1.0, max(0.0, pressure))
		evidence.Sources = append(evidence.Sources, capacity.Source)
	}
	snapshot, source, _ := c.metricEstimateLocked("checkpoint_total_ms", config.SnapshotCostMS)
	evidence.CostSources["snapshot"] = source
	if source == "startup_config_prior" {
		suspend, ss, ns := c.metricEstimateLocked("substrate_suspend_ms", config.SnapshotCostMS)
		durable, ds, nd := c.metricEstimateLocked("durability_wait_ms", 0)
		if ns > 0 || nd > 0 {
			snapshot = suspend + durable
			evidence.CostSources["snapshot"] = ss + "+" + ds
		}
	}
	resume, source, _ := c.metricEstimateLocked("resume_ready_ms", config.ResumeCostMS)
	evidence.CostSources["resume"] = source
	journal, source, _ := c.metricEstimateLocked("journal_commit_ms", config.JournalCostMS)
	evidence.CostSources["journal"] = source
	input.SnapshotCostMS = &snapshot
	input.ResumeCostMS = &resume
	input.JournalCostMS = &journal
	if c.cfg.SchedulerPolicy == "cost-aware" {
		input = c.conditionalEstimateLocked(s, input, evidence, now)
	}
	return input, evidence
}
func (c *Controller) autonomousLoop() {
	defer c.workers.Done()
	ticker := time.NewTicker(c.cfg.SchedulerInterval)
	defer ticker.Stop()
	semaphore := make(chan struct{}, c.cfg.SchedulerConcurrency)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.lock()
			var candidates []*Session
			if c.ready() == nil {
				for _, s := range c.state.Sessions {
					if s.AblationGroup != "" && s.AblationGroup != "E" {
						continue
					}
					if s.State != "RUNNING" && s.State != "CHECKPOINTED" && s.State != "SUSPENDED" {
						continue
					}
					if (s.State == "CHECKPOINTED" || s.State == "SUSPENDED") && (s.Autonomous == nil || !s.Autonomous.ReturnDemand) {
						continue
					}
					if c.schedulerBusy[key(s.TenantID, s.SessionID)] {
						continue
					}
					waiting := false
					for _, r := range c.state.Requests {
						if r.TenantID == s.TenantID && r.SessionID == s.SessionID && (r.Status == "PENDING" || r.Status == "DISPATCHING") {
							waiting = true
							break
						}
					}
					for _, window := range s.IdleWindows {
						if window.FinishedAt == nil && time.Now().Before(window.ExpiresAt) {
							waiting = true
							break
						}
					}
					if !waiting && !s.IdleSince.IsZero() && s.ActiveTools == 0 {
						waiting = true
					}
					if !waiting && (s.Autonomous == nil || !s.Autonomous.ReturnDemand) {
						continue
					}
					candidates = append(candidates, schedulingSnapshot(s))
				}
			}
			sort.Slice(candidates, func(i, j int) bool {
				var a, b time.Time
				if candidates[i].Autonomous != nil {
					a = candidates[i].Autonomous.LastEvaluatedAt
				}
				if candidates[j].Autonomous != nil {
					b = candidates[j].Autonomous.LastEvaluatedAt
				}
				return a.Before(b)
			})
			for _, s := range candidates {
				select {
				case semaphore <- struct{}{}:
					k := key(s.TenantID, s.SessionID)
					c.schedulerBusy[k] = true
					c.operations.Add(1)
					go func(snapshot *Session) {
						defer c.operations.Done()
						defer func() {
							<-semaphore
							c.lock()
							delete(c.schedulerBusy, key(snapshot.TenantID, snapshot.SessionID))
							c.mu.Unlock()
						}()
						c.autonomousStep(snapshot)
					}(s)
				default:
				}
			}
			c.mu.Unlock()
		}
	}
}
func (c *Controller) autonomousStep(snapshot *Session) {
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.ModelTimeout)
	defer cancel()
	observer := c.cfg.QueueObserver
	if observer == nil {
		observer, _ = c.cfg.Signals.(QueueObserver)
	}
	var queue *QueueObservation
	var observationErr error
	if observer != nil {
		probe, stop := context.WithTimeout(ctx, 2*time.Second)
		value, err := observer.Waiting(probe, snapshot)
		stop()
		observationErr = err
		if err == nil {
			queue = &value
		}
	}
	capacityObserver := c.cfg.CapacityObserver
	if capacityObserver == nil {
		capacityObserver, _ = c.cfg.Backend.(CapacityObserver)
	}
	var capacity *CapacityObservation
	if capacityObserver != nil {
		probe, stop := context.WithTimeout(ctx, 2*time.Second)
		value, err := capacityObserver.SchedulingCapacity(probe, snapshot)
		stop()
		if err == nil {
			capacity = &value
		} else {
			observationErr = err
		}
	}
	p, _ := policy.NewLifecycle(c.cfg.Scheduling)
	c.lock()
	s, err := c.find(snapshot.TenantID, snapshot.SessionID)
	if err != nil || c.ready() != nil || s.Epoch != snapshot.Epoch {
		c.mu.Unlock()
		return
	}
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	a := s.Autonomous
	now := time.Now().UTC()
	a.LastEvaluatedAt = now
	if a.Epoch != s.Epoch {
		a.Epoch = s.Epoch
		a.CandidateSince = time.Time{}
		a.ReleasePlan = nil
	}
	request, evidence := c.serverEstimateLocked(s, ModelBoundaryRequest{ExpectedEpoch: s.Epoch}, queue, capacity, p.Config(), now)
	a.Evidence = evidence
	previousAction, previousReason := a.LastAction, a.Reason
	action, reason := "KEEP", "no_expected_wait"
	costWaiting := false
	if observationErr != nil {
		reason = "upstream_observation_unavailable"
	} else if capacity != nil && (capacity.ObservedAt.IsZero() || capacity.ObservedAt.After(now.Add(time.Second)) || now.Sub(capacity.ObservedAt) > 5*time.Second) {
		reason = "worker_capacity_stale"
	} else if queue != nil && (queue.ObservedAt.IsZero() || queue.ObservedAt.After(now.Add(time.Second)) || now.Sub(queue.ObservedAt) > 5*time.Second) {
		reason = "queue_observation_stale"
	} else if s.State == "SUSPENDED" || s.State == "CHECKPOINTED" {
		reason = "await_return_demand"
		if a.ReturnDemand {
			available := capacity == nil && c.cfg.Backend.Name() != "substrate" || capacity != nil && (capacity.CapacityActors == 0 || capacity.AllocatedActors < capacity.CapacityActors) && (capacity.CapacityMemoryBytes == 0 || capacity.CapacityMemoryBytes-capacity.AllocatedMemoryBytes >= s.Actor.AssignedMemoryBytes)
			if capacity != nil && capacity.FitsActor != nil {
				available = *capacity.FitsActor
			}
			if available {
				action, reason = "RESUME", "model_ready_return_demand_capacity_available"
			} else {
				reason = "await_worker_capacity"
			}
		}
	} else {
		decision, e := p.Decide(boundaryInput(s, request, p.Config(), now))
		if c.cfg.SchedulerPolicy == "cost-aware" {
			decision, e = c.costDecisionLocked(s, request, evidence, p.Config(), now)
			costWaiting = e == nil && decision.Estimates.SafePoint && !a.ReturnDemand && !evidence.WaitStartedAt.IsZero() && a.ReleasePlan != nil
			if costWaiting && a.CandidateSince.IsZero() {
				a.CandidateSince = now
			}
		}
		if e == nil {
			action, reason = decision.Action, decision.Reason
		}
		if a.ReturnDemand {
			action, reason = "KEEP", "return_demand"
		}
	}
	if action == "SUSPEND" {
		if a.CandidateSince.IsZero() {
			a.CandidateSince = now
			action, reason = "KEEP", "stable_wait_confirmation"
		} else if now.Sub(a.CandidateSince) < c.cfg.SchedulerStableFor {
			action, reason = "KEEP", "stable_wait_confirmation"
		}
	} else if !costWaiting && (action != "KEEP" || reason != "stable_wait_confirmation") {
		a.CandidateSince = time.Time{}
	}
	var retry *LifecycleReceipt
	if action == "RESUME" {
		prior := s.Lifecycle[a.LastOperationID]
		if prior != nil && prior.Action == "resume" && prior.Epoch == s.Epoch && prior.Status == "WAITING_CAPACITY" {
			retry = prior
			if now.Before(prior.RetryAfter) {
				action, reason = "KEEP", "await_worker_capacity_retry"
			}
		}
	}
	a.LastAction, a.Reason = action, reason
	if action == "KEEP" {
		if action != previousAction || reason != previousReason {
			c.event(s, "autonomous.keep", reason)
			_ = c.persistContext(c.operationContext(ctx, snapshot.TenantID, snapshot.SessionID, "autonomous"))
		} else if c.cfg.SchedulerPolicy == "cost-aware" && c.dirty["sessions"][key(s.TenantID, s.SessionID)] {
			_ = c.persistContext(c.operationContext(ctx, snapshot.TenantID, snapshot.SessionID, "autonomous"))
		}
		c.mu.Unlock()
		return
	}
	request.ServerEvidence = evidence
	operation := "auto-" + newID()
	if retry != nil {
		operation = retry.OperationID
	}
	a.LastOperationID = operation
	request.OperationID = operation
	c.event(s, "autonomous."+action, operation)
	_ = c.persistContext(c.operationContext(ctx, snapshot.TenantID, snapshot.SessionID, "autonomous"))
	tenant, id, epoch := s.TenantID, s.SessionID, s.Epoch
	c.mu.Unlock()
	if action == "RESUME" {
		_, _, err = c.Lifecycle(ctx, tenant, id, "resume", LifecycleRequest{OperationID: operation, ExpectedEpoch: epoch})
	} else {
		_, err = c.ModelBoundary(ctx, tenant, id, request)
	}
	c.lock()
	if current := c.state.Sessions[key(tenant, id)]; current != nil && current.Autonomous != nil {
		if err == nil && action == "RESUME" {
			current.Autonomous.ReturnDemand = false
			for _, demand := range current.ReturnDemands {
				if demand.Status == "PENDING" {
					demand.Status = "COMPLETED"
					demand.ResultEpoch = current.Epoch
				}
			}
			current.Autonomous.CandidateSince = time.Time{}
		}
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Code == "resume_capacity_wait" {
				current.Autonomous.LastAction = "KEEP"
				current.Autonomous.Reason = "await_worker_capacity_retry"
			} else {
				current.Autonomous.Reason = "lifecycle_requires_reconciliation"
			}
		}
		c.markSession(current)
		_ = c.persistContext(c.operationContext(ctx, snapshot.TenantID, snapshot.SessionID, "autonomous"))
	}
	c.mu.Unlock()
}

type ReturnDemandReceipt struct {
	OperationID       string    `json:"operation_id"`
	ExpectedEpoch     uint64    `json:"expected_epoch"`
	AfterModelRequest string    `json:"after_model_request,omitempty"`
	ResultEpoch       uint64    `json:"result_epoch,omitempty"`
	Status            string    `json:"status"`
	RequestedAt       time.Time `json:"requested_at"`
}

func (c *Controller) requestReturnLocked(s *Session, input LifecycleRequest) (*ReturnDemandReceipt, error) {
	if !c.cfg.EnableAutonomousScheduler {
		return nil, problem(409, "autonomous_scheduler_disabled", "enable autonomous scheduler for managed return")
	}
	if s.ReturnDemands == nil {
		s.ReturnDemands = map[string]*ReturnDemandReceipt{}
	}
	if prior := s.ReturnDemands[input.OperationID]; prior != nil {
		if prior.ExpectedEpoch != input.ExpectedEpoch || prior.AfterModelRequest != input.AfterModelRequest {
			return nil, problem(409, "payload_conflict", "return operation has another epoch or model condition")
		}
		return copyValue(prior), nil
	}
	if s.Epoch != input.ExpectedEpoch {
		return nil, problem(409, "stale_epoch", "execution epoch differs")
	}
	if s.State == "RECOVERY_REQUIRED" || s.State == "CREATING" {
		return nil, problem(409, "recovery_required", "return requires a reconciled session")
	}
	// Check the observer's condition in the same critical section as Submit.
	// A completed model can wake its tool/publication continuation without
	// accidentally cancelling the safe wait of a newly registered model.
	if input.AfterModelRequest != "" {
		r := c.state.Requests[key(s.TenantID, input.AfterModelRequest)]
		if r == nil || r.SessionID != s.SessionID || r.Status != "COMPLETED" {
			return nil, problem(409, "return_condition_unmet", "the referenced model request must be completed in this session")
		}
		for _, pending := range c.state.Requests {
			if pending.TenantID == s.TenantID && pending.SessionID == s.SessionID && (pending.Status == "PENDING" || pending.Status == "DISPATCHING" || pending.Status == "OUTCOME_UNKNOWN") {
				return nil, problem(409, "return_condition_unmet", "all model outcomes in this session must be resolved")
			}
		}
	}
	if len(s.ReturnDemands) >= 4096 {
		return nil, problem(429, "return_demand_quota", "return demand capacity reached")
	}
	now := time.Now().UTC()
	resetReleasePlan(s)
	receipt := &ReturnDemandReceipt{OperationID: input.OperationID, ExpectedEpoch: input.ExpectedEpoch, AfterModelRequest: input.AfterModelRequest, Status: "PENDING", RequestedAt: now}
	if s.Autonomous == nil {
		s.Autonomous = &AutonomousState{Epoch: s.Epoch}
	}
	s.Autonomous.ReturnDemand = true
	s.Autonomous.ReturnRequestedAt = now
	if s.State == "RUNNING" && s.Gate == "OPEN" {
		receipt.Status = "COMPLETED"
		receipt.ResultEpoch = s.Epoch
	}
	s.ReturnDemands[input.OperationID] = receipt
	c.event(s, "autonomous.return_demand", input.OperationID)
	return copyValue(receipt), nil
}
func (c *Controller) ReturnDemand(ctx context.Context, tenant, id string, input LifecycleRequest) (*ReturnDemandReceipt, error) {
	if !validID(input.OperationID) || input.ExpectedEpoch == 0 || input.AfterModelRequest != "" && !validID(input.AfterModelRequest) {
		return nil, problem(400, "invalid_operation", "operation_id and expected_epoch are required")
	}
	ctx = c.operationContext(ctx, tenant, id, input.OperationID)
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	receipt, err := c.requestReturnLocked(s, input)
	if err != nil {
		return nil, err
	}
	if err := c.persistContext(ctx); err != nil {
		return nil, err
	}
	return receipt, nil
}
