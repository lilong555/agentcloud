package control

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type observationIdentity struct{ tenant, session string }
type observationIdentityKey struct{}

func (c *Controller) operationContext(ctx context.Context, tenant, id, operation string) context.Context {
	ctx = observability.WithRuntime(ctx, c.telemetry)
	ctx = context.WithValue(ctx, observationIdentityKey{}, observationIdentity{tenant, id})
	return observability.WithScope(ctx, observability.Scope{OperationID: operation, Record: func(m observability.Measurement) {
		c.lock()
		defer c.mu.Unlock()
		c.appendObservationLocked(tenant, id, m)
	}})
}
func (c *Controller) appendObservationLocked(tenant, id string, m observability.Measurement) {
	c.state.ObservationSequence++
	item := RecordedMeasurement{Sequence: c.state.ObservationSequence, TenantID: tenant, SessionID: id, Measurement: m}
	c.state.Observations = append(c.state.Observations, item)
	observationKey := fmt.Sprintf("%020d", item.Sequence)
	c.markEntity("observations", observationKey)
	if c.pendingObservations == nil {
		c.pendingObservations = map[string]RecordedMeasurement{}
	}
	c.pendingObservations[observationKey] = item
	if len(c.state.Observations) > 16384 {
		c.state.Observations = c.state.Observations[len(c.state.Observations)-16384:]
	}
}
func (c *Controller) observeLocked(ctx context.Context, name string, value float64, source string) {
	identity, ok := ctx.Value(observationIdentityKey{}).(observationIdentity)
	var recorded observability.Measurement
	record := func(m observability.Measurement) { recorded = m }
	operation := ""
	// Preserve the operation correlation while avoiding reentrant controller locking.
	if scope, exists := observability.ScopeFromContext(ctx); exists {
		operation = scope.OperationID
	}
	ctx = observability.WithScope(ctx, observability.Scope{OperationID: operation, Record: record})
	observability.Observe(ctx, name, value, source)
	if ok {
		c.appendObservationLocked(identity.tenant, identity.session, recorded)
	}
}

type DiagnosticSession struct {
	QuiesceRetryAfter time.Time         `json:"quiesce_retry_after,omitempty"`
	QuiesceTimeouts   uint64            `json:"quiesce_timeouts,omitempty"`
	ToolRecovery      *ToolRecoveryHint `json:"tool_recovery,omitempty"`
	WorkspaceID       string            `json:"workspace_id"`
	SessionID         string            `json:"session_id"`
	State             string            `json:"state"`
	Gate              string            `json:"gate"`
	Epoch             uint64            `json:"epoch"`
	ActiveTools       int               `json:"active_tools"`
	Backend           string            `json:"backend"`
	ActualFidelity    string            `json:"actual_fidelity"`
	Manifest          *Manifest         `json:"manifest,omitempty"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

func diagnosticSession(s *Session) DiagnosticSession {
	return DiagnosticSession{WorkspaceID: s.WorkspaceID, SessionID: s.SessionID, State: s.State, Gate: s.Gate, Epoch: s.Epoch, ActiveTools: s.ActiveTools, Backend: s.Backend, ActualFidelity: s.ActualFidelity, Manifest: copyValue(s.Manifest), UpdatedAt: s.UpdatedAt, QuiesceRetryAfter: s.QuiesceRetryAfter, QuiesceTimeouts: s.QuiesceTimeouts, ToolRecovery: unknownTools(s)}
}

type DiagnosticStatus struct {
	At                   time.Time           `json:"at"`
	Status               string              `json:"status"`
	ExecutionBackend     string              `json:"execution_backend"`
	JournalBackend       string              `json:"journal_backend"`
	PendingModelRequests int                 `json:"pending_model_requests"`
	UnknownModelRequests int                 `json:"unknown_model_requests"`
	Sessions             []DiagnosticSession `json:"sessions"`
	Checks               map[string]string   `json:"checks"`
}

func (c *Controller) DiagnosticStatus(ctx context.Context, tenant string, probe bool) DiagnosticStatus {
	result := DiagnosticStatus{At: time.Now().UTC(), Status: "ready", ExecutionBackend: c.cfg.Backend.Name(), JournalBackend: c.store.Backend(), Checks: map[string]string{}, Sessions: []DiagnosticSession{}}
	c.lock()
	if c.ready() != nil {
		result.Status = "unavailable"
		result.Checks["coordinator"] = "unavailable"
	} else {
		result.Checks["coordinator"] = "ready"
	}
	var sample *Session
	for _, s := range c.state.Sessions {
		if s.TenantID == tenant {
			result.Sessions = append(result.Sessions, diagnosticSession(s))
			if sample == nil {
				sample = copyValue(s)
			}
		}
	}
	for _, r := range c.state.Requests {
		if r.TenantID == tenant {
			switch r.Status {
			case "PENDING", "DISPATCHING":
				result.PendingModelRequests++
			case "OUTCOME_UNKNOWN":
				result.UnknownModelRequests++
			}
		}
	}
	c.mu.Unlock()
	sort.Slice(result.Sessions, func(i, j int) bool { return result.Sessions[i].WorkspaceID < result.Sessions[j].WorkspaceID })
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if c.store.Check(checkCtx) == nil {
		result.Checks["journal"] = "ready"
	} else {
		result.Checks["journal"] = "unavailable"
		result.Status = "unavailable"
	}
	if probe {
		if b, ok := c.cfg.Signals.(*DynamoBridge); ok {
			client, clientErr := b.clientFor(&Session{TenantID: tenant})
			if clientErr != nil {
				result.Checks["dynamo"] = "unavailable"
				result.Status = "degraded"
			} else if _, err := client.Capabilities(checkCtx); err != nil {
				result.Checks["dynamo"] = "unavailable"
				result.Status = "degraded"
			} else {
				result.Checks["dynamo"] = "ready"
			}
		}
		if sample != nil {
			if b, ok := c.cfg.Backend.(*SubstrateBackend); ok {
				if _, err := b.cfg.Client.GetActor(checkCtx, sample.Actor.Name); err != nil {
					result.Checks["substrate"] = "unavailable"
					result.Status = "degraded"
				} else {
					result.Checks["substrate"] = "ready"
				}
			}
		}
	}
	return result
}

type EpochEntry struct {
	Epoch           uint64    `json:"epoch"`
	FirstObservedAt time.Time `json:"first_observed_at"`
	LastObservedAt  time.Time `json:"last_observed_at"`
}
type DiagnosticTrace struct {
	At                   time.Time                   `json:"at"`
	Session              DiagnosticSession           `json:"session"`
	Events               []Event                     `json:"events"`
	EpochHistory         []EpochEntry                `json:"epoch_history"`
	Manifests            []*Manifest                 `json:"manifests"`
	Observations         []observability.Measurement `json:"observations"`
	LatencyMS            map[string]float64          `json:"latency_ms"`
	UnavailableMetrics   []string                    `json:"unavailable_metrics"`
	HistoryFirstSequence uint64                      `json:"history_first_sequence"`
	HistoryLimit         int                         `json:"history_limit"`
}

func (c *Controller) DiagnosticTrace(tenant, workspace string) (*DiagnosticTrace, error) {
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	var s *Session
	for _, candidate := range c.state.Sessions {
		if candidate.TenantID == tenant && candidate.WorkspaceID == workspace {
			s = candidate
			break
		}
	}
	if s == nil {
		return nil, problem(404, "workspace_not_found", "workspace not found")
	}
	r := &DiagnosticTrace{At: time.Now().UTC(), Session: diagnosticSession(s), Events: []Event{}, EpochHistory: []EpochEntry{}, Manifests: []*Manifest{}, Observations: []observability.Measurement{}, LatencyMS: map[string]float64{}, UnavailableMetrics: []string{}, HistoryLimit: 8192}
	epochs := map[uint64]*EpochEntry{}
	if len(c.state.Events) > 0 {
		r.HistoryFirstSequence = c.state.Events[0].Sequence
	}
	for _, event := range c.state.Events {
		if event.TenantID == tenant && event.SessionID == s.SessionID {
			// Error details can contain upstream bodies; diagnostic migration history is metadata only.
			event.Detail = ""
			r.Events = append(r.Events, event)
			if event.Epoch > 0 {
				e := epochs[event.Epoch]
				if e == nil {
					e = &EpochEntry{Epoch: event.Epoch, FirstObservedAt: event.At}
					epochs[event.Epoch] = e
				}
				e.LastObservedAt = event.At
			}
		}
	}
	for _, e := range epochs {
		r.EpochHistory = append(r.EpochHistory, *e)
	}
	sort.Slice(r.EpochHistory, func(i, j int) bool { return r.EpochHistory[i].Epoch < r.EpochHistory[j].Epoch })
	manifests := map[string]*Manifest{}
	if s.Manifest != nil {
		manifests[s.Manifest.ID] = s.Manifest
	}
	for _, receipt := range s.Lifecycle {
		if receipt.Manifest != nil {
			manifests[receipt.Manifest.ID] = receipt.Manifest
		}
	}
	for _, manifest := range manifests {
		r.Manifests = append(r.Manifests, copyValue(manifest))
	}
	sort.Slice(r.Manifests, func(i, j int) bool { return r.Manifests[i].CreatedAt.Before(r.Manifests[j].CreatedAt) })
	seen := map[string]bool{}
	for _, item := range c.state.Observations {
		if item.TenantID == tenant && item.SessionID == s.SessionID {
			r.Observations = append(r.Observations, item.Measurement)
			seen[item.Metric] = true
			if strings.HasSuffix(item.Metric, "_ms") {
				r.LatencyMS[item.Metric] += item.Value
			}
		}
	}
	for _, metric := range []string{"gateway_queue_ms", "dynamo_admission_wait_ms", "substrate_suspend_ms", "durability_wait_ms", "substrate_resume_ms", "stranded_worker_gb_s", "dynamo_released_acting_tokens"} {
		if !seen[metric] {
			r.UnavailableMetrics = append(r.UnavailableMetrics, metric)
		}
	}
	return r, nil
}
func (c *Controller) registerGauges() {
	for name, help := range map[string]string{"agentcloud_sessions": "Current coordinator sessions.", "agentcloud_active_tools": "Current in-flight tools.", "agentcloud_pending_model_requests": "Durable PENDING and DISPATCHING requests.", "agentcloud_unknown_model_requests": "Durable OUTCOME_UNKNOWN requests."} {
		c.telemetry.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
			c.lock()
			defer c.mu.Unlock()
			value := 0
			for _, s := range c.state.Sessions {
				switch name {
				case "agentcloud_sessions":
					value++
				case "agentcloud_active_tools":
					value += s.ActiveTools
				}
			}
			for _, r := range c.state.Requests {
				switch name {
				case "agentcloud_pending_model_requests":
					if r.Status == "PENDING" || r.Status == "DISPATCHING" {
						value++
					}
				case "agentcloud_unknown_model_requests":
					if r.Status == "OUTCOME_UNKNOWN" {
						value++
					}
				}
			}
			return float64(value)
		}))
	}
}
func (c *Controller) traceHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := observability.Extract(observability.WithRuntime(r.Context(), c.telemetry), r.Header)
		ctx, span := observability.Start(ctx, "coordinator.http", trace.SpanKindServer)
		defer span.End()
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		span.SetAttributes(attribute.String("http.request.method", r.Method), attribute.String("http.route", route))
	})
}
