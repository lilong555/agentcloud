// Package dynamo connects an authenticated coordinator to native ThunderAgent feedback.
package dynamo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/signals"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	SessionHeader        = "X-Dynamo-Session-ID"
	InferenceLeaseHeader = "X-AgentCloud-Inference-Lease"
	FinalHeader          = "X-Dynamo-Session-Final"
)

type Client struct {
	baseURL, token string
	http           *http.Client
	mu             sync.Mutex
	generations    map[bindingIdentity]registration
}

type bindingIdentity struct{ Source, Tenant, Workspace, Session string }
type registration struct {
	Binding    signals.Binding
	Generation string
}

func identity(b signals.Binding) bindingIdentity {
	return bindingIdentity{b.SourceID, b.TenantID, b.WorkspaceID, b.SessionID}
}

func IsRetiredGeneration(err error) bool {
	var api *APIError
	return errors.As(err, &api) && api.Status == http.StatusConflict && api.Code == "stale_generation"
}

type Capabilities struct {
	ProtocolVersion       int    `json:"protocol_version"`
	ExecutionFeedback     bool   `json:"execution_feedback"`
	TrustedBindings       bool   `json:"trusted_bindings"`
	OpaqueRoutingSessions bool   `json:"opaque_routing_sessions"`
	InferenceLeaseHeader  string `json:"inference_lease_header"`
	ClassifierCount       int    `json:"classifier_count"`
	MaxBodyBytes          int    `json:"max_body_bytes"`
	NativeFrontend        bool   `json:"native_frontend"`
}

type AdmissionObservation struct {
	RequestID       string     `json:"request_id"`
	ClientRequestID string     `json:"client_request_id"`
	WaitMS          float64    `json:"wait_ms"`
	QueuedAt        *time.Time `json:"queued_at,omitempty"`
	AdmittedAt      *time.Time `json:"admitted_at,omitempty"`
}
type Snapshot struct {
	SessionWaitingRequests uint64                 `json:"session_waiting_requests,omitempty"`
	SessionActiveRequests  uint64                 `json:"session_active_requests,omitempty"`
	AdmissionHistory       []AdmissionObservation `json:"admission_history,omitempty"`
	ReleasedActingTokens   *uint64                `json:"released_acting_tokens,omitempty"`
	ProgramPresent         bool                   `json:"program_present"`
	Phase                  *string                `json:"phase"`
	Lifecycle              *string                `json:"lifecycle"`
	BaselineTokens         *uint64                `json:"baseline_tokens"`
	LogicalBudgetTokens    *uint64                `json:"logical_budget_tokens"`
	ExecutionMultiplier    *float64               `json:"execution_multiplier"`
	State                  *signals.State         `json:"state"`
	Sequence               uint64                 `json:"sequence"`
	Fresh                  bool                   `json:"fresh"`
	WaitingRequests        uint64                 `json:"waiting_requests"`
	PausedPrograms         uint64                 `json:"paused_programs"`
	CompletedEvents        uint64                 `json:"completed_events"`
	AbortedEvents          uint64                 `json:"aborted_events"`
	PauseEvents            uint64                 `json:"pause_events"`
	FinalEvents            uint64                 `json:"final_events"`
}

type Receipt struct {
	BindingGeneration     string          `json:"binding_generation,omitempty"`
	SessionActiveRequests uint64          `json:"session_active_requests,omitempty"`
	ReleasedActingTokens  *uint64         `json:"released_acting_tokens,omitempty"`
	ProtocolVersion       int             `json:"protocol_version"`
	Accepted              bool            `json:"accepted"`
	Applied               bool            `json:"applied"`
	Reconciled            bool            `json:"reconciled"`
	Binding               signals.Binding `json:"binding"`
	RoutingSessionID      string          `json:"routing_session_id"`
	InferenceLease        string          `json:"inference_lease,omitempty"`
	ClassifierID          int             `json:"classifier_id"`
	Sequence              uint64          `json:"sequence"`
	ReceivedAt            time.Time       `json:"received_at"`
	Snapshot              Snapshot        `json:"snapshot"`
}
type RegisterResponse = Receipt

type APIError struct {
	Status int
	Code   string
}

func (e *APIError) Error() string { return fmt.Sprintf("dynamo control HTTP %d: %s", e.Status, e.Code) }

func NewClient(baseURL, token string, client *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid Dynamo control URL")
	}
	if len(token) < 32 || len(token) > 512 || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("Dynamo token must contain 32..512 bytes")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	// Preserve control credentials at their configured origin.
	copyClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), token: token, http: &copyClient, generations: map[bindingIdentity]registration{}}, nil
}

func (c *Client) call(ctx context.Context, method, path string, body any, response any) (err error) {
	ctx, span := observability.Start(ctx, "dynamo.control "+method+" "+path, trace.SpanKindClient)
	defer func() { observability.Finish(span, err) }()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		if len(data) > 8192 {
			return fmt.Errorf("Dynamo control request exceeds 8192 bytes")
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	observability.Inject(ctx, request.Header)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	result, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("Dynamo control: %w", err)
	}
	defer result.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", result.StatusCode))
	data, err := io.ReadAll(io.LimitReader(result.Body, 65537))
	if err != nil {
		return err
	}
	if len(data) > 65536 {
		return fmt.Errorf("Dynamo control response exceeds 65536 bytes")
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(result.StatusCode)
		}
		return &APIError{result.StatusCode, failure.Error}
	}
	if err = json.Unmarshal(data, response); err != nil {
		return fmt.Errorf("Dynamo control response: %w", err)
	}
	return nil
}
func (c *Client) Capabilities(ctx context.Context) (Capabilities, error) {
	var r Capabilities
	err := c.call(ctx, http.MethodGet, "/v1/execution/capabilities", nil, &r)
	if err == nil && (r.ProtocolVersion != 1 || !r.ExecutionFeedback || !r.TrustedBindings) {
		err = fmt.Errorf("Dynamo execution feedback capability is unavailable")
	}
	return r, err
}

type bindingRequest struct {
	RegistrationGeneration string `json:"registration_generation,omitempty"`
	ProtocolVersion        int    `json:"protocol_version"`
	signals.Binding
}

func validateReceipt(r Receipt, b signals.Binding) error {
	if r.ProtocolVersion != 1 || !r.Accepted || !r.Reconciled || r.Binding != b || !strings.HasPrefix(r.RoutingSessionID, "ac:") {
		return fmt.Errorf("Dynamo control receipt failed identity validation")
	}
	return nil
}
func (c *Client) Register(ctx context.Context, b signals.Binding) (RegisterResponse, error) {
	return c.register(ctx, b, c.generation(b))
}

// RegisterFresh is reserved for a new durable attempt, before sending inference.
func (c *Client) RegisterFresh(ctx context.Context, b signals.Binding) (RegisterResponse, error) {
	return c.register(ctx, b, "")
}
func (c *Client) register(ctx context.Context, b signals.Binding, generation string) (RegisterResponse, error) {
	var r Receipt
	err := c.call(ctx, http.MethodPost, "/v1/execution/bindings", bindingRequest{ProtocolVersion: 1, Binding: b, RegistrationGeneration: generation}, &r)
	if err == nil {
		err = validateReceipt(r, b)
		if err == nil && r.InferenceLease == "" {
			err = fmt.Errorf("Dynamo registration has no inference lease")
		}
	}
	if err == nil && r.BindingGeneration != "" {
		c.mu.Lock()
		c.generations[identity(b)] = registration{Binding: b, Generation: r.BindingGeneration}
		c.mu.Unlock()
	}
	return r, err
}
func (c *Client) Update(ctx context.Context, u signals.SandboxStateUpdate) (Receipt, error) {
	var r Receipt
	u.RegistrationGeneration = c.generation(u.Binding())
	err := c.call(ctx, http.MethodPost, "/v1/execution/state", u, &r)
	if err == nil {
		err = validateReceipt(r, u.Binding())
		if err == nil && r.Sequence != u.Sequence {
			err = fmt.Errorf("Dynamo state receipt sequence mismatch")
		}
	}
	return r, err
}
func (c *Client) Session(ctx context.Context, b signals.Binding) (Receipt, error) {
	var r Receipt
	err := c.call(ctx, http.MethodPost, "/v1/execution/session", bindingRequest{ProtocolVersion: 1, Binding: b, RegistrationGeneration: c.generation(b)}, &r)
	if err == nil {
		err = validateReceipt(r, b)
	}
	return r, err
}

// InferenceHeaders binds an inference turn to the registered native classifier program.
func (r Receipt) InferenceHeaders(final bool) http.Header {
	h := make(http.Header)
	h.Set(SessionHeader, r.RoutingSessionID)
	h.Set(InferenceLeaseHeader, r.InferenceLease)
	if final {
		h.Set(FinalHeader, "true")
	}
	return h
}

func (c *Client) generation(b signals.Binding) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.generations[identity(b)]
	if entry.Binding != b {
		return ""
	}
	return entry.Generation
}

type FinalizeReceipt struct {
	ProtocolVersion   int      `json:"protocol_version"`
	Accepted          bool     `json:"accepted"`
	Finalized         bool     `json:"finalized"`
	BindingGeneration string   `json:"binding_generation"`
	Snapshot          Snapshot `json:"snapshot"`
}

func (c *Client) Finalize(ctx context.Context, b signals.Binding) (FinalizeReceipt, error) {
	generation := c.generation(b)
	if generation == "" {
		return FinalizeReceipt{}, fmt.Errorf("Dynamo binding generation is unavailable")
	}
	var result FinalizeReceipt
	err := c.call(ctx, http.MethodPost, "/v1/execution/finalize", bindingRequest{ProtocolVersion: 1, Binding: b, RegistrationGeneration: generation}, &result)
	if err == nil && (result.ProtocolVersion != 1 || !result.Accepted || !result.Finalized || result.BindingGeneration != generation) {
		err = fmt.Errorf("Dynamo finalize receipt failed generation validation")
	}
	if err == nil {
		c.mu.Lock()
		if c.generations[identity(b)].Generation == generation {
			delete(c.generations, identity(b))
		}
		c.mu.Unlock()
	}
	return result, err
}
