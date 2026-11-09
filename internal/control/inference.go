package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"go.opentelemetry.io/otel/trace"
)

type InferenceIdentity interface {
	Headers(context.Context, *Session) (http.Header, error)
}

func (c *Controller) Submit(tenant, id, logicalID string, payload json.RawMessage) (*ModelRequest, error) {
	return c.SubmitWithOptions(tenant, id, logicalID, payload, false)
}

func (c *Controller) SubmitWithOptions(tenant, id, logicalID string, payload json.RawMessage, terminalTurn bool) (*ModelRequest, error) {
	return c.submitWithContext(context.Background(), tenant, id, logicalID, payload, terminalTurn)
}
func (c *Controller) submitWithContext(ctx context.Context, tenant, id, logicalID string, payload json.RawMessage, terminalTurn bool) (*ModelRequest, error) {
	ctx = c.operationContext(ctx, tenant, id, logicalID)
	ctx, span := observability.Start(ctx, "coordinator.model.register", trace.SpanKindInternal)
	defer span.End()
	if !validID(logicalID) {
		return nil, problem(400, "invalid_request_id", "logical_request_id is required")
	}
	if len(payload) == 0 || len(payload) > 1<<20 {
		return nil, problem(400, "invalid_payload", "model payload must fit 1 MiB")
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return nil, problem(400, "invalid_payload", "model payload must be a JSON object")
	}
	if stream, exists := object["stream"]; exists {
		if _, ok := stream.(bool); !ok {
			return nil, problem(400, "invalid_payload", "stream must be a boolean")
		}
	} else {
		object["stream"] = false
	}
	if nvext, ok := object["nvext"].(map[string]any); ok {
		if _, exists := nvext["agent_context"]; exists {
			return nil, problem(400, "identity_override", "agent_context is derived from the authenticated session")
		}
	}
	canonical, _ := json.Marshal(object)
	hash := payloadHash(json.RawMessage(canonical))
	if terminalTurn {
		hash = payloadHash(struct {
			Payload      json.RawMessage `json:"payload"`
			TerminalTurn bool            `json:"terminal_turn"`
		}{canonical, true})
	}
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	if r := c.state.Requests[key(tenant, logicalID)]; r != nil {
		if r.PayloadHash != hash || r.SessionID != id {
			return nil, problem(409, "payload_conflict", "logical request ID has another payload or session")
		}
		return copyValue(r), nil
	}
	if s.Gate != "OPEN" || s.State != "RUNNING" {
		return nil, problem(423, "admission_closed", "model registration requires open session admission")
	}
	if len(c.state.Requests) >= c.cfg.MaxRequests {
		return nil, problem(429, "request_quota", "durable request capacity reached")
	}
	pending := 0
	for _, r := range c.state.Requests {
		if r.TenantID == tenant && (r.Status == "PENDING" || r.Status == "DISPATCHING") {
			pending++
		}
	}
	if pending >= 64 {
		return nil, problem(429, "pending_quota", "tenant pending request capacity reached")
	}
	now := time.Now().UTC()
	r := &ModelRequest{ID: logicalID, TenantID: tenant, SessionID: id, PayloadHash: hash, Payload: canonical, TerminalTurn: terminalTurn, Status: "PENDING", CreatedAt: now, UpdatedAt: now}
	headers := make(http.Header)
	observability.Inject(ctx, headers)
	r.TraceParent = headers.Get("traceparent")
	r.TraceState = headers.Get("tracestate")
	s.IdleSince = time.Time{}
	s.DynamoFinalized = false
	s.DynamoFinalizedSnapshot = nil
	c.state.Requests[key(tenant, logicalID)] = r
	c.event(s, "model.registered", logicalID)
	if err = c.persistContext(ctx); err != nil {
		return nil, err
	}
	return copyValue(r), nil
}
func (c *Controller) Request(tenant, id, requestID string, claimEpoch uint64) (*ModelRequest, error) {
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	r := c.state.Requests[key(tenant, requestID)]
	if r == nil || r.SessionID != id {
		return nil, problem(404, "request_not_found", "model request not found")
	}
	if claimEpoch != 0 {
		if claimEpoch != s.Epoch || s.Gate != "OPEN" {
			return nil, problem(409, "stale_epoch", "claim requires the active execution epoch")
		}
		if r.Status != "COMPLETED" {
			return nil, problem(409, "result_unavailable", "model outcome is not completed")
		}
		r.ClaimedEpoch = claimEpoch
		c.markEntity("requests", key(tenant, requestID))
		if err = c.persistContext(c.operationContext(c.ctx, tenant, id, requestID)); err != nil {
			return nil, err
		}
	}
	return copyValue(r), nil
}
func (c *Controller) dispatchLoop() {
	defer c.workers.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		c.dispatchOne()
	}
}
func (c *Controller) dispatchOne() {
	c.lock()
	if c.unhealthy != nil || c.closing {
		c.mu.Unlock()
		return
	}
	pending := []*ModelRequest{}
	for _, r := range c.state.Requests {
		if r.Status == "PENDING" {
			pending = append(pending, r)
		}
	}
	if len(pending) == 0 {
		c.mu.Unlock()
		return
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].CreatedAt.Before(pending[j].CreatedAt) })
	r := pending[0]
	headers := make(http.Header)
	headers.Set("traceparent", r.TraceParent)
	headers.Set("tracestate", r.TraceState)
	parent := observability.Extract(c.ctx, headers)
	parent = c.operationContext(parent, r.TenantID, r.SessionID, r.ID)
	parent, span := observability.Start(parent, "coordinator.model.dispatch", trace.SpanKindInternal)
	defer span.End()
	selectedAt := time.Now().UTC()
	r.DispatchedAt = &selectedAt
	c.observeLocked(parent, "gateway_queue_ms", float64(selectedAt.Sub(r.CreatedAt))/float64(time.Millisecond), "durable_pending_to_dispatch_selection")
	s := c.state.Sessions[key(r.TenantID, r.SessionID)]
	r.Status = "DISPATCHING"
	r.AttemptID = newID()
	r.UpdatedAt = time.Now().UTC()
	c.event(s, "model.dispatching", r.ID)
	if err := c.persistContext(parent); err != nil {
		c.mu.Unlock()
		return
	}
	snapshot := copyValue(s)
	request := copyValue(r)
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, c.cfg.ModelTimeout)
	result, err := c.infer(ctx, snapshot, request)
	cancel()
	c.lock()
	r.UpdatedAt = time.Now().UTC()
	if err != nil {
		r.Status = "OUTCOME_UNKNOWN"
		r.Error = err.Error()
	} else {
		r.Status = "COMPLETED"
		r.Result = result
		if s.ActiveTools == 0 {
			s.IdleSince = time.Now().UTC()
		}
		if c.cfg.EnableAutonomousScheduler && s.Mode == "in-actor" {
			if s.Autonomous == nil {
				s.Autonomous = &AutonomousState{Epoch: s.Epoch}
			}
			s.Autonomous.ReturnDemand = true
			s.Autonomous.ReturnRequestedAt = r.UpdatedAt
		}
	}
	c.event(s, "model."+strings.ToLower(r.Status), r.ID)
	_ = c.persistContext(parent)
	snapshot = copyValue(s)
	c.mu.Unlock()
	if err == nil {
		c.signalContext(parent, snapshot, "RUNNING_FAST")
	}
}
func (c *Controller) infer(ctx context.Context, s *Session, r *ModelRequest) (output json.RawMessage, err error) {
	ctx, span := observability.Start(ctx, "dynamo.inference", trace.SpanKindClient)
	defer func() { observability.Finish(span, err) }()
	if c.cfg.ModelURL == "mock://deterministic" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		var p map[string]any
		_ = json.Unmarshal(r.Payload, &p)
		content := "agentcloud deterministic response"
		if messages, ok := p["messages"].([]any); ok && len(messages) > 0 {
			if last, ok := messages[len(messages)-1].(map[string]any); ok {
				content = fmt.Sprint(last["content"])
			}
		}
		return json.Marshal(map[string]any{"id": r.AttemptID, "object": "chat.completion", "model": "agentcloud-deterministic", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}, "execution_kind": "deterministic-model"})
	}
	if c.cfg.ModelURL == "" {
		return nil, errors.New("model endpoint is not configured")
	}
	url := strings.TrimRight(c.cfg.ModelURL, "/")
	if !strings.HasSuffix(url, "/chat/completions") {
		if strings.HasSuffix(url, "/v1") {
			url += "/chat/completions"
		} else {
			url += "/v1/chat/completions"
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(r.Payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dynamo-Session-ID", payloadHash([]string{s.TenantID, s.WorkspaceID, s.SessionID})[:32])
	req.Header.Set("X-Request-ID", r.AttemptID)
	req.Header.Set("X-AgentCloud-Attempt-ID", r.AttemptID)
	observability.Inject(ctx, req.Header)
	if c.cfg.ModelAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.ModelAPIKey)
	}
	if identity, ok := c.cfg.Signals.(InferenceIdentity); ok {
		headers, err := identity.Headers(ctx, s)
		if err != nil {
			return nil, err
		}
		for k, values := range headers {
			req.Header[k] = values
		}
	}
	if r.TerminalTurn || s.AblationGroup == "A" || s.AblationGroup == "B" {
		req.Header.Set("X-Dynamo-Session-Final", "true")
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, MaxIdleConns: 16, IdleConnTimeout: 30 * time.Second}, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	httpStart := time.Now()
	defer func() {
		observability.Observe(ctx, "model_http_ms", float64(time.Since(httpStart))/float64(time.Millisecond), "inference_http_roundtrip_and_body")
		c.observeAdmission(context.WithoutCancel(ctx), s, r, err == nil && (r.TerminalTurn || s.AblationGroup == "A" || s.AblationGroup == "B"))
	}()
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var payload struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(r.Payload, &payload)
	if response.StatusCode >= 200 && response.StatusCode < 300 && payload.Stream {
		return c.readSSE(response.Body, r)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 2<<20 {
		return nil, errors.New("model response exceeds 2 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("model HTTP %d: %.1024s", response.StatusCode, data)
	}
	if !json.Valid(data) {
		return nil, errors.New("model returned invalid JSON")
	}
	return data, nil
}

// First-token progress is durable while the same physical attempt is in flight.
func (c *Controller) recordFirstToken(request *ModelRequest, kind, token string) error {
	if token == "" {
		return nil
	}
	now := time.Now().UTC()
	c.lock()
	defer c.mu.Unlock()
	r := c.state.Requests[key(request.TenantID, request.ID)]
	if r == nil || r.AttemptID != request.AttemptID || r.Status != "DISPATCHING" {
		return errors.New("stream progress has no active dispatch intent")
	}
	if r.FirstTokenAt != nil {
		return nil
	}
	previousUpdatedAt := r.UpdatedAt
	r.FirstToken, r.FirstTokenKind, r.FirstTokenAt, r.UpdatedAt = token, kind, &now, now
	c.event(c.state.Sessions[key(r.TenantID, r.SessionID)], "model.first_token", r.ID)
	if err := c.persistContext(c.operationContext(c.ctx, request.TenantID, request.SessionID, request.ID)); err != nil {
		r.FirstToken, r.FirstTokenKind, r.FirstTokenAt, r.UpdatedAt = "", "", nil, previousUpdatedAt
		return err
	}
	return nil
}

type streamText struct {
	value strings.Builder
	seen  bool
}

func (s *streamText) append(value any) error {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return errors.New("stream text delta must be a string")
	}
	s.seen = true
	s.value.WriteString(text)
	return nil
}

type streamFunction struct {
	name      streamText
	arguments streamText
}

func (f *streamFunction) append(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return errors.New("stream function delta must be an object")
	}
	for field, value := range object {
		switch field {
		case "name":
			if err := f.name.append(value); err != nil {
				return err
			}
		case "arguments":
			if err := f.arguments.append(value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported stream function field %q", field)
		}
	}
	return nil
}
func (f *streamFunction) result() map[string]any {
	return map[string]any{"name": f.name.value.String(), "arguments": f.arguments.value.String()}
}

type streamTool struct {
	id       string
	kind     string
	function streamFunction
}
type streamChoice struct {
	role      string
	content   streamText
	reasoning streamText
	refusal   streamText
	function  *streamFunction
	tools     map[int]*streamTool
	finish    any
	logprobs  map[string]any
}
type streamCompletion struct {
	metadata map[string]any
	choices  map[int]*streamChoice
}

func streamIndex(value any) (int, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("stream choice/tool index must be an integer")
	}
	index, err := number.Int64()
	if err != nil || index < 0 || index > 65535 {
		return 0, errors.New("stream choice/tool index is out of range")
	}
	return int(index), nil
}
func (s *streamCompletion) append(data string, first func(string, string) error) error {
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.UseNumber()
	var chunk map[string]any
	if err := decoder.Decode(&chunk); err != nil || chunk == nil {
		return errors.New("model stream returned invalid JSON object")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("model stream returned invalid JSON")
	}
	if value := chunk["error"]; value != nil {
		return fmt.Errorf("model stream error: %v", value)
	}
	for field, value := range chunk {
		if field != "choices" && field != "object" && value != nil {
			s.metadata[field] = value
		}
	}
	values, ok := chunk["choices"].([]any)
	if !ok {
		return errors.New("model stream choices must be an array")
	}
	for _, value := range values {
		item, ok := value.(map[string]any)
		if !ok {
			return errors.New("model stream choice must be an object")
		}
		index, err := streamIndex(item["index"])
		if err != nil {
			return err
		}
		choice := s.choices[index]
		if choice == nil {
			choice = &streamChoice{role: "assistant", tools: map[int]*streamTool{}}
			s.choices[index] = choice
		}
		if finish := item["finish_reason"]; finish != nil {
			if _, ok := finish.(string); !ok {
				return errors.New("stream finish_reason must be a string")
			}
			choice.finish = finish
		}
		if value := item["logprobs"]; value != nil {
			object, ok := value.(map[string]any)
			if !ok {
				return errors.New("stream logprobs must be an object")
			}
			if choice.logprobs == nil {
				choice.logprobs = map[string]any{}
			}
			for field, value := range object {
				if parts, ok := value.([]any); ok {
					prior, _ := choice.logprobs[field].([]any)
					choice.logprobs[field] = append(prior, parts...)
				} else {
					choice.logprobs[field] = value
				}
			}
		}
		delta, ok := item["delta"].(map[string]any)
		if !ok {
			return errors.New("model stream delta must be an object")
		}
		// Keep an explicit order for reproducible observation when a chunk has multiple text fields.
		for _, field := range []string{"content", "reasoning_content", "refusal"} {
			value, exists := delta[field]
			if !exists {
				continue
			}
			var target *streamText
			switch field {
			case "content":
				target = &choice.content
			case "reasoning_content":
				target = &choice.reasoning
			case "refusal":
				target = &choice.refusal
			}
			if err := target.append(value); err != nil {
				return err
			}
			if text, ok := value.(string); ok && text != "" {
				if err := first(field, text); err != nil {
					return err
				}
			}
		}
		for field, value := range delta {
			if value == nil {
				continue
			}
			switch field {
			case "content", "reasoning_content", "refusal":
			case "role":
				role, ok := value.(string)
				if !ok {
					return errors.New("stream role must be a string")
				}
				if role != "" {
					choice.role = role
				}
			case "function_call":
				if choice.function == nil {
					choice.function = &streamFunction{}
				}
				if err := choice.function.append(value); err != nil {
					return err
				}
				if err := firstArguments(value, first); err != nil {
					return err
				}
			case "tool_calls":
				calls, ok := value.([]any)
				if !ok {
					return errors.New("stream tool_calls must be an array")
				}
				for _, raw := range calls {
					call, ok := raw.(map[string]any)
					if !ok {
						return errors.New("stream tool call must be an object")
					}
					tindex, err := streamIndex(call["index"])
					if err != nil {
						return err
					}
					tool := choice.tools[tindex]
					if tool == nil {
						tool = &streamTool{}
						choice.tools[tindex] = tool
					}
					for key, value := range call {
						switch key {
						case "index":
						case "id", "type":
							text, ok := value.(string)
							if !ok {
								return errors.New("stream tool identity must be a string")
							}
							if key == "id" {
								tool.id += text
							} else {
								tool.kind = text
							}
						case "function":
							if err := tool.function.append(value); err != nil {
								return err
							}
							if err := firstArguments(value, first); err != nil {
								return err
							}
						default:
							return fmt.Errorf("unsupported stream tool field %q", key)
						}
					}
				}
			default:
				return fmt.Errorf("unsupported stream delta field %q", field)
			}
		}
	}
	return nil
}
func firstArguments(value any, first func(string, string) error) error {
	object, _ := value.(map[string]any)
	if text, ok := object["arguments"].(string); ok && text != "" {
		return first("tool_call_arguments", text)
	}
	return nil
}
func (s *streamCompletion) result() (json.RawMessage, error) {
	if len(s.choices) == 0 {
		return nil, errors.New("model stream completed without choices")
	}
	indexes := []int{}
	for index := range s.choices {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	choices := []any{}
	for _, index := range indexes {
		c := s.choices[index]
		if c.finish == nil {
			return nil, errors.New("model stream completed without a finish reason")
		}
		message := map[string]any{"role": c.role, "content": nil}
		if c.content.seen {
			message["content"] = c.content.value.String()
		}
		if c.reasoning.seen {
			message["reasoning_content"] = c.reasoning.value.String()
		}
		if c.refusal.seen {
			message["refusal"] = c.refusal.value.String()
		}
		if c.function != nil {
			message["function_call"] = c.function.result()
		}
		if len(c.tools) > 0 {
			indexes := []int{}
			for index := range c.tools {
				indexes = append(indexes, index)
			}
			sort.Ints(indexes)
			tools := []any{}
			for _, index := range indexes {
				tool := c.tools[index]
				if tool.id == "" || tool.kind != "function" || tool.function.name.value.Len() == 0 {
					return nil, errors.New("model stream completed with incomplete tool call")
				}
				tools = append(tools, map[string]any{"id": tool.id, "type": tool.kind, "function": tool.function.result()})
			}
			message["tool_calls"] = tools
		}
		item := map[string]any{"index": index, "message": message, "finish_reason": c.finish}
		if c.logprobs != nil {
			item["logprobs"] = c.logprobs
		}
		choices = append(choices, item)
	}
	s.metadata["object"], s.metadata["choices"] = "chat.completion", choices
	result, err := json.Marshal(s.metadata)
	if len(result) > 2<<20 {
		return nil, errors.New("model response exceeds 2 MiB")
	}
	return result, err
}
func (c *Controller) readSSE(body io.Reader, request *ModelRequest) (json.RawMessage, error) {
	const maxResponse = 2 << 20
	limited := &io.LimitedReader{R: body, N: maxResponse + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxResponse+1)
	completion := streamCompletion{metadata: map[string]any{}, choices: map[int]*streamChoice{}}
	var data []string
	seenFirst := false
	first := func(kind, token string) error {
		if seenFirst {
			return nil
		}
		if err := c.recordFirstToken(request, kind, token); err != nil {
			return err
		}
		seenFirst = true
		return nil
	}
	consume := func() (bool, error) {
		if len(data) == 0 {
			return false, nil
		}
		payload := strings.Join(data, "\n")
		data = nil
		if strings.TrimSpace(payload) == "[DONE]" {
			return true, nil
		}
		if payload == "" {
			return false, nil
		}
		return false, completion.append(payload, first)
	}
	for scanner.Scan() {
		if limited.N == 0 {
			return nil, errors.New("model response exceeds 2 MiB")
		}
		line := scanner.Text()
		if line == "" {
			done, err := consume()
			if err != nil {
				return nil, err
			}
			if done {
				return completion.result()
			}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		if field == "data" {
			data = append(data, strings.TrimPrefix(value, " "))
		}
	}
	if limited.N == 0 {
		return nil, errors.New("model response exceeds 2 MiB")
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("model stream read failed: %w", err)
	}
	done, err := consume()
	if err != nil {
		return nil, err
	}
	if done {
		return completion.result()
	}
	return nil, errors.New("model stream ended before [DONE]")
}

func (c *Controller) observeAdmission(ctx context.Context, s *Session, r *ModelRequest, finalize bool) {
	bridge, ok := c.cfg.Signals.(*DynamoBridge)
	if !ok {
		observability.Unavailable(ctx, "dynamo_admission_wait_ms")
		return
	}
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	receipt, err := bridge.Session(probe, s)
	if err != nil {
		observability.Unavailable(ctx, "dynamo_admission_wait_ms")
		return
	}
	observed := false
	for _, item := range receipt.Snapshot.AdmissionHistory {
		if item.ClientRequestID == r.AttemptID {
			observability.Observe(ctx, "dynamo_admission_wait_ms", item.WaitMS, "native_classifier_admission_history")
			observed = true
			break
		}
	}
	if !observed {
		observability.Unavailable(ctx, "dynamo_admission_wait_ms")
	}
	if finalize {
		if finalized, err := bridge.Finalize(probe, s); err == nil {
			c.lock()
			c.recordFinalizationLocked(s, finalized, r.ID)
			c.mu.Unlock()
		}
	}
}
