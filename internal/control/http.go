package control

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func decode(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return problem(400, "invalid_json", err.Error())
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return problem(400, "invalid_json", "expected exactly one JSON object")
	}
	return nil
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, err error) {
	var api *APIError
	if !errors.As(err, &api) {
		api = &APIError{"internal_error", "request could not be durably completed", 500}
	}
	if api.Code == "resume_capacity_wait" {
		w.Header().Set("Retry-After", "1")
	}
	respond(w, api.Status, map[string]any{"error": map[string]string{"code": api.Code, "message": api.Message}})
}

type endpoint func(http.ResponseWriter, *http.Request, string) error

func (c *Controller) authenticated(next endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		tenant := ""
		if token != r.Header.Get("Authorization") {
			for candidate, value := range c.cfg.Tokens {
				if subtle.ConstantTimeCompare([]byte(token), []byte(candidate)) == 1 {
					tenant = value
				}
			}
		}
		if tenant == "" {
			failure(w, problem(401, "unauthorized", "valid bearer credentials are required"))
			return
		}
		if err := next(w, r, tenant); err != nil {
			failure(w, err)
		}
	}
}
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions/{id}/model-boundary", c.ModelBoundaryEndpoint())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]any{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c.lock()
		err := c.ready()
		c.mu.Unlock()
		if err == nil {
			err = c.store.Check(r.Context())
		}
		if err != nil {
			respond(w, 503, map[string]any{"status": "unavailable"})
			return
		}
		respond(w, 200, map[string]any{"status": "ready"})
	})
	mux.HandleFunc("GET /v1/capabilities", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		groups := []string{}
		if c.cfg.EnableExperimentControls {
			groups = []string{"A", "B", "C", "D", "E"}
		}
		respond(w, 200, map[string]any{"protocol_version": 1, "execution_backend": c.cfg.Backend.Name(), "journal_backend": c.store.Backend(), "durable_requests": true, "streaming": true, "first_token_observation": "upstream_sse_nonempty_content", "first_token_kinds": []string{"content", "reasoning_content", "refusal", "tool_call_arguments"}, "execution_feedback": c.cfg.Signals != nil, "autonomous_scheduler_enabled": c.cfg.EnableAutonomousScheduler, "scheduler_policy": c.cfg.SchedulerPolicy, "scheduler_effective_policy": c.schedulerPolicy(), "scheduler_shadow": c.cfg.SchedulerShadow, "experiment_controls_enabled": c.cfg.EnableExperimentControls, "ablation_groups": groups, "ablation_controls": map[string]any{"state_feedback_groups": []string{"E"}, "request_final_groups": []string{"A", "B"}, "model_boundary_groups": []string{"D", "E"}, "native_inference_admission": true}, "single_writer": true, "tools": []string{"file_read", "file_write", "shell"}})
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input CreateSession
		if err := decode(w, r, &input); err != nil {
			return err
		}
		s, err := c.Create(r.Context(), tenant, input)
		if err != nil {
			return err
		}
		respond(w, 201, s)
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		respond(w, 200, map[string]any{"sessions": c.List(tenant)})
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions/{id}", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		s, err := c.Get(tenant, r.PathValue("id"))
		if err != nil {
			return err
		}
		respond(w, 200, s)
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions/{id}/tools", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input ToolRequest
		if err := decode(w, r, &input); err != nil {
			return err
		}
		receipt, err := c.Tool(r.Context(), tenant, r.PathValue("id"), input)
		if err != nil {
			return err
		}
		code := 200
		if receipt.Status == "RUNNING" {
			code = 202
		}
		respond(w, code, receipt)
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions/{id}/status", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		result, err := c.SessionStatus(tenant, r.PathValue("id"))
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions/{id}/tools/{tool}", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		receipt, err := c.ToolStatus(tenant, r.PathValue("id"), r.PathValue("tool"))
		if err != nil {
			return err
		}
		respond(w, 200, receipt)
		return nil
	}))
	for _, action := range []string{"checkpoint", "suspend", "resume"} {
		mux.HandleFunc("POST /v1/sessions/{id}/"+action, c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
			var input LifecycleRequest
			if err := decode(w, r, &input); err != nil {
				return err
			}
			s, manifest, err := c.Lifecycle(r.Context(), tenant, r.PathValue("id"), action, input)
			if err != nil {
				return err
			}
			if action == "checkpoint" {
				respond(w, 200, manifest)
			} else {
				respond(w, 200, s)
			}
			return nil
		}))
	}
	mux.HandleFunc("POST /v1/sessions/{id}/requests", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input struct {
			ID           string          `json:"logical_request_id"`
			Payload      json.RawMessage `json:"payload"`
			TerminalTurn bool            `json:"terminal_turn,omitempty"`
		}
		if err := decode(w, r, &input); err != nil {
			return err
		}
		result, err := c.submitWithContext(r.Context(), tenant, r.PathValue("id"), input.ID, input.Payload, input.TerminalTurn)
		if err != nil {
			return err
		}
		respond(w, 202, result)
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions/{id}/requests/{request}", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		result, err := c.Request(tenant, r.PathValue("id"), r.PathValue("request"), 0)
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions/{id}/requests/{request}/claim", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input struct {
			Epoch uint64 `json:"epoch"`
		}
		if err := decode(w, r, &input); err != nil {
			return err
		}
		if input.Epoch == 0 {
			return problem(400, "invalid_epoch", "claim epoch is required")
		}
		result, err := c.Request(tenant, r.PathValue("id"), r.PathValue("request"), input.Epoch)
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("GET /v1/events", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		after, err := strconv.ParseUint(r.URL.Query().Get("after_seq"), 10, 64)
		if r.URL.Query().Get("after_seq") != "" && err != nil {
			return problem(400, "invalid_cursor", "after_seq must be an unsigned integer")
		}
		c.lock()
		defer c.mu.Unlock()
		if after > 0 && len(c.state.Events) > 0 && after < c.state.Events[0].Sequence-1 {
			return problem(410, "event_gap", "event cursor precedes retained history")
		}
		events := []Event{}
		for _, event := range c.state.Events {
			if event.TenantID == tenant && event.Sequence > after {
				events = append(events, event)
			}
		}
		respond(w, 200, map[string]any{"events": events, "last_sequence": c.state.EventSequence})
		return nil
	}))
	mux.HandleFunc("GET /v1/sessions/{id}/native", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		s, err := c.Get(tenant, r.PathValue("id"))
		if err != nil {
			return err
		}
		bridge, ok := c.cfg.Signals.(*DynamoBridge)
		if !ok {
			return problem(409, "native_dynamo_unavailable", "native Dynamo bridge is required")
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		receipt, err := bridge.Session(ctx, s)
		if err != nil {
			return problem(503, "native_observation_unavailable", err.Error())
		}
		respond(w, 200, map[string]any{"epoch": s.Epoch, "snapshot": receipt.Snapshot})
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions/{id}/finalize", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		result, err := c.FinalizeDynamo(r.Context(), tenant, r.PathValue("id"))
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions/{id}/return-demand", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input LifecycleRequest
		if err := decode(w, r, &input); err != nil {
			return err
		}
		result, err := c.ReturnDemand(r.Context(), tenant, r.PathValue("id"), input)
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("POST /v1/sessions/{id}/idle-window", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		var input IdleWindowRequest
		if err := decode(w, r, &input); err != nil {
			return err
		}
		result, err := c.ObserveIdle(r.Context(), tenant, r.PathValue("id"), input)
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("GET /v1/diagnostics/status", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		respond(w, 200, c.DiagnosticStatus(r.Context(), tenant, r.URL.Query().Get("probe") == "true"))
		return nil
	}))
	mux.HandleFunc("GET /v1/workspaces/{id}/trace", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		result, err := c.DiagnosticTrace(tenant, r.PathValue("id"))
		if err != nil {
			return err
		}
		respond(w, 200, result)
		return nil
	}))
	mux.HandleFunc("GET /metrics", c.authenticated(func(w http.ResponseWriter, r *http.Request, tenant string) error {
		c.telemetry.Handler().ServeHTTP(w, r)
		return nil
	}))

	return c.traceHTTP(mux)
}
