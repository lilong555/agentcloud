package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const streamPayload = `{"model":"live","messages":[{"role":"user","content":"test"}],"stream":true}`

func writeChunk(w http.ResponseWriter, delta string, finish string) {
	fmt.Fprintf(w, "data: {\"id\":\"completion-1\",\"object\":\"chat.completion.chunk\",\"created\":123,\"model\":\"live\",\"choices\":[{\"index\":0,\"delta\":%s,\"finish_reason\":%s}]}\n\n", delta, finish)
	w.(http.Flusher).Flush()
}

func streamRequest(t *testing.T, c *Controller, id string) *ModelRequest {
	t.Helper()
	r, err := c.Request("tenant-a", "stream-session", id, 0)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSSEFirstTokenDurableWhileStreamingAndReplay(t *testing.T) {
	var calls atomic.Int32
	roleSent, generate, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["stream"] != true {
			t.Errorf("upstream stream flag: %v %v", payload, err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": heartbeat\n\n")
		writeChunk(w, `{"role":"assistant","content":""}`, "null")
		close(roleSent)
		select {
		case <-generate:
		case <-r.Context().Done():
			return
		}
		writeChunk(w, `{"reasoning_content":"思考"}`, "null")
		select {
		case <-finish:
		case <-r.Context().Done():
			return
		}
		writeChunk(w, `{"reasoning_content":"步骤","content":"你好"}`, "null")
		writeChunk(w, `{"content":"，世界"}`, "null")
		writeChunk(w, `{}`, `"stop"`)
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7}}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	cfg := testConfig(t)
	cfg.ModelURL, cfg.ModelConcurrency = upstream.URL, 4
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()
	s := createTestSession(t, c, "tenant-a", "stream-session")
	if _, err := c.Submit("tenant-a", s.SessionID, "stream-request", json.RawMessage(streamPayload)); err != nil {
		t.Fatal(err)
	}
	<-roleSent
	if r := streamRequest(t, c, "stream-request"); r.FirstTokenAt != nil || r.FirstToken != "" {
		t.Fatalf("role-only observed as token: %+v", r)
	}
	var group sync.WaitGroup
	for i := 0; i < 12; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := c.Submit("tenant-a", s.SessionID, "stream-request", json.RawMessage(streamPayload)); err != nil {
				t.Errorf("replay: %v", err)
			}
		}()
	}
	group.Wait()
	close(generate)
	waitFor(t, func() bool { return streamRequest(t, c, "stream-request").FirstTokenAt != nil })
	progress := streamRequest(t, c, "stream-request")
	if progress.Status != "DISPATCHING" || progress.FirstToken != "思考" || progress.FirstTokenKind != "reasoning_content" {
		t.Fatalf("progress: %+v", progress)
	}
	journal, err := os.ReadFile(filepath.Join(cfg.DataDir, "journal.json"))
	if err != nil || !strings.Contains(string(journal), `"first_token"`) {
		t.Fatalf("durable first token: %v %s", err, journal)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/sessions/stream-session/requests/stream-request", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	w := httptest.NewRecorder()
	c.Handler().ServeHTTP(w, req)
	var api ModelRequest
	if err := json.Unmarshal(w.Body.Bytes(), &api); err != nil || api.FirstTokenAt == nil || api.Status != "DISPATCHING" {
		t.Fatalf("API progress %s: %v", w.Body, err)
	}
	close(finish)
	waitFor(t, func() bool { return streamRequest(t, c, "stream-request").Status == "COMPLETED" })
	result := streamRequest(t, c, "stream-request")
	var completion struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				Reasoning string `json:"reasoning_content"`
			} `json:"message"`
			Finish string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			Total int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(result.Result, &completion); err != nil || completion.Object != "chat.completion" || len(completion.Choices) != 1 || completion.Choices[0].Message.Content != "你好，世界" || completion.Choices[0].Message.Reasoning != "思考步骤" || completion.Choices[0].Finish != "stop" || completion.Usage.Total != 7 {
		t.Fatalf("accumulation %s: %v", result.Result, err)
	}
	claimed, err := c.Request("tenant-a", s.SessionID, "stream-request", s.Epoch)
	if err != nil || claimed.ClaimedEpoch != s.Epoch {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = nil
	c, err = New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.Submit("tenant-a", s.SessionID, "stream-request", json.RawMessage(streamPayload))
	if err != nil || replayed.Status != "COMPLETED" || replayed.FirstTokenAt == nil || !replayed.FirstTokenAt.Equal(*progress.FirstTokenAt) {
		t.Fatalf("durable replay %+v %v", replayed, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("physical requests=%d", calls.Load())
	}
}

func TestSSETerminalErrorsPreserveFirstTokenAndSingleAttempt(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"missing_done", "", 200},
		{"invalid_json", "data: {broken}\n\n", 200},
		{"missing_finish", "data: [DONE]\n\n", 200},
		{"oversized", "data: " + strings.Repeat("x", 2<<20) + "\n\n", 200},
		{"unsupported_delta", "data: {\"choices\":[{\"index\":0,\"delta\":{\"audio\":{}},\"finish_reason\":null}]}\n\n", 200},
		{"http_failure", "upstream error", 502},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(test.status)
				if test.status == 200 {
					writeChunk(w, `{"content":"first"}`, "null")
				}
				fmt.Fprint(w, test.body)
			}))
			defer upstream.Close()
			cfg := testConfig(t)
			cfg.ModelURL = upstream.URL
			c := newTestController(t, cfg)
			createTestSession(t, c, "tenant-a", "stream-session")
			if _, err := c.Submit("tenant-a", "stream-session", "failed", json.RawMessage(streamPayload)); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return streamRequest(t, c, "failed").Status == "OUTCOME_UNKNOWN" })
			r := streamRequest(t, c, "failed")
			if r.Error == "" || len(r.Result) != 0 || test.status == 200 && (r.FirstToken != "first" || r.FirstTokenAt == nil) {
				t.Fatalf("unknown outcome: %+v", r)
			}
			if replay, err := c.Submit("tenant-a", "stream-session", "failed", json.RawMessage(streamPayload)); err != nil || replay.Status != "OUTCOME_UNKNOWN" {
				t.Fatalf("error replay %+v %v", replay, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("requests=%d", calls.Load())
			}
		})
	}
}

func TestSSEInterruptedRestartKeepsProgressWithoutRedispatch(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeChunk(w, `{"content":"partial"}`, "null")
		<-r.Context().Done()
	}))
	defer upstream.Close()
	cfg := testConfig(t)
	cfg.ModelURL = upstream.URL
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, c, "tenant-a", "stream-session")
	if _, err := c.Submit("tenant-a", "stream-session", "interrupted", json.RawMessage(streamPayload)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return streamRequest(t, c, "interrupted").FirstTokenAt != nil })
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	r := streamRequest(t, c, "interrupted")
	if r.Status != "OUTCOME_UNKNOWN" || r.FirstToken != "partial" || r.FirstTokenAt == nil {
		t.Fatalf("interrupted replay: %+v", r)
	}
	if _, err := c.Submit("tenant-a", "stream-session", "interrupted", json.RawMessage(streamPayload)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("requests=%d", calls.Load())
	}
}

func TestSSEToolCallsAccumulateAndClaim(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeChunk(w, `{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":""}}]}`, "null")
		writeChunk(w, `{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]}`, "null")
		writeChunk(w, `{"tool_calls":[{"index":0,"function":{"arguments":"\"北京\"}"}}]}`, "null")
		writeChunk(w, `{}`, `"tool_calls"`)
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	cfg := testConfig(t)
	cfg.ModelURL = upstream.URL
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "stream-session")
	if _, err := c.Submit("tenant-a", s.SessionID, "tools", json.RawMessage(streamPayload)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return streamRequest(t, c, "tools").Status != "PENDING" && streamRequest(t, c, "tools").Status != "DISPATCHING"
	})
	r, err := c.Request("tenant-a", s.SessionID, "tools", s.Epoch)
	if err != nil || r.FirstTokenKind != "tool_call_arguments" {
		t.Fatalf("tool claim %+v %v", r, err)
	}
	var result struct {
		Choices []struct {
			Message struct {
				Tools []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(r.Result, &result); err != nil || len(result.Choices) != 1 || len(result.Choices[0].Message.Tools) != 1 {
		t.Fatalf("tool result %s %v", r.Result, err)
	}
	tool := result.Choices[0].Message.Tools[0]
	if tool.ID != "call-1" || tool.Function.Name != "lookup" || tool.Function.Arguments != `{"city":"北京"}` {
		t.Fatalf("tool accumulation %+v", tool)
	}
}

type experimentSignal struct {
	sends   atomic.Int32
	headers atomic.Int32
}

func (s *experimentSignal) Send(context.Context, *Session, string) error { s.sends.Add(1); return nil }
func (s *experimentSignal) Headers(context.Context, *Session) (http.Header, error) {
	s.headers.Add(1)
	return http.Header{"X-Dynamo-Inference-Lease": []string{"binding-lease"}}, nil
}

func TestExperimentGroupsIdentityHeadersAndFeedback(t *testing.T) {
	for _, group := range []string{"", "A", "B", "C", "D", "E"} {
		t.Run("group_"+group, func(t *testing.T) {
			sink := &experimentSignal{}
			var final string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				final = r.Header.Get("X-Dynamo-Session-Final")
				if r.Header.Get("X-Dynamo-Inference-Lease") != "binding-lease" {
					t.Error("missing binding lease")
				}
				fmt.Fprint(w, `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer upstream.Close()
			cfg := testConfig(t)
			cfg.ModelURL, cfg.Signals, cfg.EnableExperimentControls = upstream.URL, sink, true
			c := newTestController(t, cfg)
			input := CreateSession{SessionID: "stream-session", WorkspaceID: "workspace", AblationGroup: group}
			s, err := c.Create(context.Background(), "tenant-a", input)
			if err != nil || s.AblationGroup != group {
				t.Fatalf("group create %+v %v", s, err)
			}
			if replay, err := c.Create(context.Background(), "tenant-a", input); err != nil || replay.AblationGroup != group {
				t.Fatalf("group replay %+v %v", replay, err)
			}
			input.AblationGroup = "D"
			if group == "D" {
				input.AblationGroup = "E"
			}
			_, err = c.Create(context.Background(), "tenant-a", input)
			assertCode(t, err, "identity_conflict")
			if _, err := c.Submit("tenant-a", s.SessionID, "model", json.RawMessage(`{"model":"live"}`)); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return streamRequest(t, c, "model").Status == "COMPLETED" })
			c.signal(s, "RUNNING_FAST")
			if wantFinal := group == "A" || group == "B"; (final == "true") != wantFinal {
				t.Fatalf("group %s final=%q", group, final)
			}
			if sink.headers.Load() != 1 {
				t.Fatalf("group %s binding calls=%d", group, sink.headers.Load())
			}
			if wantFeedback := group == "" || group == "E"; (sink.sends.Load() > 0) != wantFeedback {
				t.Fatalf("group %s feedback=%d", group, sink.sends.Load())
			}
			if group == "A" || group == "B" || group == "C" {
				_, err := c.ModelBoundary(context.Background(), "tenant-a", s.SessionID, ModelBoundaryRequest{OperationID: "boundary", ExpectedEpoch: s.Epoch})
				assertCode(t, err, "ablation_policy_conflict")
			}
		})
	}
}

func TestExperimentDefaultDisabledCapabilitiesAndTerminalIdentity(t *testing.T) {
	cfg := testConfig(t)
	c := newTestController(t, cfg)
	_, err := c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "explicit", WorkspaceID: "explicit", AblationGroup: "A"})
	assertCode(t, err, "experiment_controls_disabled")
	_, err = c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "invalid", WorkspaceID: "invalid", AblationGroup: "F"})
	assertCode(t, err, "invalid_ablation_group")
	s := createTestSession(t, c, "tenant-a", "stream-session")
	_, err = c.SubmitWithOptions("tenant-a", s.SessionID, "terminal", json.RawMessage(streamPayload), true)
	if err != nil {
		t.Fatal("production terminal turn rejected", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/capabilities", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	w := httptest.NewRecorder()
	c.Handler().ServeHTTP(w, req)
	var caps map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &caps); err != nil || caps["streaming"] != true || caps["experiment_controls_enabled"] != false || len(caps["ablation_groups"].([]any)) != 0 {
		t.Fatalf("capabilities %s %v", w.Body, err)
	}

	var final atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		final.Store(r.Header.Get("X-Dynamo-Session-Final") == "true")
		fmt.Fprint(w, `{"choices":[]}`)
	}))
	defer upstream.Close()
	cfg = testConfig(t)
	cfg.EnableExperimentControls, cfg.ModelURL = true, upstream.URL
	c = newTestController(t, cfg)
	s, err = c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "stream-session", WorkspaceID: "terminal", AblationGroup: "C"})
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"model":"live"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/stream-session/requests", strings.NewReader(`{"logical_request_id":"terminal","payload":{"model":"live"},"terminal_turn":true}`))
	request.Header.Set("Authorization", "Bearer token-a")
	response := httptest.NewRecorder()
	c.Handler().ServeHTTP(response, request)
	var r ModelRequest
	if err := json.Unmarshal(response.Body.Bytes(), &r); response.Code != http.StatusAccepted || err != nil || !r.TerminalTurn {
		t.Fatalf("terminal HTTP %d %+v %v", response.Code, r, err)
	}
	if _, err := c.SubmitWithOptions("tenant-a", s.SessionID, "terminal", payload, true); err != nil {
		t.Fatal(err)
	}
	_, err = c.Submit("tenant-a", s.SessionID, "terminal", payload)
	assertCode(t, err, "payload_conflict")
	waitFor(t, func() bool { return streamRequest(t, c, "terminal").Status == "COMPLETED" })
	if !final.Load() {
		t.Fatal("terminal request did not finalize program")
	}
	_, err = c.Submit("tenant-a", s.SessionID, "invalid-stream", json.RawMessage(`{"stream":"true"}`))
	assertCode(t, err, "invalid_payload")
}
