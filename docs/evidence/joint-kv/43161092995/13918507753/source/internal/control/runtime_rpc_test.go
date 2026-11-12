package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeToolRetryPreservesPayloadAndPendingClaim(t *testing.T) {
	var lock sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lock.Lock()
		bodies = append(bodies, body)
		attempt := len(bodies)
		lock.Unlock()
		switch attempt {
		case 1:
			w.WriteHeader(503)
			_, _ = w.Write([]byte("upstream call failed"))
		case 2:
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"error":{"code":"RUNNING"}}`))
		default:
			_ = json.NewEncoder(w).Encode(ToolReceipt{ID: "original", Epoch: 3, Status: "COMPLETED", Sequence: 7, Result: ToolResult{Content: "durable"}})
		}
	}))
	defer server.Close()
	b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: server.URL}, http: server.Client()}
	input := ToolRequest{ID: "original", Epoch: 3, Kind: "file_write", Path: "answer.txt", Content: "durable", TimeoutMS: 3000}
	result, err := b.Tool(context.Background(), &Session{Epoch: 3, Actor: Actor{Name: "actor"}}, input)
	if err != nil || result.Content != "durable" {
		t.Fatalf("durable claim was not recovered: %+v %v", result, err)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(bodies) != 3 || !bytes.Equal(bodies[0], bodies[1]) || !bytes.Equal(bodies[0], bodies[2]) {
		t.Fatalf("retry changed the original epoch or payload: %q", bodies)
	}
}

func TestRuntimeRetryStopsAtUnknownConflictAuthAndCancellation(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		code   string
		cancel bool
	}{
		{name: "unknown", status: 409, code: "OUTCOME_UNKNOWN"},
		{name: "tool-timeout", status: 504, code: "OUTCOME_UNKNOWN"},
		{name: "identity-unavailable", status: 503, code: "ACTOR_IDENTITY_UNAVAILABLE"},
		{name: "payload", status: 409, code: "PAYLOAD_MISMATCH"},
		{name: "epoch", status: 409, code: "STALE_EPOCH"},
		{name: "auth", status: 401, code: "UNAUTHORIZED"},
		{name: "cancel", status: 503, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if test.cancel {
					cancel()
				}
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": test.code}})
			}))
			defer server.Close()
			b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: server.URL}, http: server.Client()}
			_, err := b.Tool(ctx, &Session{Epoch: 1, Actor: Actor{Name: "actor"}}, ToolRequest{ID: "original", Epoch: 1, Kind: "file_read", Path: "answer"})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("terminal failure was retried: calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

func TestRuntimeTransportRetriesAreBoundedAndClearPartialReplies(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "bounded-503", true: "partial-identity"}[partial], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := calls.Add(1)
				if !partial {
					w.WriteHeader(503)
					return
				}
				if attempt == 1 {
					_, _ = w.Write([]byte(`{"tool_op_id":"original","epoch":1,"status":"COMPLETED",`))
				} else {
					_, _ = w.Write([]byte(`{"epoch":1,"status":"COMPLETED","result":{"content":"missing identity"}}`))
				}
			}))
			defer server.Close()
			b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: server.URL}, http: server.Client()}
			_, err := b.Tool(context.Background(), &Session{Epoch: 1, Actor: Actor{Name: "actor"}}, ToolRequest{ID: "original", Epoch: 1, Kind: "file_read", Path: "answer"})
			want := int32(4)
			if partial {
				want = 2
			}
			if err == nil || calls.Load() != want {
				t.Fatalf("retry bound or identity validation failed: calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

func TestAttachRetryRequiresRuntimeReplayCapability(t *testing.T) {
	for _, version := range []int{0, 1} {
		t.Run(map[int]string{0: "legacy", 1: "fenced"}[version], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/readyz" {
					_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "idempotent_lifecycle_rpc_version": version})
					return
				}
				if calls.Add(1) == 1 {
					w.WriteHeader(503)
					return
				}
				_ = json.NewEncoder(w).Encode(runtimeAttachReceipt{Epoch: 2, Gate: "OPEN", ActorUID: "trusted", ManifestID: "snapshot", IdentityAttested: true})
			}))
			defer server.Close()
			b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: server.URL}, http: server.Client()}
			err := b.attach(context.Background(), &Session{Manifest: &Manifest{ID: "snapshot"}}, Actor{Name: "actor", UID: "trusted"}, 2)
			if version == 0 && (err == nil || calls.Load() != 1) || version == 1 && (err != nil || calls.Load() != 2) {
				t.Fatalf("runtime capability was ignored: version=%d calls=%d error=%v", version, calls.Load(), err)
			}
		})
	}
}

type runtimeRoundTripper func(*http.Request) (*http.Response, error)

func (f runtimeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRealPythonRuntimeRecoversLostReceiptsAndFencesLateAttach(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required for the real Actor runtime contract")
	}
	directory := t.TempDir()
	identity := filepath.Join(directory, "actor-uid")
	if err := os.WriteFile(identity, []byte("trusted-actor"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	program := `import sys
import runtime.tools_server as runtime
class ReadyServer(runtime.ThreadingHTTPServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(self.server_address[1], flush=True)
runtime.ThreadingHTTPServer = ReadyServer
runtime.serve(sys.argv[1], 0, '127.0.0.1', 'runtime-test-token')
`
	command := exec.CommandContext(ctx, python, "-u", "-c", program, filepath.Join(directory, "workspace"))
	command.Dir = filepath.Join("..", "..")
	command.Env = append(os.Environ(), "AGENTCLOUD_ACTOR_UID_FILE="+identity, "AGENTCLOUD_COORDINATOR_URL=", "AGENTCLOUD_COORDINATOR_TOKEN=")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	port, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("runtime start: %v", err)
	}
	origin := "http://127.0.0.1:" + strings.TrimSpace(port)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	var attachCalls, toolCalls, quiesceCalls atomic.Int32
	client := &http.Client{Transport: runtimeRoundTripper(func(request *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		drop := request.URL.Path == "/runtime/v1/attach" && attachCalls.Add(1) == 1 || request.URL.Path == "/runtime/v1/tools" && toolCalls.Add(1) == 1 || request.URL.Path == "/runtime/v1/quiesce" && quiesceCalls.Add(1) == 1
		if drop {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			response.StatusCode = 503
			response.Body = io.NopCloser(strings.NewReader("upstream call failed"))
			response.ContentLength = int64(len("upstream call failed"))
		}
		return response, nil
	})}
	b := &SubstrateBackend{cfg: SubstrateConfig{GatewayURL: origin, RuntimeToken: "runtime-test-token"}, http: client}
	s := &Session{Epoch: 1, Actor: Actor{Name: "actor", UID: "trusted-actor"}}
	if err := b.attach(ctx, s, s.Actor, 1); err != nil {
		t.Fatal(err)
	}
	input := ToolRequest{ID: "one-effect", Epoch: 1, Kind: "shell", Command: "printf x >> count.txt", TimeoutMS: 2000}
	if _, err := b.Tool(ctx, s, input); err != nil {
		t.Fatal(err)
	}
	if attachCalls.Load() != 2 || toolCalls.Load() != 2 {
		t.Fatalf("lost responses were not retried: attach=%d tool=%d", attachCalls.Load(), toolCalls.Load())
	}
	count, err := os.ReadFile(filepath.Join(directory, "workspace", "count.txt"))
	if err != nil || string(count) != "x" {
		t.Fatalf("effect was repeated: %q %v", count, err)
	}
	var barrier runtimeBarrierReceipt
	if err := retryRuntimeRPC(ctx, "/runtime/v1/quiesce", 5*time.Second, func(attempt context.Context) error {
		barrier = runtimeBarrierReceipt{}
		return b.call(attempt, s.Actor.Name, "/runtime/v1/quiesce", map[string]any{"operation_id": "safe", "expected_epoch": 1, "barrier_generation": 1}, &barrier)
	}); err != nil {
		t.Fatal(err)
	}
	if quiesceCalls.Load() != 2 || barrier.OperationID != "safe" || barrier.Epoch != 1 || barrier.Generation != 1 || barrier.Watermark != 1 || barrier.Gate != "CLOSED" || barrier.Active != 0 {
		t.Fatalf("quiesce retry lost the original safe point: calls=%d barrier=%+v", quiesceCalls.Load(), barrier)
	}
	if err := b.attach(ctx, s, s.Actor, 1); err == nil {
		t.Fatal("late attach reopened the quiesced epoch")
	}
	var state struct {
		Gate string `json:"gate"`
	}
	if err := b.call(ctx, s.Actor.Name, "/runtime/v1/status", nil, &state); err != nil || state.Gate != "CLOSED" {
		t.Fatalf("safe point was lost: %+v %v", state, err)
	}
	if err := b.attach(ctx, s, s.Actor, 2); err != nil {
		t.Fatal(err)
	}
	s.Epoch, input.Epoch = 2, 2
	if _, err := b.Tool(ctx, s, input); err != nil {
		t.Fatal(err)
	}
	count, err = os.ReadFile(filepath.Join(directory, "workspace", "count.txt"))
	if err != nil || string(count) != "x" {
		t.Fatalf("new epoch repeated the durable effect: %q %v", count, err)
	}
}
