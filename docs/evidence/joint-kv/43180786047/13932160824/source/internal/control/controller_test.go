package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	directory := t.TempDir()
	return Config{DataDir: directory, Backend: &LocalBackend{Root: directory, AllowShell: true}, ModelURL: "mock://deterministic", ModelConcurrency: 1,
		Tokens: map[string]string{"token-a": "tenant-a", "token-b": "tenant-b"}, ModelTimeout: time.Second}
}
func newTestController(t *testing.T, cfg Config) *Controller {
	t.Helper()
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func createTestSession(t *testing.T, c *Controller, tenant, id string) *Session {
	t.Helper()
	s, err := c.Create(context.Background(), tenant, CreateSession{SessionID: id, WorkspaceID: id, Mode: "external"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var problem *APIError
	if !errors.As(err, &problem) || problem.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(until) {
			t.Fatal("acceptance condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTenantGlobalLogicalRequestIdentityAndClaim(t *testing.T) {
	c := newTestController(t, testConfig(t))
	a := createTestSession(t, c, "tenant-a", "first")
	createTestSession(t, c, "tenant-a", "second")
	createTestSession(t, c, "tenant-b", "first")
	payload := json.RawMessage(`{"model":"controlled","messages":[{"role":"user","content":"固定任务"}],"max_tokens":8}`)
	first, err := c.Submit("tenant-a", "first", "stable-request", payload)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := c.Submit("tenant-a", "first", "stable-request", payload)
	if err != nil || duplicate.PayloadHash != first.PayloadHash {
		t.Fatal(err)
	}
	_, err = c.Submit("tenant-a", "second", "stable-request", payload)
	assertCode(t, err, "payload_conflict")
	_, err = c.Submit("tenant-a", "first", "stable-request", json.RawMessage(`{"model":"changed"}`))
	assertCode(t, err, "payload_conflict")
	if _, err = c.Submit("tenant-b", "first", "stable-request", payload); err != nil {
		t.Fatal("other tenant identity rejected", err)
	}
	_, err = c.Request("tenant-a", "second", "stable-request", 0)
	assertCode(t, err, "request_not_found")
	_, err = c.Submit("tenant-a", "first", "forged", json.RawMessage(`{"nvext":{"agent_context":{"session":"forged"}}}`))
	assertCode(t, err, "identity_override")
	waitFor(t, func() bool {
		r, e := c.Request("tenant-a", "first", "stable-request", 0)
		return e == nil && r.Status == "COMPLETED"
	})
	result, err := c.Request("tenant-a", "first", "stable-request", a.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := c.Request("tenant-a", "first", "stable-request", a.Epoch)
	if err != nil || !bytes.Equal(result.Result, replay.Result) || result.AttemptID != replay.AttemptID {
		t.Fatal("result claim generated a second execution", err)
	}
	_, err = c.Request("tenant-a", "first", "stable-request", a.Epoch+1)
	assertCode(t, err, "stale_epoch")
}

type blockedTools struct {
	*LocalBackend
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockedTools) Tool(ctx context.Context, s *Session, input ToolRequest) (ToolResult, error) {
	b.calls.Add(1)
	close(b.entered)
	select {
	case <-ctx.Done():
		return ToolResult{}, ctx.Err()
	case <-b.release:
		return ToolResult{Stdout: "drained", ExitCode: 0}, nil
	}
}
func TestGateDrainAndDuplicateInFlightTool(t *testing.T) {
	cfg := testConfig(t)
	backend := &blockedTools{LocalBackend: cfg.Backend.(*LocalBackend), entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Backend = backend
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "drain")
	result := make(chan *ToolReceipt, 1)
	toolErr := make(chan error, 1)
	input := ToolRequest{ID: "parallel", Epoch: s.Epoch, Kind: "shell", Command: "controlled", TimeoutMS: 1000}
	go func() {
		r, e := c.Tool(context.Background(), "tenant-a", s.SessionID, input)
		result <- r
		toolErr <- e
	}()
	<-backend.entered
	duplicate, err := c.Tool(context.Background(), "tenant-a", s.SessionID, input)
	if err != nil || duplicate.Status != "RUNNING" {
		t.Fatalf("duplicate result: %v %v", duplicate, err)
	}
	checkpoint := make(chan *Manifest, 1)
	checkpointErr := make(chan error, 1)
	go func() {
		_, m, e := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "checkpoint", LifecycleRequest{OperationID: "safe-point", ExpectedEpoch: s.Epoch})
		checkpoint <- m
		checkpointErr <- e
	}()
	waitFor(t, func() bool { state, _ := c.Get("tenant-a", s.SessionID); return state.Gate == "CLOSED" })
	_, err = c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "late-upload", Epoch: s.Epoch, Kind: "file_write", Path: "upload", Content: "late"})
	assertCode(t, err, "admission_closed")
	_, err = c.Submit("tenant-a", s.SessionID, "late-model", json.RawMessage(`{"model":"controlled"}`))
	assertCode(t, err, "admission_closed")
	select {
	case <-checkpoint:
		t.Fatal("checkpoint published before admitted tool drained")
	default:
	}
	close(backend.release)
	r := <-result
	if err := <-toolErr; err != nil {
		t.Fatal(err)
	}
	m := <-checkpoint
	if err := <-checkpointErr; err != nil {
		t.Fatal(err)
	}
	if backend.calls.Load() != 1 || r.Sequence != m.ReceiptWatermark || m.State != "COMMITTED" {
		t.Fatalf("invalid drain result: calls=%d receipt=%v manifest=%v", backend.calls.Load(), r, m)
	}
	after, _ := c.Get("tenant-a", s.SessionID)
	if after.Gate != "CLOSED" || after.State != "CHECKPOINTED" {
		t.Fatalf("safe-point admission reopened: %s %s", after.Gate, after.State)
	}
}

func TestApplicationSnapshotRestoreAndOldEpoch(t *testing.T) {
	cfg := testConfig(t)
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "restore")
	_, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "write", Epoch: s.Epoch, Kind: "file_write", Path: "answer.txt", Content: "55"})
	if err != nil {
		t.Fatal(err)
	}
	_, manifest, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "checkpoint", LifecycleRequest{OperationID: "checkpoint", ExpectedEpoch: s.Epoch, CheckpointID: "graph-fixed"})
	if err != nil {
		t.Fatal(err)
	}
	suspended, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "suspend", ExpectedEpoch: s.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if suspended.Manifest.ID != manifest.ID || suspended.Manifest.CheckpointID != "graph-fixed" {
		t.Fatal("suspend replaced the graph/sandbox manifest")
	}
	backend := cfg.Backend.(*LocalBackend)
	if err := os.WriteFile(filepath.Join(backend.workspace(s), "answer.txt"), []byte("changed-after-suspend"), 0600); err != nil {
		t.Fatal(err)
	}
	resumed, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "resume", LifecycleRequest{OperationID: "resume", ExpectedEpoch: s.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Epoch != s.Epoch+1 || resumed.ActualFidelity != "VOLUMES" || resumed.Actor.BootID == s.Actor.BootID {
		t.Fatal("application restore receipt invalid")
	}
	read, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "read", Epoch: resumed.Epoch, Kind: "file_read", Path: "answer.txt"})
	if err != nil || read.Result.Content != "55" {
		t.Fatalf("restore read: %v %v", read, err)
	}
	reclaimed, err := c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "write", Epoch: resumed.Epoch, Kind: "file_write", Path: "answer.txt", Content: "55"})
	if err != nil || reclaimed.Sequence != 1 || reclaimed.Status != "COMPLETED" {
		t.Fatalf("old receipt was not reclaimable with the resumed epoch: %v %v", reclaimed, err)
	}
	_, err = c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "late-old-write", Epoch: s.Epoch, Kind: "file_write", Path: "answer.txt", Content: "stale"})
	assertCode(t, err, "stale_epoch")
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "late-suspend", ExpectedEpoch: s.Epoch})
	assertCode(t, err, "stale_epoch")
}

func TestSnapshotDigestCorruptionFailsClosed(t *testing.T) {
	cfg := testConfig(t)
	c := newTestController(t, cfg)
	s := createTestSession(t, c, "tenant-a", "corrupt")
	_, _ = c.Tool(context.Background(), "tenant-a", s.SessionID, ToolRequest{ID: "write", Epoch: 1, Kind: "file_write", Path: "file", Content: "original"})
	suspended, _, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "suspend", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.DataDir, "snapshots", "tenant-a", s.WorkspaceID, suspended.Actor.SnapshotUUID, "file")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "resume", LifecycleRequest{OperationID: "resume", ExpectedEpoch: 1})
	assertCode(t, err, "recovery_required")
	state, _ := c.Get("tenant-a", s.SessionID)
	if state.Gate != "CLOSED" || state.State != "RECOVERY_REQUIRED" {
		t.Fatal("corrupt snapshot opened execution")
	}
}

func isolatedControllerDatabase(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AGENTCLOUD_TEST_PG_ADMIN")
	if dsn == "" {
		t.Skip("set AGENTCLOUD_TEST_PG_ADMIN for actual isolated PostgreSQL control acceptance")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid PostgreSQL test configuration")
	}
	admin, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal("PostgreSQL administrator unavailable")
	}
	database := fmt.Sprintf("agentcloud_control_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		admin.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})
	uri, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test administrator URI")
	}
	uri.Path = "/" + database
	return uri.String()
}

func TestPostgresRestartKeepsUnknownAndClosedGate(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "restart")
	input := ToolRequest{ID: "interrupted-tool", Epoch: 1, Kind: "shell", Command: "side-effect", TimeoutMS: 30000}
	c.mu.Lock()
	state := c.state.Sessions[key("tenant-a", s.SessionID)]
	business := input
	business.Epoch = 0
	state.Tools[input.ID] = &ToolReceipt{ID: input.ID, Epoch: 1, Status: "RUNNING", PayloadHash: payloadHash(business), Sequence: 1}
	state.ActiveTools = 1
	state.NextReceipt = 1
	state.State = "QUIESCING"
	state.Gate = "CLOSED"
	state.Lifecycle["interrupted-suspend"] = &LifecycleReceipt{OperationID: "interrupted-suspend", Status: "RUNNING"}
	c.state.Requests[key("tenant-a", "interrupted-model")] = &ModelRequest{ID: "interrupted-model", TenantID: "tenant-a", SessionID: s.SessionID, Status: "DISPATCHING", AttemptID: "original-attempt", Payload: json.RawMessage(`{"model":"controlled"}`)}
	err = c.persist()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, _ := reopened.Get("tenant-a", s.SessionID)
	if recovered.State != "RECOVERY_REQUIRED" || recovered.Gate != "CLOSED" || recovered.ActiveTools != 0 || recovered.Tools[input.ID].Status != "OUTCOME_UNKNOWN" {
		t.Fatalf("invalid recovery state: %+v", recovered)
	}
	result, err := reopened.Request("tenant-a", s.SessionID, "interrupted-model", 0)
	if err != nil || result.Status != "OUTCOME_UNKNOWN" || result.AttemptID != "original-attempt" {
		t.Fatalf("request was re-dispatched: %v %v", result, err)
	}
	replay, err := reopened.Tool(context.Background(), "tenant-a", s.SessionID, input)
	if err != nil || replay.Status != "OUTCOME_UNKNOWN" {
		t.Fatal("unknown tool replayed", err)
	}
	_, _, err = reopened.Lifecycle(context.Background(), "tenant-a", s.SessionID, "resume", LifecycleRequest{OperationID: "resume", ExpectedEpoch: 1})
	assertCode(t, err, "lifecycle_busy")
}

func TestHTTPIdentityStrictDecodeAndEndToEnd(t *testing.T) {
	c := newTestController(t, testConfig(t))
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	call := func(method, path, token, body string) (int, map[string]any) {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(string(data))
		}
		return response.StatusCode, result
	}
	if status, _ := call("GET", "/v1/sessions", "", ""); status != 401 {
		t.Fatal(status)
	}
	if status, _ := call("POST", "/v1/sessions", "token-a", `{"session_id":"http","workspace_id":"http","tenant_id":"tenant-b"}`); status != 400 {
		t.Fatal("tenant override accepted", status)
	}
	if status, _ := call("POST", "/v1/sessions", "token-a", `{"session_id":"http","workspace_id":"http"} {}`); status != 400 {
		t.Fatal("multiple JSON documents accepted", status)
	}
	if status, _ := call("POST", "/v1/sessions", "token-a", `{"session_id":"http","workspace_id":"http","mode":"external"}`); status != 201 {
		t.Fatal(status)
	}
	if status, _ := call("GET", "/v1/sessions/http", "token-b", ""); status != 404 {
		t.Fatal("cross-tenant session exposed", status)
	}
	if status, _ := call("POST", "/v1/sessions/http/tools", "token-a", `{"tool_op_id":"write","epoch":1,"kind":"file_write","path":"file","content":"hello"}`); status != 200 {
		t.Fatal(status)
	}
	if status, _ := call("POST", "/v1/sessions/http/tools", "token-a", `{"tool_op_id":"escape","epoch":1,"kind":"file_write","path":"../escape","content":"bad"}`); status != 400 {
		t.Fatal("traversal reached execution", status)
	}
	if status, result := call("POST", "/v1/sessions/http/checkpoint", "token-a", `{"operation_id":"checkpoint","expected_epoch":1}`); status != 200 || result["state"] != "COMMITTED" {
		t.Fatal(status, result)
	}
	if status, _ := call("POST", "/v1/sessions/http/tools", "token-a", `{"tool_op_id":"late","epoch":1,"kind":"file_write","path":"file","content":"late"}`); status != 423 {
		t.Fatal("checkpoint admission opened", status)
	}
	if status, _ := call("POST", "/v1/sessions/http/suspend", "token-a", `{"operation_id":"suspend","expected_epoch":1}`); status != 200 {
		t.Fatal(status)
	}
	if status, result := call("POST", "/v1/sessions/http/resume", "token-a", `{"operation_id":"resume","expected_epoch":1}`); status != 200 || result["epoch"] != float64(2) {
		t.Fatal(status, result)
	}
	if status, result := call("POST", "/v1/sessions/http/tools", "token-a", `{"tool_op_id":"read","epoch":2,"kind":"file_read","path":"file"}`); status != 200 || result["result"].(map[string]any)["content"] != "hello" {
		t.Fatal(status, result)
	}
}
