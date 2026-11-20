package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/signals"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestHTTPDispatchTraceCorrelationNativeAdmissionAndPrivacy(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime, err := observability.New(context.Background(), observability.Config{Exporter: exporter})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	const traceID = "11111111111111111111111111111111"
	const private = "model-content-and-private-inference-lease"
	var mu sync.Mutex
	attempt := ""
	seenControl := false
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("traceparent"), traceID) {
			t.Errorf("control trace was disconnected: %q", r.Header.Get("traceparent"))
		}
		var body map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		data, _ := json.Marshal(body)
		var binding signals.Binding
		_ = json.Unmarshal(data, &binding)
		receipt := dynamo.Receipt{ProtocolVersion: 1, Accepted: true, Applied: true, Reconciled: true, Binding: binding, RoutingSessionID: "ac:opaque", InferenceLease: private}
		if r.URL.Path == "/v1/execution/state" {
			var update signals.SandboxStateUpdate
			_ = json.Unmarshal(data, &update)
			receipt.Sequence = update.Sequence
			released := uint64(23)
			receipt.ReleasedActingTokens = &released
		}
		if r.URL.Path == "/v1/execution/session" {
			mu.Lock()
			receipt.Snapshot.AdmissionHistory = []dynamo.AdmissionObservation{{ClientRequestID: attempt, WaitMS: 17.75}, {ClientRequestID: "another-attempt", WaitMS: 999}}
			mu.Unlock()
		}
		mu.Lock()
		seenControl = true
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(receipt)
	}))
	defer native.Close()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("traceparent"), traceID) {
			t.Errorf("model trace was disconnected: %q", r.Header.Get("traceparent"))
		}
		if r.Header.Get(dynamo.InferenceLeaseHeader) != private {
			t.Error("identity lease missing")
		}
		mu.Lock()
		attempt = r.Header.Get("X-AgentCloud-Attempt-ID")
		mu.Unlock()
		time.Sleep(25 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer model.Close()
	client, _ := dynamo.NewClient(native.URL, "control-token-private-0123456789abcdef", nil)
	cfg := testConfig(t)
	cfg.Observability = runtime
	cfg.ModelURL = model.URL
	cfg.Signals = &DynamoBridge{Client: client, SourceID: "source"}
	c := newTestController(t, cfg)
	// Creation has its own trace; arm strict downstream propagation after creation.
	createTestSession(t, c, "tenant-a", "workspace")
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/workspace/requests", strings.NewReader(`{"logical_request_id":"request","payload":{"messages":[{"role":"user","content":"`+private+`"}]}}`))
	request.Header.Set("Authorization", "Bearer token-a")
	request.Header.Set("traceparent", "00-"+traceID+"-2222222222222222-01")
	response := httptest.NewRecorder()
	c.Handler().ServeHTTP(response, request)
	if response.Code != 202 {
		t.Fatalf("submission %d %s", response.Code, response.Body.String())
	}
	waitFor(t, func() bool {
		r, err := c.Request("tenant-a", "workspace", "request", 0)
		return err == nil && r.Status == "COMPLETED"
	})
	diagnostic, err := c.DiagnosticTrace("tenant-a", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	if diagnostic.LatencyMS["dynamo_admission_wait_ms"] != 17.75 || diagnostic.LatencyMS["gateway_queue_ms"] <= 0 || diagnostic.LatencyMS["model_http_ms"] < 25 {
		t.Fatalf("latency %+v", diagnostic.LatencyMS)
	}
	for _, item := range diagnostic.Observations {
		if item.Metric == "dynamo_admission_wait_ms" && item.TraceID != traceID {
			t.Fatalf("native observation trace %+v", item)
		}
	}
	if err := runtime.Provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) < 6 {
		t.Fatalf("only %d spans", len(spans))
	}
	for _, span := range spans {
		data, _ := json.Marshal(span)
		if strings.Contains(string(data), private) || strings.Contains(string(data), "control-token-private") || strings.Contains(string(data), "token-a") {
			t.Fatalf("sensitive span %s", data)
		}
	}
	mu.Lock()
	if !seenControl || attempt == "" {
		t.Error("native observation absent")
	}
	mu.Unlock()
	metrics := httptest.NewRecorder()
	scrape := httptest.NewRequest("GET", "/metrics", nil)
	scrape.Header.Set("Authorization", "Bearer token-a")
	c.Handler().ServeHTTP(metrics, scrape)
	if metrics.Code != 200 || !strings.Contains(metrics.Body.String(), "# TYPE gateway_queue_ms histogram") || !strings.Contains(metrics.Body.String(), "dynamo_admission_wait_ms_sum 17.75") {
		t.Fatalf("metrics %d %s", metrics.Code, metrics.Body.String())
	}
}

func TestDiagnosticsSurviveRestartAndIsolateTenants(t *testing.T) {
	cfg := testConfig(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := createTestSession(t, c, "tenant-a", "workspace")
	_, manifest, err := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "sleep", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "resume", LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	trace, err := c.DiagnosticTrace("tenant-a", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.EpochHistory) != 2 || trace.Session.Epoch != 2 || len(trace.Manifests) != 1 || trace.Manifests[0].ID != manifest.ID || trace.LatencyMS["journal_commit_ms"] <= 0 {
		t.Fatalf("trace %+v", trace)
	}
	if _, err := c.DiagnosticTrace("tenant-b", "workspace"); err == nil {
		t.Fatal("cross-tenant trace leaked")
	}
	if value := c.DiagnosticStatus(context.Background(), "tenant-b", false); len(value.Sessions) != 0 {
		t.Fatalf("status leaked %+v", value)
	}
	for _, event := range trace.Events {
		if event.Detail != "" {
			t.Fatal("raw detail exposed")
		}
	}
}

func TestStrandedIntegralUnionToolSubtractionAndSamplingGap(t *testing.T) {
	c := newTestController(t, testConfig(t))
	s := createTestSession(t, c, "tenant-a", "workspace")
	c.mu.Lock()
	live := c.state.Sessions[key("tenant-a", s.SessionID)]
	start := time.Now().UTC()
	live.IdleWindows = map[string]*IdleWindow{"one": {Epoch: 1, StartedAt: start, ExpiresAt: start.Add(time.Second)}, "two": {Epoch: 1, StartedAt: start, ExpiresAt: start.Add(time.Second)}}
	c.stranded[key("tenant-a", s.SessionID)] = &strandedInterval{At: start, ObservedAt: start, MemoryBytes: 2e9, Idle: true, LastEmitted: start}
	c.updateStrandingLocked(live, start.Add(40*time.Millisecond))
	live.ActiveTools = 1
	c.updateStrandingLocked(live, start.Add(60*time.Millisecond))
	c.updateStrandingLocked(live, start.Add(80*time.Millisecond))
	live.ActiveTools = 0
	c.updateStrandingLocked(live, start.Add(90*time.Millisecond))
	// The missing sample tail ends at 100ms despite the later timestamp.
	c.closing = true
	c.updateStrandingLocked(live, start.Add(500*time.Millisecond))
	c.closing = false
	c.mu.Unlock()
	families, err := c.telemetry.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := 0.0
	for _, family := range families {
		if family.GetName() == "stranded_worker_gb_s" {
			got = family.Metric[0].GetCounter().GetValue()
		}
	}
	if got < .139999 || got > .140001 {
		t.Fatalf("stranded %f want .14", got)
	}
}
