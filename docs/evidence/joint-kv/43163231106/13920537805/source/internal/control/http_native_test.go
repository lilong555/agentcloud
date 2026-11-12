package control

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	"github.com/lilong555/agentcloud/internal/signals"
)

func TestHTTPNativeReceiptExposesBindingGenerationAndRetainsFinalSnapshot(t *testing.T) {
	const generation = "native-generation-1"
	const privateLease = "private-native-inference-lease"
	worker := uint64(18446744073709551610)
	snapshot := dynamo.Snapshot{SelectedWorkerID: &worker}
	var nativeCalls atomic.Int64
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nativeCalls.Add(1)
		var input struct {
			signals.Binding
			RegistrationGeneration string `json:"registration_generation"`
			Sequence               uint64 `json:"sequence"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if r.URL.Path == "/v1/execution/session" || r.URL.Path == "/v1/execution/finalize" {
			if input.RegistrationGeneration != generation {
				t.Errorf("native generation = %q", input.RegistrationGeneration)
			}
		}
		if r.URL.Path == "/v1/execution/finalize" {
			final := snapshot
			final.FinalEvents = 1
			_ = json.NewEncoder(w).Encode(dynamo.FinalizeReceipt{ProtocolVersion: 1, Accepted: true,
				Finalized: true, BindingGeneration: generation, Snapshot: final})
			return
		}
		_ = json.NewEncoder(w).Encode(dynamo.Receipt{ProtocolVersion: 1, Accepted: true,
			Applied: true, Reconciled: true, Binding: input.Binding, BindingGeneration: generation,
			RoutingSessionID: "ac:native-session", InferenceLease: privateLease,
			Sequence: input.Sequence, Snapshot: snapshot})
	}))
	defer native.Close()
	client, err := dynamo.NewClient(native.URL, "test-control-token-0123456789abcdef", nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	bridge := &DynamoBridge{Client: client, SourceID: "native-source"}
	cfg.Signals = bridge
	c := newTestController(t, cfg)
	session := createTestSession(t, c, "tenant-a", "native-session")
	if _, err := bridge.Headers(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	call := func(method, suffix string) map[string]json.RawMessage {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+"/v1/sessions/native-session/"+suffix, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer token-a")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("HTTP %s %s: %d %s %v", method, suffix, response.StatusCode, data, err)
		}
		if strings.Contains(string(data), "inference_lease") || strings.Contains(string(data), privateLease) {
			t.Fatalf("native response exposes inference lease: %s", data)
		}
		var result map[string]json.RawMessage
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	active := call(http.MethodGet, "native")
	var binding signals.Binding
	var actualGeneration string
	var epoch uint64
	if err := json.Unmarshal(active["binding"], &binding); err != nil || binding != bridge.binding(session) {
		t.Fatalf("active binding: %s %v", active["binding"], err)
	}
	if err := json.Unmarshal(active["binding_generation"], &actualGeneration); err != nil || actualGeneration != generation {
		t.Fatalf("active generation: %s %v", active["binding_generation"], err)
	}
	if err := json.Unmarshal(active["epoch"], &epoch); err != nil || epoch != session.Epoch || len(active) != 4 {
		t.Fatalf("active receipt: %+v %v", active, err)
	}
	call(http.MethodPost, "finalize")
	before := nativeCalls.Load()
	final := call(http.MethodGet, "native")
	var retained dynamo.Snapshot
	if err := json.Unmarshal(final["snapshot"], &retained); err != nil || retained.SelectedWorkerID == nil || *retained.SelectedWorkerID != worker || retained.FinalEvents != 1 {
		t.Fatalf("final snapshot: %s %v", final["snapshot"], err)
	}
	if len(final) != 2 || final["binding"] != nil || final["binding_generation"] != nil || nativeCalls.Load() != before {
		t.Fatalf("final receipt: %+v; native calls %d -> %d", final, before, nativeCalls.Load())
	}
}
