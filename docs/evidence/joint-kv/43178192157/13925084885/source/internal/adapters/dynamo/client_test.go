package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/signals"
)

func TestClientReadsPrefixObservationWithoutRefreshingItsTimestamp(t *testing.T) {
	b := signals.Binding{SourceID: "coordinator", TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", ActorUID: "actor", Epoch: 1}
	at := time.Now().UTC().Add(-time.Second)
	events, samples, exposure := uint64(0), uint64(8), 8000.0
	o := &signals.CacheObservation{SchemaVersion: 1, ObservedAt: at, SourceKind: "live_kvindexer_observation", BackendKind: "mocker", SourceVersion: "observer-v1", WorkerID: 7, WorkerEpoch: "producer-a", ExecutionEpoch: 1, PrefixDigest: "prefix-a", HistoryTokens: 256, DeviceOverlapTokens: 240, Valid: true, LossEvents: &events, SampleCount: &samples, ExposureMS: &exposure, StatsObservedAt: &at}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Receipt{ProtocolVersion: 1, Accepted: true, Reconciled: true, Binding: b, RoutingSessionID: "ac:test", InferenceLease: "lease", Snapshot: Snapshot{CacheObservation: o}})
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "test-control-token-0123456789abcdef", nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Session(context.Background(), b)
	if err != nil || !reflect.DeepEqual(r.Snapshot.CacheObservation, o) {
		t.Fatalf("cache observation: %+v %v", r.Snapshot.CacheObservation, err)
	}
}

func TestClientTransportsTrustedBindingsAndReceipts(t *testing.T) {
	binding := signals.Binding{SourceID: "coordinator", TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", ActorUID: "actor", Epoch: 1}
	token := "test-control-token-0123456789abcdef"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("trusted token missing")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/execution/capabilities":
			_ = json.NewEncoder(w).Encode(Capabilities{ProtocolVersion: 1, ExecutionFeedback: true, TrustedBindings: true})
		case "/v1/execution/bindings":
			var request bindingRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.Binding != binding || request.ProtocolVersion != 1 {
				t.Error("binding contract mismatch")
			}
			_ = json.NewEncoder(w).Encode(Receipt{ProtocolVersion: 1, Accepted: true, Reconciled: true, Binding: binding, RoutingSessionID: "ac:opaque", InferenceLease: "lease"})
		case "/v1/execution/state":
			var u signals.SandboxStateUpdate
			_ = json.NewDecoder(r.Body).Decode(&u)
			_ = json.NewEncoder(w).Encode(Receipt{ProtocolVersion: 1, Accepted: true, Reconciled: true, Applied: true, Binding: binding, RoutingSessionID: "ac:opaque", Sequence: u.Sequence, Snapshot: Snapshot{ProgramPresent: true, Sequence: u.Sequence}})
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Capabilities(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := client.Register(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if r.InferenceHeaders(true).Get(SessionHeader) != "ac:opaque" || r.InferenceHeaders(true).Get(InferenceLeaseHeader) != "lease" || r.InferenceHeaders(true).Get(FinalHeader) != "true" {
		t.Fatal("inference identity headers missing")
	}
	u := signals.SandboxStateUpdate{ProtocolVersion: 1, SourceID: binding.SourceID, TenantID: binding.TenantID, WorkspaceID: binding.WorkspaceID, SessionID: binding.SessionID, ActorUID: binding.ActorUID, Epoch: 1, Sequence: 2, ObservedAt: time.Now().UTC(), TTLMS: 2000, State: signals.Suspended}
	if _, err = client.Update(context.Background(), u); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationsAreBoundedAcrossEpochsAndFreshRegistrationIsExplicit(t *testing.T) {
	b := signals.Binding{SourceID: "coordinator", TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", ActorUID: "actor", Epoch: 1}
	generation := "g-1"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input bindingRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		requests++
		if r.URL.Path == "/v1/execution/finalize" {
			if input.RegistrationGeneration != generation {
				t.Error("finalize lost generation")
			}
			_ = json.NewEncoder(w).Encode(FinalizeReceipt{ProtocolVersion: 1, Accepted: true, Finalized: true, BindingGeneration: generation})
			return
		}
		if input.RegistrationGeneration != "" && input.RegistrationGeneration != generation {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "stale_generation"})
			return
		}
		_ = json.NewEncoder(w).Encode(Receipt{ProtocolVersion: 1, Accepted: true, Reconciled: true, Binding: input.Binding, RoutingSessionID: "ac:opaque", InferenceLease: "lease-" + generation, BindingGeneration: generation})
	}))
	defer server.Close()
	c, _ := NewClient(server.URL, "test-control-token-0123456789abcdef", nil)
	for epoch := uint64(1); epoch <= 64; epoch++ {
		b.Epoch = epoch
		generation = fmt.Sprintf("g-%d", epoch)
		if _, err := c.Register(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		if len(c.generations) != 1 {
			t.Fatal("old epoch generations retained")
		}
	}
	before := requests
	generation = "frontend-restarted"
	if _, err := c.Register(context.Background(), b); !IsRetiredGeneration(err) {
		t.Fatalf("stale generation accepted: %v", err)
	}
	if requests != before+1 {
		t.Fatal("ordinary registration silently retried")
	}
	if _, err := c.RegisterFresh(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Finalize(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if len(c.generations) != 0 {
		t.Fatal("final binding credential retained")
	}
}
func TestClientDoesNotForwardTokenAcrossRedirects(t *testing.T) {
	forwarded := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded = true }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, "test-control-token-0123456789abcdef", nil)
	_, err := client.Capabilities(context.Background())
	var api *APIError
	if !errors.As(err, &api) || api.Status != 307 || forwarded {
		t.Fatalf("redirect result: %v forwarded=%v", err, forwarded)
	}
}
func TestClientRejectsMismatchedReceiptIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Receipt{ProtocolVersion: 1, Accepted: true, Reconciled: true, Binding: signals.Binding{TenantID: "other"}, RoutingSessionID: "ac:opaque", InferenceLease: "lease"})
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, "test-control-token-0123456789abcdef", nil)
	if _, err := client.Register(context.Background(), signals.Binding{TenantID: "tenant"}); err == nil {
		t.Fatal("cross-tenant receipt accepted")
	}
}
