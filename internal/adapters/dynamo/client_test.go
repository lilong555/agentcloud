package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/signals"
)

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
