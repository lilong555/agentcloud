package control

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	"github.com/lilong555/agentcloud/internal/signals"
)

// DynamoBridge sends authenticated lifecycle updates to the native scheduler.
type DynamoBridge struct {
	Client   *dynamo.Client
	SourceID string
	TTL      time.Duration
	mu       sync.Mutex
}

func (b *DynamoBridge) binding(s *Session) signals.Binding {
	return signals.Binding{SourceID: b.SourceID, TenantID: s.TenantID, WorkspaceID: s.WorkspaceID, SessionID: s.SessionID, ActorUID: s.Actor.UID, Epoch: s.Epoch}
}
func (b *DynamoBridge) Headers(ctx context.Context, s *Session) (http.Header, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	receipt, err := b.Client.Register(ctx, b.binding(s))
	if err != nil {
		return nil, err
	}
	return receipt.InferenceHeaders(s.AblationGroup == "A" || s.AblationGroup == "B"), nil
}
func (b *DynamoBridge) Send(ctx context.Context, s *Session, state string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	binding := b.binding(s)
	if _, err := b.Client.Register(ctx, binding); err != nil {
		return err
	}
	ttl := b.TTL
	if ttl == 0 {
		ttl = 5 * time.Second
	}
	var remaining int64
	for _, r := range s.Tools {
		if r.Status == "RUNNING" && r.Deadline.After(time.Now()) {
			millis := time.Until(r.Deadline).Milliseconds()
			if millis > remaining {
				remaining = millis
			}
		}
	}
	receipt, err := b.Client.Update(ctx, signals.SandboxStateUpdate{ProtocolVersion: 1, SourceID: binding.SourceID, TenantID: binding.TenantID, WorkspaceID: binding.WorkspaceID, SessionID: binding.SessionID, ActorUID: binding.ActorUID, Epoch: binding.Epoch, Sequence: s.SignalSequence, ObservedAt: time.Now().UTC(), TTLMS: ttl.Milliseconds(), State: signals.State(state), EstimatedReturnMS: remaining})
	if err != nil {
		return err
	}
	if !receipt.Applied {
		return fmt.Errorf("Dynamo accepted the state without applying it")
	}
	return nil
}

func (b *DynamoBridge) Waiting(ctx context.Context, s *Session) (QueueObservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	binding := b.binding(s)
	if _, err := b.Client.Register(ctx, binding); err != nil {
		return QueueObservation{}, err
	}
	receipt, err := b.Client.Session(ctx, binding)
	return QueueObservation{Source: "native_dynamo", Scope: "classifier", WaitingRequests: receipt.Snapshot.WaitingRequests, ObservedAt: time.Now().UTC()}, err
}
