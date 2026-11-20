package control

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/signals"
)

// DynamoBridge selects credentials by authenticated tenant and serializes only
// each binding's register/update/finalize sequence.
type DynamoBridge struct {
	Client    *dynamo.Client
	Clients   map[string]*dynamo.Client
	SourceID  string
	TTL       time.Duration
	mu        sync.Mutex
	locks     keyedMutex[signals.Binding]
	finalized map[signals.Binding]dynamo.Snapshot
	released  map[signals.Binding]uint64
}

func (b *DynamoBridge) clientFor(s *Session) (*dynamo.Client, error) {
	if b.Clients != nil {
		client := b.Clients[s.TenantID]
		if client == nil {
			return nil, fmt.Errorf("Dynamo tenant credential is unavailable")
		}
		return client, nil
	}
	if b.Client == nil {
		return nil, fmt.Errorf("Dynamo control client is unavailable")
	}
	return b.Client, nil
}
func (b *DynamoBridge) binding(s *Session) signals.Binding {
	return signals.Binding{SourceID: b.SourceID, TenantID: s.TenantID, WorkspaceID: s.WorkspaceID, SessionID: s.SessionID, ActorUID: s.Actor.UID, Epoch: s.Epoch}
}
func (b *DynamoBridge) lockBinding(binding signals.Binding) func() {
	// One lock spans all actor incarnations and epochs of a native session.
	lockKey := binding
	lockKey.Epoch = 0
	lockKey.ActorUID = ""
	unlock := b.locks.lock(lockKey)
	b.mu.Lock()
	if b.finalized == nil {
		b.finalized = map[signals.Binding]dynamo.Snapshot{}
		b.released = map[signals.Binding]uint64{}
	}
	b.mu.Unlock()
	return unlock
}
func (b *DynamoBridge) observeReleased(ctx context.Context, binding signals.Binding, receipt dynamo.Receipt) {
	if receipt.Snapshot.ReleasedActingTokens != nil {
		binding.Epoch, binding.ActorUID = 0, ""
		observed := *receipt.Snapshot.ReleasedActingTokens
		b.mu.Lock()
		prior := b.released[binding]
		b.mu.Unlock()
		if observed >= prior {
			observability.Observe(ctx, "dynamo_released_acting_tokens", float64(observed-prior), "native_session_cumulative_release_delta")
		}
		b.mu.Lock()
		b.released[binding] = observed
		b.mu.Unlock()
		return
	}
	if receipt.Applied && receipt.ReleasedActingTokens != nil {
		observability.Observe(ctx, "dynamo_released_acting_tokens", float64(*receipt.ReleasedActingTokens), "native_applied_state_receipt")
	} else {
		observability.Unavailable(ctx, "dynamo_released_acting_tokens")
	}
}
func (b *DynamoBridge) Headers(ctx context.Context, s *Session) (http.Header, error) {
	client, err := b.clientFor(s)
	if err != nil {
		return nil, err
	}
	binding := b.binding(s)
	unlock := b.lockBinding(binding)
	defer unlock()
	b.mu.Lock()
	delete(b.finalized, binding)
	b.mu.Unlock()
	receipt, err := client.Register(ctx, binding)
	if dynamo.IsRetiredGeneration(err) {
		// A durable new model attempt may acquire a fresh generation after a
		// frontend restart. The inference POST itself is sent exactly once.
		receipt, err = client.RegisterFresh(ctx, binding)
	}
	if err != nil {
		return nil, err
	}
	return receipt.InferenceHeaders(s.AblationGroup == "A" || s.AblationGroup == "B"), nil
}
func (b *DynamoBridge) Send(ctx context.Context, s *Session, state string) error {
	client, err := b.clientFor(s)
	if err != nil {
		return err
	}
	binding := b.binding(s)
	unlock := b.lockBinding(binding)
	defer unlock()
	if s.DynamoFinalized && s.DynamoFinalizedEpoch == s.Epoch {
		return nil
	}
	b.mu.Lock()
	_, wasFinal := b.finalized[binding]
	b.mu.Unlock()
	if wasFinal {
		return nil
	}
	if _, err := client.Register(ctx, binding); err != nil {
		return err
	}
	ttl := b.TTL
	if ttl == 0 {
		ttl = 5 * time.Second
	}
	remaining := int64(0)
	for _, r := range s.Tools {
		if r.Status == "RUNNING" && r.Deadline.After(time.Now()) {
			remaining = max(remaining, time.Until(r.Deadline).Milliseconds())
		}
	}
	receipt, err := client.Update(ctx, signals.SandboxStateUpdate{ProtocolVersion: 1, SourceID: binding.SourceID, TenantID: binding.TenantID, WorkspaceID: binding.WorkspaceID, SessionID: binding.SessionID, ActorUID: binding.ActorUID, Epoch: binding.Epoch, Sequence: s.SignalSequence, ObservedAt: time.Now().UTC(), TTLMS: ttl.Milliseconds(), State: signals.State(state), EstimatedReturnMS: remaining})
	if err != nil {
		return err
	}
	if !receipt.Applied {
		return fmt.Errorf("Dynamo accepted the state without applying it")
	}
	b.observeReleased(ctx, binding, receipt)
	return nil
}
func (b *DynamoBridge) Waiting(ctx context.Context, s *Session) (QueueObservation, error) {
	client, err := b.clientFor(s)
	if err != nil {
		return QueueObservation{}, err
	}
	binding := b.binding(s)
	unlock := b.lockBinding(binding)
	defer unlock()
	b.mu.Lock()
	snapshot, wasFinal := b.finalized[binding]
	b.mu.Unlock()
	if s.DynamoFinalized && s.DynamoFinalizedEpoch == s.Epoch && s.DynamoFinalizedSnapshot != nil {
		snapshot = *s.DynamoFinalizedSnapshot
		wasFinal = true
	}
	if wasFinal {
		return QueueObservation{Source: "native_dynamo_final_snapshot", Scope: "classifier", WaitingRequests: snapshot.WaitingRequests, SessionWaitingRequests: snapshot.SessionWaitingRequests, AdmissionHistory: snapshot.AdmissionHistory, CacheObservation: snapshot.CacheObservation, SelectedWorkerID: snapshot.SelectedWorkerID, SelectedDPRank: snapshot.SelectedDPRank, ObservedAt: time.Now().UTC()}, nil
	}
	if _, err := client.Register(ctx, binding); err != nil {
		return QueueObservation{}, err
	}
	receipt, err := client.Session(ctx, binding)
	if err == nil {
		b.observeReleased(ctx, binding, receipt)
	}
	return QueueObservation{Source: "native_dynamo", Scope: "classifier", WaitingRequests: receipt.Snapshot.WaitingRequests, SessionWaitingRequests: receipt.Snapshot.SessionWaitingRequests, AdmissionHistory: receipt.Snapshot.AdmissionHistory, CacheObservation: receipt.Snapshot.CacheObservation, SelectedWorkerID: receipt.Snapshot.SelectedWorkerID, SelectedDPRank: receipt.Snapshot.SelectedDPRank, ObservedAt: time.Now().UTC()}, err
}
func (b *DynamoBridge) Session(ctx context.Context, s *Session) (dynamo.Receipt, error) {
	client, err := b.clientFor(s)
	if err != nil {
		return dynamo.Receipt{}, err
	}
	binding := b.binding(s)
	unlock := b.lockBinding(binding)
	defer unlock()
	b.mu.Lock()
	snapshot, wasFinal := b.finalized[binding]
	b.mu.Unlock()
	if s.DynamoFinalized && s.DynamoFinalizedEpoch == s.Epoch && s.DynamoFinalizedSnapshot != nil {
		snapshot = *s.DynamoFinalizedSnapshot
		wasFinal = true
	}
	if wasFinal {
		return dynamo.Receipt{Snapshot: snapshot}, nil
	}
	receipt, err := client.Session(ctx, binding)
	if err == nil {
		b.observeReleased(ctx, binding, receipt)
	}
	return receipt, err
}
func (b *DynamoBridge) Finalize(ctx context.Context, s *Session) (dynamo.FinalizeReceipt, error) {
	client, err := b.clientFor(s)
	if err != nil {
		return dynamo.FinalizeReceipt{}, err
	}
	binding := b.binding(s)
	unlock := b.lockBinding(binding)
	defer unlock()
	b.mu.Lock()
	snapshot, wasFinal := b.finalized[binding]
	b.mu.Unlock()
	if s.DynamoFinalized && s.DynamoFinalizedEpoch == s.Epoch && s.DynamoFinalizedSnapshot != nil {
		snapshot = *s.DynamoFinalizedSnapshot
		wasFinal = true
	}
	if wasFinal {
		return dynamo.FinalizeReceipt{ProtocolVersion: 1, Accepted: true, Finalized: true, Snapshot: snapshot}, nil
	}
	receipt, err := client.Session(ctx, binding)
	if err != nil {
		return dynamo.FinalizeReceipt{}, err
	}
	if receipt.Snapshot.ProgramPresent || receipt.Snapshot.SessionActiveRequests != 0 {
		return dynamo.FinalizeReceipt{}, problem(409, "native_program_active", "Dynamo finalization requires no program and no active requests")
	}
	result, err := client.Finalize(ctx, binding)
	if err == nil {
		b.mu.Lock()
		b.finalized[binding] = result.Snapshot
		releaseKey := binding
		releaseKey.Epoch, releaseKey.ActorUID = 0, ""
		delete(b.released, releaseKey)
		if len(b.finalized) > 512 {
			for old := range b.finalized {
				if old != binding {
					delete(b.finalized, old)
					break
				}
			}
		}
		b.mu.Unlock()
	}
	return result, err
}
