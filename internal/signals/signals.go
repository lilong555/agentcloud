// Package signals validates trusted execution-layer lifecycle feedback.
package signals

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type State string

const (
	RunningFast State = "RUNNING_FAST"
	HeavyTool   State = "HEAVY_TOOL"
	Parked      State = "PARKED"
	Suspended   State = "SUSPENDED"
	Resuming    State = "RESUMING"
)

// Key identifies a session within its authenticated tenant and workspace.
type Key struct {
	TenantID    string `json:"tenant_id"`
	WorkspaceID string `json:"workspace_id"`
	SessionID   string `json:"session_id"`
}

// Binding is established by a trusted coordinator, separately from state events.
// An Actor replacement or source change requires a strictly greater epoch.
type Binding struct {
	SourceID    string `json:"source_id"`
	TenantID    string `json:"tenant_id"`
	WorkspaceID string `json:"workspace_id"`
	SessionID   string `json:"session_id"`
	ActorUID    string `json:"actor_uid"`
	Epoch       uint64 `json:"epoch"`
}

func (b Binding) Key() Key { return Key{b.TenantID, b.WorkspaceID, b.SessionID} }

// SandboxStateUpdate is the version-1 feedback contract. Durations are milliseconds.
type SandboxStateUpdate struct {
	ProtocolVersion   int       `json:"protocol_version"`
	SourceID          string    `json:"source_id"`
	TenantID          string    `json:"tenant_id"`
	WorkspaceID       string    `json:"workspace_id"`
	SessionID         string    `json:"session_id"`
	ActorUID          string    `json:"actor_uid"`
	Epoch             uint64    `json:"epoch"`
	Sequence          uint64    `json:"sequence"`
	ObservedAt        time.Time `json:"observed_at"`
	TTLMS             int64     `json:"ttl_ms"`
	State             State     `json:"state"`
	EstimatedReturnMS int64     `json:"estimated_return_ms"`
}

func (u SandboxStateUpdate) Binding() Binding {
	return Binding{u.SourceID, u.TenantID, u.WorkspaceID, u.SessionID, u.ActorUID, u.Epoch}
}

type Config struct {
	MinTTLMS             int64         `json:"min_ttl_ms"`
	MaxTTLMS             int64         `json:"max_ttl_ms"`
	MaxEstimatedReturnMS int64         `json:"max_estimated_return_ms"`
	FutureSkew           time.Duration `json:"-"`
	RecoveryHold         time.Duration `json:"-"`
}

func DefaultConfig() Config {
	return Config{MinTTLMS: 100, MaxTTLMS: 60_000, MaxEstimatedReturnMS: 600_000, FutureSkew: time.Second, RecoveryHold: time.Second}
}

func (c Config) validate() error {
	if c.MinTTLMS < 1 || c.MaxTTLMS < c.MinTTLMS || c.MaxTTLMS > 86_400_000 || c.MaxEstimatedReturnMS < 0 || c.MaxEstimatedReturnMS > 86_400_000 || c.FutureSkew < 0 || c.FutureSkew > time.Minute || c.RecoveryHold < 0 || c.RecoveryHold > time.Minute {
		return reject("invalid_config", "duration limits are invalid")
	}
	return nil
}

type Rejection struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *Rejection) Error() string     { return e.Code + ": " + e.Detail }
func reject(code, detail string) error { return &Rejection{Code: code, Detail: detail} }

func Code(err error) string {
	if e, ok := err.(*Rejection); ok {
		return e.Code
	}
	return "invalid_input"
}

// View retains the replay watermark after TTL expiry, preventing stale resurrection.
type View struct {
	Binding           Binding   `json:"binding"`
	State             State     `json:"state"`
	Sequence          uint64    `json:"sequence"`
	ObservedAt        time.Time `json:"observed_at,omitempty"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	StateSince        time.Time `json:"state_since,omitempty"`
	RecoveryUntil     time.Time `json:"recovery_until,omitempty"`
	EstimatedReturnMS int64     `json:"estimated_return_ms"`
	Fresh             bool      `json:"fresh"`
	FallbackReason    string    `json:"fallback_reason,omitempty"`
}

type entry struct {
	binding                   Binding
	update                    SandboxStateUpdate
	hasUpdate                 bool
	stateSince, recoveryUntil time.Time
}

type Tracker struct {
	mu      sync.Mutex
	config  Config
	entries map[Key]*entry
}

func NewTracker(config Config) (*Tracker, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Tracker{config: config, entries: make(map[Key]*entry)}, nil
}

// Register requires caller authorization. The replay CLI obtains it from the
// explicit local input fixture; a network receiver must authenticate separately.
func (t *Tracker) Register(binding Binding) error {
	if err := validateBinding(binding); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.entries[binding.Key()]; ok {
		if binding == old.binding {
			return nil
		}
		if binding.Epoch <= old.binding.Epoch {
			return reject("registration_epoch", "binding changes require a newer epoch")
		}
	}
	t.entries[binding.Key()] = &entry{binding: binding}
	return nil
}

func (t *Tracker) Accept(update SandboxStateUpdate, now time.Time) error {
	if err := t.validateUpdate(update, now); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[update.Binding().Key()]
	if !ok {
		return reject("unregistered_session", "session has no trusted binding")
	}
	if update.SourceID != e.binding.SourceID || update.ActorUID != e.binding.ActorUID {
		return reject("identity_mismatch", "source or Actor differs from trusted binding")
	}
	if update.Epoch != e.binding.Epoch {
		return reject("epoch_mismatch", "epoch differs from trusted binding")
	}
	if e.hasUpdate && update.Sequence <= e.update.Sequence {
		return reject("replay_or_out_of_order", "sequence must strictly increase within the epoch")
	}
	if e.hasUpdate && update.ObservedAt.Before(e.update.ObservedAt) {
		return reject("observation_out_of_order", "observation time decreased")
	}
	expired := e.hasUpdate && !now.Before(e.update.ObservedAt.Add(time.Duration(e.update.TTLMS)*time.Millisecond))
	if !e.hasUpdate || expired || update.State != e.update.State {
		e.stateSince = now
	}
	if update.State == Resuming && (!e.hasUpdate || e.update.State != Resuming || expired) {
		e.recoveryUntil = now.Add(t.config.RecoveryHold)
	}
	e.update = update
	e.hasUpdate = true
	return nil
}

func (t *Tracker) View(key Key, now time.Time) (View, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[key]
	if !ok {
		return View{}, reject("unregistered_session", "session has no trusted binding")
	}
	v := View{Binding: e.binding, State: RunningFast, FallbackReason: "no_signal"}
	if !e.hasUpdate {
		return v, nil
	}
	v.Sequence = e.update.Sequence
	v.ObservedAt = e.update.ObservedAt
	v.ExpiresAt = e.update.ObservedAt.Add(time.Duration(e.update.TTLMS) * time.Millisecond)
	if !now.Before(v.ExpiresAt) {
		v.FallbackReason = "ttl_expired"
		return v, nil
	}
	v.State, v.StateSince, v.RecoveryUntil = e.update.State, e.stateSince, e.recoveryUntil
	v.EstimatedReturnMS, v.Fresh, v.FallbackReason = e.update.EstimatedReturnMS, true, ""
	return v, nil
}

func (t *Tracker) validateUpdate(u SandboxStateUpdate, now time.Time) error {
	if u.ProtocolVersion != 1 {
		return reject("unsupported_version", "protocol_version must be 1")
	}
	if err := validateBinding(u.Binding()); err != nil {
		return err
	}
	if u.Sequence == 0 {
		return reject("invalid_sequence", "sequence must be positive")
	}
	if u.State != RunningFast && u.State != HeavyTool && u.State != Parked && u.State != Suspended && u.State != Resuming {
		return reject("invalid_state", "unknown lifecycle state")
	}
	if u.TTLMS < t.config.MinTTLMS || u.TTLMS > t.config.MaxTTLMS {
		return reject("invalid_ttl", "ttl_ms is outside configured limits")
	}
	if u.EstimatedReturnMS < 0 || u.EstimatedReturnMS > t.config.MaxEstimatedReturnMS {
		return reject("invalid_estimate", "estimated_return_ms is outside configured limits")
	}
	if now.IsZero() || u.ObservedAt.IsZero() {
		return reject("invalid_time", "timestamps must be provided")
	}
	if u.ObservedAt.After(now.Add(t.config.FutureSkew)) {
		return reject("future_observation", "observation exceeds allowed clock skew")
	}
	if !now.Before(u.ObservedAt.Add(time.Duration(u.TTLMS) * time.Millisecond)) {
		return reject("expired_update", "update has already expired")
	}
	return nil
}

func validateBinding(b Binding) error {
	for _, field := range []struct{ name, value string }{{"source_id", b.SourceID}, {"tenant_id", b.TenantID}, {"workspace_id", b.WorkspaceID}, {"session_id", b.SessionID}, {"actor_uid", b.ActorUID}} {
		if !validID(field.value) {
			return reject("invalid_identity", fmt.Sprintf("%s must contain 1..128 ASCII identifier characters", field.name))
		}
	}
	if b.Epoch == 0 {
		return reject("invalid_epoch", "epoch must be positive")
	}
	return nil
}

func validID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:-", r))
	}) == -1
}
