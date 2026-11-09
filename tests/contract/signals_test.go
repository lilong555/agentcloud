package contract_test

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
	"github.com/lilong555/agentcloud/internal/signals"
)

var start = time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)

func binding() signals.Binding {
	return signals.Binding{SourceID: "coordinator", TenantID: "tenant-a", WorkspaceID: "workspace-a", SessionID: "session-a", ActorUID: "actor-a", Epoch: 1}
}

func update(sequence uint64, at time.Time, state signals.State) signals.SandboxStateUpdate {
	b := binding()
	return signals.SandboxStateUpdate{ProtocolVersion: 1, SourceID: b.SourceID, TenantID: b.TenantID, WorkspaceID: b.WorkspaceID, SessionID: b.SessionID, ActorUID: b.ActorUID, Epoch: b.Epoch, Sequence: sequence, ObservedAt: at, TTLMS: 2000, State: state, EstimatedReturnMS: 5000}
}

func tracker(t *testing.T) *signals.Tracker {
	t.Helper()
	v, err := signals.NewTracker(signals.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Register(binding()); err != nil {
		t.Fatal(err)
	}
	return v
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || signals.Code(err) != code {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

func TestIdentityAndFreshness(t *testing.T) {
	cases := []struct {
		name, code string
		change     func(*signals.SandboxStateUpdate)
	}{
		{"cross_tenant", "unregistered_session", func(u *signals.SandboxStateUpdate) { u.TenantID = "tenant-b" }},
		{"cross_workspace", "unregistered_session", func(u *signals.SandboxStateUpdate) { u.WorkspaceID = "workspace-b" }},
		{"cross_session", "unregistered_session", func(u *signals.SandboxStateUpdate) { u.SessionID = "session-b" }},
		{"actor_replacement", "identity_mismatch", func(u *signals.SandboxStateUpdate) { u.ActorUID = "actor-b" }},
		{"source_spoof", "identity_mismatch", func(u *signals.SandboxStateUpdate) { u.SourceID = "other-source" }},
		{"unregistered_epoch", "epoch_mismatch", func(u *signals.SandboxStateUpdate) { u.Epoch = 2 }},
		{"old_epoch", "invalid_epoch", func(u *signals.SandboxStateUpdate) { u.Epoch = 0 }},
		{"expired", "expired_update", func(u *signals.SandboxStateUpdate) { u.ObservedAt = start.Add(-2 * time.Second) }},
		{"future", "future_observation", func(u *signals.SandboxStateUpdate) { u.ObservedAt = start.Add(time.Second + time.Nanosecond) }},
		{"invalid_ttl_small", "invalid_ttl", func(u *signals.SandboxStateUpdate) { u.TTLMS = 99 }},
		{"invalid_ttl_extreme", "invalid_ttl", func(u *signals.SandboxStateUpdate) { u.TTLMS = math.MaxInt64 }},
		{"invalid_estimate", "invalid_estimate", func(u *signals.SandboxStateUpdate) { u.EstimatedReturnMS = math.MaxInt64 }},
		{"negative_estimate", "invalid_estimate", func(u *signals.SandboxStateUpdate) { u.EstimatedReturnMS = -1 }},
		{"unknown_state", "invalid_state", func(u *signals.SandboxStateUpdate) { u.State = "RUNNING_HEAVY" }},
		{"unknown_version", "unsupported_version", func(u *signals.SandboxStateUpdate) { u.ProtocolVersion = 2 }},
		{"zero_sequence", "invalid_sequence", func(u *signals.SandboxStateUpdate) { u.Sequence = 0 }},
		{"empty_source", "invalid_identity", func(u *signals.SandboxStateUpdate) { u.SourceID = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := tracker(t)
			u := update(1, start, signals.Suspended)
			tc.change(&u)
			requireCode(t, v.Accept(u, start), tc.code)
			view, err := v.View(binding().Key(), start)
			if err != nil || view.Fresh || view.Sequence != 0 {
				t.Fatalf("rejected event changed state: %+v, %v", view, err)
			}
		})
	}
}

func TestMonotonicSequenceAndObservation(t *testing.T) {
	v := tracker(t)
	u := update(2, start.Add(100*time.Millisecond), signals.Parked)
	if err := v.Accept(u, u.ObservedAt); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{1, 2} {
		candidate := update(seq, start.Add(200*time.Millisecond), signals.Resuming)
		requireCode(t, v.Accept(candidate, candidate.ObservedAt), "replay_or_out_of_order")
	}
	u = update(3, start, signals.Resuming)
	requireCode(t, v.Accept(u, start.Add(200*time.Millisecond)), "observation_out_of_order")
	u = update(3, start.Add(300*time.Millisecond), signals.Resuming)
	if err := v.Accept(u, u.ObservedAt); err != nil {
		t.Fatal(err)
	}
}

func TestEpochRotationFencesOldActor(t *testing.T) {
	v := tracker(t)
	if err := v.Accept(update(100, start, signals.Suspended), start); err != nil {
		t.Fatal(err)
	}
	b := binding()
	b.ActorUID = "actor-b"
	requireCode(t, v.Register(b), "registration_epoch")
	b.Epoch = 2
	if err := v.Register(b); err != nil {
		t.Fatal(err)
	}
	requireCode(t, v.Accept(update(101, start, signals.Suspended), start), "identity_mismatch")
	u := update(1, start, signals.Resuming)
	u.ActorUID, u.Epoch = b.ActorUID, b.Epoch
	if err := v.Accept(u, start); err != nil {
		t.Fatal(err)
	}
	u.Epoch = 1
	u.Sequence = 200
	requireCode(t, v.Accept(u, start), "epoch_mismatch")
	// Repeating trusted registration preserves the accepted event watermark.
	if err := v.Register(b); err != nil {
		t.Fatal(err)
	}
	u.Epoch = 2
	u.Sequence = 1
	requireCode(t, v.Accept(u, start), "replay_or_out_of_order")
}

func TestTTLExpiryPreservesReplayWatermark(t *testing.T) {
	v := tracker(t)
	u := update(7, start, signals.Suspended)
	if err := v.Accept(u, start); err != nil {
		t.Fatal(err)
	}
	view, err := v.View(binding().Key(), start.Add(2*time.Second))
	if err != nil || view.Fresh || view.State != signals.RunningFast || view.Sequence != 7 || view.FallbackReason != "ttl_expired" {
		t.Fatalf("TTL fallback: %+v %v", view, err)
	}
	u.ObservedAt = start.Add(3 * time.Second)
	requireCode(t, v.Accept(u, u.ObservedAt), "replay_or_out_of_order")
	u.Sequence = 8
	if err := v.Accept(u, u.ObservedAt); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDuplicateHasOneWinner(t *testing.T) {
	v := tracker(t)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v.Accept(update(1, start, signals.Suspended), start) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted duplicates = %d", accepted.Load())
	}
}

func decision(t *testing.T, v *signals.Tracker, now time.Time, status policy.ProgramStatus) policy.Decision {
	t.Helper()
	p, err := policy.New(policy.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	view, err := v.View(binding().Key(), now)
	if err != nil {
		t.Fatal(err)
	}
	d, err := p.Decide(view, now, status, 1000, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestActingWeightsAndReasoningPreserved(t *testing.T) {
	for _, tc := range []struct {
		state  signals.State
		weight float64
		budget uint64
	}{
		{signals.RunningFast, 0.8, 800}, {signals.HeavyTool, 0.2, 200}, {signals.Parked, 0.08000000000000002, 80}, {signals.Suspended, 0, 0}, {signals.Resuming, 0.8, 800},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			v := tracker(t)
			if err := v.Accept(update(1, start, tc.state), start); err != nil {
				t.Fatal(err)
			}
			d := decision(t, v, start.Add(300*time.Millisecond), policy.Acting)
			if math.Abs(d.EffectiveWeight-tc.weight) > 1e-12 || d.LogicalBudgetTokens != tc.budget || d.PhysicalKVAction != "none" {
				t.Fatalf("acting decision %+v", d)
			}
			d = decision(t, v, start.Add(300*time.Millisecond), policy.Reasoning)
			if d.EffectiveWeight != 1 || d.LogicalBudgetTokens != 1000 || d.BaselineWeight != 1 || d.ActingBaselineWeight != 0.8 || d.AppliedMultiplier != 1 || d.Reason != "reasoning_preserved" {
				t.Fatalf("reasoning decision %+v", d)
			}
		})
	}
}

func TestResumeRestoresImmediatelyAndDebouncesReparking(t *testing.T) {
	v := tracker(t)
	if err := v.Accept(update(1, start, signals.Suspended), start); err != nil {
		t.Fatal(err)
	}
	d := decision(t, v, start.Add(100*time.Millisecond), policy.Acting)
	if d.Reason != "state_debounce" || d.EffectiveWeight != 0.8 {
		t.Fatalf("debounce %+v", d)
	}
	d = decision(t, v, start.Add(300*time.Millisecond), policy.Acting)
	if d.EffectiveWeight != 0 {
		t.Fatalf("suspend %+v", d)
	}
	if err := v.Accept(update(2, start.Add(500*time.Millisecond), signals.Resuming), start.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	d = decision(t, v, start.Add(500*time.Millisecond), policy.Acting)
	if d.Reason != "resuming_baseline" || d.EffectiveWeight != 0.8 {
		t.Fatalf("resume %+v", d)
	}
	if err := v.Accept(update(3, start.Add(600*time.Millisecond), signals.Parked), start.Add(600*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	d = decision(t, v, start.Add(time.Second), policy.Acting)
	if d.Reason != "recovery_hold" || d.EffectiveWeight != 0.8 {
		t.Fatalf("hold %+v", d)
	}
	d = decision(t, v, start.Add(1600*time.Millisecond), policy.Acting)
	if d.Reason != "parked_acting" || d.LogicalBudgetTokens != 80 {
		t.Fatalf("stable park %+v", d)
	}
	d = decision(t, v, start.Add(2600*time.Millisecond), policy.Acting)
	if d.Reason != "ttl_expired" || d.EffectiveWeight != 0.8 {
		t.Fatalf("expiry %+v", d)
	}
}

func TestHeartbeatKeepsStateResidence(t *testing.T) {
	v := tracker(t)
	if err := v.Accept(update(1, start, signals.Suspended), start); err != nil {
		t.Fatal(err)
	}
	if err := v.Accept(update(2, start.Add(200*time.Millisecond), signals.Suspended), start.Add(200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if d := decision(t, v, start.Add(300*time.Millisecond), policy.Acting); d.EffectiveWeight != 0 {
		t.Fatalf("heartbeat reset debounce: %+v", d)
	}
}

func TestCachedViewExpiresBeforeDecision(t *testing.T) {
	v := tracker(t)
	if err := v.Accept(update(1, start, signals.Suspended), start); err != nil {
		t.Fatal(err)
	}
	view, err := v.View(binding().Key(), start.Add(time.Second))
	if err != nil || !view.Fresh {
		t.Fatalf("fresh view: %+v %v", view, err)
	}
	p, err := policy.New(policy.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	d, err := p.Decide(view, start.Add(2*time.Second), policy.Acting, 1000, 0.8)
	if err != nil || d.SignalFresh || d.EffectiveWeight != 0.8 || d.LogicalBudgetTokens != 800 || d.Reason != "ttl_expired" {
		t.Fatalf("cached expiry decision: %+v %v", d, err)
	}
}

func TestPolicyNumericValidation(t *testing.T) {
	p, err := policy.New(policy.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	view, _ := tracker(t).View(binding().Key(), start)
	for _, weight := range []float64{-1, 2, math.NaN(), math.Inf(1)} {
		if _, err := p.Decide(view, start, policy.Acting, 1000, weight); err == nil {
			t.Fatalf("accepted weight %v", weight)
		}
	}
	if _, err := p.Decide(view, start, policy.Acting, math.MaxUint64, 1); err == nil {
		t.Fatal("accepted extreme tokens")
	}
	c := signals.DefaultConfig()
	c.MaxTTLMS = math.MaxInt64
	if _, err := signals.NewTracker(c); err == nil {
		t.Fatal("accepted overflow-prone TTL config")
	}
}
