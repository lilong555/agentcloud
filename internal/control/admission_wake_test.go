package control

import (
	"context"
	"testing"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"github.com/lilong555/agentcloud/internal/policy"
)

type delayedDurabilityBackend struct {
	*initialFixtureBackend
	released, durable chan struct{}
}

func (b *delayedDurabilityBackend) SchedulingCapacity(context.Context, *Session) (CapacityObservation, error) {
	return CapacityObservation{ObservedAt: time.Now().UTC()}, nil
}

func (b *delayedDurabilityBackend) Checkpoint(ctx context.Context, s *Session, op string) (Actor, string, error) {
	a, digest, err := b.initialFixtureBackend.Checkpoint(ctx, s, op)
	if err != nil {
		return a, digest, err
	}
	released := ctx.Value(capacityReleaseKey{}).(func(substrateapi.Summary) error)
	proof := substrateapi.Summary{UID: s.Actor.UID, State: "SUSPENDED"}
	if err = released(proof); err != nil {
		return Actor{}, "", err
	}
	if err = released(proof); err != nil { // Duplicate receipts remain idempotent.
		return Actor{}, "", err
	}
	close(b.released)
	select {
	case <-b.durable:
	case <-ctx.Done():
		return Actor{}, "", ctx.Err()
	}
	return a, digest, nil
}

func TestCapacityReleaseWakesBeforeSnapshotDurability(t *testing.T) {
	cfg := testConfig(t)
	cfg.EnableAutonomousScheduler = true
	cfg.SchedulerPolicy = "agentcloud-v2"
	cfg.SchedulerConcurrency = 1
	cfg.SchedulerInterval, cfg.SchedulerStableFor = 10*time.Millisecond, time.Millisecond
	cfg.ModelTimeout = 5 * time.Second
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5}
	cfg.QueueObserver = costQueueFunc(func(context.Context, *Session) (QueueObservation, error) {
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, nil
	})
	backend, upstream := admissionFixture(t)
	upstream.capacity = 1
	b := &delayedDurabilityBackend{initialFixtureBackend: &initialFixtureBackend{
		SubstrateBackend: backend, local: cfg.Backend.(*LocalBackend), server: upstream},
		released: make(chan struct{}), durable: make(chan struct{})}
	cfg.Backend = b
	c := newTestController(t, cfg)
	defer close(b.durable)
	createTestSession(t, c, "tenant-a", "holder")
	createTestSession(t, c, "tenant-b", "waiter")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "holder")]
	s.IdleSince, s.RunningSince = time.Now().Add(-3*time.Second), time.Now().Add(-time.Minute)
	s.LastResumedAt = s.RunningSince
	s.Autonomous = nil
	c.mu.Unlock()
	select {
	case <-b.released:
	case <-time.After(2 * time.Second):
		current, _ := c.Get("tenant-a", "holder")
		t.Fatalf("Substrate release was not recorded: state=%s", current.State)
	}
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-b", "waiter")
		return s.State == "RUNNING" && s.Admissions["initial"].AttemptCount == 2
	})
	holder, _ := c.Get("tenant-a", "holder")
	var release *LifecycleReceipt
	for _, receipt := range holder.Lifecycle {
		if receipt.Action == "suspend" {
			release = receipt
		}
	}
	if holder.State != "QUIESCING" || holder.Manifest != nil || release == nil || release.ResourceReleaseSequence == 0 {
		t.Fatalf("release implied snapshot durability: %+v", holder)
	}
	b.durable <- struct{}{}
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "holder")
		return s.State == "SUSPENDED" && s.Lifecycle[release.OperationID].Status == "COMPLETED"
	})
	c.lock()
	defer c.mu.Unlock()
	releases := 0
	for _, e := range c.state.Events {
		if e.Kind == "admission.resource_released" {
			releases++
		}
	}
	if releases != 1 {
		t.Fatalf("duplicate release: %d", releases)
	}
}

func TestCapacityReleaseWakesOldestAndSurvivesRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "restart"}[restart], func(t *testing.T) {
			cfg := testConfig(t)
			cfg.DatabaseURL, cfg.ResearchRunID = isolatedControllerDatabase(t), "release-wake"
			cfg.SchedulerInterval = time.Hour // Any progress here requires an event.
			backend, upstream := admissionFixture(t)
			upstream.capacity = 1
			cfg.Backend = &initialFixtureBackend{SubstrateBackend: backend, local: cfg.Backend.(*LocalBackend), server: upstream}
			c, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { c.Close() }()
			createTestSession(t, c, "tenant-a", "holder")
			createTestSession(t, c, "tenant-b", "oldest")
			createTestSession(t, c, "tenant-a", "newest")
			c.lock()
			for _, s := range c.state.Sessions {
				if d := activeAdmission(s); d != nil {
					d.RetryAfter = time.Now().Add(time.Hour)
					c.markSession(s)
				}
			}
			if restart {
				c.schedulerBusy[key("tenant-b", "oldest")] = true
			}
			if err := c.persistContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.mu.Unlock()
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "holder", "suspend", LifecycleRequest{OperationID: "release", ExpectedEpoch: 1})
			if err != nil {
				t.Fatal(err)
			}
			if restart {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c, err = New(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				s, _ := c.Get("tenant-b", "oldest")
				if s.State == "RUNNING" {
					d := s.Admissions["initial"]
					if d.AttemptCount != 2 || d.RetryTrigger != "slot_release" || d.ReleaseSequence == 0 || d.WakePending {
						t.Fatalf("lost wake provenance: %+v", d)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("release did not wake oldest: %+v", s)
				}
				time.Sleep(5 * time.Millisecond)
			}
			newest, _ := c.Get("tenant-a", "newest")
			if newest.State != "WAITING_CAPACITY" || newest.Admissions["initial"].AttemptCount != 1 {
				t.Fatalf("release woke extra waiter: %+v", newest)
			}
		})
	}
}

func TestCapacityFallbackProbesAreGloballyBounded(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerInterval = 10 * time.Millisecond
	backend, _ := admissionFixture(t)
	cfg.Backend = backend
	c := newTestController(t, cfg)
	c.lock()
	c.nextAdmissionProbe = time.Now().Add(time.Hour)
	c.mu.Unlock()
	for _, id := range []string{"a", "b", "c", "d"} {
		createTestSession(t, c, "tenant-a", id)
	}
	c.lock()
	for _, s := range c.state.Sessions {
		s.Admissions["initial"].RetryAfter = time.Time{}
		c.markSession(s)
	}
	c.nextAdmissionProbe = time.Time{}
	c.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		attempts := uint64(0)
		for _, s := range c.List("tenant-a") {
			attempts += s.Admissions["initial"].AttemptCount
		}
		if attempts == 5 {
			break
		}
		if attempts > 5 || time.Now().After(deadline) {
			t.Fatalf("fallback attempts=%d", attempts)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	attempts := uint64(0)
	for _, s := range c.List("tenant-a") {
		attempts += s.Admissions["initial"].AttemptCount
	}
	if attempts != 5 {
		t.Fatalf("fallback storm: attempts=%d", attempts)
	}
}
