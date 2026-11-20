package control

import (
	"context"
	"testing"
	"time"
)

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
