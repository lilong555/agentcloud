package control

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type capacityBackend struct {
	*LocalBackend
	allowed atomic.Bool
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	failure error
	hook    func()
}

func (b *capacityBackend) Resume(ctx context.Context, s *Session) (Actor, error) {
	b.calls.Add(1)
	if b.entered != nil {
		b.entered <- struct{}{}
		select {
		case <-b.release:
		case <-ctx.Done():
			return Actor{}, ctx.Err()
		}
	}
	if b.hook != nil {
		b.hook()
	}
	if b.failure != nil {
		return Actor{}, b.failure
	}
	if !b.allowed.Load() {
		return Actor{}, &resumeCapacityWaitError{cause: status.Error(codes.ResourceExhausted, "no Worker capacity"), proof: ResumeCapacityProof{
			ActorUID: s.Actor.UID, ActorVersion: s.Actor.Version, SnapshotUUID: s.Actor.SnapshotUUID,
			SnapshotGeneration: s.Actor.SnapshotGeneration, ObservedAt: time.Now().UTC(),
		}}
	}
	return b.LocalBackend.Resume(ctx, s)
}

func capacityConfig(t *testing.T) (Config, *capacityBackend) {
	t.Helper()
	cfg := testConfig(t)
	b := &capacityBackend{LocalBackend: cfg.Backend.(*LocalBackend)}
	cfg.Backend = b
	return cfg, b
}

func suspendCapacitySession(t *testing.T, c *Controller, action string) *Session {
	t.Helper()
	createTestSession(t, c, "tenant-a", "capacity")
	s, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", action, LifecycleRequest{OperationID: "sleep", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func expireCapacityRetry(t *testing.T, c *Controller, operation string) {
	t.Helper()
	c.lock()
	defer c.mu.Unlock()
	c.state.Sessions["tenant-a/capacity"].Lifecycle[operation].RetryAfter = time.Now().Add(-time.Second)
	c.markEntity("lifecycle", "tenant-a/capacity/"+operation)
	if err := c.persistContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityWaitPreservesSnapshotAndRetriesAcrossRestart(t *testing.T) {
	for _, action := range []string{"suspend", "checkpoint"} {
		t.Run(action, func(t *testing.T) {
			cfg, backend := capacityConfig(t)
			c, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			before := suspendCapacitySession(t, c, action)
			request := LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			assertCode(t, err, "resume_capacity_wait")
			waiting, err := c.Get("tenant-a", "capacity")
			if err != nil || waiting.State != before.State || waiting.Gate != "CLOSED" || waiting.Epoch != before.Epoch || waiting.BarrierGeneration != before.BarrierGeneration || !reflect.DeepEqual(before.Manifest, waiting.Manifest) || !reflect.DeepEqual(before.Actor, waiting.Actor) {
				t.Fatalf("capacity refusal changed recovery identity: %+v %v", waiting, err)
			}
			receipt := waiting.Lifecycle["wake"]
			if receipt.Status != "WAITING_CAPACITY" || receipt.AttemptCount != 1 || receipt.CapacityWait == nil || receipt.RetryAfter.Sub(receipt.LastAttemptAt) < resumeCapacityRetryDelay {
				t.Fatalf("missing durable capacity receipt: %+v", receipt)
			}
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			assertCode(t, err, "resume_capacity_wait")
			if backend.calls.Load() != 1 {
				t.Fatal("retry cooldown dispatched another Resume")
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			c, err = New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			waiting, _ = c.Get("tenant-a", "capacity")
			if waiting.State != before.State || waiting.Lifecycle["wake"].Status != "WAITING_CAPACITY" || !reflect.DeepEqual(waiting.Lifecycle["wake"].CapacityWait, receipt.CapacityWait) {
				t.Fatalf("restart lost the verified wait: %+v", waiting)
			}
			expireCapacityRetry(t, c, "wake")
			backend.allowed.Store(true)
			after, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			if err != nil || after.State != "RUNNING" || after.Gate != "OPEN" || after.Epoch != 2 || after.Lifecycle["wake"].AttemptCount != 2 || after.Lifecycle["wake"].Status != "COMPLETED" {
				t.Fatalf("capacity return did not resume once: %+v %v", after, err)
			}
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			if err != nil || backend.calls.Load() != 2 {
				t.Fatalf("completion replay repeated Resume: %d %v", backend.calls.Load(), err)
			}
		})
	}
}

func TestCapacityWaitPreservesIdempotencyAndEpochFences(t *testing.T) {
	cfg, backend := capacityConfig(t)
	c := newTestController(t, cfg)
	suspendCapacitySession(t, c, "suspend")
	request := LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}
	_, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
	assertCode(t, err, "resume_capacity_wait")
	conflict := request
	conflict.CheckpointID = "changed"
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", conflict)
	assertCode(t, err, "payload_conflict")
	_, _, err = c.Lifecycle(context.Background(), "tenant-b", "capacity", "resume", request)
	assertCode(t, err, "session_not_found")
	backend.allowed.Store(true)
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", LifecycleRequest{OperationID: "other-wake", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
	assertCode(t, err, "stale_epoch")
	if backend.calls.Load() != 2 {
		t.Fatalf("stale wait dispatched Resume: %d", backend.calls.Load())
	}
}

func TestCapacityWaitSerializesConcurrentResumeAttempts(t *testing.T) {
	cfg, backend := capacityConfig(t)
	backend.entered, backend.release = make(chan struct{}, 1), make(chan struct{})
	c := newTestController(t, cfg)
	suspendCapacitySession(t, c, "suspend")
	request := LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}
	done := make(chan error, 1)
	go func() {
		_, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
		done <- err
	}()
	<-backend.entered
	_, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
	assertCode(t, err, "lifecycle_unknown")
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", LifecycleRequest{OperationID: "parallel", ExpectedEpoch: 1})
	assertCode(t, err, "lifecycle_busy")
	close(backend.release)
	assertCode(t, <-done, "resume_capacity_wait")
	if backend.calls.Load() != 1 {
		t.Fatal("concurrent Resume escaped the durable reservation")
	}
}

func TestUnverifiedCapacityAndLostResponsesRemainUnknown(t *testing.T) {
	for _, failure := range []error{status.Error(codes.ResourceExhausted, "runtime resource failure"), context.DeadlineExceeded,
		&resumeCapacityWaitError{cause: errors.New("unrelated Actor"), proof: ResumeCapacityProof{ActorUID: "other", ActorVersion: 7, ObservedAt: time.Now()}}} {
		t.Run(failure.Error(), func(t *testing.T) {
			cfg, backend := capacityConfig(t)
			backend.failure = failure
			c := newTestController(t, cfg)
			suspendCapacitySession(t, c, "suspend")
			request := LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}
			_, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			assertCode(t, err, "recovery_required")
			s, _ := c.Get("tenant-a", "capacity")
			if s.State != "RECOVERY_REQUIRED" || s.Gate != "CLOSED" || s.Lifecycle["wake"].Status != "OUTCOME_UNKNOWN" {
				t.Fatalf("unverified outcome was made retryable: %+v", s)
			}
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			assertCode(t, err, "lifecycle_unknown")
			if backend.calls.Load() != 1 {
				t.Fatal("unknown resume was replayed")
			}
		})
	}
}

func TestAutonomousCapacityRetryReusesOneReceiptAndReturnDemand(t *testing.T) {
	cfg, backend := capacityConfig(t)
	cfg.EnableAutonomousScheduler = true
	cfg.SchedulerInterval = time.Hour
	c := newTestController(t, cfg)
	s := suspendCapacitySession(t, c, "suspend")
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", "capacity", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	c.autonomousStep(s)
	waiting, _ := c.Get("tenant-a", "capacity")
	op := waiting.Autonomous.LastOperationID
	if op == "" || !waiting.Autonomous.ReturnDemand || waiting.ReturnDemands["return"].Status != "PENDING" || waiting.Autonomous.Reason != "await_worker_capacity_retry" {
		t.Fatalf("return demand lost on capacity rejection: %+v", waiting)
	}
	for i := 0; i < 5; i++ {
		c.autonomousStep(waiting)
	}
	if backend.calls.Load() != 1 {
		t.Fatal("scheduler polled Resume during the cooldown")
	}
	expireCapacityRetry(t, c, op)
	c.autonomousStep(waiting)
	waiting, _ = c.Get("tenant-a", "capacity")
	if waiting.Autonomous.LastOperationID != op || len(waiting.Lifecycle) != 2 || waiting.Lifecycle[op].AttemptCount != 2 {
		t.Fatalf("capacity retries grew receipt history: %+v", waiting.Lifecycle)
	}
	expireCapacityRetry(t, c, op)
	backend.allowed.Store(true)
	c.autonomousStep(waiting)
	after, _ := c.Get("tenant-a", "capacity")
	if after.Epoch != 2 || after.Autonomous.ReturnDemand || after.ReturnDemands["return"].Status != "COMPLETED" || after.Lifecycle[op].AttemptCount != 3 || len(after.Lifecycle) != 2 {
		t.Fatalf("managed capacity return failed: %+v", after)
	}
}

func TestPostgresCapacityWaitSurvivesRestartAndFailedCommit(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable-wait", true: "rejected-commit"}[failCommit], func(t *testing.T) {
			cfg, backend := capacityConfig(t)
			cfg.DatabaseURL = isolatedControllerDatabase(t)
			c, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			suspendCapacitySession(t, c, "suspend")
			if failCommit {
				db := postgresInspector(t, cfg.DatabaseURL)
				backend.hook = func() {
					if _, e := db.Exec(context.Background(), "UPDATE agentcloud.journal SET revision=revision+1 WHERE id=1"); e != nil {
						t.Fatal(e)
					}
				}
			}
			request := LifecycleRequest{OperationID: "wake", ExpectedEpoch: 1}
			_, _, err = c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request)
			if failCommit {
				var apiErr *APIError
				if err == nil || errors.As(err, &apiErr) && apiErr.Code == "resume_capacity_wait" {
					t.Fatalf("uncommitted proof exposed as retryable: %v", err)
				}
				_, err = c.Get("tenant-a", "capacity")
				assertCode(t, err, "journal_unavailable")
			} else {
				assertCode(t, err, "resume_capacity_wait")
			}
			_ = c.Close()
			backend.hook = nil
			c, err = New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			s, err := c.Get("tenant-a", "capacity")
			if err != nil {
				t.Fatal(err)
			}
			if failCommit {
				if s.State != "RECOVERY_REQUIRED" || s.Lifecycle["wake"].Status != "OUTCOME_UNKNOWN" {
					t.Fatalf("interrupted intent was made retryable: %+v", s)
				}
			} else {
				if s.State != "SUSPENDED" || s.Lifecycle["wake"].Status != "WAITING_CAPACITY" || s.Lifecycle["wake"].CapacityWait == nil {
					t.Fatalf("entity commit lost capacity proof: %+v", s)
				}
				expireCapacityRetry(t, c, "wake")
				backend.allowed.Store(true)
				if _, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "resume", request); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
