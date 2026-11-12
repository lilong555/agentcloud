package substrate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type contendedControl struct {
	ateapi.UnimplementedControlServer
	mu          sync.Mutex
	requests    []proto.Message
	mutations   int
	replacement bool
}

func (s *contendedControl) check(request proto.Message, uid string, version int64) error {
	s.requests = append(s.requests, proto.Clone(request))
	if len(s.requests) == 1 {
		return status.Error(codes.Aborted, "another operation is in progress for this actor")
	}
	if s.replacement || uid != testActorUID || version != 10 {
		return status.Error(codes.FailedPrecondition, "Actor lifecycle precondition failed")
	}
	s.mutations++
	return nil
}

func (s *contendedControl) SuspendActor(_ context.Context, request *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(request, request.ExpectedUid, request.ExpectedVersion); err != nil {
		return nil, err
	}
	return &ateapi.SuspendActorResponse{Actor: &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: testActorUID, Version: 11},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	}}, nil
}

func (s *contendedControl) ResumeActor(_ context.Context, request *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(request, request.ExpectedUid, request.ExpectedVersion); err != nil {
		return nil, err
	}
	return &ateapi.ResumeActorResponse{Resumed: true, Actor: &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: testActorUID, Version: 11},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING},
	}, Restore: &ateapi.ActorRestoreResult{SnapshotUuid: testSnapshotUID, SnapshotGeneration: 3, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}}, nil
}

func TestLifecycleLeaseContentionPreservesWireIdentityAndFencing(t *testing.T) {
	for _, action := range []string{"suspend", "resume"} {
		for _, replaced := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "/released", true: "/identity-changed"}[replaced], func(t *testing.T) {
				server := &contendedControl{replacement: replaced}
				client := testClient(t, server)
				call := client.Suspend
				if action == "resume" {
					call = client.Resume
				}
				got, err := call(context.Background(), "actor", testActorUID, 10)
				server.mu.Lock()
				defer server.mu.Unlock()
				if len(server.requests) != 2 || !proto.Equal(server.requests[0], server.requests[1]) {
					t.Fatalf("retry changed the original wire request: %v", server.requests)
				}
				if replaced {
					if status.Code(err) != codes.FailedPrecondition || server.mutations != 0 {
						t.Fatalf("retry crossed a replacement: mutations=%d error=%v", server.mutations, err)
					}
					return
				}
				if err != nil || server.mutations != 1 || got.UID != testActorUID || got.Version != 11 {
					t.Fatalf("admitted operation failed: %+v mutations=%d error=%v", got, server.mutations, err)
				}
				if action == "resume" && (!got.Resumed || got.ActualFidelity != "MEMORY" || got.RestoredSnapshotUUID != testSnapshotUID || got.RestoredSnapshotGeneration != 3) {
					t.Fatalf("retry lost the actual restore receipt: %+v", got)
				}
			})
		}
	}
}

func TestActorLeaseRetryRequiresGuardedPreAdmissionRejection(t *testing.T) {
	for _, test := range []struct {
		name    string
		uid     string
		version int64
		code    codes.Code
		message string
	}{
		{"partial-workflow", testActorUID, 10, codes.Aborted, "concurrent update conflict, please retry"},
		{"worker-gone", testActorUID, 10, codes.Aborted, "worker of actor is gone"},
		{"read-failure", testActorUID, 10, codes.Internal, "workflow failed at step FinalizeRunning"},
		{"response-lost", testActorUID, 10, codes.Unavailable, "response lost"},
		{"authentication", testActorUID, 10, codes.Unauthenticated, "another operation is in progress for this actor"},
		{"stale-version", testActorUID, 10, codes.FailedPrecondition, "Actor lifecycle precondition failed"},
		{"unfenced-uid", "", 10, codes.Aborted, "another operation is in progress for this actor"},
		{"unfenced-version", testActorUID, 0, codes.Aborted, "another operation is in progress for this actor"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			_, err := actorLeaseCall(context.Background(), test.uid, test.version, func() (int, error) {
				calls++
				return 0, status.Error(test.code, test.message)
			})
			if calls != 1 || status.Code(err) != test.code {
				t.Fatalf("unproven rejection retried: calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestActorLeaseRetryHasAttemptAndContextBounds(t *testing.T) {
	calls := 0
	_, err := actorLeaseCall(context.Background(), testActorUID, 10, func() (int, error) {
		calls++
		return 0, status.Error(codes.Aborted, "another operation is in progress for this actor")
	})
	if status.Code(err) != codes.Aborted || calls != 6 {
		t.Fatalf("lease retry bound failed: calls=%d error=%v", calls, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	calls = 0
	_, err = actorLeaseCall(ctx, testActorUID, 10, func() (int, error) {
		calls++
		return 0, status.Error(codes.Aborted, "another operation is in progress for this actor")
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("deadline allowed another attempt: calls=%d error=%v", calls, err)
	}
	cancel()
	_, err = actorLeaseCall(ctx, testActorUID, 10, func() (int, error) {
		calls++
		return 1, nil
	})
	if err == nil || calls != 1 {
		t.Fatalf("canceled operation reached Substrate: calls=%d error=%v", calls, err)
	}
}

type contendedSnapshotControl struct {
	testControl
	mu       sync.Mutex
	requests []proto.Message
	busy     string
	changed  bool
}

func (s *contendedSnapshotControl) check(request proto.Message) error {
	s.requests = append(s.requests, proto.Clone(request))
	if len(s.requests) == 1 {
		return status.Error(codes.Aborted, s.busy)
	}
	if s.changed {
		return status.Error(codes.FailedPrecondition, "Actor source snapshot UUID changed")
	}
	return nil
}

func (s *contendedSnapshotControl) WaitActorSnapshotDurable(ctx context.Context, request *ateapi.WaitActorSnapshotDurableRequest) (*ateapi.WaitActorSnapshotDurableResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(request); err != nil {
		return nil, err
	}
	return s.testControl.WaitActorSnapshotDurable(ctx, request)
}

func (s *contendedSnapshotControl) CreateTag(ctx context.Context, request *ateapi.CreateTagRequest) (*ateapi.Tag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(request); err != nil {
		return nil, err
	}
	return s.testControl.CreateTag(ctx, request)
}

func TestSnapshotLeaseContentionRetainsImmutableSourceAndTag(t *testing.T) {
	for _, action := range []string{"wait", "retain-actor", "retain-tag"} {
		for _, changed := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "/released", true: "/source-changed"}[changed], func(t *testing.T) {
				server := &contendedSnapshotControl{changed: changed, busy: "another operation is in progress for this actor"}
				if action == "retain-tag" {
					server.busy = "another operation is in progress for this Tag"
				}
				server.actor = &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: testActorUID, Version: 10},
					Status: &ateapi.ActorStatus{Snapshots: []*ateapi.Snapshot{{Uuid: testSnapshotUID, Generation: 3, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
						DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}}}}
				client := testClient(t, server)
				var err error
				if action == "wait" {
					var result Summary
					result, err = client.AwaitDurable(context.Background(), "actor", testActorUID, testSnapshotUID, 3)
					if !changed && (result.UID != testActorUID || result.SnapshotUUID != testSnapshotUID || result.SnapshotGeneration != 3 || !result.Durable) {
						t.Fatalf("durability receipt lost its identity: %+v", result)
					}
				} else {
					var result RetainedSummary
					result, err = client.RetainSnapshot(context.Background(), "actor", testActorUID, testSnapshotUID, 3, "checkpoint")
					if !changed && (result.TagName != "checkpoint" || result.SourceActorUID != testActorUID || result.SourceSnapshotUUID != testSnapshotUID || result.SourceSnapshotGeneration != 3 || !result.Durable) {
						t.Fatalf("retained snapshot lost its identity: %+v", result)
					}
				}
				server.mu.Lock()
				defer server.mu.Unlock()
				if len(server.requests) != 2 || !proto.Equal(server.requests[0], server.requests[1]) {
					t.Fatalf("retry changed the immutable source: %v", server.requests)
				}
				if changed && status.Code(err) != codes.FailedPrecondition || !changed && err != nil {
					t.Fatalf("source fencing failed: changed=%v error=%v", changed, err)
				}
			})
		}
	}
}
