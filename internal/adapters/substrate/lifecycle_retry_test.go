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
