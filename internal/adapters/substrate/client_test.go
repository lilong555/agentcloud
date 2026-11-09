package substrate

import (
	"context"
	"net"
	"testing"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testActorUID = "95fba3d6-ad12-46e4-82b5-64cc8e7f302d"
const testSnapshotUID = "45ee083b-1807-481d-a52c-885faad684ad"
const testTagUID = "0b1d813f-0ccd-4848-bfe5-862de3b60c45"

type testControl struct {
	ateapi.UnimplementedControlServer
	actor        *ateapi.Actor
	tag          *ateapi.Tag
	suspend      *ateapi.SuspendActorRequest
	resume       *ateapi.ResumeActorRequest
	await        *ateapi.WaitActorSnapshotDurableRequest
	createTag    *ateapi.CreateTagRequest
	createErr    error
	egress       *ateapi.EgressPolicy
	createEgress *ateapi.CreateActorEgressPolicyRequest
}

func (s *testControl) CreateActorEgressPolicy(_ context.Context, r *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	s.createEgress = r
	if s.egress != nil {
		return nil, status.Error(codes.AlreadyExists, "present")
	}
	s.egress = r.EgressPolicy
	return s.egress, nil
}
func (s *testControl) GetActorEgressPolicy(context.Context, *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	return s.egress, nil
}

func TestCoordinatorEgressWire(t *testing.T) {
	s := &testControl{}
	c := testClient(t, s)
	ctx := context.Background()
	if err := c.AllowCoordinatorEgress(ctx, "actor", "coordinator.agentcloud.test", 8092); err != nil {
		t.Fatal(err)
	}
	if s.createEgress.Actor.Name != "actor" || s.createEgress.Actor.Atespace != "team-a" || s.egress.Rules[0].Http.Ports.Numbers[0] != 8092 {
		t.Fatalf("egress=%v", s.createEgress)
	}
	if err := c.AllowCoordinatorEgress(ctx, "actor", "coordinator.agentcloud.test", 8092); err != nil {
		t.Fatal(err)
	}
	if err := c.AllowCoordinatorEgress(ctx, "actor", "other.agentcloud.test", 8092); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("different origin=%v", err)
	}
	if err := c.AllowCoordinatorEgress(ctx, "actor", "127.0.0.1", 8092); err == nil {
		t.Fatal("IP literal accepted")
	}
}

func (s *testControl) GetActor(context.Context, *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	return s.actor, nil
}
func (s *testControl) SuspendActor(_ context.Context, r *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	s.suspend = r
	return &ateapi.SuspendActorResponse{Actor: s.actor}, nil
}
func (s *testControl) ResumeActor(_ context.Context, r *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	s.resume = r
	return &ateapi.ResumeActorResponse{Actor: s.actor, Resumed: true, Restore: s.actor.GetStatus().GetLastRestore()}, nil
}
func (s *testControl) WaitActorSnapshotDurable(_ context.Context, r *ateapi.WaitActorSnapshotDurableRequest) (*ateapi.WaitActorSnapshotDurableResponse, error) {
	s.await = r
	return &ateapi.WaitActorSnapshotDurableResponse{Actor: s.actor, Snapshot: s.actor.Status.Snapshots[0]}, nil
}
func (s *testControl) GetTag(context.Context, *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	if s.tag == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return s.tag, nil
}
func (s *testControl) CreateTag(_ context.Context, r *ateapi.CreateTagRequest) (*ateapi.Tag, error) {
	s.createTag = r
	s.tag = &ateapi.Tag{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: r.Tag.Metadata.Name, Uid: testTagUID, Version: 3}, SourceActor: r.Tag.SourceActor, Status: &ateapi.TagStatus{SourceActorUid: r.ExpectedUid, SourceSnapshotUuid: r.ExpectedSnapshotUuid, SourceSnapshotGeneration: r.ExpectedSnapshotGeneration, Snapshot: &ateapi.Snapshot{Uuid: testTagUID, Generation: 1, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED, Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://snapshot/tag-copy"}}}}}
	if s.createErr != nil {
		return nil, s.createErr
	}
	return s.tag, nil
}

func testClient(t *testing.T, s *testControl) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	ateapi.RegisterControlServer(server, s)
	go server.Serve(listener)
	client, err := New(context.Background(), Config{Address: listener.Addr().String(), Insecure: true, Atespace: "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Stop(); listener.Close() })
	return client
}
func TestPublicLifecycleWire(t *testing.T) {
	s := &testControl{actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: testActorUID, Version: 10}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, Snapshots: []*ateapi.Snapshot{{Uuid: testSnapshotUID, Generation: 3, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}}, LastRestore: &ateapi.ActorRestoreResult{Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, SnapshotUuid: testSnapshotUID, SnapshotGeneration: 3}}}}
	c := testClient(t, s)
	ctx := context.Background()
	suspended, err := c.Suspend(ctx, "actor", testActorUID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if s.suspend.GetExpectedUid() != testActorUID || s.suspend.GetExpectedVersion() != 10 || s.suspend.GetActor().GetAtespace() != "team-a" {
		t.Fatalf("suspend guards=%v", s.suspend)
	}
	if suspended.SnapshotUUID != testSnapshotUID || suspended.SnapshotGeneration != 3 {
		t.Fatalf("suspend=%+v", suspended)
	}
	durable, err := c.AwaitDurable(ctx, "actor", testActorUID, testSnapshotUID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !durable.Durable || s.await.GetSnapshotUuid() != testSnapshotUID || s.await.GetSnapshotGeneration() != 3 {
		t.Fatalf("await=%v summary=%+v", s.await, durable)
	}
	resumed, err := c.Resume(ctx, "actor", testActorUID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Resumed || resumed.ActualFidelity != "ROOTFS" || resumed.SnapshotFidelity != "MEMORY" || resumed.ColdStarted || s.resume.GetExpectedVersion() != 10 {
		t.Fatalf("resume=%+v", resumed)
	}
}
func TestRetainReconcilesUnknownCreate(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "response lost"}[unknown], func(t *testing.T) {
			s := &testControl{}
			if unknown {
				s.createErr = status.Error(codes.Unavailable, "response lost")
			}
			c := testClient(t, s)
			retained, err := c.RetainSnapshot(context.Background(), "actor", testActorUID, testSnapshotUID, 3, "checkpoint")
			if err != nil {
				t.Fatal(err)
			}
			if !retained.Durable || retained.SourceSnapshotUUID != testSnapshotUID || retained.SnapshotUUID != testTagUID || retained.SourceActorUID != testActorUID {
				t.Fatalf("retained=%+v", retained)
			}
			s.createTag = nil
			if _, err := c.RetainSnapshot(context.Background(), "actor", testActorUID, testSnapshotUID, 3, "checkpoint"); err != nil {
				t.Fatal(err)
			}
			if s.createTag != nil {
				t.Fatal("existing tag was created again")
			}
			if _, err := c.RetainSnapshot(context.Background(), "actor", testActorUID, testSnapshotUID, 4, "checkpoint"); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("source generation mismatch=%v", err)
			}
			s.tag.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
			if _, err := c.RetainSnapshot(context.Background(), "actor", testActorUID, testSnapshotUID, 3, "checkpoint"); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("pending tag=%v", err)
			}
		})
	}
}
