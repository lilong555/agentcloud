package control

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type capacityControlServer struct {
	ateapi.UnimplementedControlServer
	before, after *ateapi.Actor
	tag           *ateapi.Tag
	resumeErr     error
	readErr       error
	reads         atomic.Int32
	fenced        atomic.Bool
}

func (s *capacityControlServer) GetActor(context.Context, *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	if s.reads.Add(1) == 1 {
		return s.before, nil
	}
	return s.after, s.readErr
}

func (s *capacityControlServer) GetTag(context.Context, *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	return s.tag, nil
}

func (s *capacityControlServer) ResumeActor(_ context.Context, request *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	s.fenced.Store(request.ExpectedUid == s.before.Metadata.Uid && request.ExpectedVersion == s.before.Metadata.Version)
	return nil, s.resumeErr
}

func TestSubstrateCapacityWaitRequiresUnchangedAuthoritativeActor(t *testing.T) {
	for _, test := range []struct {
		name     string
		mutate   func(*ateapi.Actor)
		code     codes.Code
		readFail bool
		verified bool
	}{
		{name: "unchanged-suspended", code: codes.ResourceExhausted, verified: true},
		{name: "version-changed", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Metadata.Version++ }},
		{name: "uid-changed", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Metadata.Uid = "replacement" }},
		{name: "snapshot-changed", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Status.Snapshots[0].Uuid = "replacement" }},
		{name: "generation-changed", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Status.Snapshots[0].Generation++ }},
		{name: "already-resuming", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING }},
		{name: "already-running", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING }},
		{name: "durability-changed", code: codes.ResourceExhausted, mutate: func(a *ateapi.Actor) {
			a.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		}},
		{name: "verification-unavailable", code: codes.ResourceExhausted, readFail: true},
		{name: "lost-response", code: codes.Unavailable},
		{name: "timeout", code: codes.DeadlineExceeded},
		{name: "aborted", code: codes.Aborted},
	} {
		t.Run(test.name, func(t *testing.T) {
			const uid = "95fba3d6-ad12-46e4-82b5-64cc8e7f302d"
			const snapshot = "45ee083b-1807-481d-a52c-885faad684ad"
			const tag = "0b1d813f-0ccd-4848-bfe5-862de3b60c45"
			before := &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: uid, Version: 9}, Status: &ateapi.ActorStatus{
				State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, Snapshots: []*ateapi.Snapshot{{Uuid: snapshot, Generation: 3, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
					DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}},
			}}
			after := proto.Clone(before).(*ateapi.Actor)
			if test.mutate != nil {
				test.mutate(after)
			}
			server := &capacityControlServer{before: before, after: after, resumeErr: status.Error(test.code, "resume rejected"), tag: &ateapi.Tag{
				Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "retained", Uid: tag, Version: 2},
				SourceActor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"},
				Status: &ateapi.TagStatus{SourceActorUid: uid, SourceSnapshotUuid: snapshot, SourceSnapshotGeneration: 3,
					Snapshot: &ateapi.Snapshot{Uuid: tag, Generation: 1, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
						DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED, Object: &ateapi.ObjectSnapshot{SnapshotUri: "s3://snapshots/retained"}}}},
			}}
			if test.readFail {
				server.readErr = status.Error(codes.Unavailable, "verification unavailable")
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			grpcServer := grpc.NewServer()
			ateapi.RegisterControlServer(grpcServer, server)
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()
			client, err := substrateapi.New(context.Background(), substrateapi.Config{Address: listener.Addr().String(), Insecure: true, Atespace: "team-a"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			backend := &SubstrateBackend{cfg: SubstrateConfig{Client: client}}
			s := &Session{Mode: "in-actor", Actor: Actor{Name: "actor", UID: uid, SnapshotUUID: snapshot, SnapshotGeneration: 3},
				Manifest: &Manifest{ActorUID: uid, SnapshotUUID: snapshot, SnapshotGeneration: 3, RetainedTag: "retained", RetainedTagUID: tag}}
			_, err = backend.Resume(context.Background(), s)
			var waiting *resumeCapacityWaitError
			verified := errors.As(err, &waiting)
			if verified != test.verified || !server.fenced.Load() {
				t.Fatalf("verified=%t fenced=%t error=%v", verified, server.fenced.Load(), err)
			}
			if verified && (!waiting.proof.matches(s) || waiting.proof.ActorVersion != 9) {
				t.Fatalf("capacity proof does not describe the checked Actor: %+v", waiting.proof)
			}
			expectedReads := int32(1)
			if test.code == codes.ResourceExhausted {
				expectedReads++
			}
			if server.reads.Load() != expectedReads {
				t.Fatalf("authoritative reads=%d want=%d", server.reads.Load(), expectedReads)
			}
		})
	}
}
