// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type durabilityStore struct {
	store.Interface
	actor *ateapi.Actor
	reads int
	read  func(*durabilityStore) error
}

func (s *durabilityStore) GetActor(context.Context, resources.ActorRef) (*ateapi.Actor, error) {
	s.reads++
	if s.read != nil {
		if err := s.read(s); err != nil {
			return nil, err
		}
	}
	return proto.CloneOf(s.actor), nil
}

func TestWaitActorSnapshotDurable(t *testing.T) {
	const uid = "95fba3d6-ad12-46e4-82b5-64cc8e7f302d"
	const snapUID = "45ee083b-1807-481d-a52c-885faad684ad"
	for _, tt := range []struct {
		name   string
		mutate func(*durabilityStore)
		read   func(*durabilityStore) error
		want   codes.Code
	}{
		{name: "completed"},
		{name: "suspended pending becomes durable", mutate: func(s *durabilityStore) {
			s.actor.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		}, read: func(s *durabilityStore) error {
			if s.reads == 2 {
				s.actor.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
				s.actor.Metadata.Version++
			}
			return nil
		}},
		{name: "actor replaced while upload pending", mutate: func(s *durabilityStore) {
			s.actor.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		}, read: func(s *durabilityStore) error {
			if s.reads == 2 {
				s.actor.Metadata.Uid = snapUID
			}
			return nil
		}, want: codes.FailedPrecondition},
		{name: "wrong generation", mutate: func(s *durabilityStore) { s.actor.Status.Snapshots[0].Generation++ }, want: codes.NotFound},
		{name: "wrong snapshot uuid", mutate: func(s *durabilityStore) { s.actor.Status.Snapshots[0].Uuid = uid }, want: codes.NotFound},
		{name: "snapshot pruned", mutate: func(s *durabilityStore) { s.actor.Status.Snapshots = nil }, want: codes.NotFound},
		{name: "upload absent", mutate: func(s *durabilityStore) { s.actor.Status.Snapshots[0].DurableSnapshot = nil }, want: codes.DataLoss},
		{name: "upload invalid", mutate: func(s *durabilityStore) {
			s.actor.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_UNSPECIFIED
		}, want: codes.DataLoss},
		{name: "actor crashed while pending", mutate: func(s *durabilityStore) {
			s.actor.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
			s.actor.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		}, want: codes.FailedPrecondition},
		{name: "actor deleted", read: func(*durabilityStore) error { return store.ErrNotFound }, want: codes.NotFound},
		{name: "store failed", read: func(*durabilityStore) error { return errors.New("database unavailable") }, want: codes.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := &durabilityStore{actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: uid, Version: 4}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, Snapshots: []*ateapi.Snapshot{{Uuid: snapUID, Generation: 3, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}}}}, read: tt.read}
			if tt.mutate != nil {
				tt.mutate(st)
			}
			service := &RPCService{impl: newServiceImpl(st, nil)}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := service.WaitActorSnapshotDurable(ctx, &ateapi.WaitActorSnapshotDurableRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"}, ExpectedUid: uid, SnapshotUuid: snapUID, SnapshotGeneration: 3})
			if apierror.Code(err) != tt.want {
				t.Fatalf("code=%s want=%s err=%v", apierror.Code(err), tt.want, err)
			}
			if err == nil && (resp.GetSnapshot().GetUuid() != snapUID || resp.GetSnapshot().GetGeneration() != 3 || resp.GetSnapshot().GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED) {
				t.Fatalf("unexpected durable receipt: %v", resp)
			}
		})
	}
	t.Run("cancelled before read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		service := &RPCService{}
		_, err := service.WaitActorSnapshotDurable(ctx, &ateapi.WaitActorSnapshotDurableRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"}, ExpectedUid: uid, SnapshotUuid: snapUID, SnapshotGeneration: 3})
		if status.Code(err) != codes.Canceled {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestRuntimeRestoreReceipt(t *testing.T) {
	for _, tt := range []struct {
		name     string
		stored   ateapi.SnapshotFidelity
		reported ateletpb.SnapshotFidelity
		cold     bool
		replaced bool
	}{
		{name: "memory", stored: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, reported: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{name: "runtime reports rootfs", stored: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, reported: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{name: "template replaced volumes", stored: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, reported: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, replaced: true},
		{name: "older runtime unknown", stored: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{name: "cold start", cold: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := &capturingAtelet{restoredFidelity: &tt.reported}
			dialer := newNodeAteletDialer(t, fake)
			actor := &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: "95fba3d6-ad12-46e4-82b5-64cc8e7f302d"}, ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RESUMING, WorkerAssignment: &ateapi.WorkerAssignment{NodeName: "node-1"}}}
			tmpl := &ateapi.ActorTemplate{Metadata: &ateapi.ResourceMetadata{Uid: "tmpl-current"}}
			if !tt.cold {
				actor.Status.Snapshots = []*ateapi.Snapshot{{Uuid: "45ee083b-1807-481d-a52c-885faad684ad", Generation: 3, Fidelity: tt.stored, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED, Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://snapshots/source"}}}}
				if tt.replaced {
					actor.Status.Snapshots[0].ActorTemplateUid = "tmpl-previous"
				}
			}
			workflow := &ActorWorkflow{dialer: dialer}
			receipt, _, err := workflow.ensureAteletRestored(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "actor"}, actor, tmpl, nil)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Result == nil || receipt.Result.GetColdStarted() != tt.cold || receipt.Result.GetFidelity() != ateapi.SnapshotFidelity(tt.reported) {
				t.Fatalf("receipt=%v", receipt.Result)
			}
			if !tt.cold && (receipt.Result.GetSnapshotGeneration() != 3 || receipt.Result.GetSnapshotUuid() != actor.Status.Snapshots[0].GetUuid()) {
				t.Fatalf("snapshot receipt=%v", receipt.Result)
			}
		})
	}
}

func TestResumeAlreadyRunningReceipt(t *testing.T) {
	receipt := &ateapi.ActorRestoreResult{Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, SnapshotUuid: "45ee083b-1807-481d-a52c-885faad684ad", SnapshotGeneration: 2}
	st := &lifecyclePreconditionStore{actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: "95fba3d6-ad12-46e4-82b5-64cc8e7f302d", Version: 10}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING, LastRestore: receipt}}}
	service := NewRPCService(st, nil, nil, nil, nil, nil, "", nil, nil, "", nil, "")
	resp, err := service.ResumeActor(context.Background(), &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"}, ExpectedUid: st.actor.Metadata.Uid, ExpectedVersion: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetResumed() || !proto.Equal(resp.GetRestore(), receipt) {
		t.Fatalf("resume receipt=%v", resp)
	}
}
