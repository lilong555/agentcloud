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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type lifecyclePreconditionStore struct {
	store.Interface
	actor          *ateapi.Actor
	leaseHeld      bool
	acquireCalls   int
	unguardedReads int
	writes         int
	changeVersion  bool
	changeUID      bool
	runAfterLease  bool
}

func (s *lifecyclePreconditionStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	s.acquireCalls++
	s.leaseHeld = true
	if s.changeVersion {
		s.actor.Metadata.Version++
	}
	if s.changeUID {
		s.actor.Metadata.Uid = "0b1d813f-0ccd-4848-bfe5-862de3b60c45"
	}
	if s.runAfterLease {
		s.actor.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	}
	return store.NewLease(ctx, func() { s.leaseHeld = false }), nil
}

func (s *lifecyclePreconditionStore) GetActor(context.Context, resources.ActorRef) (*ateapi.Actor, error) {
	if !s.leaseHeld {
		s.unguardedReads++
	}
	return proto.CloneOf(s.actor), nil
}

func (s *lifecyclePreconditionStore) GetActorTemplate(context.Context, resources.ActorTemplateRef) (*ateapi.ActorTemplate, error) {
	return &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: "team-a", Name: "tmpl"},
		SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://snapshots", PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
	}, nil
}

func (s *lifecyclePreconditionStore) UpdateActor(context.Context, resources.ActorRef, store.Precondition, func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	s.writes++
	return nil, errors.New("unexpected lifecycle write")
}

func TestActorLifecyclePreconditions(t *testing.T) {
	const actorUID = "95fba3d6-ad12-46e4-82b5-64cc8e7f302d"
	tests := []struct {
		name          string
		uid           string
		version       int64
		changeVersion bool
		changeUID     bool
		runAfterLease bool
		wantCode      codes.Code
	}{
		{name: "matching guards", uid: actorUID, version: 10},
		{name: "UID only", uid: actorUID},
		{name: "version only", version: 10},
		{name: "UID replaced under lease", uid: actorUID, version: 10, changeUID: true, wantCode: codes.FailedPrecondition},
		{name: "version advanced under lease", uid: actorUID, version: 10, changeVersion: true, wantCode: codes.FailedPrecondition},
		{name: "stale version", version: 9, wantCode: codes.FailedPrecondition},
		{name: "resumed with same snapshot generation", uid: actorUID, version: 10, changeVersion: true, runAfterLease: true, wantCode: codes.FailedPrecondition},
		{name: "unguarded"},
	}
	for _, operation := range []string{"suspend", "resume"} {
		for _, tt := range tests {
			t.Run(operation+"/"+tt.name, func(t *testing.T) {
				state := ateapi.ActorState_ACTOR_STATE_SUSPENDED
				if operation == "resume" {
					state = ateapi.ActorState_ACTOR_STATE_RUNNING
				}
				st := &lifecyclePreconditionStore{
					actor: &ateapi.Actor{
						Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor", Uid: actorUID, Version: 10},
						ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"},
						Status:        &ateapi.ActorStatus{State: state, LastAssignedGeneration: 4},
					},
					changeUID: tt.changeUID, changeVersion: tt.changeVersion, runAfterLease: tt.runAfterLease,
				}
				s := &RPCService{actorWorkflow: &ActorWorkflow{store: st}}
				ref := &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"}
				var err error
				if operation == "suspend" {
					_, err = s.SuspendActor(context.Background(), &ateapi.SuspendActorRequest{Actor: ref, ExpectedUid: tt.uid, ExpectedVersion: tt.version})
				} else {
					_, err = s.ResumeActor(context.Background(), &ateapi.ResumeActorRequest{Actor: ref, ExpectedUid: tt.uid, ExpectedVersion: tt.version})
				}
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("RPC code = %v, want %v: %v", got, tt.wantCode, err)
				}
				guarded := tt.uid != "" || tt.version != 0
				wantLeases := 1
				if operation == "resume" && !guarded {
					wantLeases = 0
				}
				if st.acquireCalls != wantLeases || st.leaseHeld {
					t.Errorf("leases = %d, held = %v; want %d, false", st.acquireCalls, st.leaseHeld, wantLeases)
				}
				if guarded && st.unguardedReads != 0 {
					t.Errorf("guarded RPC read actor %d times outside lease", st.unguardedReads)
				}
				if st.writes != 0 || st.actor.Status.LastAssignedGeneration != 4 {
					t.Errorf("RPC wrote actor %d times or changed snapshot generation", st.writes)
				}
			})
		}
	}
}
