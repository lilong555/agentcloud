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
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/status"
)

// WaitActorSnapshotDurable observes one snapshot while its upload progresses.
// Reads do not hold the lifecycle lease, so upload reports can commit.
func (s *RPCService) WaitActorSnapshotDurable(ctx context.Context, req *ateapi.WaitActorSnapshotDurableRequest) (*ateapi.WaitActorSnapshotDurableResponse, error) {
	if errs := apivalidation.ValidateWaitActorSnapshotDurableRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	ref := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, ref)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		actor, err := s.impl.GetActor(ctx, ref)
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", ref)
		}
		if err != nil {
			return nil, err
		}
		if actor.GetMetadata().GetUid() != req.GetExpectedUid() {
			return nil, apierror.FailedPrecondition("Actor %s UID changed", ref)
		}
		var snap *ateapi.Snapshot
		for _, candidate := range actor.GetStatus().GetSnapshots() {
			if candidate.GetUuid() == req.GetSnapshotUuid() && candidate.GetGeneration() == req.GetSnapshotGeneration() {
				snap = candidate
				break
			}
		}
		if snap == nil {
			return nil, apierror.NotFound("Actor %s snapshot %s generation %d is absent", ref, req.GetSnapshotUuid(), req.GetSnapshotGeneration())
		}
		if snap.GetDurableSnapshot() == nil {
			return nil, apierror.DataLoss("Actor %s snapshot %s records no upload", ref, snap.GetUuid())
		}
		if snap.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
			setSpanActorAttributes(ctx, actor)
			return &ateapi.WaitActorSnapshotDurableResponse{Actor: actor, Snapshot: snap}, nil
		}
		if snap.GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING {
			return nil, apierror.DataLoss("Actor %s snapshot %s has invalid storage state", ref, snap.GetUuid())
		}
		if actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_CRASHED || actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_DELETING {
			return nil, apierror.FailedPrecondition("Actor %s upload interrupted in state %s", ref, actor.GetStatus().GetState())
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
			timer.Reset(250 * time.Millisecond)
		}
	}
}
