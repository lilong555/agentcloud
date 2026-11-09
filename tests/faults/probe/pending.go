package main

import (
	"errors"

	"github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
)

// A timeout alone supplies no evidence about snapshot or Tag safety.
func validatePendingSnapshot(actor *ateapi.Actor, expected substrate.Summary) error {
	if expected.UID == "" || expected.SnapshotUUID == "" || expected.SnapshotGeneration < 1 ||
		actor.GetMetadata().GetUid() != expected.UID || actor.GetMetadata().GetName() != expected.Name ||
		actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		return errors.New("PENDING Actor identity or suspended state changed")
	}
	var latest *ateapi.Snapshot
	for _, snapshot := range actor.GetStatus().GetSnapshots() {
		if latest == nil || snapshot.GetGeneration() > latest.GetGeneration() {
			latest = snapshot
		}
	}
	if latest.GetUuid() != expected.SnapshotUUID || latest.GetGeneration() != expected.SnapshotGeneration {
		return errors.New("PENDING snapshot UUID or generation changed")
	}
	if latest.GetFidelity() != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY || latest.GetLocality() == "" ||
		latest.GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING ||
		latest.GetDurableSnapshot().GetObject().GetSnapshotUri() == "" {
		return errors.New("injection requires a local MEMORY snapshot with a recorded PENDING upload")
	}
	return nil
}
