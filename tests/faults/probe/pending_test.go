package main

import (
	"testing"

	"github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

func TestPendingSnapshotRequiresIdentityMemoryAndRecordedUpload(t *testing.T) {
	expected := substrate.Summary{Name: "actor", UID: "actor-uid", SnapshotUUID: "snapshot", SnapshotGeneration: 3}
	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Name: expected.Name, Uid: expected.UID},
		Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, Snapshots: []*ateapi.Snapshot{{
			Uuid: "snapshot", Generation: 3, Locality: "worker", Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING,
				Object: &ateapi.ObjectSnapshot{SnapshotUri: "s3://snapshots/pending"}},
		}}},
	}
	if err := validatePendingSnapshot(actor, expected); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*ateapi.Actor){
		"actor replaced":      func(a *ateapi.Actor) { a.Metadata.Uid = "replacement" },
		"actor resumed":       func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING },
		"snapshot missing":    func(a *ateapi.Actor) { a.Status.Snapshots = nil },
		"snapshot replaced":   func(a *ateapi.Actor) { a.Status.Snapshots[0].Uuid = "other" },
		"generation advanced": func(a *ateapi.Actor) { a.Status.Snapshots[0].Generation++ },
		"rootfs downgrade": func(a *ateapi.Actor) {
			a.Status.Snapshots[0].Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		},
		"node lost":      func(a *ateapi.Actor) { a.Status.Snapshots[0].Locality = "" },
		"upload missing": func(a *ateapi.Actor) { a.Status.Snapshots[0].DurableSnapshot = nil },
		"object missing": func(a *ateapi.Actor) { a.Status.Snapshots[0].DurableSnapshot.Object = nil },
		"upload completed": func(a *ateapi.Actor) {
			a.Status.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.CloneOf(actor)
			mutate(changed)
			if err := validatePendingSnapshot(changed, expected); err == nil {
				t.Fatal("invalid PENDING fault window was accepted")
			}
		})
	}
}
