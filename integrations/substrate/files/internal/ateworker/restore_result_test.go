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

package ateworker

import (
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestRestoreWorkloadResultWire(t *testing.T) {
	for _, fidelity := range []ateworkerpb.SnapshotFidelity{
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	} {
		encoded, err := proto.Marshal(RestoreWorkloadResult(42, fidelity))
		if err != nil {
			t.Fatal(err)
		}
		var received ateworkerpb.RestoreWorkloadResponse
		if err := proto.Unmarshal(encoded, &received); err != nil {
			t.Fatal(err)
		}
		if received.GetWorkerEpoch() != 42 || received.GetRestoredFidelity() != fidelity {
			t.Fatalf("wire receipt=%v; want epoch=42 fidelity=%v", &received, fidelity)
		}
	}
	// An older worker sends only field 1. The new reader retains unknown fidelity.
	var legacy ateworkerpb.RestoreWorkloadResponse
	if err := proto.Unmarshal([]byte{0x08, 0x2a}, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.GetWorkerEpoch() != 42 || legacy.GetRestoredFidelity() != ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		t.Fatalf("legacy response=%v", &legacy)
	}
}
