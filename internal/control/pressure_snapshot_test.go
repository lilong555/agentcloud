package control

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPressureSnapshotCurrentEpochAndImmutable(t *testing.T) {
	now := time.Date(2026, 11, 20, 0, 0, 10, 0, time.UTC)
	s := &Session{Epoch: 2, Admissions: map[string]*AdmissionDemand{
		"initial": {Kind: "initial_admission", Status: "WAITING_CAPACITY", Epoch: 2, RequestedAt: now.Add(-3 * time.Second)},
		"resume":  {Kind: "resume_admission", Status: "PENDING", Epoch: 2, RequestedAt: now.Add(-time.Second)},
		"rpc":     {Kind: "resume_admission", Status: "ATTEMPTING", Epoch: 2, RequestedAt: now.Add(-8 * time.Second)},
		"old":     {Kind: "initial_admission", Status: "WAITING_CAPACITY", Epoch: 1, RequestedAt: now.Add(-9 * time.Second)},
		"future":  {Kind: "initial_admission", Status: "PENDING", Epoch: 2, RequestedAt: now.Add(time.Second)},
		"done":    {Kind: "initial_admission", Status: "READY", Epoch: 2, RequestedAt: now.Add(-9 * time.Second)},
	}, SnapshotGC: &SnapshotGCState{Tags: map[string]*SnapshotGCRecord{
		"keep":    {TagUID: "uid1", CreatedAt: now.Add(-time.Second)},
		"alias":   {TagUID: "uid1", CreatedAt: now.Add(-time.Second)},
		"pending": {CreatedAt: now.Add(-time.Second)},
		"deleted": {TagUID: "uid2", DeleteState: "DELETED"},
	}}}
	sessions := map[string]*Session{"s": s}
	before, _ := json.Marshal(sessions)
	r := pressureSnapshot(sessions, now)
	if r.Initial.WaitingCapacity != 1 || r.Initial.OldestWaitMS != 3000 || r.Resume.Pending != 1 || r.Resume.Attempting != 1 || r.Resume.OldestWaitMS != 1000 {
		t.Fatalf("unexpected queues: %+v", r)
	}
	if r.RetainedTags != 1 || r.PendingTags != 1 {
		t.Fatalf("unexpected inventory: %+v", r)
	}
	if r.PhysicalDisk != nil || r.GCStates["RETAINED"] != 3 || r.GCStates["DELETED"] != 1 {
		t.Fatalf("inventory must preserve GC states and leave disk measurement absent: %+v", r)
	}
	after, _ := json.Marshal(sessions)
	if string(before) != string(after) {
		t.Fatal("read-only aggregation mutated state")
	}
}
