package control

import (
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
)

const resumeCapacityRetryDelay = time.Second

// ResumeCapacityProof records the authoritative read following a rejected
// ResumeActor RPC. The unchanged resource version fences every Actor mutation.
type ResumeCapacityProof struct {
	ActorUID           string    `json:"actor_uid"`
	ActorVersion       int64     `json:"actor_version"`
	SnapshotUUID       string    `json:"snapshot_uuid"`
	SnapshotGeneration int64     `json:"snapshot_generation"`
	ObservedAt         time.Time `json:"observed_at"`
}

type resumeCapacityWaitError struct {
	proof ResumeCapacityProof
	cause error
}

func (e *resumeCapacityWaitError) Error() string { return e.cause.Error() }
func (e *resumeCapacityWaitError) Unwrap() error { return e.cause }

func unchangedSuspendedActor(before, after substrateapi.Summary) bool {
	return before.State == "SUSPENDED" && before.UID != "" && before.Version > 0 &&
		before.SnapshotUUID != "" && before.Durable && before == after
}

func (p ResumeCapacityProof) matches(s *Session) bool {
	return s.Manifest != nil && p.ActorUID == s.Actor.UID &&
		p.ActorUID == s.Manifest.ActorUID && p.ActorVersion > 0 &&
		p.SnapshotUUID == s.Actor.SnapshotUUID && p.SnapshotUUID == s.Manifest.SnapshotUUID &&
		p.SnapshotGeneration == s.Actor.SnapshotGeneration && p.SnapshotGeneration == s.Manifest.SnapshotGeneration &&
		!p.ObservedAt.IsZero()
}
