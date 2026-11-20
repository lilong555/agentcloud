package control

import (
	"errors"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
)

type initialCapacityWaitError struct {
	actor Actor
	proof *substrateapi.Summary
	cause error
}

func (e *initialCapacityWaitError) Error() string { return e.cause.Error() }
func (e *initialCapacityWaitError) Unwrap() error { return e.cause }
func (e *initialCapacityWaitError) matches(s *Session) bool {
	if e.proof == nil {
		return s.Actor.UID == "" && e.actor.UID == ""
	}
	return e.proof.State == "SUSPENDED" && e.actor.UID != "" && e.actor.UID == e.proof.UID &&
		e.actor.Version == e.proof.Version && e.actor.Version > 0 &&
		(s.Actor.UID == "" || s.Actor.UID == e.actor.UID && s.Actor.Version == e.actor.Version)
}

func initialActorMatches(s *Session, current substrateapi.Summary) bool {
	d := s.Admissions["initial"]
	return d != nil && d.InitialActor != nil && current == *d.InitialActor &&
		current.State == "SUSPENDED" && current.UID != "" && current.UID == s.Actor.UID && current.Version == s.Actor.Version
}

var errInitialActorChanged = errors.New("initial Actor identity or state changed; explicit reconciliation is required")

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
