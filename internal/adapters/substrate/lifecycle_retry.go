package substrate

import (
	"context"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func actorLeaseCall[T any](ctx context.Context, uid string, version int64, invoke func() (T, error)) (T, error) {
	return leaseAdmissionCall(ctx, uid != "" && version > 0, false, invoke)
}

// The pinned Substrate workflow acquires its leases before doing work. Callers
// retain the original UID/version or immutable snapshot identity. CreateTag may
// also contend on its Tag lease, before reserving a name or copying a snapshot.
// Other Aborted errors may follow partial execution and remain unreplayed.
func leaseAdmissionCall[T any](ctx context.Context, guarded, tagLease bool, invoke func() (T, error)) (T, error) {
	for retries := 0; ; retries++ {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		value, err := invoke()
		if err == nil || !guarded || retries == 5 || status.Code(err) != codes.Aborted {
			return value, err
		}
		message := status.Convert(err).Message()
		if message != "another operation is in progress for this actor" &&
			(!tagLease || message != "another operation is in progress for this Tag") {
			return value, err
		}
		observability.Observe(ctx, "substrate_actor_lease_retry", 1, "fenced_actor_lease_contention")
		timer := time.NewTimer(min(100*time.Millisecond<<retries, time.Second))
		select {
		case <-ctx.Done():
			timer.Stop()
			var zero T
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
}
