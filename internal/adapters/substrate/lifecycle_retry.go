package substrate

import (
	"context"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The pinned Substrate acquireLease implementation emits this rejection before
// it enters the Actor workflow. Every retry retains the original UID/version;
// a competing lifecycle operation can therefore invalidate, but never refresh,
// the caller's authority. Other Aborted errors may follow partial execution.
func actorLeaseCall[T any](ctx context.Context, uid string, version int64, invoke func() (T, error)) (T, error) {
	for retries := 0; ; retries++ {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		value, err := invoke()
		if err == nil || uid == "" || version <= 0 || retries == 5 || status.Code(err) != codes.Aborted ||
			status.Convert(err).Message() != "another operation is in progress for this actor" {
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
