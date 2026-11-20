package control

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A definite quiesce refusal happens before SuspendActor. Transport ambiguity
// and a lost cancellation acknowledgement retain the recovery-required path.
func (b *SubstrateBackend) rejectQuiesce(parent context.Context, s *Session, operation string, cause error) error {
	var response *runtimeHTTPError
	if !errors.As(cause, &response) || (response.code != "DRAIN_TIMEOUT" || response.status != 504) &&
		(response.code != "OUTCOME_UNKNOWN" || response.status != 409) {
		return cause
	}
	ctx, stop := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer stop()
	var ack struct {
		OperationID      string `json:"operation_id"`
		Epoch            uint64 `json:"epoch"`
		Generation       uint64 `json:"barrier_generation"`
		State            string `json:"state"`
		Gate             string `json:"gate"`
		ActorUID         string `json:"actor_uid"`
		IdentityAttested bool   `json:"identity_attested"`
	}
	if err := retryRuntimeRPC(ctx, "/runtime/v1/quiesce/cancel", 5*time.Second, func(attempt context.Context) error {
		ack.OperationID, ack.State, ack.Gate, ack.ActorUID = "", "", "", ""
		ack.Epoch, ack.Generation, ack.IdentityAttested = 0, 0, false
		return b.call(attempt, s.Actor.Name, "/runtime/v1/quiesce/cancel", map[string]any{
			"operation_id": operation, "expected_epoch": s.Epoch, "barrier_generation": s.BarrierGeneration, "actor_uid": s.Actor.UID}, &ack)
	}); err != nil {
		return fmt.Errorf("quiesce cancellation requires reconciliation: %w", err)
	}
	if ack.OperationID != operation || ack.Epoch != s.Epoch || ack.Generation != s.BarrierGeneration ||
		ack.State != "CANCELLED" || ack.Gate != "OPEN" || ack.ActorUID != s.Actor.UID || !ack.IdentityAttested {
		return errors.New("quiesce cancellation identity is unverified")
	}
	rejected := &quiesceRejected{code: response.code}
	if response.code == "OUTCOME_UNKNOWN" {
		rejected.recovery = runtimeUnknownHint(cause)
	}
	return rejected
}
