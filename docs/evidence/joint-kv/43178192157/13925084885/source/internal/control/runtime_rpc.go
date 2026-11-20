package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
)

type runtimeHTTPError struct {
	status  int
	code    string
	body    string
	details json.RawMessage
}

func (e *runtimeHTTPError) Error() string {
	return fmt.Sprintf("runtime HTTP %d: %.512s", e.status, e.body)
}

type runtimeAttachReceipt struct {
	Epoch            uint64 `json:"epoch"`
	Gate             string `json:"gate"`
	ActorUID         string `json:"actor_uid"`
	ManifestID       string `json:"manifest_id"`
	IdentityAttested bool   `json:"identity_attested"`
}

type runtimeBarrierReceipt struct {
	OperationID string `json:"operation_id"`
	Epoch       uint64 `json:"epoch"`
	Generation  uint64 `json:"barrier_generation"`
	Watermark   uint64 `json:"receipt_watermark"`
	State       string `json:"state"`
	Gate        string `json:"gate"`
	Active      int    `json:"active_tools"`
}

func runtimeTransportRetryable(err error) bool {
	var response *runtimeHTTPError
	if errors.As(err, &response) {
		return response.code == "" && (response.status == http.StatusBadGateway || response.status == http.StatusServiceUnavailable || response.status == http.StatusGatewayTimeout)
	}
	var network net.Error
	if errors.As(err, &network) && (network.Timeout() || network.Temporary()) {
		return true
	}
	var malformed *json.SyntaxError
	return errors.As(err, &malformed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE)
}

// retryRuntimeRPC reuses a stable, server-fenced RPC identity. A RUNNING tool
// response observes the original durable claim; UNKNOWN and conflicts terminate.
func retryRuntimeRPC(parent context.Context, path string, limit time.Duration, invoke func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, limit)
	defer cancel()
	transportRetries := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptLimit := 10 * time.Second
		if path == "/runtime/v1/quiesce" {
			// The runtime owns a 30-second drain deadline; allow its terminal reply.
			attemptLimit = 35 * time.Second
		}
		attempt, stop := context.WithTimeout(ctx, attemptLimit)
		err := invoke(attempt)
		stop()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		var response *runtimeHTTPError
		pending := path == "/runtime/v1/tools" && errors.As(err, &response) && response.status == http.StatusConflict && response.code == "RUNNING"
		delay, source := time.Second, "durable_tool_claim_pending"
		if !pending {
			if transportRetries >= 3 || !runtimeTransportRetryable(err) {
				return err
			}
			delay, source = 100*time.Millisecond<<transportRetries, "idempotent_runtime_transport_retry"
			transportRetries++
		}
		observability.Observe(ctx, "runtime_rpc_retry", 1, source)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
