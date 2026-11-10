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

package atepg

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
	"time"
)

// retryRead accepts a side-effect-free SELECT outside a transaction. Each
// attempt acquires its connection through the pool, which retires connections
// whose protocol exchange failed. Lifecycle writes and whole workflows retain
// their original uncertain-outcome semantics.
func retryRead(ctx context.Context, operation string, read func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := read()
		if err == nil || attempt == 3 || !readTransportError(err) {
			return err
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		slog.WarnContext(ctx, "Retrying read-only PostgreSQL query", "operation", operation, "retry", attempt+1, "err", err)
		timer := time.NewTimer(50 * time.Millisecond << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func readTransportError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkError net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		(errors.As(err, &networkError) && networkError.Timeout())
}
