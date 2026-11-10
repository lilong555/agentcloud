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
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestReadRetryTransportClassification(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, 2},
		{"EOF", io.EOF, 2},
		{"truncated", io.ErrUnexpectedEOF, 2},
		{"reset", syscall.ECONNRESET, 2},
		{"closed", net.ErrClosed, 2},
		{"driver-closed", pgconn.ErrConnClosed, 2},
		{"broken-pipe", syscall.EPIPE, 2},
		{"canceled", context.Canceled, 1},
		{"deadline", context.DeadlineExceeded, 1},
		{"query", errors.New("syntax error"), 1},
		{"message-only", errors.New("timeout: read tcp"), 1},
		{"closed-message-only", errors.New("conn closed"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := retryRead(t.Context(), "test", func() error {
				calls++
				if calls == 1 {
					return fmt.Errorf("query failed: %w", test.err)
				}
				return nil
			})
			if calls != test.want || test.want == 1 && !errors.Is(err, test.err) || test.want == 2 && err != nil {
				t.Fatalf("calls=%d error=%v; want calls=%d", calls, err, test.want)
			}
		})
	}
}

func TestReadRetryBoundAndContext(t *testing.T) {
	calls := 0
	err := retryRead(t.Context(), "test", func() error { calls++; return io.EOF })
	if calls != 4 || !errors.Is(err, io.EOF) {
		t.Fatalf("unbounded read retry: calls=%d error=%v", calls, err)
	}
	for _, duringRead := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		if !duringRead {
			cancel()
		}
		calls = 0
		err = retryRead(ctx, "test", func() error { calls++; cancel(); return io.EOF })
		cancel()
		if !errors.Is(err, context.Canceled) || !duringRead && calls != 0 || duringRead && calls != 1 {
			t.Fatalf("cancellation ignored: during=%v calls=%d error=%v", duringRead, calls, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	calls = 0
	err = retryRead(ctx, "test", func() error { calls++; return io.EOF })
	if calls != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backoff exceeded original deadline: calls=%d error=%v", calls, err)
	}
}
