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
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

type queryReadFault struct {
	prefix   string
	queries  atomic.Int32
	pending  atomic.Bool
	injected atomic.Int32
}

func (f *queryReadFault) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(strings.TrimSpace(data.SQL), f.prefix) && f.queries.Add(1) == 1 {
		f.pending.Store(true)
	}
	return ctx
}

func (*queryReadFault) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type queryFaultConn struct {
	net.Conn
	fault *queryReadFault
}

func (c *queryFaultConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if n > 0 && c.fault.pending.CompareAndSwap(true, false) {
		c.fault.injected.Add(1)
		return 0, &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	}
	return n, err
}

func actorReadFaultPool(t *testing.T, source *pgxpool.Pool, fault *queryReadFault) *pgxpool.Pool {
	t.Helper()
	config := source.Config()
	config.MaxConns, config.MinConns, config.MinIdleConns = 1, 0, 0
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.ConnConfig.Tracer = fault
	dial := config.ConnConfig.DialFunc
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &queryFaultConn{Conn: conn, fault: fault}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestGetActorRecoversReadTransport(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	createTestAtespace(t, s, "team-read-retry")
	actor, err := s.CreateActor(ctx, &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-read-retry", Name: "actor"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDING},
	})
	if err != nil {
		t.Fatal(err)
	}
	fault := &queryReadFault{prefix: "SELECT proto FROM actors"}
	reader := &Persistence{pool: actorReadFaultPool(t, s.pool, fault)}
	actual, err := reader.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil || !proto.Equal(actual, actor) || fault.queries.Load() != 2 || fault.injected.Load() != 1 {
		t.Fatalf("read did not recover the original record: actor=%v error=%v queries=%d faults=%d", actual, err, fault.queries.Load(), fault.injected.Load())
	}
	_, err = reader.GetActor(ctx, resources.ActorRef{Atespace: "team-read-retry", Name: "missing"})
	if !errors.Is(err, store.ErrNotFound) || fault.queries.Load() != 3 {
		t.Fatalf("missing row was retried or remapped: error=%v queries=%d", err, fault.queries.Load())
	}
}

func TestActorWriteTransportRemainsSingleAttempt(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	createTestAtespace(t, s, "team-write-unknown")
	actor, err := s.CreateActor(ctx, &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-write-unknown", Name: "actor"}})
	if err != nil {
		t.Fatal(err)
	}
	fault := &queryReadFault{prefix: "UPDATE actors"}
	writer := &Persistence{pool: actorReadFaultPool(t, s.pool, fault)}
	mutations := 0
	_, err = writer.UpdateActor(ctx, resources.ActorRefFromActor(actor), store.PreconditionFrom(actor), func(current *ateapi.Actor) error {
		mutations++
		current.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
		return nil
	})
	if err == nil || mutations != 1 || fault.queries.Load() != 1 || fault.injected.Load() != 1 {
		t.Fatalf("ambiguous write was replayed: mutations=%d queries=%d faults=%d error=%v", mutations, fault.queries.Load(), fault.injected.Load(), err)
	}
	stored, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil || stored.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion()+1 || stored.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("lost write response did not preserve one committed mutation: actor=%v error=%v", stored, err)
	}
}
