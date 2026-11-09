package substrate

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestNativeGRPCTraceMetadataAndSeparateDurabilityTimings(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime, err := observability.New(context.Background(), observability.Config{Exporter: exporter})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if !strings.Contains(strings.Join(md.Get("traceparent"), ""), "-") {
			t.Errorf("W3C trace missing %v", md)
		}
		if len(md.Get("baggage")) != 0 {
			t.Error("baggage propagated")
		}
		if strings.HasSuffix(info.FullMethod, "/SuspendActor") {
			time.Sleep(20 * time.Millisecond)
		}
		if strings.HasSuffix(info.FullMethod, "/WaitActorSnapshotDurable") {
			time.Sleep(35 * time.Millisecond)
		}
		return next(ctx, req)
	}))
	defer server.Stop()
	actor := &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: "actor", Uid: testActorUID, Version: 4}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, Snapshots: []*ateapi.Snapshot{{Uuid: testSnapshotUID, Generation: 3, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}}}}}
	ateapi.RegisterControlServer(server, &testControl{actor: actor})
	go server.Serve(listener)
	client, err := New(context.Background(), Config{Address: listener.Addr().String(), Atespace: "team-a", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, parent := observability.Start(observability.WithRuntime(context.Background(), runtime), "coordinator.lifecycle", trace.SpanKindInternal)
	var observations []observability.Measurement
	ctx = observability.WithScope(ctx, observability.Scope{Record: func(m observability.Measurement) { observations = append(observations, m) }})
	if _, err := client.Suspend(ctx, "actor", testActorUID, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AwaitDurable(ctx, "actor", testActorUID, testSnapshotUID, 3); err != nil {
		t.Fatal(err)
	}
	parent.End()
	if err := runtime.Provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 || observations[0].Metric != "substrate_suspend_ms" || observations[0].Value < 20 || observations[1].Metric != "durability_wait_ms" || observations[1].Value < 35 {
		t.Fatalf("observations %+v", observations)
	}
	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("spans %d", len(spans))
	}
	for _, span := range spans {
		if span.Name != "coordinator.lifecycle" && span.Parent.SpanID() != parent.SpanContext().SpanID() {
			t.Fatalf("lost parent %s", span.Name)
		}
	}
}
