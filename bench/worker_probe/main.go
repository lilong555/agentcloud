// worker_probe records real Substrate Worker assignments without mutating Actors.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	dir := flag.String("client-dir", "artifacts/substrate/client", "private credentials")
	interval := flag.Duration("interval", 100*time.Millisecond, "assignment sampling interval")
	flag.Parse()
	if *interval < 10*time.Millisecond || *interval > time.Minute {
		fmt.Fprintln(os.Stderr, "sampling interval must be within 10ms..1m")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client, err := substrate.New(ctx, substrate.Config{Address: "127.0.0.1:9443", Atespace: "agentcloud", ServerName: "api.ate-system.svc", CAFile: filepath.Join(*dir, "ca.pem"), TokenFile: filepath.Join(*dir, "token")})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer client.Close()
	encoder := json.NewEncoder(os.Stdout)
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		callctx, stop := context.WithTimeout(ctx, 5*time.Second)
		workers, err := client.Control().ListWorkers(callctx, &ateapi.ListWorkersRequest{})
		if err != nil {
			stop()
			if ctx.Err() != nil {
				return
			}
			_ = encoder.Encode(map[string]any{"at": time.Now().UTC(), "error": err.Error()})
			// Unavailable APIs still obey the poll cadence. This also bounds error logs.
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		for _, worker := range workers.GetWorkers() {
			assignments, err := client.Control().ListWorkerActorAssignments(callctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: worker.GetMetadata().GetName()}})
			if err != nil {
				_ = encoder.Encode(map[string]any{"at": time.Now().UTC(), "error": err.Error()})
				continue
			}
			data, err := protojson.Marshal(assignments)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			allocated, _ := protojson.Marshal(worker.GetStatus().GetAllocated())
			_ = encoder.Encode(map[string]any{"at": time.Now().UTC(), "worker_name": worker.GetMetadata().GetName(), "assignments": json.RawMessage(data), "worker_allocated": json.RawMessage(allocated)})
		}
		stop()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
