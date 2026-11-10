package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshotTestBackend struct {
	*LocalBackend
	mu               sync.Mutex
	tags             map[string]substrateapi.SnapshotTag
	reads            []string
	deleted          []string
	readErrors       map[string]error
	deleteErrors     map[string]error
	lostResponse     map[string]bool
	beforeDelete     func(substrateapi.SnapshotTag)
	beforeCheckpoint func(*Session, string)
}

func (b *snapshotTestBackend) SnapshotTag(_ context.Context, name string) (substrateapi.SnapshotTag, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads = append(b.reads, name)
	if err := b.readErrors[name]; err != nil {
		return substrateapi.SnapshotTag{}, err
	}
	tag, ok := b.tags[name]
	if !ok {
		return tag, status.Error(codes.NotFound, "Tag absent")
	}
	return tag, nil
}

func (b *snapshotTestBackend) DeleteSnapshotTag(_ context.Context, tag substrateapi.SnapshotTag) error {
	if b.beforeDelete != nil {
		b.beforeDelete(tag)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleted = append(b.deleted, tag.TagName)
	if err := b.deleteErrors[tag.TagName]; err != nil {
		return err
	}
	if current, ok := b.tags[tag.TagName]; ok && current.TagUID != tag.TagUID {
		return status.Error(codes.FailedPrecondition, "Tag replaced")
	}
	delete(b.tags, tag.TagName)
	if b.lostResponse[tag.TagName] {
		return status.Error(codes.Unavailable, "DELETE response lost")
	}
	return nil
}

func (b *snapshotTestBackend) Checkpoint(ctx context.Context, s *Session, operation string) (Actor, string, error) {
	if b.beforeCheckpoint != nil {
		b.beforeCheckpoint(s, operation)
	}
	actor, digest, err := b.LocalBackend.Checkpoint(ctx, s, operation)
	if err != nil {
		return actor, digest, err
	}
	actor.RetainedTag, actor.RetainedTagUID = snapshotTagName(s, operation), newID()
	b.mu.Lock()
	b.tags[actor.RetainedTag] = substrateapi.SnapshotTag{RetainedSummary: substrateapi.RetainedSummary{
		TagName: actor.RetainedTag, TagUID: actor.RetainedTagUID, TagVersion: 1, SourceActorUID: actor.UID,
		SourceSnapshotUUID: actor.SnapshotUUID, SourceSnapshotGeneration: int32(actor.SnapshotGeneration),
		SnapshotUUID: newID(), SnapshotGeneration: 1, SnapshotURI: "s3://snapshot/test", Durable: true,
	}, SourceActorName: actor.Name, CreatedAt: time.Now().UTC()}
	b.mu.Unlock()
	return actor, digest, nil
}

func snapshotTestController(t *testing.T, databaseURL string) (*Controller, *snapshotTestBackend) {
	t.Helper()
	cfg := testConfig(t)
	backend := &snapshotTestBackend{LocalBackend: cfg.Backend.(*LocalBackend), tags: map[string]substrateapi.SnapshotTag{}, readErrors: map[string]error{}, deleteErrors: map[string]error{}, lostResponse: map[string]bool{}}
	cfg.Backend, cfg.DatabaseURL = backend, databaseURL
	cfg.SnapshotGC.Disabled = true
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.lock()
		closed := c.closing
		c.mu.Unlock()
		if !closed {
			_ = c.Close()
		}
	})
	// Run sweeps explicitly; the production worker is exercised separately.
	c.lock()
	c.cfg.SnapshotGC.Disabled = false
	c.mu.Unlock()
	return c, backend
}

func seedCommittedSnapshots(t *testing.T, c *Controller, backend *snapshotTestBackend, tenant, id string, count int) []*Manifest {
	t.Helper()
	createTestSession(t, c, tenant, id)
	c.lock()
	defer c.mu.Unlock()
	s := c.state.Sessions[key(tenant, id)]
	manifests := make([]*Manifest, 0, count)
	for i := 0; i < count; i++ {
		operation := fmt.Sprintf("checkpoint-%d", i)
		name := snapshotTagName(s, operation)
		created := time.Now().UTC().Add(-2*time.Hour + time.Duration(i)*time.Second)
		m := &Manifest{ID: fmt.Sprintf("manifest-%d", i), State: "COMMITTED", ActorUID: s.Actor.UID, SnapshotUUID: fmt.Sprintf("source-%d", i), SnapshotGeneration: int64(i + 1),
			RetainedTag: name, RetainedTagUID: fmt.Sprintf("tag-%s-%d", id, i), CreatedAt: created}
		s.Lifecycle[operation] = &LifecycleReceipt{OperationID: operation, Action: "checkpoint", Status: "COMPLETED", Manifest: m}
		c.recordCommittedSnapshotLocked(s, operation, m)
		backend.mu.Lock()
		backend.tags[name] = substrateapi.SnapshotTag{RetainedSummary: substrateapi.RetainedSummary{TagName: name, TagUID: m.RetainedTagUID, TagVersion: 1,
			SourceActorUID: s.Actor.UID, SourceSnapshotUUID: m.SnapshotUUID, SourceSnapshotGeneration: int32(m.SnapshotGeneration),
			SnapshotUUID: "copy-" + m.SnapshotUUID, SnapshotGeneration: 1, SnapshotURI: "s3://snapshot/" + name, Durable: true}, SourceActorName: s.Actor.Name, CreatedAt: created}
		backend.mu.Unlock()
		manifests = append(manifests, copyValue(m))
		s.Manifest, s.Actor.RetainedTag, s.Actor.RetainedTagUID = m, name, m.RetainedTagUID
	}
	if err := c.persist(); err != nil {
		t.Fatal(err)
	}
	return manifests
}

func expireSnapshotRetry(t *testing.T, c *Controller, tenant, id, name string) {
	t.Helper()
	c.lock()
	defer c.mu.Unlock()
	r := c.state.Sessions[key(tenant, id)].SnapshotGC.Tags[name]
	r.RetryAfter = time.Now().UTC().Add(-time.Second)
	c.markSession(c.state.Sessions[key(tenant, id)])
	if err := c.persistContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotGCRotatesVerifiedCommittedPointsAndCommitsDeleteIntent(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 5)
	backend.beforeDelete = func(tag substrateapi.SnapshotTag) {
		var durable State
		if err := c.store.Load(&durable); err != nil {
			t.Fatal(err)
		}
		r := durable.Sessions[key("tenant-a", "workspace")].SnapshotGC.Tags[tag.TagName]
		if r.DeleteState != "DELETE_PENDING" || r.TagUID != tag.TagUID || r.DeleteAttempts != 1 || r.RetryAfter.IsZero() {
			t.Fatalf("DELETE lacks durable guarded intent: %+v", r)
		}
	}
	if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8); attempts != 3 {
		t.Fatalf("delete attempts=%d", attempts)
	}
	for i, manifest := range manifests {
		_, exists := backend.tags[manifest.RetainedTag]
		if exists != (i >= 3) {
			t.Fatalf("retention[%d]=%t", i, exists)
		}
	}
	if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8); attempts != 0 {
		t.Fatalf("historical receipts resurrected a deleted Tag: %d", attempts)
	}
}

func TestSnapshotGCKeepsOlderRecoveryPointsWhenRecentTagsAreUnavailable(t *testing.T) {
	for _, failure := range []string{"missing", "replaced", "pending", "read-unavailable"} {
		t.Run(failure, func(t *testing.T) {
			c, backend := snapshotTestController(t, "")
			manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 4)
			newest := manifests[3].RetainedTag
			tag := backend.tags[newest]
			switch failure {
			case "missing":
				delete(backend.tags, newest)
			case "replaced":
				tag.TagUID = "foreign-replacement"
				backend.tags[newest] = tag
			case "pending":
				tag.Durable = false
				backend.tags[newest] = tag
			case "read-unavailable":
				backend.readErrors[newest] = status.Error(codes.Unavailable, "metadata store unavailable")
			}
			c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
			for _, i := range []int{1, 2} {
				if _, exists := backend.tags[manifests[i].RetainedTag]; !exists {
					t.Fatalf("verified fallback recovery point %d deleted", i)
				}
			}
			if len(backend.deleted) != 1 || backend.deleted[0] != manifests[0].RetainedTag {
				t.Fatalf("unsafe rotation: %v", backend.deleted)
			}
		})
	}
}

func TestSnapshotGCProtectsCurrentManifestAndCrossSessionReferences(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 4)
	other := createTestSession(t, c, "tenant-b", "other")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "workspace")]
	s.Manifest, s.Actor.RetainedTag, s.Actor.RetainedTagUID = manifests[0], manifests[0].RetainedTag, manifests[0].RetainedTagUID
	c.state.Sessions[key(other.TenantID, other.SessionID)].Manifest = copyValue(manifests[1])
	if err := c.persist(); err != nil {
		t.Fatal(err)
	}
	c.mu.Unlock()
	if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8); attempts != 0 {
		t.Fatalf("protected recovery points deleted: %v", backend.deleted)
	}
}

func TestSnapshotGCPinsPendingUnknownAndCapacityOperations(t *testing.T) {
	for _, state := range []string{"RUNNING", "OUTCOME_UNKNOWN", "WAITING_CAPACITY", "UNRECOGNIZED_STATUS"} {
		t.Run(state, func(t *testing.T) {
			c, backend := snapshotTestController(t, "")
			seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 4)
			c.lock()
			s := c.state.Sessions[key("tenant-a", "workspace")]
			s.Lifecycle["in-flight"] = &LifecycleReceipt{OperationID: "in-flight", Action: "checkpoint", Status: state}
			c.mu.Unlock()
			c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
			if len(backend.reads) != 0 || len(backend.deleted) != 0 {
				t.Fatalf("GC ran during unresolved operation: reads=%v deletes=%v", backend.reads, backend.deleted)
			}
		})
	}
}

func TestSnapshotGCBoundsDeletionBatchAndMetadataReads(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 12)
	if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 2); attempts != 2 {
		t.Fatalf("batch attempts=%d", attempts)
	}
	if len(backend.reads) > 4 || len(backend.deleted) != 2 {
		t.Fatalf("batch exceeded: reads=%d deletes=%d", len(backend.reads), len(backend.deleted))
	}
}

func TestSnapshotGCDeleteFailureAndLostResponseSurviveRestart(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-response-%t", lost), func(t *testing.T) {
			c, backend := snapshotTestController(t, "")
			manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 3)
			oldest := manifests[0].RetainedTag
			if lost {
				backend.lostResponse[oldest] = true
			} else {
				backend.deleteErrors[oldest] = status.Error(codes.Unavailable, "object storage unavailable")
			}
			c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
			s, err := c.Get("tenant-a", "workspace")
			if err != nil || s.SnapshotGC.Tags[oldest].DeleteState != "DELETE_PENDING" || s.SnapshotGC.Tags[oldest].LastError == "" {
				t.Fatalf("retry intent missing: %+v %v", s, err)
			}
			if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8); attempts != 0 {
				t.Fatal("retry ignored persisted backoff")
			}
			cfg := c.cfg
			cfg.SnapshotGC.Disabled = true
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			reopened.cfg.SnapshotGC.Disabled = false
			delete(backend.deleteErrors, oldest)
			delete(backend.lostResponse, oldest)
			expireSnapshotRetry(t, reopened, "tenant-a", "workspace", oldest)
			reopened.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
			out, err := reopened.Get("tenant-a", "workspace")
			if err != nil || out.SnapshotGC.Tags[oldest].DeleteState != "DELETED" {
				t.Fatalf("restart reconciliation failed: %+v %v", out, err)
			}
			wantCalls := 2
			if lost {
				wantCalls = 1
			}
			if len(backend.deleted) != wantCalls {
				t.Fatalf("DELETE calls=%d want=%d", len(backend.deleted), wantCalls)
			}
		})
	}
}

func TestSnapshotGCCollectsOnlyExpiredOwnedTerminalOrphans(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	createTestSession(t, c, "tenant-a", "workspace")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "workspace")]
	names := map[string]string{}
	for _, kind := range []string{"expired", "young", "foreign"} {
		c.recordSnapshotIntentLocked(s, kind)
		name := snapshotTagName(s, kind)
		names[kind] = name
		s.Lifecycle[kind] = &LifecycleReceipt{OperationID: kind, Action: "checkpoint", Status: "ABORTED"}
		created := time.Now().UTC().Add(-2 * time.Hour)
		if kind == "young" {
			created = time.Now().UTC()
		}
		s.SnapshotGC.Tags[name].CreatedAt = created
		tag := substrateapi.SnapshotTag{RetainedSummary: substrateapi.RetainedSummary{TagName: name, TagUID: "orphan-" + kind, TagVersion: 1,
			SourceActorUID: s.Actor.UID, SourceSnapshotUUID: "orphan-source", SourceSnapshotGeneration: 1}, SourceActorName: s.Actor.Name, CreatedAt: created}
		if kind == "foreign" {
			tag.SourceActorUID = "another-actor"
		}
		backend.tags[name] = tag
	}
	if err := c.persist(); err != nil {
		t.Fatal(err)
	}
	c.mu.Unlock()
	if attempts := c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 2); attempts != 1 {
		t.Fatalf("orphan attempts=%d", attempts)
	}
	if len(backend.deleted) != 1 || backend.deleted[0] != names["expired"] {
		t.Fatalf("orphan ownership/grace violated: %v", backend.deleted)
	}
}

func TestSnapshotGCStopsOnUnhealthyJournal(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 4)
	c.lock()
	c.unhealthy = errors.New("durable journal commit failed")
	c.mu.Unlock()
	c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
	if len(backend.deleted) != 0 {
		t.Fatal("GC deleted without a healthy durable journal")
	}
}

func TestSnapshotGCFailedDeleteIntentCommitPreservesStorage(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 3)
	directory := c.cfg.DataDir
	if err := os.Rename(directory, directory+".offline"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(directory+".offline", directory); err != nil {
			t.Fatal(err)
		}
	}()
	c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
	if len(backend.deleted) != 0 || c.unhealthy == nil {
		t.Fatal("DELETE proceeded after failed durable intent commit")
	}
	var durable State
	if err := c.store.Load(&durable); err != nil {
		t.Fatal(err)
	}
	if r := durable.Sessions[key("tenant-a", "workspace")].SnapshotGC.Tags[manifests[0].RetainedTag]; r.DeleteState != "" {
		t.Fatalf("failed intent became durable: %+v", r)
	}
}

func TestSnapshotGCPostgresSessionEntityPreservesDeleteTombstones(t *testing.T) {
	c, backend := snapshotTestController(t, isolatedControllerDatabase(t))
	manifests := seedCommittedSnapshots(t, c, backend, "tenant-a", "workspace", 3)
	c.collectSessionSnapshots(context.Background(), key("tenant-a", "workspace"), 8)
	var durable State
	if err := c.store.Load(&durable); err != nil {
		t.Fatal(err)
	}
	r := durable.Sessions[key("tenant-a", "workspace")].SnapshotGC.Tags[manifests[0].RetainedTag]
	if r.DeleteState != "DELETED" || r.TagUID != manifests[0].RetainedTagUID {
		t.Fatalf("PostgreSQL session entity lost tombstone: %+v", r)
	}
}

func TestSnapshotGCConfigAndRetryBackoff(t *testing.T) {
	var cfg SnapshotGCConfig
	if err := cfg.defaults(); err != nil || cfg.Keep != 2 || cfg.Batch != 8 {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	if snapshotRetryDelay(cfg, 1) != time.Second || snapshotRetryDelay(cfg, 1000) != cfg.RetryMax {
		t.Fatal("retry delay is unbounded")
	}
	cfg.Keep = -1
	if cfg.defaults() == nil {
		t.Fatal("invalid retention accepted")
	}
}

func TestSnapshotGCLifecyclePublishesTagIntentAndWakesRotation(t *testing.T) {
	c, backend := snapshotTestController(t, "")
	c.startSnapshotGC()
	backend.beforeCheckpoint = func(s *Session, operation string) {
		var durable State
		if err := c.store.Load(&durable); err != nil {
			t.Fatal(err)
		}
		r := durable.Sessions[key(s.TenantID, s.SessionID)].SnapshotGC.Tags[snapshotTagName(s, operation)]
		if r == nil || r.State != "PENDING" || r.ActorUID != s.Actor.UID {
			t.Fatalf("checkpoint lacks durable Tag intent: %+v", r)
		}
	}
	s := createTestSession(t, c, "tenant-a", "workspace")
	for i := 0; i < 4; i++ {
		var err error
		s, _, err = c.Lifecycle(context.Background(), "tenant-a", "workspace", "suspend", LifecycleRequest{OperationID: fmt.Sprintf("suspend-%d", i), ExpectedEpoch: s.Epoch})
		if err != nil {
			t.Fatal(err)
		}
		s, _, err = c.Lifecycle(context.Background(), "tenant-a", "workspace", "resume", LifecycleRequest{OperationID: fmt.Sprintf("resume-%d", i), ExpectedEpoch: s.Epoch})
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { backend.mu.Lock(); defer backend.mu.Unlock(); return len(backend.tags) == 2 })
}
