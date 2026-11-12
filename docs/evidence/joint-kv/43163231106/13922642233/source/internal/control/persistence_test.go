package control

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lilong555/agentcloud/internal/journal"
)

func postgresInspector(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestPostgresIncrementalEntitiesPreserveOtherSessionsAndClaims(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()
	for _, id := range []string{"first", "second", "third"} {
		createTestSession(t, c, "tenant-a", id)
	}
	db := postgresInspector(t, cfg.DatabaseURL)
	var prior, current int64
	query := `SELECT updated_revision FROM agentcloud.journal_entities WHERE kind='sessions' AND entity_key='tenant-a/second'`
	if err = db.QueryRow(context.Background(), query).Scan(&prior); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Tool(context.Background(), "tenant-a", "first", ToolRequest{ID: "write", Epoch: 1, Kind: "file_write", Path: "state.txt", Content: "durable"}); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(context.Background(), query).Scan(&current); err != nil || current != prior {
		t.Fatalf("unrelated session rewritten: %d -> %d %v", prior, current, err)
	}
	if _, err = c.Submit("tenant-a", "first", "request", json.RawMessage(`{"model":"controlled"}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		r, e := c.Request("tenant-a", "first", "request", 0)
		return e == nil && r.Status == "COMPLETED"
	})
	if _, err = c.Request("tenant-a", "first", "request", 1); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c = nil
	reopened, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s, err := reopened.Get("tenant-a", "first")
	if err != nil || s.Tools["write"].Result.ExitCode != 0 || s.ReceiptWatermark != 1 {
		t.Fatalf("receipt lost: %+v %v", s, err)
	}
	r, err := reopened.Request("tenant-a", "first", "request", 0)
	if err != nil || r.ClaimedEpoch != 1 || r.Status != "COMPLETED" {
		t.Fatalf("claim lost: %+v %v", r, err)
	}
}

func TestPostgresCommitGateHidesUncommittedState(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	c := newTestController(t, cfg)
	db := postgresInspector(t, cfg.DatabaseURL)
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), "SELECT id FROM agentcloud.journal WHERE id=1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	created := make(chan error, 1)
	go func() {
		_, e := c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "blocked", WorkspaceID: "blocked"})
		created <- e
	}()
	waitFor(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.committing })
	read := make(chan error, 1)
	go func() { _, e := c.Get("tenant-a", "blocked"); read <- e }()
	select {
	case e := <-read:
		t.Fatalf("uncommitted session exposed: %v", e)
	case <-time.After(30 * time.Millisecond):
	}
	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-created; err != nil {
		t.Fatal(err)
	}
	if err = <-read; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFailedRevisionKeepsDurableStateAndFailsClosed(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := postgresInspector(t, cfg.DatabaseURL)
	if _, err = db.Exec(context.Background(), "UPDATE agentcloud.journal SET revision=revision+1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "uncommitted", WorkspaceID: "uncommitted"}); err == nil {
		t.Fatal("revision conflict accepted")
	}
	_, err = c.Get("tenant-a", "uncommitted")
	assertCode(t, err, "journal_unavailable")
	_ = c.Close()
	reopened, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, err = reopened.Get("tenant-a", "uncommitted")
	assertCode(t, err, "session_not_found")
}

func TestPostgresLegacyDocumentMigratesAtomically(t *testing.T) {
	cfg := testConfig(t)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, c, "tenant-a", "legacy")
	if _, err = c.Tool(context.Background(), "tenant-a", "legacy", ToolRequest{ID: "write", Epoch: 1, Kind: "file_write", Path: "legacy.txt", Content: "preserved"}); err != nil {
		t.Fatal(err)
	}
	c.lock()
	state := copyValue(c.state)
	c.mu.Unlock()
	_ = c.Close()
	cfg.DatabaseURL = isolatedControllerDatabase(t)
	store, err := journal.Open(context.Background(), "", cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	reopened := newTestController(t, cfg)
	s, err := reopened.Get("tenant-a", "legacy")
	if err != nil || s.Tools["write"].Status != "COMPLETED" {
		t.Fatalf("migration lost receipt: %+v %v", s, err)
	}
	db := postgresInspector(t, cfg.DatabaseURL)
	var version int
	var oldSessions bool
	if err = db.QueryRow(context.Background(), `SELECT (data->>'entity_format')::int, data ? 'sessions' FROM agentcloud.journal`).Scan(&version, &oldSessions); err != nil || version != 1 || oldSessions {
		t.Fatalf("legacy document remains: %d %t %v", version, oldSessions, err)
	}
}

func TestKeyedLocksSerializeWaitersAndReleaseAllKeys(t *testing.T) {
	var locks keyedMutex[string]
	var group sync.WaitGroup
	active := 0
	for i := 0; i < 128; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			unlock := locks.lock("session")
			active++
			if active != 1 {
				t.Error("duplicate lock for a live key")
			}
			time.Sleep(time.Microsecond)
			active--
			unlock()
		}()
	}
	group.Wait()
	if len(locks.entries) != 0 {
		t.Fatalf("retained %d inactive locks", len(locks.entries))
	}
}
