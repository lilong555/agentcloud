package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestFileJournalDurabilityAndSingleWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.json")
	store, err := Open(ctx, path, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, path, ""); err == nil {
		t.Fatal("second writer acquired lock")
	}
	value := map[string]any{"logical_request_id": "stable", "status": "COMPLETED", "result": "持久结果"}
	if err := store.Save(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, make(chan int)); err == nil {
		t.Fatal("invalid state was accepted")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var loaded map[string]any
	if err := reopened.Load(&loaded); err != nil {
		t.Fatal(err)
	}
	if loaded["result"] != "持久结果" || loaded["status"] != "COMPLETED" {
		t.Fatalf("unexpected durable state: %v", loaded)
	}
	if reopened.Backend() != "fsync-json" {
		t.Fatal(reopened.Backend())
	}
}

func TestInvalidFileJournalFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.json")
	if err := os.WriteFile(path, []byte("{truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path, ""); err == nil {
		t.Fatal("corrupt durable document accepted")
	}
}

// Each PostgreSQL test owns a freshly created database; application databases
// remain untouched. AGENTCLOUD_TEST_PG_ADMIN requires CREATE DATABASE privilege.
func isolatedPostgres(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AGENTCLOUD_TEST_PG_ADMIN")
	if dsn == "" {
		t.Skip("set AGENTCLOUD_TEST_PG_ADMIN to run real isolated PostgreSQL acceptance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid PostgreSQL test connection configuration")
	}
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("PostgreSQL administrator connection failed")
	}
	name := fmt.Sprintf("agentcloud_journal_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})
	uri, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test administrator URI")
	}
	uri.Path = "/" + name
	return uri.String()
}

func TestPostgresDurableReopenAndAdvisoryOwnership(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	store, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if store.Backend() != "postgresql" {
		t.Fatal(store.Backend())
	}
	if _, err := Open(ctx, "", dsn); err == nil {
		t.Fatal("second PostgreSQL writer acquired advisory lock")
	}
	if err := store.Save(ctx, map[string]any{"request": "stable", "status": "OUTCOME_UNKNOWN"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var state map[string]string
	if err := reopened.Load(&state); err != nil {
		t.Fatal(err)
	}
	if state["status"] != "OUTCOME_UNKNOWN" || state["request"] != "stable" {
		t.Fatal(state)
	}
	if err := reopened.Check(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRevisionConflictPoisonsWriter(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	store, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(ctx, json.RawMessage(`{"state":"accepted"}`)); err != nil {
		t.Fatal(err)
	}
	external, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close(ctx)
	if _, err := external.Exec(ctx, "UPDATE agentcloud.journal SET revision=revision+1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, json.RawMessage(`{"state":"stale"}`)); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	if err := store.Check(ctx); err == nil {
		t.Fatal("poisoned writer remained ready")
	}
	if err := store.Save(ctx, json.RawMessage(`{"state":"retry"}`)); err == nil {
		t.Fatal("poisoned writer accepted retry")
	}
}
