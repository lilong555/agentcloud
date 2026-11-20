package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgresEntityTransactionRollsBackMetadataAndRowsTogether(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	s, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	changes := EntityChanges{Metadata: json.RawMessage(`{"entity_format":1,"version":1}`), Entities: []Entity{{Kind: "sessions", Key: "a/s", Data: json.RawMessage(`{"session_id":"s"}`)}}}
	if err = s.SaveEntities(ctx, changes); err != nil {
		t.Fatal(err)
	}
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if _, err = db.Exec(ctx, `ALTER TABLE agentcloud.journal_entities ADD CONSTRAINT reject_poison CHECK (entity_key <> 'poison')`); err != nil {
		t.Fatal(err)
	}
	changes.Metadata = json.RawMessage(`{"entity_format":1,"version":2}`)
	changes.Entities = []Entity{{Kind: "sessions", Key: "a/s", Data: json.RawMessage(`{"session_id":"overwritten"}`)}, {Kind: "sessions", Key: "poison", Data: json.RawMessage(`{}`)}}
	if err = s.SaveEntities(ctx, changes); err == nil {
		t.Fatal("invalid entity transaction committed")
	}
	var version int
	var session string
	if err = db.QueryRow(ctx, `SELECT (data->>'version')::int FROM agentcloud.journal`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("metadata escaped rollback: %d %v", version, err)
	}
	if err = db.QueryRow(ctx, `SELECT data->>'session_id' FROM agentcloud.journal_entities WHERE entity_key='a/s'`).Scan(&session); err != nil || session != "s" {
		t.Fatalf("entity escaped rollback: %q %v", session, err)
	}
	if err = s.Check(ctx); err == nil {
		t.Fatal("failed writer reused")
	}
}
