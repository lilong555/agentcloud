package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func researchChange(seq uint64, floor uint64) EntityChanges {
	metadata, _ := json.Marshal(map[string]any{"entity_format": 1, "event_sequence": seq})
	payload, _ := json.Marshal(map[string]any{"sequence": seq, "kind": "controlled"})
	return EntityChanges{Metadata: metadata, EventFloor: floor, Entities: []Entity{{Kind: "events", Key: string(rune('a' + seq)), Sequence: seq, Data: payload}}}
}

func TestResearchPostgresPreservesPrunedRecordsAndRestart(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	s, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnableResearch(ctx, "capture"); err != nil {
		t.Fatal(err)
	}
	if err = s.EnableResearch(ctx, "another-run"); err == nil {
		t.Fatal("a second run was allowed to share one journal")
	}
	for i := uint64(1); i <= 3; i++ {
		if err = s.SaveEntities(ctx, researchChange(i, i)); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err = s.conn.QueryRow(ctx, "SELECT count(*) FROM agentcloud.journal_entities WHERE kind='events'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("diagnostic pruning: %d %v", count, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.EnableResearch(ctx, "capture"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveEntities(ctx, researchChange(3, 3)); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveEntities(ctx, researchChange(4, 4)); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err = ExportResearch(ctx, dsn, "capture", &output); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 6 {
		t.Fatalf("export records missing: %s", output.String())
	}
	var header map[string]any
	_ = json.Unmarshal(lines[0], &header)
	if header["initial_events"] != "0" || header["through_events"] != "4" {
		t.Fatalf("wrong frontiers: %v", header)
	}
	var footer map[string]any
	_ = json.Unmarshal(lines[5], &footer)
	if footer["records"] != float64(4) || footer["snapshot_complete"] != true {
		t.Fatal(footer)
	}
}

func TestResearchPostgresConflictRollsBackControlAndCapture(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	s, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.EnableResearch(ctx, "capture"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveEntities(ctx, researchChange(1, 0)); err != nil {
		t.Fatal(err)
	}
	conflict := researchChange(1, 0)
	conflict.Entities[0].Data = json.RawMessage(`{"sequence":1,"kind":"changed"}`)
	conflict.Metadata = json.RawMessage(`{"entity_format":1,"event_sequence":99}`)
	if err = s.SaveEntities(ctx, conflict); err == nil {
		t.Fatal("immutable event overwritten")
	}
	var frontier string
	if err = s.conn.QueryRow(ctx, "SELECT data->>'event_sequence' FROM agentcloud.journal").Scan(&frontier); err != nil || frontier != "1" {
		t.Fatalf("control escaped rollback: %s %v", frontier, err)
	}
	var kind string
	if err = s.conn.QueryRow(ctx, "SELECT payload->>'kind' FROM agentcloud.research_records").Scan(&kind); err != nil || kind != "controlled" {
		t.Fatalf("capture escaped rollback: %s %v", kind, err)
	}
}

func TestResearchPostgresStartsAtRecordedFrontier(t *testing.T) {
	dsn := isolatedPostgres(t)
	ctx := context.Background()
	s, err := Open(ctx, "", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SaveEntities(ctx, researchChange(7, 7)); err != nil {
		t.Fatal(err)
	}
	if err = s.EnableResearch(ctx, "late"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveEntities(ctx, researchChange(7, 7)); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveEntities(ctx, researchChange(8, 8)); err != nil {
		t.Fatal(err)
	}
	var count, initial int
	if err = s.conn.QueryRow(ctx, "SELECT count(*) FROM agentcloud.research_records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("retroactive capture: %d %v", count, err)
	}
	if err = s.conn.QueryRow(ctx, "SELECT initial_events FROM agentcloud.research_runs").Scan(&initial); err != nil || initial != 7 {
		t.Fatalf("wrong start %d %v", initial, err)
	}
}
