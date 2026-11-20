package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/jackc/pgx/v5"
)

// EnableResearch binds capture to a durable run. Existing diagnostics are excluded.
// Research records share the journal transaction and survive diagnostic pruning.
func (s *Store) EnableResearch(ctx context.Context, runID string) error {
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,159}$`).MatchString(runID) {
		return errors.New("invalid research run ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return errors.New("research capture requires PostgreSQL")
	}
	_, err := s.conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS agentcloud.research_runs (
 run_id text PRIMARY KEY, start_revision bigint NOT NULL, initial_events bigint NOT NULL,
 initial_observations bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS agentcloud.research_records (
 run_id text NOT NULL REFERENCES agentcloud.research_runs, stream text NOT NULL,
 sequence bigint NOT NULL, payload jsonb NOT NULL, committed_revision bigint NOT NULL,
 staged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_id,stream,sequence), CHECK(stream IN ('events','observations')));`)
	if err != nil {
		return err
	}
	var otherRuns int
	if err = s.conn.QueryRow(ctx, "SELECT count(*) FROM agentcloud.research_runs WHERE run_id<>$1", runID).Scan(&otherRuns); err != nil {
		return err
	}
	if otherRuns > 0 {
		return errors.New("one research run per journal database is required")
	}
	_, err = s.conn.Exec(ctx, `INSERT INTO agentcloud.research_runs(run_id,start_revision,initial_events,initial_observations)
SELECT $1,revision,coalesce((data->>'event_sequence')::bigint,0),coalesce((data->>'observation_sequence')::bigint,0)
FROM agentcloud.journal WHERE id=1 ON CONFLICT DO NOTHING`, runID)
	if err == nil {
		s.researchRun = runID
	}
	return err
}

// ExportResearch uses one read-only repeatable-read snapshot for rows and frontiers.
// Consumers verify initial+1..frontier for both streams before admitting a run.
func ExportResearch(ctx context.Context, dsn, runID string, output io.Writer) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return errors.New("research database connection failed")
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var header json.RawMessage
	err = tx.QueryRow(ctx, `SELECT jsonb_build_object('record_type','manifest','schema_version',1,
 'run_id',r.run_id,'start_revision',r.start_revision,'snapshot_revision',j.revision,
 'initial_events',r.initial_events::text,'initial_observations',r.initial_observations::text,
 'through_events',coalesce(j.data->>'event_sequence','0'),
 'through_observations',coalesce(j.data->>'observation_sequence','0'),
 'capture_scope','committed_coordinator_events_and_observations',
 'snapshot_at',transaction_timestamp(),'execution_kind','native-coordinator-capture')
FROM agentcloud.research_runs r CROSS JOIN agentcloud.journal j WHERE r.run_id=$1 AND j.id=1`, runID).Scan(&header)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintln(output, string(header)); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT jsonb_build_object('record_type','record','run_id',run_id,
 'stream',stream,'sequence',sequence::text,'committed_revision',committed_revision,
 'staged_at',staged_at,'payload',payload) FROM agentcloud.research_records
 WHERE run_id=$1 ORDER BY stream,sequence`, runID)
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		var row json.RawMessage
		if err = rows.Scan(&row); err == nil {
			_, err = fmt.Fprintln(output, string(row))
		}
		if err != nil {
			rows.Close()
			return err
		}
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"record_type": "footer", "run_id": runID, "records": count, "snapshot_complete": true})
}
