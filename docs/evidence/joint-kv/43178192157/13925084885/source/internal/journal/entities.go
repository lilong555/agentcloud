package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// EntityChanges publishes one revision atomically while writing only changed
// sessions, requests, receipts and append-only diagnostic records.
type Entity struct {
	Kind     string
	Key      string
	Data     json.RawMessage
	Sequence uint64
}
type EntityChanges struct {
	Metadata         json.RawMessage
	Entities         []Entity
	Deletes          []Entity
	EventFloor       uint64
	ObservationFloor uint64
}

func (s *Store) SupportsEntities() bool { return s.conn != nil }
func (s *Store) SaveEntities(ctx context.Context, changes EntityChanges) error {
	if !json.Valid(changes.Metadata) {
		return errors.New("invalid journal metadata")
	}
	for _, entity := range changes.Entities {
		if !json.Valid(entity.Data) || entity.Kind == "" || entity.Key == "" {
			return errors.New("invalid journal entity")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return errors.New("journal requires process restart and reconciliation")
	}
	if s.conn == nil {
		return errors.New("entity persistence requires PostgreSQL")
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		s.poisoned = true
		return err
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, "UPDATE agentcloud.journal SET revision=revision+1,data=$1 WHERE id=1 AND revision=$2", changes.Metadata, s.revision)
	if err == nil && tag.RowsAffected() != 1 {
		err = errors.New("journal revision conflict")
	}
	if err != nil {
		s.poisoned = true
		return err
	}
	batch := &pgx.Batch{}
	for _, entity := range changes.Entities {
		batch.Queue(`INSERT INTO agentcloud.journal_entities(kind,entity_key,data,sequence,updated_revision) VALUES($1,$2,$3,$4,$5) ON CONFLICT(kind,entity_key) DO UPDATE SET data=EXCLUDED.data,sequence=EXCLUDED.sequence,updated_revision=EXCLUDED.updated_revision`, entity.Kind, entity.Key, entity.Data, int64(entity.Sequence), s.revision+1)
	}
	for _, entity := range changes.Deletes {
		batch.Queue("DELETE FROM agentcloud.journal_entities WHERE kind=$1 AND entity_key=$2", entity.Kind, entity.Key)
	}
	if changes.EventFloor > 0 {
		batch.Queue("DELETE FROM agentcloud.journal_entities WHERE kind='events' AND sequence<$1", int64(changes.EventFloor))
	}
	if changes.ObservationFloor > 0 {
		batch.Queue("DELETE FROM agentcloud.journal_entities WHERE kind='observations' AND sequence<$1", int64(changes.ObservationFloor))
	}
	result := tx.SendBatch(ctx, batch)
	err = result.Close()
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		s.poisoned = true
		return err
	}
	s.revision++
	s.data = append(s.data[:0], changes.Metadata...)
	s.entityFormat = true
	return nil
}
func (s *Store) loadEntities(ctx context.Context) ([]byte, error) {
	rows, err := s.conn.Query(ctx, "SELECT kind,entity_key,data FROM agentcloud.journal_entities ORDER BY kind,sequence,entity_key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := map[string]map[string]json.RawMessage{}
	requests := map[string]json.RawMessage{}
	receipts := map[string]map[string]map[string]json.RawMessage{}
	events := []json.RawMessage{}
	observations := []json.RawMessage{}
	for rows.Next() {
		var kind, key string
		var data json.RawMessage
		if err := rows.Scan(&kind, &key, &data); err != nil {
			return nil, err
		}
		switch kind {
		case "sessions":
			var session map[string]json.RawMessage
			if err := json.Unmarshal(data, &session); err != nil {
				return nil, err
			}
			sessions[key] = session
		case "requests":
			requests[key] = data
		case "events":
			events = append(events, data)
		case "observations":
			observations = append(observations, data)
		case "tools", "lifecycle", "scheduling":
			split := strings.LastIndex(key, "/")
			if split < 0 {
				return nil, fmt.Errorf("invalid %s receipt identity", kind)
			}
			sessionKey, id := key[:split], key[split+1:]
			if receipts[sessionKey] == nil {
				receipts[sessionKey] = map[string]map[string]json.RawMessage{}
			}
			if receipts[sessionKey][kind] == nil {
				receipts[sessionKey][kind] = map[string]json.RawMessage{}
			}
			receipts[sessionKey][kind][id] = data
		default:
			return nil, fmt.Errorf("unknown journal entity kind %q", kind)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for sessionKey, kinds := range receipts {
		session := sessions[sessionKey]
		if session == nil {
			return nil, errors.New("orphan journal receipt")
		}
		for kind, values := range kinds {
			data, _ := json.Marshal(values)
			session[kind] = data
		}
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(s.data, &metadata); err != nil {
		return nil, err
	}
	metadata["sessions"], _ = json.Marshal(sessions)
	metadata["requests"], _ = json.Marshal(requests)
	metadata["events"], _ = json.Marshal(events)
	metadata["observations"], _ = json.Marshal(observations)
	delete(metadata, "entity_format")
	return json.Marshal(metadata)
}
func SortEntities(entities []Entity) {
	sort.Slice(entities, func(i, j int) bool {
		if entities[i].Kind == entities[j].Kind {
			return entities[i].Key < entities[j].Key
		}
		return entities[i].Kind < entities[j].Kind
	})
}
