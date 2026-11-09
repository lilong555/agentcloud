// Package journal persists the coordinator's single-writer state and receipts.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5"
)

// Store serializes a durable JSON document. A dedicated PostgreSQL connection
// owns the process lock for its whole lifetime; transactions publish one revision.
type Store struct {
	mu           sync.Mutex
	path         string
	lock         *os.File
	conn         *pgx.Conn
	revision     int64
	data         []byte
	poisoned     bool
	entityFormat bool
}

func Open(ctx context.Context, path, databaseURL string) (*Store, error) {
	s := &Store{path: path, data: []byte(`{}`)}
	if databaseURL != "" {
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			return nil, err
		}
		s.conn = conn
		ok := false
		if err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(71821462830801)").Scan(&ok); err != nil || !ok {
			conn.Close(ctx)
			if err != nil {
				return nil, err
			}
			return nil, errors.New("another coordinator owns the journal")
		}
		_, err = conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS agentcloud; CREATE TABLE IF NOT EXISTS agentcloud.journal (id integer PRIMARY KEY CHECK(id=1), revision bigint NOT NULL, data jsonb NOT NULL); INSERT INTO agentcloud.journal VALUES(1,0,'{}') ON CONFLICT DO NOTHING; CREATE TABLE IF NOT EXISTS agentcloud.journal_entities(kind text NOT NULL,entity_key text NOT NULL,data jsonb NOT NULL,sequence bigint NOT NULL DEFAULT 0,updated_revision bigint NOT NULL,PRIMARY KEY(kind,entity_key)); CREATE INDEX IF NOT EXISTS journal_entities_sequence ON agentcloud.journal_entities(kind,sequence)`)
		if err == nil {
			err = conn.QueryRow(ctx, "SELECT revision,data FROM agentcloud.journal WHERE id=1").Scan(&s.revision, &s.data)
		}
		if err != nil {
			conn.Close(ctx)
			return nil, err
		}
		var metadata struct {
			Format int `json:"entity_format"`
		}
		_ = json.Unmarshal(s.data, &metadata)
		s.entityFormat = metadata.Format == 1
		return s, nil
	}
	if path == "" {
		return nil, errors.New("journal path or database URL is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another coordinator owns the journal")
	}
	s.lock = lock
	data, err := os.ReadFile(path)
	if err == nil {
		if !json.Valid(data) {
			s.Close()
			return nil, errors.New("invalid journal data")
		}
		s.data = data
	} else if !os.IsNotExist(err) {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Load(out any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.data
	if s.entityFormat {
		var err error
		data, err = s.loadEntities(context.Background())
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(data, out)
}

func (s *Store) Save(ctx context.Context, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return errors.New("journal requires process restart and reconciliation")
	}
	if s.entityFormat {
		return errors.New("coordinator entity journal requires SaveEntities")
	}
	if s.conn != nil {
		tag, err := s.conn.Exec(ctx, "UPDATE agentcloud.journal SET revision=revision+1,data=$1 WHERE id=1 AND revision=$2", data, s.revision)
		if err != nil {
			s.poisoned = true
			return err
		}
		if tag.RowsAffected() != 1 {
			s.poisoned = true
			return errors.New("journal revision conflict")
		}
		s.revision++
	} else {
		file, err := os.CreateTemp(filepath.Dir(s.path), ".journal-*")
		if err != nil {
			return err
		}
		name := file.Name()
		defer os.Remove(name)
		if _, err = file.Write(data); err == nil {
			err = file.Sync()
		}
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if err = os.Rename(name, s.path); err != nil {
			return err
		}
		dir, err := os.Open(filepath.Dir(s.path))
		if err != nil {
			s.poisoned = true
			return err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			s.poisoned = true
			return err
		}
	}
	s.data = data
	return nil
}

func (s *Store) Backend() string {
	if s.conn != nil {
		return "postgresql"
	}
	return "fsync-json"
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close(context.Background())
	}
	if s.lock != nil {
		err := s.lock.Close()
		s.lock = nil
		return err
	}
	return nil
}
func (s *Store) Check(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return fmt.Errorf("journal unavailable")
	}
	if s.conn != nil {
		if err := s.conn.Ping(ctx); err != nil {
			s.poisoned = true
			return err
		}
	}
	return nil
}
