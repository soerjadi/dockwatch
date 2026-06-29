// Package history persists container update records to SQLite, enabling
// rollbacks that target a specific prior state rather than just the previous
// in-memory digest.
package history

import (
	"database/sql"
	"fmt"
	"time"
)

const schema = `
CREATE TABLE IF NOT EXISTS update_history (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  app_name    TEXT    NOT NULL,
  service     TEXT    NOT NULL,
  old_image   TEXT    NOT NULL,
  new_image   TEXT    NOT NULL,
  backup_path TEXT    NOT NULL DEFAULT '',
  created_at  DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_service ON update_history(service);
`

// Entry is a single update history record.
type Entry struct {
	ID         int64
	AppName    string
	Service    string
	OldImage   string
	NewImage   string
	BackupPath string // path to compose file backup, or "" for non-compose
	CreatedAt  time.Time
}

// Store is a persistent update history backed by SQLite.
type Store struct {
	db      *sql.DB
	histDir string // directory where compose file backups are written
}

// New creates a Store using the provided database connection and ensures the schema exists.
// histDir is the directory used to store compose file backups.
func New(db *sql.DB, histDir string) (*Store, error) {
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("history: apply schema: %w", err)
	}
	return &Store{db: db, histDir: histDir}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// HistDir returns the directory where compose file backups are stored.
func (s *Store) HistDir() string { return s.histDir }

// Record inserts a new history entry.
func (s *Store) Record(e Entry) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO update_history (app_name, service, old_image, new_image, backup_path, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.AppName, e.Service, e.OldImage, e.NewImage, e.BackupPath, e.CreatedAt.UTC(),
	)
	if err != nil {
		return 0, fmt.Errorf("history: record: %w", err)
	}
	return res.LastInsertId()
}

// List returns all history entries for a service, newest first.
func (s *Store) List(service string) ([]Entry, error) {
	rows, err := s.db.Query(
		`SELECT id, app_name, service, old_image, new_image, backup_path, created_at
		 FROM update_history WHERE service = ? ORDER BY id DESC`,
		service,
	)
	if err != nil {
		return nil, fmt.Errorf("history: list: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// ListAll returns all history entries, newest first.
func (s *Store) ListAll() ([]Entry, error) {
	rows, err := s.db.Query(
		`SELECT id, app_name, service, old_image, new_image, backup_path, created_at
		 FROM update_history ORDER BY id DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("history: list all: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// Get returns the entry with the given id.
func (s *Store) Get(id int64) (Entry, error) {
	row := s.db.QueryRow(
		`SELECT id, app_name, service, old_image, new_image, backup_path, created_at
		 FROM update_history WHERE id = ?`, id,
	)
	return scanRow(row)
}

// Latest returns the most recent entry for a service.
func (s *Store) Latest(service string) (Entry, error) {
	row := s.db.QueryRow(
		`SELECT id, app_name, service, old_image, new_image, backup_path, created_at
		 FROM update_history WHERE service = ? ORDER BY id DESC LIMIT 1`, service,
	)
	return scanRow(row)
}

func scanRows(rows *sql.Rows) ([]Entry, error) {
	var entries []Entry
	for rows.Next() {
		e, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRow(s scanner) (Entry, error) {
	var e Entry
	var createdAt string
	if err := s.Scan(&e.ID, &e.AppName, &e.Service, &e.OldImage, &e.NewImage, &e.BackupPath, &createdAt); err != nil {
		return Entry{}, fmt.Errorf("history: scan: %w", err)
	}
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		// SQLite may store as "2006-01-02 15:04:05 +0000 UTC"
		t, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", createdAt)
		if err != nil {
			t = time.Time{}
		}
	}
	e.CreatedAt = t
	return e, nil
}
