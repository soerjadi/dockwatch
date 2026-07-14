package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

const servicesSchema = `
CREATE TABLE IF NOT EXISTS services (
    name             TEXT PRIMARY KEY,
    image            TEXT NOT NULL,
    compose_file     TEXT NOT NULL DEFAULT '',
    trigger_mode     TEXT NOT NULL DEFAULT 'poll'
                     CHECK (trigger_mode IN ('webhook','poll')),
    poller_interval  INTEGER NOT NULL DEFAULT 300
                     CHECK (poller_interval >= 30),
    last_digest      TEXT,
    last_checked_at  DATETIME,
    created_at       DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at       DATETIME NOT NULL DEFAULT (datetime('now'))
);
`

// Open opens (or creates) the SQLite database at path and ensures the core schemas exist.
func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("db: create dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer

	if _, err := db.Exec(servicesSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("db: apply schema: %w", err)
	}

	return db, nil
}
