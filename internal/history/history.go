// Package history persists container update records to SQLite, enabling
// rollbacks that target a specific prior state rather than just the previous
// in-memory digest.
package history

import (
	"database/sql"
	"fmt"
	"strings"
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

// migrations adds columns introduced in Phase D to existing databases.
// Each entry is [columnName, alterTableFragment]. The helper checks
// PRAGMA table_info before executing so repeated Open() calls are safe.
var migrations = [][2]string{
	{"status", "ALTER TABLE update_history ADD COLUMN status TEXT NOT NULL DEFAULT 'success'"},
	{"trigger_source", "ALTER TABLE update_history ADD COLUMN trigger_source TEXT NOT NULL DEFAULT 'webhook'"},
	{"image_tag", "ALTER TABLE update_history ADD COLUMN image_tag TEXT NOT NULL DEFAULT ''"},
	{"deploy_id", "ALTER TABLE update_history ADD COLUMN deploy_id TEXT"},
	{"duration_ms", "ALTER TABLE update_history ADD COLUMN duration_ms INTEGER"},
	{"ended_at", "ALTER TABLE update_history ADD COLUMN ended_at DATETIME"},
	{"reason", "ALTER TABLE update_history ADD COLUMN reason TEXT"},
}

const (
	selectCols = `id, app_name, service, old_image, new_image, backup_path, created_at,
	              status, trigger_source, image_tag, deploy_id, duration_ms, ended_at, reason`
)

// Entry is a single update history record.
type Entry struct {
	ID            int64
	AppName       string
	Service       string
	OldImage      string
	NewImage      string
	ImageTag      string
	BackupPath    string
	DeployID      string
	TriggerSource string
	Status        string // "running" | "success" | "failed" | "skipped"
	Reason        string
	DurationMs    *int64
	StartedAt     time.Time
	EndedAt       *time.Time
	CreatedAt     time.Time // alias for StartedAt; kept for JSON compat
}

// Store is a persistent update history backed by SQLite.
type Store struct {
	db      *sql.DB
	histDir string
}

// New creates a Store using the provided database connection, ensures the
// base schema exists, and runs additive Phase-D column migrations.
func New(db *sql.DB, histDir string) (*Store, error) {
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("history: apply schema: %w", err)
	}
	s := &Store{db: db, histDir: histDir}
	if err := s.runMigrations(); err != nil {
		return nil, fmt.Errorf("history: migrate: %w", err)
	}
	return s, nil
}

// runMigrations applies additive ALTER TABLE statements idempotently.
func (s *Store) runMigrations() error {
	existing, err := s.columnNames()
	if err != nil {
		return err
	}
	for _, m := range migrations {
		col, stmt := m[0], m[1]
		if existing[col] {
			continue
		}
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("alter column %q: %w", col, err)
		}
	}
	// Indexes are safe to re-create with IF NOT EXISTS.
	for _, idx := range []string{
		`CREATE INDEX IF NOT EXISTS idx_status    ON update_history(status)`,
		`CREATE INDEX IF NOT EXISTS idx_deploy_id ON update_history(deploy_id)`,
	} {
		if _, err := s.db.Exec(idx); err != nil {
			return err
		}
	}
	return nil
}

// columnNames returns the set of column names currently in update_history.
func (s *Store) columnNames() (map[string]bool, error) {
	rows, err := s.db.Query(`PRAGMA table_info(update_history)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// HistDir returns the directory where compose file backups are stored.
func (s *Store) HistDir() string { return s.histDir }

// RecordDeploy inserts a row at job-creation time with status "running".
// Returns the new row ID.
func (s *Store) RecordDeploy(e Entry) (int64, error) {
	startedAt := e.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	res, err := s.db.Exec(
		`INSERT INTO update_history
		   (app_name, service, old_image, new_image, backup_path, created_at,
		    status, trigger_source, image_tag, deploy_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.AppName, e.Service, e.OldImage, e.NewImage, e.BackupPath, startedAt.UTC(),
		e.Status, e.TriggerSource, e.ImageTag, nullString(e.DeployID),
	)
	if err != nil {
		return 0, fmt.Errorf("history: record deploy: %w", err)
	}
	return res.LastInsertId()
}

// FinishDeploy updates the row identified by deployID with the final status,
// ended_at, and computed duration_ms. It is idempotent on the deploy_id.
func (s *Store) FinishDeploy(deployID, status string, endedAt time.Time) error {
	var startedStr string
	err := s.db.QueryRow(
		`SELECT created_at FROM update_history WHERE deploy_id = ?`, deployID,
	).Scan(&startedStr)
	if err != nil {
		return fmt.Errorf("history: finish deploy: fetch started_at: %w", err)
	}
	startedAt := parseTime(startedStr)
	durationMs := endedAt.Sub(startedAt).Milliseconds()

	_, err = s.db.Exec(
		`UPDATE update_history SET status = ?, ended_at = ?, duration_ms = ? WHERE deploy_id = ?`,
		status, endedAt.UTC(), durationMs, deployID,
	)
	if err != nil {
		return fmt.Errorf("history: finish deploy: %w", err)
	}
	return nil
}

// RecordSkipped inserts a complete, terminal row for a webhook-rejected deploy.
func (s *Store) RecordSkipped(service, image, reason string) error {
	now := time.Now().UTC()
	_, err := s.db.Exec(
		`INSERT INTO update_history
		   (app_name, service, old_image, new_image, backup_path, created_at,
		    status, trigger_source, image_tag, duration_ms, ended_at, reason)
		 VALUES (?, ?, '', ?, '', ?, 'skipped', 'webhook', ?, 0, ?, ?)`,
		service, service, image, now, parseImageTag(image), now, reason,
	)
	if err != nil {
		return fmt.Errorf("history: record skipped: %w", err)
	}
	return nil
}

// List returns history rows, newest first, with optional service filter and
// mandatory pagination. Pass service="" to query all services.
// limit=0 defaults to 50.
func (s *Store) List(service string, limit, offset int) ([]Entry, error) {
	if limit <= 0 {
		limit = 50
	}
	var (
		rows *sql.Rows
		err  error
	)
	if service != "" {
		rows, err = s.db.Query(
			`SELECT `+selectCols+`
			 FROM update_history WHERE service = ?
			 ORDER BY id DESC LIMIT ? OFFSET ?`,
			service, limit, offset,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT `+selectCols+`
			 FROM update_history
			 ORDER BY id DESC LIMIT ? OFFSET ?`,
			limit, offset,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("history: list: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// Count returns the total number of history rows for a service.
// Pass service="" to count all rows.
func (s *Store) Count(service string) (int, error) {
	var n int
	var err error
	if service != "" {
		err = s.db.QueryRow(
			`SELECT COUNT(*) FROM update_history WHERE service = ?`, service,
		).Scan(&n)
	} else {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM update_history`).Scan(&n)
	}
	if err != nil {
		return 0, fmt.Errorf("history: count: %w", err)
	}
	return n, nil
}

// Get returns the entry with the given id (used by rollback engine).
func (s *Store) Get(id int64) (Entry, error) {
	row := s.db.QueryRow(
		`SELECT `+selectCols+` FROM update_history WHERE id = ?`, id,
	)
	return scanRow(row)
}

// Latest returns the most recent entry for a service (used by rollback engine).
func (s *Store) Latest(service string) (Entry, error) {
	row := s.db.QueryRow(
		`SELECT `+selectCols+`
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
	var startedStr string
	var deployID, triggerSource, imageTag, reason sql.NullString
	var durationMs sql.NullInt64
	var endedAtStr sql.NullString

	if err := s.Scan(
		&e.ID, &e.AppName, &e.Service, &e.OldImage, &e.NewImage, &e.BackupPath, &startedStr,
		&e.Status, &triggerSource, &imageTag, &deployID, &durationMs, &endedAtStr, &reason,
	); err != nil {
		return Entry{}, fmt.Errorf("history: scan: %w", err)
	}

	e.StartedAt = parseTime(startedStr)
	e.CreatedAt = e.StartedAt
	e.TriggerSource = triggerSource.String
	e.ImageTag = imageTag.String
	e.DeployID = deployID.String
	e.Reason = reason.String

	if durationMs.Valid {
		v := durationMs.Int64
		e.DurationMs = &v
	}
	if endedAtStr.Valid && endedAtStr.String != "" {
		t := parseTime(endedAtStr.String)
		e.EndedAt = &t
	}
	return e, nil
}

func parseTime(s string) time.Time {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 +0000 UTC",
		"2006-01-02 15:04:05+00:00",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// parseImageTag extracts the tag portion from an image reference like
// "registry/repo:tag@sha256:..." → "tag".
func parseImageTag(image string) string {
	if idx := strings.Index(image, "@"); idx != -1 {
		image = image[:idx]
	}
	if idx := strings.LastIndex(image, ":"); idx != -1 {
		return image[idx+1:]
	}
	return ""
}
