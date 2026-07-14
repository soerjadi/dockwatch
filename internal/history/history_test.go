package history_test

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/soerjadi/dockwatch/internal/history"
)

func openStore(t *testing.T) *history.Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	s, err := history.New(db, t.TempDir())
	if err != nil {
		t.Fatalf("history.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// AC#1 — completed deploy has non-null status, duration_ms, trigger_source, image_tag.
func TestRecordDeploy_FinishDeploy(t *testing.T) {
	s := openStore(t)

	start := time.Now().UTC().Truncate(time.Millisecond)
	id, err := s.RecordDeploy(history.Entry{
		AppName:       "myapp",
		Service:       "myapp",
		OldImage:      "myapp:1.0.0",
		NewImage:      "myapp:1.1.0",
		ImageTag:      "1.1.0",
		DeployID:      "deploy-abc",
		TriggerSource: "webhook",
		Status:        "running",
		StartedAt:     start,
	})
	if err != nil {
		t.Fatalf("RecordDeploy: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero row id")
	}

	endedAt := start.Add(500 * time.Millisecond)
	if err := s.FinishDeploy("deploy-abc", "success", endedAt); err != nil {
		t.Fatalf("FinishDeploy: %v", err)
	}

	entries, err := s.List("myapp", 10, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]

	if e.Status != "success" {
		t.Errorf("status = %q, want %q", e.Status, "success")
	}
	if e.TriggerSource != "webhook" {
		t.Errorf("trigger_source = %q, want %q", e.TriggerSource, "webhook")
	}
	if e.ImageTag != "1.1.0" {
		t.Errorf("image_tag = %q, want %q", e.ImageTag, "1.1.0")
	}
	if e.DurationMs == nil {
		t.Fatal("duration_ms is nil")
	}
	if *e.DurationMs < 400 || *e.DurationMs > 600 {
		t.Errorf("duration_ms = %d, want ~500", *e.DurationMs)
	}
	if e.EndedAt == nil {
		t.Fatal("ended_at is nil")
	}
	if e.DeployID != "deploy-abc" {
		t.Errorf("deploy_id = %q, want %q", e.DeployID, "deploy-abc")
	}
}

// AC#2 — skipped webhook produces status=skipped with a reason.
func TestRecordSkipped(t *testing.T) {
	s := openStore(t)

	if err := s.RecordSkipped("svc", "myrepo/svc:2.0.0", "webhook rejected: service trigger_mode is poll"); err != nil {
		t.Fatalf("RecordSkipped: %v", err)
	}

	entries, err := s.List("svc", 10, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Status != "skipped" {
		t.Errorf("status = %q, want skipped", e.Status)
	}
	if e.Reason == "" {
		t.Error("reason is empty")
	}
	if e.ImageTag != "2.0.0" {
		t.Errorf("image_tag = %q, want 2.0.0", e.ImageTag)
	}
	if e.DurationMs == nil || *e.DurationMs != 0 {
		t.Errorf("duration_ms should be 0 for skipped")
	}
}

// AC#3 — pagination returns correct page + total.
func TestList_Pagination(t *testing.T) {
	s := openStore(t)

	for i := 0; i < 15; i++ {
		if _, err := s.RecordDeploy(history.Entry{
			AppName:       "svc",
			Service:       "svc",
			OldImage:      "svc:0",
			NewImage:      "svc:1",
			ImageTag:      "1",
			DeployID:      "",
			TriggerSource: "poll",
			Status:        "success",
			StartedAt:     time.Now(),
		}); err != nil {
			t.Fatalf("RecordDeploy %d: %v", i, err)
		}
	}

	entries, err := s.List("svc", 10, 0)
	if err != nil {
		t.Fatalf("List page 0: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("page 0: got %d rows, want 10", len(entries))
	}

	total, err := s.Count("svc")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if total != 15 {
		t.Errorf("total = %d, want 15", total)
	}

	entries2, err := s.List("svc", 10, 10)
	if err != nil {
		t.Fatalf("List page 1: %v", err)
	}
	if len(entries2) != 5 {
		t.Errorf("page 1: got %d rows, want 5", len(entries2))
	}
}

// AC#4 — pre-existing rows (old 7-column schema) survive migration intact.
func TestMigration_ExistingRows(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)

	// Bootstrap the old schema manually and insert a legacy row.
	_, err = db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	_, err = db.Exec(`
		INSERT INTO update_history (app_name, service, old_image, new_image, backup_path, created_at)
		VALUES ('old', 'old', 'old:1', 'old:2', '', datetime('now'))
	`)
	if err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// Now open via history.New — triggers migration.
	s, err := history.New(db, t.TempDir())
	if err != nil {
		t.Fatalf("history.New after migration: %v", err)
	}
	defer s.Close()

	entries, err := s.List("old", 10, 0)
	if err != nil {
		t.Fatalf("List legacy row: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 legacy entry, got %d", len(entries))
	}
	e := entries[0]
	if e.AppName != "old" {
		t.Errorf("app_name = %q, want old", e.AppName)
	}
	// Default status for migrated rows is "success".
	if e.Status != "success" {
		t.Errorf("migrated status = %q, want success", e.Status)
	}
}

// AC#6 — Get() and Latest() still work after migration.
func TestGetAndLatest(t *testing.T) {
	s := openStore(t)

	id, err := s.RecordDeploy(history.Entry{
		AppName:       "app",
		Service:       "app",
		OldImage:      "app:1",
		NewImage:      "app:2",
		ImageTag:      "2",
		DeployID:      "d1",
		TriggerSource: "poll",
		Status:        "success",
		StartedAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("RecordDeploy: %v", err)
	}

	e, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.Service != "app" {
		t.Errorf("Get service = %q, want app", e.Service)
	}

	latest, err := s.Latest("app")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.ID != id {
		t.Errorf("Latest id = %d, want %d", latest.ID, id)
	}
}
