package serviceconfig

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type ServiceConfig struct {
	Name           string
	Image          string
	ComposeFile    string
	TriggerMode    string        // "webhook" | "poll"
	PollerInterval time.Duration
	LastDigest     string        // "" = never polled
	LastCheckedAt  *time.Time
}

type Manager struct {
	db       *sql.DB
	mu       sync.RWMutex
	services map[string]*ServiceConfig
}

func NewManager(db *sql.DB) (*Manager, error) {
	m := &Manager{
		db:       db,
		services: make(map[string]*ServiceConfig),
	}
	if err := m.Reload(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) Reload() error {
	rows, err := m.db.Query(`
		SELECT name, image, compose_file, trigger_mode, poller_interval, last_digest, last_checked_at 
		FROM services
	`)
	if err != nil {
		return fmt.Errorf("serviceconfig: query: %w", err)
	}
	defer rows.Close()

	newServices := make(map[string]*ServiceConfig)
	for rows.Next() {
		var (
			c             ServiceConfig
			intervalSecs  int
			lastDigest    sql.NullString
			lastCheckedAt sql.NullString
		)
		if err := rows.Scan(
			&c.Name,
			&c.Image,
			&c.ComposeFile,
			&c.TriggerMode,
			&intervalSecs,
			&lastDigest,
			&lastCheckedAt,
		); err != nil {
			return fmt.Errorf("serviceconfig: scan: %w", err)
		}

		c.PollerInterval = time.Duration(intervalSecs) * time.Second
		if lastDigest.Valid {
			c.LastDigest = lastDigest.String
		}
		if lastCheckedAt.Valid {
			if t, err := time.Parse(time.RFC3339, lastCheckedAt.String); err == nil {
				c.LastCheckedAt = &t
			}
		}

		newServices[c.Name] = &c
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("serviceconfig: rows error: %w", err)
	}

	m.mu.Lock()
	m.services = newServices
	m.mu.Unlock()
	return nil
}

func (m *Manager) Get(name string) (*ServiceConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.services[name]
	return c, ok
}

func (m *Manager) All() []*ServiceConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*ServiceConfig, 0, len(m.services))
	for _, c := range m.services {
		out = append(out, c)
	}
	return out
}

func (m *Manager) UpdateDigest(name, digest string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.db.Exec(`
		UPDATE services 
		SET last_digest = ?, last_checked_at = ?, updated_at = ?
		WHERE name = ?
	`, digest, now, now, name)
	if err != nil {
		return fmt.Errorf("serviceconfig: update digest: %w", err)
	}
	return m.Reload()
}

func (m *Manager) UpdateConfig(name, triggerMode string, pollerInterval int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.db.Exec(`
		UPDATE services 
		SET trigger_mode = ?, poller_interval = ?, updated_at = ?
		WHERE name = ?
	`, triggerMode, pollerInterval, now, name)
	if err != nil {
		return fmt.Errorf("serviceconfig: update config: %w", err)
	}
	return m.Reload()
}

func (m *Manager) CreateConfig(name, image, composeFile, triggerMode string, pollerInterval int) error {
	_, err := m.db.Exec(`
		INSERT INTO services (name, image, compose_file, trigger_mode, poller_interval)
		VALUES (?, ?, ?, ?, ?)
	`, name, image, composeFile, triggerMode, pollerInterval)
	if err != nil {
		return fmt.Errorf("serviceconfig: create config: %w", err)
	}
	return m.Reload()
}
