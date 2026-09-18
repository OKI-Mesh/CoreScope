// Package testfixtures provides shared test-data helpers for
// cmd/ingestor and cmd/server tests. It depends on internal/database
// (for BaselineStampMigrate), so it must never be imported by
// internal/database or internal/database/migrate's own tests — only
// by packages above the database layer.
package testfixtures

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// TempDBPath returns a fresh, file-backed SQLite path under the test's
// temp directory, cleaning up the main file plus any -wal/-shm
// sidecar files it may create.
func TempDBPath(t testing.TB) string {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	return filepath.Join(t.TempDir(), name+".db")
}

// NewDB creates a fresh, fully-migrated SQLite database at path. Each
// helper below (Observer, Transmission, Observation, InsertNode) opens
// its own short-lived connection, so callers are free to open whatever
// connection they actually need afterward (OpenDB, a raw rw handle,
// etc.) without fighting over a shared one.
func NewDB(t testing.TB, path string) {
	t.Helper()
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for migration: %v", err)
	}
	defer conn.Close()
	if _, err := database.BaselineStampMigrate(conn, nil); err != nil {
		t.Fatalf("migrating test db: %v", err)
	}
}

func exec(t testing.TB, path string, query string, args ...interface{}) {
	t.Helper()
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for seeding: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Exec(query, args...); err != nil {
		t.Fatalf("exec failed: %v\nSQL: %s", err, query)
	}
}

// Observer inserts an observer row and returns its rowid, for use as
// observations.observer_idx.
func Observer(t testing.TB, path, id, name string) int64 {
	t.Helper()
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for seeding: %v", err)
	}
	defer conn.Close()
	res, err := conn.Exec(`INSERT INTO observers (id, name) VALUES (?, ?)`, id, name)
	if err != nil {
		t.Fatalf("insert observer: %v", err)
	}
	rowID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("observer rowid: %v", err)
	}
	return rowID
}

type Tx struct {
	ID          int
	RawHex      string
	Hash        string
	FirstSeen   time.Time
	RouteType   int
	PayloadType int
	Decoded     string
	LastSeen    time.Time // defaults to FirstSeen if zero
}

func Transmission(t testing.TB, path string, tx Tx) {
	t.Helper()
	lastSeen := tx.LastSeen
	if lastSeen.IsZero() {
		lastSeen = tx.FirstSeen
	}
	exec(t, path, `INSERT INTO transmissions
		(id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		tx.ID, tx.RawHex, tx.Hash, tx.FirstSeen.Format(time.RFC3339),
		tx.RouteType, tx.PayloadType, tx.Decoded, lastSeen.Unix())
}

type Obs struct {
	ID             int
	TransmissionID int
	ObserverRowID  int64
	Direction      string
	SNR, RSSI      float64
	Score          int
	PathJSON       string
	Timestamp      time.Time
	RawHex         string // optional
	ResolvedPath   string // optional, NULL if empty
}

func Observation(t testing.TB, path string, o Obs) {
	t.Helper()
	var resolvedPath interface{}
	if o.ResolvedPath != "" {
		resolvedPath = o.ResolvedPath
	}
	exec(t, path, `INSERT INTO observations
		(id, transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp, raw_hex, resolved_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		o.ID, o.TransmissionID, o.ObserverRowID, o.Direction, o.SNR, o.RSSI,
		o.Score, o.PathJSON, o.Timestamp.Unix(), o.RawHex, resolvedPath)
}

type Node struct {
	PublicKey   string
	Name        string
	Role        string
	Lat, Lon    float64
	LastSeen    string
	FirstSeen   string
	AdvertCount int
}

func InsertNode(t testing.TB, path string, n Node) {
	t.Helper()
	exec(t, path, `INSERT INTO nodes
		(public_key, name, role, lat, lon, last_seen, first_seen, advert_count)
		VALUES (?,?,?,?,?,?,?,?)`,
		n.PublicKey, n.Name, n.Role, n.Lat, n.Lon, n.LastSeen, n.FirstSeen, n.AdvertCount)
}
