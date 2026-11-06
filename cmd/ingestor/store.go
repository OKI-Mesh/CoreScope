package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store wraps the database for packet ingestion.
type Store struct {
	db             *database.DB
	snapshotDir    string
	pruneQueuePath string
	Stats          IngestStats

	sampleIntervalSec int
	backfillWg        sync.WaitGroup

	// prefixIdx holds the prefix → pubkey index used by the
	// resolved_path writer (#1547). Rebuilt on startup and once per
	// neighbor-edges builder tick (60s).
	prefixIdx prefixIdxHolder

	// neighborGraph holds the in-memory NeighborGraph snapshot used
	// by the context-aware resolver (#1560). Rebuilt on startup and
	// once per neighbor-edges builder tick (60s).
	neighborGraph neighborGraphHolder

	// relayTouched is the debounce map for touchRelayNodes:
	// pubkey -> rxTime of the last last_seen write (#1598). Touched only on the ingest
	// buffer worker (every InsertTransmission call runs there), so it needs no lock.
	relayTouched map[string]time.Time
}

// relayTouchDebounce is the minimum interval between two last_seen writes
// for the same relay node. A backbone repeater appears in thousands of
// paths per hour; without this the ingest path would issue one UPDATE per
// observation for no added freshness.
const relayTouchDebounce = 5 * time.Minute

// relayTouchedMaxEntries caps the debounce map. One entry per node ever
// seen relaying — ~10^3-10^4 on real deployments — but a long-lived
// process on a large mesh should not grow it without bound. On overflow
// we drop entries older than two debounce windows, which can only cause
// an extra UPDATE, never a missed one.
const relayTouchedMaxEntries = 50000

// OpenStore opens or creates a SQLite DB at the given path, applying the
// v3 schema that is compatible with the Node.js server.
func OpenStore(ctx context.Context, connStr string, pruneQueuePath string) (*Store, error) {
	return OpenStoreWithInterval(ctx, connStr, 300, pruneQueuePath)
}

// OpenStoreWithInterval connects to Postgres with a configurable sample
// interval. pruneQueuePath is the shared prune-queue anchor (see
// prunequeue); only its parent directory is used.
func OpenStoreWithInterval(ctx context.Context, connStr string, sampleIntervalSec int, pruneQueuePath string) (*Store, error) {
	db, err := database.OpenReadWrite(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	s := &Store{
		db:                db,
		sampleIntervalSec: sampleIntervalSec,
		pruneQueuePath:    pruneQueuePath,
	}
	// Seed the in-process scope-match counters from the persisted running
	// total so a restart doesn't zero the ratio. Synchronous: it's one row.
	if err := s.LoadScopeMatchTotals(ctx); err != nil {
		log.Printf("[scope-match] failed to restore totals: %v", err)
	}

	return s, nil
}

// Close checkpoints the WAL and closes the database.
func (s *Store) Close() error {
	s.backfillWg.Wait()
	s.db.Close()
	return nil
}

// RoundToInterval rounds a time to the nearest sample interval boundary.
func RoundToInterval(t time.Time, intervalSec int) time.Time {
	if intervalSec <= 0 {
		intervalSec = 300
	}
	epoch := t.Unix()
	half := int64(intervalSec) / 2
	rounded := ((epoch + half) / int64(intervalSec)) * int64(intervalSec)
	return time.Unix(rounded, 0).UTC()
}

// MetricsData holds the fields to insert into observer_metrics.
type MetricsData struct {
	ObserverID  string
	NoiseFloor  *float64
	TxAirSecs   *int
	RxAirSecs   *int
	RecvErrors  *int
	BatteryMv   *int
	PacketsSent *int
	PacketsRecv *int
}

// TODO: MOVE TO ETL program
// backfillTransportCodes does the actual batched decode-and-update work,
// returning the number of rows updated. Uses keyset pagination on id rather
// than counting decoded items per page: a page can hit the LIMIT while
// containing undecodable rows, so "items returned < batchSize" does NOT mean
// "no rows remain" — that conflation previously caused the backfill to record
// its completion migration after silently truncating on the first page with
// an undecodable row. Termination is driven by how many rows the page
// actually scanned (seen), not by how many decoded successfully; lastID
// advances past undecodable rows too, so they are stepped over rather than
// re-selected forever.
//
// Invariant: the caller records the backfill_transport_codes_v1 guard row iff
// this returns a nil error, i.e. iff the loop ran to genuine exhaustion of all
// matching rows. Every error path below returns non-nil immediately, without
// finishing the run, so the guard is withheld and the next startup retries.
// func (s *Store) backfillTransportCodes(batchSize int) (int, error) {
// 	var total int
// 	var lastID int64
// 	for {
// 		rows, err := s.db.Query(`
// 			SELECT id, raw_hex FROM transmissions
// 			WHERE id > ? AND code1 IS NULL AND route_type IN (0, 3)
// 			ORDER BY id
// 			LIMIT ?`, lastID, batchSize)
// 		if err != nil {
// 			return total, fmt.Errorf("transport_codes select: %w", err)
// 		}
// 		type item struct {
// 			id           int64
// 			code1, code2 string
// 		}
// 		var items []item
// 		var seen int
// 		for rows.Next() {
// 			var id int64
// 			var raw string
// 			if err := rows.Scan(&id, &raw); err != nil {
// 				rows.Close()
// 				return total, fmt.Errorf("transport_codes scan: %w", err)
// 			}
// 			seen++
// 			lastID = id // advance past undecodable rows too, or they are re-selected forever
// 			decoded, err := DecodePacket(raw, nil, false)
// 			if err != nil || decoded.TransportCodes == nil {
// 				continue
// 			}
// 			items = append(items, item{id, decoded.TransportCodes.Code1, decoded.TransportCodes.Code2})
// 		}
// 		rows.Close()
// 		if seen == 0 {
// 			break // no rows left matching the filter
// 		}

// 		if len(items) > 0 {
// 			tx, err := s.db.Begin()
// 			if err != nil {
// 				return total, fmt.Errorf("transport_codes begin: %w", err)
// 			}
// 			stmt, err := tx.Prepare(`UPDATE transmissions SET code1 = ?, code2 = ? WHERE id = ?`)
// 			if err != nil {
// 				tx.Rollback()
// 				return total, fmt.Errorf("transport_codes prepare: %w", err)
// 			}
// 			for _, it := range items {
// 				if _, err := stmt.Exec(it.code1, it.code2, it.id); err != nil {
// 					stmt.Close()
// 					tx.Rollback()
// 					return total, fmt.Errorf("transport_codes update id=%d: %w", it.id, err)
// 				}
// 			}
// 			stmt.Close()
// 			if err := tx.Commit(); err != nil {
// 				return total, fmt.Errorf("transport_codes commit: %w", err)
// 			}
// 			total += len(items)
// 		}
// 		if seen < batchSize {
// 			break // last partial page
// 		}
// 	}
// 	return total, nil
// }

// LogStats logs current operational metrics.
func (s *Store) LogStats() {
	log.Printf("[stats] tx_inserted=%d tx_dupes=%d obs_inserted=%d node_upserts=%d observer_upserts=%d write_errors=%d sig_drops=%d",
		s.Stats.TransmissionsInserted.Load(),
		s.Stats.DuplicateTransmissions.Load(),
		s.Stats.ObservationsInserted.Load(),
		s.Stats.NodeUpserts.Load(),
		s.Stats.ObserverUpserts.Load(),
		s.Stats.WriteErrors.Load(),
		s.Stats.SignatureDrops.Load(),
	)
}

// compactRelayTouched drops debounce entries older than two windows.
// Not safe for concurrent use: see relayTouched.
func (s *Store) compactRelayTouched(now time.Time) {
	cutoff := now.Add(-2 * relayTouchDebounce)
	for pk, t := range s.relayTouched {
		if t.Before(cutoff) {
			delete(s.relayTouched, pk)
		}
	}
}

// scopeNameForDB encodes PacketData scope semantics for DB storage:
// non-transport-scoped → nil (SQL NULL); transport-scoped → pointer to ScopeName
// (may be "" for unknown region, "#name" for matched region).
func scopeNameForDB(data *PacketData) *string {
	if !data.IsTransportScoped {
		return nil
	}
	s := data.ScopeName
	return &s
}

// normalizeScopeList canonicalises the comma-separated region-scope string an
// observer reports for a node, so configured_scope is directly comparable with
// default_scope instead of only looking similar (#1865, raised by @cwichura).
//
// The OTA scope query returns bare region names ("dk,eu,dk-aarhus"), while
// everything else in CoreScope carries the leading "#": every stored
// default_scope value on a live 1425-node instance starts with one (361 of
// 361), and the hashRegions/hashChannels config paths already prefix a missing
// "#" before deriving the channel key from it (main.go). The "#" is therefore
// part of the region's name here, not decoration, so a bare "dk" and a stored
// "#dk" are the same region and must be written the same way.
//
// Rules, in order: entries are trimmed; empty entries are dropped; "*" is a
// wildcard rather than a region name and is passed through untouched; anything
// else gains a leading "#" if it lacks one. Order is preserved and duplicates
// are NOT collapsed, because the observer's ordering carries meaning we do not
// interpret here. Case is left alone: region keys are derived as
// sha256("#name"), so folding case would silently retarget a scope.
//
// An empty or all-empty input returns "", which UpdateNodeConfiguredScope
// stores as a valid "responded, no scopes configured" statement.
func normalizeScopeList(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p != "*" && !strings.HasPrefix(p, "#") {
			p = "#" + p
		}
		out = append(out, p)
	}
	return strings.Join(out, ",")
}

// MQTTPacketMessage is the JSON payload from an MQTT raw packet message.
type MQTTPacketMessage struct {
	Raw       string   `json:"raw"`
	SNR       *float64 `json:"SNR"`
	RSSI      *float64 `json:"RSSI"`
	Score     *float64 `json:"score"`
	Direction *string  `json:"direction"`
	Origin    string   `json:"origin"`
	Region    string   `json:"region,omitempty"`    // optional region override (#788)
	Timestamp string   `json:"timestamp,omitempty"` // observer receive time, resolved by handler
}
