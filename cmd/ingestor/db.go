package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	_ "modernc.org/sqlite"
)

// DBStats tracks operational metrics for the ingestor database.
type DBStats struct {
	TransmissionsInserted  atomic.Int64
	ObservationsInserted   atomic.Int64
	DuplicateTransmissions atomic.Int64
	NodeUpserts            atomic.Int64
	ObserverUpserts        atomic.Int64
	WriteErrors            atomic.Int64
	SignatureDrops         atomic.Int64
	// WALCommits tracks every successful tx.Commit() that may have flushed
	// WAL pages.
	WALCommits atomic.Int64
	// BackfillUpdates tracks per-named-backfill row write counts so an
	// infinite-loop backfill (cf #1119) is obvious from the perf page.
	BackfillUpdates sync.Map // name (string) -> *atomic.Int64
}

// IncBackfill increments the backfill counter for the given name, allocating
// the counter on first use.
func (s *DBStats) IncBackfill(name string) {
	v, ok := s.BackfillUpdates.Load(name)
	if !ok {
		nc := new(atomic.Int64)
		actual, loaded := s.BackfillUpdates.LoadOrStore(name, nc)
		if loaded {
			v = actual
		} else {
			v = nc
		}
	}
	v.(*atomic.Int64).Add(1)
}

// SnapshotBackfills returns a name->count copy of all backfill counters.
func (s *DBStats) SnapshotBackfills() map[string]int64 {
	out := make(map[string]int64)
	s.BackfillUpdates.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// Store wraps the SQLite database for packet ingestion.
type Store struct {
	db    *sql.DB
	path  string // filesystem path to the SQLite DB (used to resolve queue dirs)
	Stats DBStats

	stmtGetTxByHash            *sql.Stmt
	stmtInsertTransmission     *sql.Stmt
	stmtUpdateTxFirstSeen      *sql.Stmt
	stmtBumpTxLastSeen         *sql.Stmt
	stmtInsertObservation      *sql.Stmt
	stmtUpsertNode             *sql.Stmt
	stmtIncrementAdvertCount   *sql.Stmt
	stmtUpsertObserver         *sql.Stmt
	stmtGetObserverRowid       *sql.Stmt
	stmtUpdateObserverLastSeen *sql.Stmt
	stmtUpdateNodeTelemetry    *sql.Stmt
	stmtUpsertMetrics          *sql.Stmt

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
}

// OpenStore opens or creates a SQLite DB at the given path, applying the
// v3 schema that is compatible with the Node.js server.
func OpenStore(dbPath string) (*Store, error) {
	return OpenStoreWithInterval(dbPath, 300)
}

// OpenStoreWithInterval opens or creates a SQLite DB with a configurable sample interval.
func OpenStoreWithInterval(dbPath string, sampleIntervalSec int) (*Store, error) {
	conn, err := database.OpenReadWrite(dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	log.Printf("SQLite config: busy_timeout=5000ms, max_open_conns=1, max_idle_conns=1, journal=WAL")

	if err := database.AssertBaselined(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("refusing to start: %w", err)
	}

	provider, err := database.NewMigrationProvider(conn, true, false)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("creating goose provider: %w", err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	if err := database.EnsureLegacyMigrationsTable(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ensure legacy migrations table: %w", err)
	}
	if err := database.NormalizePublicKeyCasing(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("normalize public_key casing: %w", err)
	}

	s := &Store{db: conn, path: dbPath, sampleIntervalSec: sampleIntervalSec}
	if err := s.prepareStatements(); err != nil {
		return nil, fmt.Errorf("preparing statements: %w", err)
	}

	// #1690: backfill transmissions.last_seen from MAX(observations.timestamp)
	// per transmission. The column is added inline by dbschema.Apply (cheap
	// metadata-only ALTER); the populate query is potentially expensive
	// (full obs scan + group) so we run it async. Subsequent observation
	// inserts maintain the column inline (see InsertTransmission below).
	// PREFLIGHT: async=true reason="full-table backfill JOIN (1.9M+ obs × 86k+ tx in prod) — must not block ingestor boot"
	if err := s.RunAsyncMigration(context.Background(), "tx_last_seen_backfill_v1",
		func(ctx context.Context, d *sql.DB) error {
			log.Println("[migration/async] Backfilling transmissions.last_seen from MAX(observations.timestamp)...")
			res, err := d.ExecContext(ctx, `
				UPDATE transmissions
				SET last_seen = COALESCE((
					SELECT MAX(timestamp) FROM observations WHERE transmission_id = transmissions.id
				), last_seen)
				WHERE last_seen = 0
			`)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			log.Printf("[migration/async] transmissions.last_seen backfill complete: %d rows updated", n)
			return nil
		}); err != nil {
		log.Printf("[migration/async] scheduling tx_last_seen_backfill_v1 failed: %v", err)
	}

	return s, nil
}

func (s *Store) prepareStatements() error {
	var err error

	s.stmtGetTxByHash, err = s.db.Prepare("SELECT id, first_seen FROM transmissions WHERE hash = ?")
	if err != nil {
		return err
	}

	s.stmtInsertTransmission, err = s.db.Prepare(`
		INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, channel_hash, scope_name, from_pubkey, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}

	s.stmtUpdateTxFirstSeen, err = s.db.Prepare("UPDATE transmissions SET first_seen = ? WHERE id = ?")
	if err != nil {
		return err
	}

	// #1690: bump transmissions.last_seen to MAX(current, ?) on every
	// observation insert so cold-load can filter on effective recency.
	// This is NOT a migration — it's the steady-state writer path. The
	// one-time backfill (BackfillPathJSONAsync-shaped) runs via
	// RunAsyncMigration above; this prepared-statement UPDATE is the
	// per-row maintenance that keeps the column current after the
	// backfill completes. Recorded in _migrations under
	// "tx_last_seen_backfill_v1".
	// PREFLIGHT: async=true reason="prepared-statement row-level UPDATE BY PRIMARY KEY (transmissions.id) — single-row touch per observation, indexed by PK, constant-time at any scale. Not a migration."
	s.stmtBumpTxLastSeen, err = s.db.Prepare("UPDATE transmissions SET last_seen = ? WHERE id = ? AND last_seen < ?")
	if err != nil {
		return err
	}

	s.stmtInsertObservation, err = s.db.Prepare(`
		INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp, raw_hex, resolved_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(transmission_id, observer_idx, COALESCE(path_json, '')) DO UPDATE SET
			snr           = COALESCE(excluded.snr,           snr),
			rssi          = COALESCE(excluded.rssi,          rssi),
			score         = COALESCE(excluded.score,         score),
			raw_hex       = COALESCE(excluded.raw_hex,       raw_hex),
			resolved_path = COALESCE(excluded.resolved_path, resolved_path)
	`)
	if err != nil {
		return err
	}

	s.stmtUpsertNode, err = s.db.Prepare(`
		INSERT INTO nodes (public_key, name, role, lat, lon, last_seen, first_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(public_key) DO UPDATE SET
			name = COALESCE(?, name),
			role = COALESCE(?, role),
			lat = COALESCE(?, lat),
			lon = COALESCE(?, lon),
			last_seen = MAX(MIN(COALESCE(last_seen, ''), ?), ?)
	`)
	if err != nil {
		return err
	}

	s.stmtIncrementAdvertCount, err = s.db.Prepare(`
		UPDATE nodes SET advert_count = advert_count + 1 WHERE public_key = ?
	`)
	if err != nil {
		return err
	}

	s.stmtUpsertObserver, err = s.db.Prepare(`
		INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count, model, firmware, client_version, radio, battery_mv, uptime_secs, noise_floor, can_relay, can_relay_seen)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, 1), CASE WHEN ? IS NULL THEN 0 ELSE 1 END)
		ON CONFLICT(id) DO UPDATE SET
			name = COALESCE(?, name),
			iata = COALESCE(?, iata),
			last_seen = MAX(MIN(COALESCE(last_seen, ''), ?), ?),
			packet_count = packet_count + 1,
			model = COALESCE(?, model),
			firmware = COALESCE(?, firmware),
			client_version = COALESCE(?, client_version),
			radio = COALESCE(?, radio),
			battery_mv = COALESCE(?, battery_mv),
			uptime_secs = COALESCE(?, uptime_secs),
			noise_floor = COALESCE(?, noise_floor),
			can_relay = COALESCE(?, can_relay),
			can_relay_seen = CASE WHEN ? IS NULL THEN can_relay_seen ELSE 1 END
	`)
	if err != nil {
		return err
	}

	s.stmtGetObserverRowid, err = s.db.Prepare("SELECT rowid FROM observers WHERE id = ?")
	if err != nil {
		return err
	}

	// Args: ingestNow, rxTime, ingestNow, rxTime, rowid
	// MIN(existing, ingestNow) clamps any future value already in the DB before
	// taking MAX with rxTime, so the guard never locks in a past bug's stale future.
	s.stmtUpdateObserverLastSeen, err = s.db.Prepare(`
		UPDATE observers SET
			last_seen      = MAX(MIN(COALESCE(last_seen, ''), ?), ?),
			last_packet_at = MAX(MIN(COALESCE(last_packet_at, ''), ?), ?)
		WHERE rowid = ?`)
	if err != nil {
		return err
	}

	s.stmtUpdateNodeTelemetry, err = s.db.Prepare(`
		UPDATE nodes SET
			battery_mv = COALESCE(?, battery_mv),
			temperature_c = COALESCE(?, temperature_c)
		WHERE public_key = ?
	`)
	if err != nil {
		return err
	}

	s.stmtUpsertMetrics, err = s.db.Prepare(`
		INSERT OR REPLACE INTO observer_metrics (observer_id, timestamp, noise_floor, tx_air_secs, rx_air_secs, recv_errors, battery_mv, packets_sent, packets_recv)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}

	return nil
}

// InsertTransmission inserts a decoded packet into transmissions + observations.
// Returns true if a new transmission was created (not a duplicate hash).
func (s *Store) InsertTransmission(data *PacketData) (bool, error) {
	hash := data.Hash
	if hash == "" {
		return false, nil
	}

	// Wait/hold instrumentation (#1340). The hot path uses prepared
	// statements that auto-commit; gate the whole function under
	// writerMu so concurrent mqtt_handler inserts queue behind any
	// other writer (vacuum, prune, neighbor-builder) and the wait is
	// Go-visible.
	mqttWaitStart := time.Now()
	writerMu.Lock()
	mqttWait := time.Since(mqttWaitStart)
	mqttHoldStart := time.Now()
	defer func() {
		mqttHold := time.Since(mqttHoldStart)
		writerMu.Unlock()
		recordWriterTiming("mqtt_handler", mqttWait, mqttHold, "InsertTransmission")
	}()

	rxTime := data.Timestamp
	ingestNow := time.Now().UTC().Format(time.RFC3339)
	if rxTime == "" {
		rxTime = ingestNow
	}

	var txID int64
	isNew := false

	// Check for existing transmission
	var existingID int64
	var existingFirstSeen string
	err := s.stmtGetTxByHash.QueryRow(hash).Scan(&existingID, &existingFirstSeen)
	if err == nil {
		// Existing transmission
		txID = existingID
		if rxTime < existingFirstSeen {
			_, _ = s.stmtUpdateTxFirstSeen.Exec(rxTime, txID)
		}
	} else {
		// New transmission
		isNew = true
		result, err := s.stmtInsertTransmission.Exec(
			data.RawHex, hash, rxTime,
			data.RouteType, data.PayloadType, data.PayloadVersion,
			data.DecodedJSON, nilIfEmpty(data.ChannelHash),
			scopeNameForDB(data),
			nilIfEmpty(data.FromPubkey),
			epochSecondsForLastSeen(rxTime),
		)
		if err != nil {
			s.Stats.WriteErrors.Add(1)
			return false, fmt.Errorf("insert transmission: %w", err)
		}
		txID, _ = result.LastInsertId()
		s.Stats.TransmissionsInserted.Add(1)
	}

	if !isNew {
		s.Stats.DuplicateTransmissions.Add(1)
	}

	// Resolve observer_idx and update last_seen
	var observerIdx *int64
	if data.ObserverID != "" {
		var rowid int64
		err := s.stmtGetObserverRowid.QueryRow(data.ObserverID).Scan(&rowid)
		if err == nil {
			observerIdx = &rowid
			// observer.last_seen and last_packet_at answer "when did the analyzer
			// last hear from this observer" — both are ingest-time questions.
			// Per-packet rxTime is stored separately on observations/transmissions
			// using envelope time (see InsertTransmission above). See #1465.
			_, _ = s.stmtUpdateObserverLastSeen.Exec(ingestNow, ingestNow, ingestNow, ingestNow, rowid)
		}
	}

	// Insert observation
	epochTs := time.Now().Unix()
	if t, err := time.Parse(time.RFC3339, rxTime); err == nil {
		epochTs = t.Unix()
	}

	// Resolve hop prefixes to full pubkeys for `observations.resolved_path`.
	// Per #1547: this writer was lost in the #1289 refactor and lives in
	// the ingestor now. Per #1560: use the context-aware resolver so
	// 1-byte prefix collisions are disambiguated via NeighborGraph
	// adjacency (anchored on from_pubkey for ADVERTs, previous hop
	// otherwise). Empty resolved JSON → NULL via nilIfEmpty.
	resolved := resolvePathWithContext(
		parsePathArray(data.PathJSON),
		strings.ToLower(data.FromPubkey),
		s.neighborGraph.load(),
		s.prefixIdx.load(),
	)
	resolvedJSON := marshalResolvedPath(resolved)

	_, err = s.stmtInsertObservation.Exec(
		txID, observerIdx, data.Direction,
		data.SNR, data.RSSI, data.Score,
		data.PathJSON, epochTs, nilIfEmpty(data.RawHex),
		nilIfEmpty(resolvedJSON),
	)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		log.Printf("[db] observation insert (non-fatal): %v", err)
	} else {
		s.Stats.ObservationsInserted.Add(1)
		// #1690: bump transmissions.last_seen so cold-load can filter on
		// effective recency. Conditional `last_seen < ?` so we never go
		// backwards on out-of-order ingest.
		if _, err := s.stmtBumpTxLastSeen.Exec(epochTs, txID, epochTs); err != nil {
			log.Printf("[db] tx last_seen bump (non-fatal): %v", err)
		}
	}

	// Each prepared-stmt Exec auto-commits. Count one WAL commit per
	// successful InsertTransmission so the perf page sees commit pressure.
	s.Stats.WALCommits.Add(1)

	return isNew, nil
}

// UpsertNode inserts or updates a node.
func (s *Store) UpsertNode(pubKey, name, role string, lat, lon *float64, lastSeen string) error {
	ingestNow := time.Now().UTC().Format(time.RFC3339)
	now := lastSeen
	if now == "" {
		now = ingestNow
	}
	_, err := s.stmtUpsertNode.Exec(
		pubKey, name, role, lat, lon, now, now,
		name, role, lat, lon, ingestNow, now,
	)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
	} else {
		s.Stats.NodeUpserts.Add(1)
	}
	return err
}

// IncrementAdvertCount increments advert_count for a node by public key.
func (s *Store) IncrementAdvertCount(pubKey string) error {
	_, err := s.stmtIncrementAdvertCount.Exec(pubKey)
	return err
}

// MarkNodeForeign sets foreign_advert=1 on the node row identified by pubKey.
// Used when an ADVERT arrives whose GPS lies outside the configured geofilter
// polygon (#730). Idempotent — safe to call repeatedly. No-op if pubKey is
// empty.
func (s *Store) MarkNodeForeign(pubKey string) error {
	if pubKey == "" {
		return nil
	}
	_, err := s.db.Exec(`UPDATE nodes SET foreign_advert = 1 WHERE public_key = ?`, pubKey)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
	}
	return err
}

// UpdateNodeTelemetry updates battery and temperature for a node.
func (s *Store) UpdateNodeTelemetry(pubKey string, batteryMv *int, temperatureC *float64) error {
	var bv, tc interface{}
	if batteryMv != nil {
		bv = *batteryMv
	}
	if temperatureC != nil {
		tc = *temperatureC
	}
	_, err := s.stmtUpdateNodeTelemetry.Exec(bv, tc, pubKey)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
	}
	return err
}

// ObserverMeta holds optional observer hardware metadata.
type ObserverMeta struct {
	Model         *string  // e.g., L1
	Firmware      *string  // firmware version string
	ClientVersion *string  // client app version string
	Radio         *string  // radio chipset/platform string
	BatteryMv     *int     // millivolts, always integer
	UptimeSecs    *int64   // seconds, always integer
	NoiseFloor    *float64 // dBm, may have decimals
	TxAirSecs     *int     // cumulative TX seconds since boot
	RxAirSecs     *int     // cumulative RX seconds since boot
	RecvErrors    *int     // cumulative CRC/decode failures since boot
	PacketsSent   *int     // cumulative packets sent since boot
	PacketsRecv   *int     // cumulative packets received since boot
	// CanRelay reflects the firmware 1.16 /status `repeat` flag (#1290).
	// nil means the firmware did not send the field — caller must
	// preserve the existing observers.can_relay value (default 1).
	// true → relay-capable (`repeat:on`); false → listener-only
	// (`repeat:off`), which causes the server-side disambiguator to
	// exclude this observer's pubkey from path-hop candidate sets.
	CanRelay *bool
}

// UpsertObserver inserts or updates an observer using the current wall-clock
// time as last_seen. Use UpsertObserverAt when the message envelope provides
// an observer receive-time (e.g. MQTT status and data packet handlers).
func (s *Store) UpsertObserver(id, name, iata string, meta *ObserverMeta) error {
	return s.UpsertObserverAt(id, name, iata, meta, time.Now().UTC().Format(time.RFC3339))
}

// UpsertObserverAt inserts or updates an observer with an explicit lastSeen
// timestamp (typically the observer receive-time from the MQTT envelope). The
// SQL uses MAX so last_seen never moves backwards — a retained or replayed
// message whose rxTime pre-dates the existing last_seen is a no-op for that
// field, preventing offline observers from flashing as Online on reconnect.
func (s *Store) UpsertObserverAt(id, name, iata string, meta *ObserverMeta, lastSeen string) error {
	ingestNow := time.Now().UTC().Format(time.RFC3339)
	if lastSeen == "" {
		lastSeen = ingestNow
	}
	normalizedIATA := strings.TrimSpace(strings.ToUpper(iata))

	var model, firmware, clientVersion, radio interface{}
	var batteryMv, uptimeSecs, noiseFloor, canRelay interface{}
	if meta != nil {
		if meta.Model != nil {
			model = *meta.Model
		}
		if meta.Firmware != nil {
			firmware = *meta.Firmware
		}
		if meta.ClientVersion != nil {
			clientVersion = *meta.ClientVersion
		}
		if meta.Radio != nil {
			radio = *meta.Radio
		}
		if meta.BatteryMv != nil {
			batteryMv = *meta.BatteryMv
		}
		if meta.UptimeSecs != nil {
			uptimeSecs = *meta.UptimeSecs
		}
		if meta.NoiseFloor != nil {
			noiseFloor = *meta.NoiseFloor
		}
		// Issue #1290: nil → leave DB column unchanged (COALESCE in
		// the prepared stmt); 0/1 written when firmware provided
		// the `repeat` field. INSERT branch defaults to 1 via the
		// COALESCE in the VALUES clause.
		if meta.CanRelay != nil {
			if *meta.CanRelay {
				canRelay = 1
			} else {
				canRelay = 0
			}
		}
	}

	_, err := s.stmtUpsertObserver.Exec(
		id, name, normalizedIATA, lastSeen, lastSeen, model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay, canRelay,
		name, normalizedIATA, ingestNow, lastSeen, model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay, canRelay,
	)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return err
	}
	s.Stats.ObserverUpserts.Add(1)

	// Reactivate if this observer was previously marked inactive
	s.db.Exec(`UPDATE observers SET inactive = 0 WHERE id = ? AND inactive = 1`, id)
	return nil
}

// Close checkpoints the WAL and closes the database.
func (s *Store) Close() error {
	s.backfillWg.Wait()
	s.Checkpoint()
	return s.db.Close()
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

// InsertMetrics inserts a metrics sample for an observer using ingestor wall clock.
func (s *Store) InsertMetrics(data *MetricsData) error {
	ts := RoundToInterval(time.Now().UTC(), s.sampleIntervalSec)
	tsStr := ts.Format(time.RFC3339)

	var nf, txAir, rxAir, recvErr, batt, pktSent, pktRecv interface{}
	if data.NoiseFloor != nil {
		nf = *data.NoiseFloor
	}
	if data.TxAirSecs != nil {
		txAir = *data.TxAirSecs
	}
	if data.RxAirSecs != nil {
		rxAir = *data.RxAirSecs
	}
	if data.RecvErrors != nil {
		recvErr = *data.RecvErrors
	}
	if data.BatteryMv != nil {
		batt = *data.BatteryMv
	}
	if data.PacketsSent != nil {
		pktSent = *data.PacketsSent
	}
	if data.PacketsRecv != nil {
		pktRecv = *data.PacketsRecv
	}

	_, err := s.stmtUpsertMetrics.Exec(data.ObserverID, tsStr, nf, txAir, rxAir, recvErr, batt, pktSent, pktRecv)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return fmt.Errorf("insert metrics: %w", err)
	}
	return nil
}

// PruneOldMetrics deletes observer_metrics rows older than retentionDays.
func (s *Store) PruneOldMetrics(retentionDays int) (int64, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format(time.RFC3339)
	// Tagged for /api/perf writer-lock visibility (#1340).
	result, err := s.instrumentedExec("prune_metrics", `DELETE FROM observer_metrics WHERE timestamp < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune metrics: %w", err)
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("[metrics] Pruned %d rows older than %d days", n, retentionDays)
	}
	return n, nil
}

// CheckAutoVacuum inspects the current auto_vacuum mode and logs a warning
// if not INCREMENTAL. Performs opt-in full VACUUM if db.vacuumOnStartup is set (#919).
func (s *Store) CheckAutoVacuum(cfg *Config) {
	var autoVacuum int
	if err := s.db.QueryRow("PRAGMA auto_vacuum").Scan(&autoVacuum); err != nil {
		log.Printf("[db] warning: could not read auto_vacuum: %v", err)
		return
	}

	if autoVacuum == 2 {
		log.Printf("[db] auto_vacuum=INCREMENTAL")
		return
	}

	modes := map[int]string{0: "NONE", 1: "FULL", 2: "INCREMENTAL"}
	mode := modes[autoVacuum]
	if mode == "" {
		mode = fmt.Sprintf("UNKNOWN(%d)", autoVacuum)
	}

	log.Printf("[db] auto_vacuum=%s — DB needs one-time VACUUM to enable incremental auto-vacuum. "+
		"Set db.vacuumOnStartup: true in config to migrate (will block startup for several minutes on large DBs). "+
		"See https://github.com/Kpa-clawbot/CoreScope/issues/919", mode)

	if cfg.DB != nil && cfg.DB.VacuumOnStartup {
		// WARNING: Full VACUUM creates a temporary copy of the entire DB file.
		// Requires ~2× the DB file size in free disk space or it will fail.
		log.Printf("[db] vacuumOnStartup=true — starting one-time full VACUUM (ensure 2x DB size free disk space)...")
		start := time.Now()

		if _, err := s.instrumentedExec("vacuum", "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
			log.Printf("[db] VACUUM failed: could not set auto_vacuum: %v", err)
			return
		}
		if _, err := s.instrumentedExec("vacuum", "VACUUM"); err != nil {
			log.Printf("[db] VACUUM failed: %v", err)
			return
		}

		elapsed := time.Since(start)
		log.Printf("[db] VACUUM complete in %v — auto_vacuum is now INCREMENTAL", elapsed.Round(time.Millisecond))
	}
}

// RunIncrementalVacuum returns free pages to the OS (#919).
// Safe to call on auto_vacuum=NONE databases (noop).
func (s *Store) RunIncrementalVacuum(pages int) {
	// Tagged for /api/perf writer-lock visibility (#1340).
	if _, err := s.instrumentedExec("vacuum", fmt.Sprintf("PRAGMA incremental_vacuum(%d)", pages)); err != nil {
		log.Printf("[vacuum] incremental_vacuum error: %v", err)
	}
}

// Checkpoint runs a WAL checkpoint (TRUNCATE mode).
// Returns the number of WAL frames checkpointed (0 if WAL was already empty).
// TRUNCATE resets the WAL file to zero bytes when all frames are checkpointed;
// if active readers hold frames, it checkpoints what it can and leaves the rest.
func (s *Store) Checkpoint() int {
	var busy, walFrames, checkpointed int
	if err := s.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &walFrames, &checkpointed); err != nil {
		log.Printf("[db] WAL checkpoint error: %v", err)
		return 0
	}
	if walFrames > 0 {
		log.Printf("[db] WAL checkpoint: %d/%d frames checkpointed (blocked=%v)", checkpointed, walFrames, busy != 0)
	}
	return checkpointed
}

// BackfillPathJSONAsync launches the path_json backfill in a background goroutine.
// It processes observations with NULL/empty path_json that have raw_hex available,
// decoding hop paths and updating the column. Safe to run concurrently with ingest
// because new observations get path_json at write time; this only touches NULL rows.
// Idempotent: skips if migration already recorded.
func (s *Store) BackfillPathJSONAsync() {
	s.backfillWg.Add(1)
	go func() {
		defer s.backfillWg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[backfill] path_json async panic recovered: %v", r)
			}
		}()

		var migDone int
		row := s.db.QueryRow("SELECT 1 FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'")
		if row.Scan(&migDone) == nil {
			return // already done
		}

		log.Println("[backfill] Starting async path_json backfill from raw_hex...")
		updated := 0
		errored := false
		const batchSize = 1000
		batchNum := 0
		for {
			rows, err := s.db.Query(`
				SELECT o.id, o.raw_hex
				FROM observations o
				JOIN transmissions t ON o.transmission_id = t.id
				WHERE o.raw_hex IS NOT NULL AND o.raw_hex != ''
				-- NB: '[]' is the "already attempted, no hops" sentinel; excluded
				-- to prevent the infinite re-UPDATE loop fixed in #1119.
				AND (o.path_json IS NULL OR o.path_json = '')
				AND t.payload_type != 9
				LIMIT ?`, batchSize)
			if err != nil {
				log.Printf("[backfill] path_json query error: %v", err)
				errored = true
				break
			}
			type pendingRow struct {
				id     int64
				rawHex string
			}
			var batch []pendingRow
			for rows.Next() {
				var r pendingRow
				if err := rows.Scan(&r.id, &r.rawHex); err == nil {
					batch = append(batch, r)
				}
			}
			rows.Close()
			if len(batch) == 0 {
				break
			}
			for _, r := range batch {
				hops, err := packetpath.DecodePathFromRawHex(r.rawHex)
				if err != nil || len(hops) == 0 {
					if _, execErr := s.db.Exec(`UPDATE observations SET path_json = '[]' WHERE id = ?`, r.id); execErr != nil {
						log.Printf("[backfill] write error (id=%d): %v", r.id, execErr)
					} else {
						s.Stats.IncBackfill("path_json")
					}
					continue
				}
				b, _ := json.Marshal(hops)
				if _, execErr := s.db.Exec(`UPDATE observations SET path_json = ? WHERE id = ?`, string(b), r.id); execErr != nil {
					log.Printf("[backfill] write error (id=%d): %v", r.id, execErr)
				} else {
					updated++
					s.Stats.IncBackfill("path_json")
				}
			}
			batchNum++
			if batchNum%50 == 0 {
				log.Printf("[backfill] progress: %d observations updated so far (%d batches)", updated, batchNum)
			}
			// Throttle: yield to ingest writers between batches
			time.Sleep(50 * time.Millisecond)
		}
		log.Printf("[backfill] Async path_json backfill complete: %d observations updated", updated)
		if !errored {
			s.db.Exec(`INSERT INTO _migrations (name) VALUES ('backfill_path_json_from_raw_hex_v1')`)
		} else {
			log.Printf("[backfill] NOT recording migration due to errors — will retry on next restart")
		}
	}()
}

// BackfillDefaultScopeAsync populates default_scope for existing nodes that have
// transport-scoped ADVERT rows (scope_name IS NOT NULL AND scope_name != “).
// Runs in a background goroutine so it does not block MQTT startup.
// Uses the from_pubkey index — O(nodes × indexed lookup), not a full table scan.
//
// Concurrency: the store uses SetMaxOpenConns(1) so all DB writes — including
// MQTT packet inserts and any concurrent backfill goroutines — serialize through
// the single connection pool. busy_timeout(5000) handles transient cross-process
// contention with the read-only server process. No additional locking is needed.
func (s *Store) BackfillDefaultScopeAsync(regionKeys map[string][]byte) {
	// No region keys configured — all scope_name values will be NULL, nothing to backfill.
	if len(regionKeys) == 0 {
		return
	}
	s.backfillWg.Add(1)
	go func() {
		defer s.backfillWg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[backfill] default_scope async panic recovered: %v", r)
			}
		}()

		var done int
		if s.db.QueryRow("SELECT 1 FROM _migrations WHERE name = 'backfill_default_scope_v1'").Scan(&done) == nil {
			return // already ran
		}

		res, err := s.db.Exec(`
			UPDATE nodes SET default_scope = (
				SELECT t.scope_name FROM transmissions t
				WHERE t.from_pubkey = nodes.public_key
				  AND t.payload_type = 4
				  AND t.scope_name IS NOT NULL AND t.scope_name != ''
				ORDER BY t.first_seen DESC LIMIT 1  -- most-recently observed scope wins; first_seen is insertion time
			) WHERE EXISTS (
				SELECT 1 FROM transmissions t
				WHERE t.from_pubkey = nodes.public_key
				  AND t.payload_type = 4
				  AND t.scope_name IS NOT NULL AND t.scope_name != ''
			)`)
		if err != nil {
			log.Printf("[backfill] default_scope: %v", err)
			return
		}
		n, _ := res.RowsAffected()
		s.Stats.IncBackfill("default_scope")
		log.Printf("[backfill] default_scope populated for %d nodes", n)
		s.db.Exec(`INSERT INTO _migrations (name) VALUES ('backfill_default_scope_v1')`)
	}()
}

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

// MoveStaleNodes moves nodes not seen in nodeDays to the inactive_nodes table.
// Returns the number of nodes moved.
func (s *Store) MoveStaleNodes(nodeDays int) (int64, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -nodeDays).Format(time.RFC3339)
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`INSERT OR REPLACE INTO inactive_nodes SELECT * FROM nodes WHERE last_seen < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("insert inactive: %w", err)
	}
	result, err := tx.Exec(`DELETE FROM nodes WHERE last_seen < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete stale: %w", err)
	}
	moved, _ := result.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	if moved > 0 {
		log.Printf("Moved %d node(s) to inactive_nodes (not seen in %d days)", moved, nodeDays)
	}
	return moved, nil
}

// RemoveStaleObservers marks observers that have not actively sent data in observerDays
// as inactive (soft-delete). This preserves JOIN integrity for observations.observer_idx
// and observer_metrics.observer_id — historical data still references the correct observer.
// An observer must actively send data to stay listed — being seen by another node does not count.
// observerDays <= -1 means never remove (keep forever).
func (s *Store) RemoveStaleObservers(observerDays int) (int64, error) {
	if observerDays <= -1 {
		return 0, nil // keep forever
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -observerDays).Format(time.RFC3339)
	// Tagged for /api/perf writer-lock visibility (#1340).
	result, err := s.instrumentedExec("prune_observers", `UPDATE observers SET inactive = 1 WHERE last_seen < ? AND (inactive IS NULL OR inactive = 0)`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("mark stale observers inactive: %w", err)
	}
	removed, _ := result.RowsAffected()
	if removed > 0 {
		// Clean up orphaned metrics for now-inactive observers
		_, _ = s.instrumentedExec("prune_observers", `DELETE FROM observer_metrics WHERE observer_id IN (SELECT id FROM observers WHERE inactive = 1)`)
		log.Printf("Marked %d observer(s) as inactive (not seen in %d days)", removed, observerDays)
	}
	return removed, nil
}

// DroppedPacket holds data for a packet rejected during ingest.
type DroppedPacket struct {
	Hash         string
	RawHex       string
	Reason       string
	ObserverID   string
	ObserverName string
	NodePubKey   string
	NodeName     string
}

// InsertDroppedPacket records a rejected packet in the dropped_packets table.
func (s *Store) InsertDroppedPacket(dp *DroppedPacket) error {
	_, err := s.db.Exec(
		`INSERT INTO dropped_packets (hash, raw_hex, reason, observer_id, observer_name, node_pubkey, node_name) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		dp.Hash, dp.RawHex, dp.Reason, dp.ObserverID, dp.ObserverName, dp.NodePubKey, dp.NodeName,
	)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return fmt.Errorf("insert dropped packet: %w", err)
	}
	s.Stats.SignatureDrops.Add(1)
	return nil
}

// PruneDroppedPackets removes dropped_packets older than retentionDays.
func (s *Store) PruneDroppedPackets(retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format(time.RFC3339)
	result, err := s.db.Exec(`DELETE FROM dropped_packets WHERE dropped_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune dropped packets: %w", err)
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("Pruned %d dropped packet(s) older than %d days", n, retentionDays)
	}
	return n, nil
}

// PacketData holds the data needed to insert a packet into the DB.
type PacketData struct {
	RawHex            string
	Timestamp         string
	ObserverID        string
	ObserverName      string
	SNR               *float64
	RSSI              *float64
	Score             *float64
	Direction         *string
	Hash              string
	RouteType         int
	PayloadType       int
	PayloadVersion    int
	PathJSON          string
	DecodedJSON       string
	ChannelHash       string // grouping key for channel queries (#762)
	ScopeName         string // matched region name, or "" for unknown-scoped
	IsTransportScoped bool   // true when route_type IN (0,3) AND Code1 ≠ "0000"
	Region            string // observer region: payload > topic > source config (#788)
	Foreign           bool   // true when ADVERT GPS lies outside configured geofilter (#730)
	FromPubkey        string // pubkey of the originating node, for exact-match attribution (#1143)
}

// nilIfEmpty returns nil for empty strings (for nullable DB columns).
func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
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

// UpdateNodeDefaultScope records the most-recently observed region scope for a
// node. Skips the UPDATE when the stored value already matches to avoid
// redundant writes on the hot MQTT ingest path. Updates both nodes and
// inactive_nodes to stay consistent.
//
// Defense-in-depth (#1534): an empty scope is treated as a no-op. The call
// site at handleMessage is the primary guard (shouldUpdateDefaultScope),
// but this layer refuses the invalid write so a future caller cannot
// reintroduce the bug by passing "" directly.
func (s *Store) UpdateNodeDefaultScope(pubkey, scope string) error {
	if scope == "" {
		return nil
	}
	// Short-circuit: skip if already stored.
	var cur sql.NullString
	row := s.db.QueryRow(`SELECT default_scope FROM nodes WHERE public_key = ?`, pubkey)
	if row.Scan(&cur) == nil && cur.Valid && cur.String == scope {
		return nil
	}
	if _, err := s.db.Exec(`UPDATE nodes SET default_scope = ? WHERE public_key = ?`, scope, pubkey); err != nil {
		return err
	}
	// Mirror to inactive_nodes (node may be there if recently moved by retention).
	_, err := s.db.Exec(`UPDATE inactive_nodes SET default_scope = ? WHERE public_key = ?`, scope, pubkey)
	return err
}

// RecordNaiveSkew is called when resolveRxTime() clamps a packet's envelope
// timestamp because the observer is emitting a zone-less local-time string
// off from UTC by more than 15 min (issue #1478). Stamps the observer's
// clock_skew_seconds / clock_skew_count_24h / clock_last_naive_at so the
// server can surface a ⚠️ chip + banner in the UI.
//
// The count is reset to 1 (not incremented) if no event has been recorded in
// the past 24h, otherwise incremented. deltaSec is signed: negative = observer
// clock is behind UTC, positive = ahead.
func (s *Store) RecordNaiveSkew(observerID string, deltaSec int64, now time.Time) error {
	if observerID == "" {
		return nil
	}
	nowStr := now.UTC().Format(time.RFC3339)
	cutoff := now.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	// One INSERT-or-UPDATE round trip. ON CONFLICT path resets the rolling
	// counter when the previous event is older than the 24h window, otherwise
	// increments it.
	_, err := s.db.Exec(`
		INSERT INTO observers (id, clock_skew_seconds, clock_skew_count_24h, clock_last_naive_at)
		VALUES (?, ?, 1, ?)
		ON CONFLICT(id) DO UPDATE SET
			clock_skew_seconds = excluded.clock_skew_seconds,
			clock_last_naive_at = excluded.clock_last_naive_at,
			clock_skew_count_24h = CASE
				WHEN clock_last_naive_at IS NULL OR clock_last_naive_at < ?
					THEN 1
				ELSE COALESCE(clock_skew_count_24h, 0) + 1
			END
	`, observerID, deltaSec, nowStr, cutoff)
	return err
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

// BuildPacketData constructs a PacketData from a decoded packet and MQTT message.
// path_json is derived directly from raw_hex header bytes (not decoded.Path.Hops)
// to guarantee the stored path always matches the raw bytes. This matters for
// TRACE packets where decoded.Path.Hops is overwritten with payload hops (#886).
//
// Timestamp is server ingest time (time.Now()), NOT msg.Timestamp (#1370):
// PR #1233 (commit 498fbc03) routed the envelope timestamp into
// PacketData.Timestamp on the premise that uploader-stamped envelope time
// was trustworthy. Issue #1370 disproved that premise — observers with
// broken client clocks (staging Voodoo3 tx 304114: 4/5 obs stamped 18:42
// while genuine receive was 01:42) poisoned transmissions.first_seen /
// observations.timestamp and dragged the /api/channels lastActivity 7h
// into the past. Packet ordering is owned by the server clock; client
// clocks are untrusted. msg.Timestamp still flows into observer.last_seen
// via UpsertObserverAt — that's #1233's MAX/MIN guarded path and is fine.
func BuildPacketData(msg *MQTTPacketMessage, decoded *DecodedPacket, observerID, region string, regionKeys map[string][]byte) *PacketData {
	pathJSON := "[]"
	// For TRACE packets, path_json must be the payload-decoded route hops
	// (decoded.Path.Hops), NOT the raw_hex header bytes which are SNR values.
	// For all other packet types, derive path from raw_hex (#886).
	if !packetpath.PathBytesAreHops(byte(decoded.Header.PayloadType)) {
		if len(decoded.Path.Hops) > 0 {
			b, _ := json.Marshal(decoded.Path.Hops)
			pathJSON = string(b)
		}
	} else if hops, err := packetpath.DecodePathFromRawHex(msg.Raw); err == nil && len(hops) > 0 {
		b, _ := json.Marshal(hops)
		pathJSON = string(b)
	}

	pd := &PacketData{
		RawHex:         msg.Raw,
		Timestamp:      time.Now().UTC().Format(time.RFC3339), // #1370 (counters #1233)
		ObserverID:     observerID,
		ObserverName:   msg.Origin,
		SNR:            msg.SNR,
		RSSI:           msg.RSSI,
		Score:          msg.Score,
		Direction:      msg.Direction,
		Hash:           ComputeContentHash(msg.Raw),
		RouteType:      decoded.Header.RouteType,
		PayloadType:    decoded.Header.PayloadType,
		PayloadVersion: decoded.Header.PayloadVersion,
		PathJSON:       pathJSON,
		DecodedJSON:    PayloadJSON(&decoded.Payload),
	}

	// Region priority: payload field > topic-derived parameter (#788)
	if msg.Region != "" {
		pd.Region = msg.Region
	} else {
		pd.Region = region
	}

	// Populate channel_hash for fast channel queries (#762)
	if decoded.Header.PayloadType == PayloadGRP_TXT {
		if decoded.Payload.Type == "CHAN" && decoded.Payload.Channel != "" {
			pd.ChannelHash = decoded.Payload.Channel
		} else if decoded.Payload.Type == "GRP_TXT" && decoded.Payload.ChannelHashHex != "" {
			pd.ChannelHash = "enc_" + decoded.Payload.ChannelHashHex
		}
	}

	if decoded.TransportCodes != nil && decoded.TransportCodes.Code1 != "0000" {
		pd.IsTransportScoped = true
		pd.ScopeName = matchScope(regionKeys, byte(decoded.Header.PayloadType), decoded.payloadRaw, decoded.TransportCodes.Code1)
	}

	// Populate from_pubkey at write time (#1143). ADVERTs carry the
	// originating node's pubkey directly; other packet types stay NULL
	// (downstream attribution queries handle NULL gracefully).
	if decoded.Header.PayloadType == PayloadADVERT && decoded.Payload.PubKey != "" {
		pd.FromPubkey = decoded.Payload.PubKey
	}

	return pd
}

// ─── Writer-lock instrumentation (issue #1340) ────────────────────────────
//
// Make SQLite writer-lock starvation visible to operators. Per-component
// wait_ms / hold_ms / contention_total histograms, surfaced via
// /api/perf/write-sources under the "writer_perf" key. Component tags:
// neighbor_builder, mqtt_handler, prune_packets, prune_observers,
// prune_metrics, mbcap_persist (deferred — see PR body), vacuum.
//
// The single writer connection (SetMaxOpenConns(1)) means writes serialise
// inside the driver and the wait is invisible to Go. writerMu measures the
// wait Go can see (everyone queueing behind the current holder) by gating
// every wrapped call site through the same package-level mutex.

// WriterStatsSnapshot is a per-component wait/hold latency snapshot
// surfaced via /api/perf to make SQLite writer-lock starvation visible
// to operators (issue #1340). Times are in milliseconds.
type WriterStatsSnapshot struct {
	Count           int64   `json:"count"`
	ContentionTotal int64   `json:"contention_total"`
	WaitMsP50       float64 `json:"wait_ms_p50"`
	WaitMsP95       float64 `json:"wait_ms_p95"`
	WaitMsP99       float64 `json:"wait_ms_p99"`
	WaitMsMax       float64 `json:"wait_ms_max"`
	HoldMsP50       float64 `json:"hold_ms_p50"`
	HoldMsP95       float64 `json:"hold_ms_p95"`
	HoldMsP99       float64 `json:"hold_ms_p99"`
	HoldMsMax       float64 `json:"hold_ms_max"`
}

const (
	// writerSampleWindow bounds the per-component rolling window so a
	// long-running ingestor doesn't grow this unbounded.
	writerSampleWindow = 1024
	// contentionThresholdMs: wait_ms above this counts as a "contended"
	// write (per #1340 spec).
	contentionThresholdMs = 100.0
	defaultSlowWriterMs   = 500.0
)

// slowWriterThresholdMsAtomic — hold_ms threshold above which writes
// emit a [db-slow-writer] log line. Read on the hot path; written once
// at startup by SetSlowWriterThresholdMs.
var slowWriterThresholdMsAtomic atomic.Uint64

// SetSlowWriterThresholdMs sets the [db-slow-writer] log threshold.
// ms<=0 restores the 500ms default. Operators can also set
// CORESCOPE_DB_SLOW_WRITER_MS at process start — see initSlowWriterFromEnv.
func SetSlowWriterThresholdMs(ms float64) {
	if ms <= 0 {
		ms = defaultSlowWriterMs
	}
	slowWriterThresholdMsAtomic.Store(uint64(ms))
}

func getSlowWriterThresholdMs() float64 {
	v := slowWriterThresholdMsAtomic.Load()
	if v == 0 {
		return defaultSlowWriterMs
	}
	return float64(v)
}

// initSlowWriterFromEnv is called once from package init so operators can
// override the threshold via CORESCOPE_DB_SLOW_WRITER_MS without a
// Go-side Config change.
func initSlowWriterFromEnv() {
	v := os.Getenv("CORESCOPE_DB_SLOW_WRITER_MS")
	if v == "" {
		return
	}
	var ms float64
	if _, err := fmt.Sscanf(v, "%f", &ms); err == nil && ms > 0 {
		SetSlowWriterThresholdMs(ms)
	}
}

func init() { initSlowWriterFromEnv() }

type writerComponentStats struct {
	mu              sync.Mutex
	count           int64
	contentionTotal int64
	waitMs          []float64
	holdMs          []float64
	waitMax         float64
	holdMax         float64
}

func (c *writerComponentStats) record(waitMs, holdMs float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	if waitMs > contentionThresholdMs {
		c.contentionTotal++
	}
	if waitMs > c.waitMax {
		c.waitMax = waitMs
	}
	if holdMs > c.holdMax {
		c.holdMax = holdMs
	}
	c.waitMs = appendBoundedFloat(c.waitMs, waitMs, writerSampleWindow)
	c.holdMs = appendBoundedFloat(c.holdMs, holdMs, writerSampleWindow)
}

func appendBoundedFloat(s []float64, v float64, max int) []float64 {
	if len(s) < max {
		return append(s, v)
	}
	copy(s, s[1:])
	s[len(s)-1] = v
	return s
}

func (c *writerComponentStats) snapshot() WriterStatsSnapshot {
	c.mu.Lock()
	wait := append([]float64(nil), c.waitMs...)
	hold := append([]float64(nil), c.holdMs...)
	snap := WriterStatsSnapshot{
		Count:           c.count,
		ContentionTotal: c.contentionTotal,
		WaitMsMax:       c.waitMax,
		HoldMsMax:       c.holdMax,
	}
	c.mu.Unlock()
	sort.Float64s(wait)
	sort.Float64s(hold)
	snap.WaitMsP50 = nearestRankPercentile(wait, 0.50)
	snap.WaitMsP95 = nearestRankPercentile(wait, 0.95)
	snap.WaitMsP99 = nearestRankPercentile(wait, 0.99)
	snap.HoldMsP50 = nearestRankPercentile(hold, 0.50)
	snap.HoldMsP95 = nearestRankPercentile(hold, 0.95)
	snap.HoldMsP99 = nearestRankPercentile(hold, 0.99)
	return snap
}

func nearestRankPercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	idx := int(p*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

type writerStatsAggregator struct {
	mu         sync.Mutex
	components map[string]*writerComponentStats
}

var writerStatsAgg = &writerStatsAggregator{
	components: make(map[string]*writerComponentStats),
}

func (a *writerStatsAggregator) get(component string) *writerComponentStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.components[component]
	if !ok {
		c = &writerComponentStats{}
		a.components[component] = c
	}
	return c
}

// reset clears all per-component samples. Test-only: lets a single
// scenario assert against a clean aggregator without prior-test noise
// in the same package run (TestWriterStarvationVisibleInPerf would
// otherwise mix this run's 5 starved samples with thousands of fast
// InsertTransmission samples from earlier tests and the p99 would
// collapse below the 50s threshold).
func (a *writerStatsAggregator) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.components = make(map[string]*writerComponentStats)
}

// ResetWriterStatsForTest wipes the per-component writer stats
// aggregator. Test-only; not safe to call from production code paths.
func ResetWriterStatsForTest() { writerStatsAgg.reset() }

func (a *writerStatsAggregator) snapshot() map[string]WriterStatsSnapshot {
	a.mu.Lock()
	keys := make([]string, 0, len(a.components))
	stats := make([]*writerComponentStats, 0, len(a.components))
	for k, v := range a.components {
		keys = append(keys, k)
		stats = append(stats, v)
	}
	a.mu.Unlock()
	out := make(map[string]WriterStatsSnapshot, len(keys))
	for i, k := range keys {
		out[k] = stats[i].snapshot()
	}
	return out
}

// WriterStatsSnapshot returns a per-component wait/hold/contention
// snapshot for exposure on /api/perf/write-sources (issue #1340).
func (s *Store) WriterStatsSnapshot() map[string]WriterStatsSnapshot {
	return writerStatsAgg.snapshot()
}

// recordWriterTiming aggregates a single sample under component and
// emits [db-slow-writer] if hold_ms > configured threshold (default
// 500ms). queryForLog is truncated to 200 chars.
func recordWriterTiming(component string, wait, hold time.Duration, queryForLog string) {
	waitMs := float64(wait.Nanoseconds()) / 1e6
	holdMs := float64(hold.Nanoseconds()) / 1e6
	writerStatsAgg.get(component).record(waitMs, holdMs)
	if holdMs > getSlowWriterThresholdMs() {
		q := queryForLog
		if len(q) > 200 {
			q = q[:200]
		}
		log.Printf("[db-slow-writer] component=%s duration=%.1fms query=%s", component, holdMs, q)
	}
}

// writerMu serialises every wrapped writer call so the wait the next
// caller sees is the wait the perf snapshot can attribute. The
// SQLite driver also enforces serial writes (SetMaxOpenConns(1)),
// but the wait inside the driver is invisible to Go — writerMu makes
// it Go-visible.
var writerMu sync.Mutex

// WriterExec wraps s.db.Exec with per-component wait/hold/contention
// instrumentation (issue #1340).
func (s *Store) WriterExec(component, query string, args ...interface{}) (sql.Result, error) {
	waitStart := time.Now()
	writerMu.Lock()
	wait := time.Since(waitStart)
	holdStart := time.Now()
	res, err := s.db.Exec(query, args...)
	hold := time.Since(holdStart)
	writerMu.Unlock()
	recordWriterTiming(component, wait, hold, query)
	return res, err
}

// WriterTx wraps Begin → fn → Commit under component tagging.
// hold_ms covers the whole tx so a slow body counts against its owner.
func (s *Store) WriterTx(component string, fn func(*sql.Tx) error) error {
	waitStart := time.Now()
	writerMu.Lock()
	wait := time.Since(waitStart)
	holdStart := time.Now()
	tx, err := s.db.Begin()
	if err != nil {
		hold := time.Since(holdStart)
		writerMu.Unlock()
		recordWriterTiming(component, wait, hold, "BEGIN")
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		hold := time.Since(holdStart)
		writerMu.Unlock()
		recordWriterTiming(component, wait, hold, "tx-body")
		return err
	}
	err = tx.Commit()
	hold := time.Since(holdStart)
	writerMu.Unlock()
	recordWriterTiming(component, wait, hold, "COMMIT")
	return err
}

// Wrap helpers below tag existing call sites with the canonical
// component names so the call sites read naturally. These keep the
// instrumentation out of the hot-path business logic.

// instrumentedExec is the package-internal pass-through used by call
// sites already inside db.go (PruneOldMetrics, RemoveStaleObservers,
// vacuum). Equivalent to WriterExec, kept short for readability.
func (s *Store) instrumentedExec(component, query string, args ...interface{}) (sql.Result, error) {
	return s.WriterExec(component, query, args...)
}

// epochSecondsForLastSeen parses an RFC3339 timestamp to a unix-second
// value for the transmissions.last_seen denormalized column (#1690).
// Falls back to the current time on parse failure so the column is
// never seeded with 0 for a brand-new row.
func epochSecondsForLastSeen(rfc3339 string) int64 {
	if t, err := time.Parse(time.RFC3339, rfc3339); err == nil {
		return t.Unix()
	}
	return time.Now().UTC().Unix()
}
