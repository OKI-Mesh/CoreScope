// internal/database/observers.go
package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

type ObserverInput struct {
	ID        string
	Name      string
	IATA      string
	Meta      *types.ObserverMeta
	LastSeen  time.Time // envelope/receive time
	IngestNow time.Time // wall clock at ingest; caps how far last_seen may advance
}

type ObserverMetadataInput struct {
	ID   string
	Name string
	IATA string
	Meta *types.ObserverMeta
}

// Observer mirrors the observers table. No JSON tags: the server owns the wire shape.
type Observer struct {
	ID                int64
	Pubkey            string
	Name              *string
	IATA              *string
	LastSeen          time.Time
	FirstSeen         time.Time
	PacketCount       int64
	Model             *string
	Firmware          *string
	ClientVersion     *string
	Radio             *string
	BatteryMv         *int
	UptimeSecs        *int
	NoiseFloor        *float64
	LastPacketAt      *time.Time
	ClockSkewSeconds  *int64
	ClockSkewCount24h int
	ClockLastNaiveAt  *time.Time
	CanRelay          bool
	CanRelaySeen      bool
}

// ObserverRelayState partitions active observers by what they have reported about
// repeating (#1290, #1624). Observers that never reported are in neither list.
type ObserverRelayState struct {
	Seen     []string // reported a repeat value (badge shown)
	NonRelay []string // subset of Seen that advertised repeat:off
}

// ResolveObserverAndTouch looks up the observer's internal id by its
// real-world identifier (pubkey/station id) and, if found, touches its
// last_seen/last_packet_at to ingestNow. Returns nil if the observer is
// unknown or empty — callers treat that as "no observer resolved" and
// proceed without failing the surrounding insert.
//
// observer.last_seen and last_packet_at answer "when did the analyzer
// last hear from this observer" — both are ingest-time questions.
// Per-packet rxTime is stored separately on observations/transmissions
// using envelope time. See #1465.
func (db *DB) ResolveObserverAndTouch(ctx context.Context, observerID string, ingestNow time.Time) *int64 {
	if observerID == "" {
		return nil
	}
	id, err := db.q.GetObserverID(ctx, observerID)
	if err != nil {
		return nil
	}
	if err := db.q.TouchObserverLastSeen(ctx, pgdb.TouchObserverLastSeenParams{
		LastSeen: ingestNow,
		ID:       id,
	}); err != nil {
		log.Printf("[db] touch observer last_seen (non-fatal): %v", err)
	}
	return &id
}

func (db *DB) CreateOrUpdateObserver(ctx context.Context, o ObserverInput) (int64, error) {
	model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay := metaToParams(o.Meta)
	o.ID = strings.ToLower(strings.TrimSpace(o.ID))
	if o.ID == "" {
		return 0, errors.New("observer ID is empty")
	}

	id, err := db.q.CreateOrUpdateObserver(ctx, pgdb.CreateOrUpdateObserverParams{
		Pubkey:        o.ID,
		Name:          toPgTextIfNotEmpty(o.Name),
		Iata:          toPgTextIfNotEmpty(o.IATA),
		LastSeen:      o.LastSeen,
		IngestNow:     o.IngestNow,
		Model:         model,
		Firmware:      firmware,
		ClientVersion: clientVersion,
		Radio:         radio,
		BatteryMv:     batteryMv,
		UptimeSecs:    uptimeSecs,
		NoiseFloor:    noiseFloor,
		CanRelay:      canRelay,
	})
	if err != nil {
		return 0, fmt.Errorf("create or update observer %s: %w", o.ID, err)
	}
	return id, nil
}

// ReactivateObserver clears the inactive flag on an observer that was
// previously soft-deleted (e.g. by RemoveStaleObservers). A no-op if the
// observer is already active or does not exist.
func (db *DB) ReactivateObserver(ctx context.Context, pubkey string) error {
	return db.q.ReactivateObserver(ctx, pubkey)
}

// UpdateObserverMetadataIfRetained applies metadata from a RETAINED status
// message to an existing observer row. It never inserts, never advances
// last_seen, and never clears inactive: a retained message is not a sign
// of life.
func (db *DB) UpdateObserverMetadataIfRetained(ctx context.Context, in ObserverMetadataInput) error {
	model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay := metaToParams(in.Meta)

	return db.q.UpdateObserverMetadataIfRetained(ctx, pgdb.UpdateObserverMetadataIfRetainedParams{
		Pubkey:        in.ID,
		Name:          toPgTextIfNotEmpty(in.Name),
		Iata:          toPgTextIfNotEmpty(in.IATA),
		Model:         model,
		Firmware:      firmware,
		ClientVersion: clientVersion,
		Radio:         radio,
		BatteryMv:     batteryMv,
		UptimeSecs:    uptimeSecs,
		NoiseFloor:    noiseFloor,
		CanRelay:      canRelay,
	})
}

func (db *DB) PurgeStaleObservers(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PurgeStaleObservers(ctx, cutoff)
}

func (db *DB) MarkStaleObserversInactive(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.MarkStaleObserversInactive(ctx, cutoff)
}

func (db *DB) DeleteMetricsForInactiveObservers(ctx context.Context) (int64, error) {
	return db.q.DeleteMetricsForInactiveObservers(ctx)
}

func (db *DB) SoftDeleteBlacklistedObservers(ctx context.Context, blacklist []string) (int64, error) {
	keys := make([]string, 0, len(blacklist))
	for _, pk := range blacklist {
		if t := strings.ToLower(strings.TrimSpace(pk)); t != "" {
			keys = append(keys, t)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return db.q.SoftDeleteBlacklistedObservers(ctx, keys)
}

func (db *DB) GetObserverID(ctx context.Context, pubkey string) (int64, error) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey)) // same normalization as the writer
	id, err := db.q.GetObserverID(ctx, pubkey)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get observer id %q: %w", pubkey, err)
	}
	return id, nil
}

// GetObserverPacketCounts returns the number of observations per observer
// pubkey with a timestamp after since. Observers with no observations in the
// window are not in the map.
func (db *DB) GetObserverPacketCounts(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := db.q.CountObservationsByObserverSince(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("count observations by observer: %w", err)
	}
	counts := make(map[string]int, len(rows))
	for _, r := range rows {
		counts[r.Pubkey] = int(r.Cnt)
	}
	return counts, nil
}

func (d *DB) GetObservers(ctx context.Context) ([]Observer, error) {
	rows, err := d.q.ListActiveObservers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active observers: %w", err)
	}
	out := make([]Observer, len(rows))
	for i, r := range rows {
		out[i] = observerFromRow(r)
	}
	return out, nil
}

func observerFromRow(r pgdb.Observer) Observer {
	o := Observer{
		ID:                r.ID,
		Pubkey:            r.Pubkey,
		Name:              fromPgText(r.Name),
		IATA:              fromPgText(r.Iata),
		LastSeen:          r.LastSeen.UTC(),
		FirstSeen:         r.FirstSeen.UTC(),
		PacketCount:       r.PacketCount,
		Model:             fromPgText(r.Model),
		Firmware:          fromPgText(r.Firmware),
		ClientVersion:     fromPgText(r.ClientVersion),
		Radio:             fromPgText(r.Radio),
		NoiseFloor:        fromPgFloat8(r.NoiseFloor),
		LastPacketAt:      utcPtr(fromPgTimestamptz(r.LastPacketAt)),
		ClockSkewCount24h: int(r.ClockSkewCount24h),
		ClockLastNaiveAt:  utcPtr(fromPgTimestamptz(r.ClockLastNaiveAt)),
		CanRelay:          r.CanRelay,
		CanRelaySeen:      r.CanRelaySeen,
	}
	if r.BatteryMv.Valid {
		v := int(r.BatteryMv.Int32)
		o.BatteryMv = &v
	}
	if r.UptimeSecs.Valid {
		v := int(r.UptimeSecs.Int64)
		o.UptimeSecs = &v
	}
	if r.ClockSkewSeconds.Valid {
		v := int64(r.ClockSkewSeconds.Int32)
		o.ClockSkewSeconds = &v
	}
	return o
}

func (d *DB) GetObserverRelayState(ctx context.Context) (ObserverRelayState, error) {
	rows, err := d.q.ListReportedRelayObservers(ctx)
	if err != nil {
		return ObserverRelayState{}, fmt.Errorf("list reported relay observers: %w", err)
	}
	var st ObserverRelayState
	for _, r := range rows {
		st.Seen = append(st.Seen, r.Pubkey)
		if !r.CanRelay {
			st.NonRelay = append(st.NonRelay, r.Pubkey)
		}
	}
	return st, nil
}

// GetObserverByID returns a single observer by pubkey, active or not. A missing observer
// is reported as ErrNotFound.
func (d *DB) GetObserverByID(ctx context.Context, pubkey string) (*Observer, error) {
	r, err := d.q.GetObserverByPubkey(ctx, pubkey)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("get observer %q: %w", pubkey, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get observer %q: %w", pubkey, err)
	}
	o := observerFromRow(r)
	return &o, nil
}

// GetObserverIdsForRegion returns the pubkeys of observers whose IATA code matches any
// of the codes in regionParam (a comma-separated list, normalized by normalizeRegionCodes).
// An empty or blank region returns nil.
func (d *DB) GetObserverIdsForRegion(ctx context.Context, regionParam string) ([]string, error) {
	codes := NormalizeRegionCodes(regionParam)
	if len(codes) == 0 {
		return nil, nil
	}
	ids, err := d.q.ListObserverPubkeysByIATA(ctx, codes)
	if err != nil {
		return nil, fmt.Errorf("list observers for region %q: %w", regionParam, err)
	}
	return ids, nil
}

// normalizeRegionCodes parses a region query parameter into a list of upper-case
// IATA codes. Returns nil to signal "no filter" (match all regions).
//
// Sentinel handling (issue #770): the frontend region filter dropdown labels its
// catch-all option "All". When that option is selected the UI may send
// ?region=All; older code interpreted that literally and tried to match an
// IATA code "ALL", which never exists, returning an empty result set. Treat
// "All" / "ALL" / "all" (case-insensitive, optionally surrounded by whitespace
// or mixed with empty CSV slots) as equivalent to an empty value.
//
// Real IATA codes (e.g. "SJC", "PDX") still pass through unchanged.
func NormalizeRegionCodes(regionParam string) []string {
	if regionParam == "" {
		return nil
	}
	tokens := strings.Split(regionParam, ",")
	codes := make([]string, 0, len(tokens))
	for _, token := range tokens {
		code := strings.TrimSpace(strings.ToUpper(token))
		if code == "" || code == "ALL" {
			continue
		}
		codes = append(codes, code)
	}
	if len(codes) == 0 {
		return nil
	}
	return codes
}

// GetDistinctIATAs returns the distinct, non-NULL IATA codes across all observers, sorted.
func (d *DB) GetDistinctIATAs(ctx context.Context) ([]string, error) {
	codes, err := d.q.ListDistinctObserverIATAs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list distinct observer iatas: %w", err)
	}
	return codes, nil
}

// ObserverName is an observer's pubkey and display name ("" when unnamed).
type ObserverName struct {
	Pubkey string
	Name   string
}

// ListObserverNames returns every observer, including inactive ones.
func (db *DB) ListObserverNames(ctx context.Context) ([]ObserverName, error) {
	rows, err := db.q.ListObserverNames(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ObserverName, 0, len(rows))
	for _, r := range rows {
		out = append(out, ObserverName{Pubkey: r.Pubkey, Name: r.Name})
	}
	return out, nil
}

// NodeBatterySample is a single (timestamp, battery_mv) point.
type NodeBatterySample struct {
	Timestamp time.Time // UTC
	BatteryMv int
}

// GetNodeBatteryHistory returns time-ordered battery_mv samples for a node whose
// key is also an observer, from observer_metrics at or after since. Rows with a
// NULL battery are skipped. Public keys are stored lowercase, so the match is exact.
func (db *DB) GetNodeBatteryHistory(ctx context.Context, pubkey string, since time.Time) ([]NodeBatterySample, error) {
	if pubkey == "" {
		return nil, nil
	}
	rows, err := db.q.ListNodeBatteryHistory(ctx, pgdb.ListNodeBatteryHistoryParams{
		Pubkey: strings.ToLower(pubkey),
		Since:  since,
	})
	if err != nil {
		return nil, err
	}
	out := make([]NodeBatterySample, 0, len(rows))
	for _, r := range rows {
		out = append(out, NodeBatterySample{Timestamp: r.Timestamp.UTC(), BatteryMv: int(r.BatteryMv)})
	}
	return out, nil
}

// GetNonRelayObserverPubkeys returns the pubkeys of active observers that
// advertised `repeat:off` (#1290).
func (db *DB) GetNonRelayObserverPubkeys(ctx context.Context) ([]string, error) {
	return db.q.ListNonRelayObserverPubkeys(ctx)
}

func (db *DB) MarkObserverInactive(ctx context.Context, pubkey string) error {
	err := db.q.MarkObserverInactive(ctx, strings.ToLower(pubkey))
	if err != nil {
		return err
	}
	return nil
}

// ReactivateObserver clears the inactive flag on an observer that was
// previously soft-deleted (e.g. by RemoveStaleObservers). A no-op if the
// observer is already active or does not exist.
func (db *DB) GetObserverLastSeen(ctx context.Context, pubkey string) (time.Time, error) {
	return db.q.GetObserverLastSeen(ctx, pubkey)
}
