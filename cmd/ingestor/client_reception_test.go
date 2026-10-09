package main

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

// TestPruneOldClientReceptions verifies the retention reaper bounds the coverage
// tables: rows older than the window (and stale companion names) are deleted,
// recent ones kept, and days=0 disables it.
func TestPruneOldClientReceptions(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC()
	recent := now.AddDate(0, 0, -1)
	old := now.AddDate(0, 0, -40)
	const companion2 = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3"

	if _, err := s.InsertClientReception(ctx, &types.ClientReception{
		RxPubkey: testCompanionPK, HeardKey: "aabbcc", HeardKeyLen: 3,
		Lat: 51, Lon: 3.7, RxAt: recent, IngestedAt: recent, Src: "rxlog",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertClientReception(ctx, &types.ClientReception{
		RxPubkey: testCompanionPK, HeardKey: "aabbcc", HeardKeyLen: 3,
		Lat: 51, Lon: 3.7, RxAt: old, IngestedAt: old, Src: "rxlog",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertClientObserver(ctx, testCompanionPK, "Fresh", recent); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertClientObserver(ctx, companion2, "Stale", old); err != nil {
		t.Fatal(err)
	}
	type obsRow struct {
		PK   string
		Name string
		Seen time.Time
	}
	rows := testCollect(t, s, `SELECT pubkey, name, last_seen FROM client_observers ORDER BY pubkey`,
		func(r pgx.CollectableRow) (obsRow, error) {
			var x obsRow
			err := r.Scan(&x.PK, &x.Name, &x.Seen)
			return x, err
		})
	t.Logf("client_observers before prune: %+v", rows)
	if n, _ := s.PruneOldClientReceptions(ctx, 0); n != 0 {
		t.Fatalf("days=0 must be a no-op, got %d", n)
	}
	n, err := s.PruneOldClientReceptions(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 old reception pruned, got %d", n)
	}
	recN := countRows(t, s, "client_receptions")
	obsN := countRows(t, s, "client_observers")
	if recN != 1 {
		t.Fatalf("expected 1 reception remaining (recent), got %d", recN)
	}
	if obsN != 1 {
		t.Fatalf("expected 1 observer remaining (fresh), got %d", obsN)
	}
}

// TestPruneClientRxObservationsUsesIndex verifies the diagnostic observations
// prune can seek idx_cro_prune instead of scanning the whole table. It guards
// against the index being dropped or no longer matching the filter column.
func TestPruneClientRxObservationsUsesIndex(t *testing.T) {
	s := newTestStore(t)
	plan := explainPlan(t, s,
		`DELETE FROM client_rx_observations WHERE rx_at < '2026-01-01T00:00:00Z'::timestamptz`)
	if !strings.Contains(plan, "idx_cro_prune") {
		t.Fatalf("retention DELETE should be able to use idx_cro_prune, plan was:\n%s", plan)
	}
}

// TestPruneOldClientRxObservations verifies the diagnostic observations prune
// deletes rows older than the window and keeps recent ones, and that days=0
// disables it, mirroring TestPruneOldClientReceptions for the sibling table.
// It also pins the cutoff boundary: a row 500ms after the cutoff instant is
// newer, so it must survive.
func rxObsFixture(at time.Time, hash string) *types.ClientRxObservation {
	return &types.ClientRxObservation{
		RxPubkey: testCompanionPK, RxAt: at, IngestedAt: at, PktHash: hash,
		RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT,
		HashSize: 1, HopCount: 0, Lat: 51.0, Lon: 3.7, // every optional field nil
	}
}

func TestPruneOldClientRxObservations(t *testing.T) {
	ctx := testCtx(t)
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	for _, o := range []*types.ClientRxObservation{
		rxObsFixture(now.AddDate(0, 0, -1), "hash-recent"),
		rxObsFixture(now.AddDate(0, 0, -40), "hash-old"),
	} {
		if _, err := s.InsertClientRxObservation(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := s.PruneOldClientRxObservations(ctx, 0); err != nil || n != 0 {
		t.Fatalf("days=0 must be a no-op: n=%d err=%v", n, err)
	}
	// Reporting 0 isn't enough: a bug could delete and still say so.
	if got := countRows(t, s, "client_rx_observations"); got != 2 {
		t.Fatalf("days=0 deleted rows: %d remain, want 2", got)
	}

	n, err := s.PruneOldClientRxObservations(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 old observation pruned, got %d", n)
	}
	if got := countWhere(t, s, "client_rx_observations", "pkt_hash = $1", "hash-recent"); got != 1 {
		t.Errorf("the recent row must survive the prune; got %d", got)
	}
	if got := countWhere(t, s, "client_rx_observations", "pkt_hash = $1", "hash-old"); got != 0 {
		t.Errorf("the old row must be deleted; got %d", got)
	}
}

// TestPruneClientRxObservationsCutoffBoundary pins which side of the cutoff
// survives, against a fixed instant so it cannot race the clock. The cutoff has
// a non-zero millisecond part on purpose.
func TestPruneClientRxObservationsCutoffBoundary(t *testing.T) {
	ctx := testCtx(t)
	s := newTestStore(t)
	cutoff := time.Date(2026, 8, 4, 10, 0, 0, 400*int(time.Millisecond), time.UTC)

	for _, o := range []*types.ClientRxObservation{
		rxObsFixture(cutoff.Add(-time.Millisecond), "hash-before"),
		rxObsFixture(cutoff.Add(time.Millisecond), "hash-boundary"),
	} {
		if _, err := s.InsertClientRxObservation(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	n, err := s.pruneOldClientRxObservationsAt(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (only the row a millisecond before the cutoff)", n)
	}
	if got := countWhere(t, s, "client_rx_observations", "pkt_hash = $1", "hash-boundary"); got != 1 {
		t.Errorf("the row a millisecond after the cutoff must survive; got %d", got)
	}
	if got := countWhere(t, s, "client_rx_observations", "pkt_hash = $1", "hash-before"); got != 0 {
		t.Errorf("the row a millisecond before the cutoff must be deleted; got %d", got)
	}
}

func TestClientReceptionsTableExists(t *testing.T) {
	s := newTestStore(t)
	cols := tableColumns(t, s, "client_receptions")
	for _, want := range []string{"id", "rx_pubkey", "heard_key", "heard_keylen", "rssi", "snr", "lat", "lon", "pos_acc_m", "rx_at", "ingested_at", "src"} {
		if _, ok := cols[want]; !ok {
			t.Errorf("missing column %q in client_receptions", want)
		}
	}
}

func crF(f float64) *float64 { return &f }
func crI(i int) *int         { return &i }

// TestClientReceptionsCoverageQueryUsesIndex verifies #5/#18: the dominant
// per-node coverage query (sargable heard_key IN-list + bbox, mirroring
// cmd/server coverageHeardKeyCandidates) can use a client_receptions index
// instead of scanning the table. Without idx_client_recept_heard_geo the plan
// is a Seq Scan even with sequential scans disabled.
func TestClientReceptionsCoverageQueryUsesIndex(t *testing.T) {
	s := newTestStore(t)
	plan := explainPlan(t, s, `SELECT lat, lon, snr, rssi, heard_key, rx_at
		FROM client_receptions
		WHERE heard_key IN ('aabbccddeeff00112233', 'aabbcc', 'aabb')
		  AND lat BETWEEN 50.0 AND 52.0 AND lon BETWEEN 3.0 AND 4.0`)
	if !strings.Contains(plan, "idx_client_recept") {
		t.Fatalf("coverage query should be able to use a client_recept index, plan was:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on client_receptions") {
		t.Fatalf("coverage query should not need a full scan, plan was:\n%s", plan)
	}
}

// TestClientReceptionsRetentionUsesRxAtIndex verifies the retention prune's
// DELETE ... WHERE rx_at < ? (and the leaderboard's rx_at window) can seek
// idx_client_recept_rxat instead of scanning the table.
func TestClientReceptionsRetentionUsesRxAtIndex(t *testing.T) {
	s := newTestStore(t)
	plan := explainPlan(t, s,
		`DELETE FROM client_receptions WHERE rx_at < '2026-01-01T00:00:00Z'::timestamptz`)
	if !strings.Contains(plan, "idx_client_recept_rxat") {
		t.Fatalf("retention DELETE should be able to use idx_client_recept_rxat, plan was:\n%s", plan)
	}
}

func TestDeriveHeardKey(t *testing.T) {
	full := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	k, l, src, ok := deriveHeardKey("rx", types.RouteFlood, types.PayloadADVERT, nil, strings.ToUpper(full), true)
	if !ok || l != 32 || src != "advert" || k != full {
		t.Fatalf("0-hop advert: got k=%q l=%d src=%q ok=%v", k, l, src, ok)
	}
	k, l, src, ok = deriveHeardKey("rx", types.RouteFlood, types.PayloadGRP_TXT, []string{"aa", "bbccdd"}, "", false)
	if !ok || k != "bbccdd" || l != 3 || src != "rxlog" {
		t.Fatalf("flood path: got k=%q l=%d src=%q ok=%v", k, l, src, ok)
	}
	// DIRECT route: path[last] is the route's far end, not the transmitter — must be rejected.
	if _, _, _, ok = deriveHeardKey("rx", types.RouteDirect, types.PayloadGRP_TXT, []string{"aa", "bbccdd"}, "", false); ok {
		t.Fatalf("direct-route path must be rejected")
	}
	if _, _, _, ok = deriveHeardKey("rx", types.RouteTransportDirect, types.PayloadGRP_TXT, []string{"aa", "bbccdd"}, "", false); ok {
		t.Fatalf("transport-direct-route path must be rejected")
	}
	if _, _, _, ok = deriveHeardKey("rx", types.RouteFlood, types.PayloadGRP_TXT, []string{"aa", "bb"}, "", false); ok {
		t.Fatalf("1-byte last hop should be rejected")
	}
	if _, _, _, ok = deriveHeardKey("tx", types.RouteFlood, types.PayloadGRP_TXT, []string{"aabbcc"}, "", false); ok {
		t.Fatalf("tx must be rejected")
	}
	if _, _, _, ok = deriveHeardKey("rx", types.RouteFlood, types.PayloadGRP_TXT, nil, "", false); ok {
		t.Fatalf("no hops + non-advert must be rejected")
	}
	// TRACE repurposes the header path bytes as per-hop SNR values, not node
	// hashes — a FLOOD-routed TRACE must never be attributable, even though the
	// route type and hop shape are otherwise identical to the accepted case above.
	if _, _, _, ok = deriveHeardKey("rx", types.RouteFlood, types.PayloadTRACE, []string{"aa", "bbccdd"}, "", false); ok {
		t.Fatalf("FLOOD-routed TRACE must be rejected (path bytes are SNR values, not node hashes)")
	}
}

func TestBuildClientReception(t *testing.T) {
	acc := 8.0
	rxAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	ingestedAt := rxAt.Add(time.Second)

	rec, ok := buildClientReception("companionpk", "rx", types.RouteFlood, types.PayloadGRP_TXT, []string{"aa", "bbccdd"}, "", false,
		crF(-7.5), crI(-92), 51.05, 3.72, &acc, rxAt, ingestedAt)
	if !ok || rec.HeardKey != "bbccdd" || rec.HeardKeyLen != 3 || rec.Src != "rxlog" {
		t.Fatalf("bad reception: %+v ok=%v", rec, ok)
	}
	if !rec.RxAt.Equal(rxAt) {
		t.Errorf("RxAt = %v, want %v", rec.RxAt, rxAt)
	}
	if _, ok := buildClientReception("c", "rx", types.RouteDirect, types.PayloadGRP_TXT, []string{"bbccdd"}, "", false,
		crF(-7.5), crI(-92), 51.05, 3.72, nil, rxAt, ingestedAt); ok {
		t.Fatal("direct-route path must be rejected (not the transmitter)")
	}
	if _, ok := buildClientReception("c", "rx", types.RouteFlood, types.PayloadGRP_TXT, []string{"bbccdd"}, "", false,
		nil, nil, 99.0, 3.72, nil, rxAt, ingestedAt); ok {
		t.Fatal("out-of-range lat must be rejected")
	}
	if _, ok := buildClientReception("c", "rx", types.RouteFlood, types.PayloadTRACE, []string{"aa", "bbccdd"}, "", false,
		crF(-7.5), crI(-92), 51.05, 3.72, nil, rxAt, ingestedAt); ok {
		t.Fatal("FLOOD-routed TRACE must be rejected (path bytes are SNR values, not node hashes)")
	}
	if _, ok := buildClientReception("c", "rx", types.RouteFlood, types.PayloadGRP_TXT, []string{"aa", "bbccdd"}, "", false,
		crF(-7.5), crI(-92), 51.05, 3.72, nil, time.Time{}, time.Time{}); ok {
		t.Fatal("zero rx time must be rejected")
	}
}

func TestInsertClientReceptionRoundTripAndIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	rec := types.ClientReception{
		RxPubkey: "companionpk", HeardKey: "bbccdd", HeardKeyLen: 3, RSSI: crI(-92),
		Lat: 51.05, Lon: 3.72,
		RxAt:       time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		IngestedAt: time.Date(2026, 6, 9, 12, 0, 1, 0, time.UTC),
		Src:        "rxlog",
	}
	if ins, err := s.InsertClientReception(ctx, &rec); err != nil || !ins {
		t.Fatalf("first insert: ins=%v err=%v", ins, err)
	}
	if ins, err := s.InsertClientReception(ctx, &rec); err != nil || ins {
		t.Fatalf("second insert should be a no-op: ins=%v err=%v", ins, err)
	}
	if n := countRows(t, s, "client_receptions"); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
}

func TestHandleClientPacketRelayedAdvertWritesReception(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	advertHex := "11451000D818206D3AAC152C8A91F89957E6D30CA51F36E28790228971C473B755F244F718754CF5EE4A2FD58D944466E42CDED140C66D0CC590183E32BAF40F112BE8F3F2BDF6012B4B2793C52F1D36F69EE054D9A05593286F78453E56C0EC4A3EB95DDA2A7543FCCC00B939CACC009278603902FC12BCF84B706120526F6F6620536F6C6172" // unchanged
	msg := map[string]interface{}{
		"raw":       advertHex,
		"direction": "rx",
		"timestamp": recentTS(time.Minute),
		"origin":    "MyMob",
		"SNR":       -7.0,
		"RSSI":      -92.0,
		"gps":       map[string]interface{}{"lat": 51.05, "lon": 3.72, "acc_m": 8.0},
	}
	handleClientPacket(ctx, s, &Config{}, "test", testCompanionPK, msg, nil, nil)

	var obsName string
	mustScan(t, testRow(t, s, `SELECT name FROM client_observers WHERE pubkey = $1`, testCompanionPK), &obsName)
	if obsName != "MyMob" {
		t.Fatalf("expected client_observers name 'MyMob', got %q", obsName)
	}

	// relayed advert (non-empty path): by the capture HARD RULE we record the
	// directly-heard LAST hop (multibyte), not the originator. The 0-hop
	// advert-to-full-pubkey branch is covered by TestDeriveHeardKey.
	var n, keylen int
	var src string
	mustScan(t, testRow(t, s,
		`SELECT COUNT(*), COALESCE(MAX(heard_keylen), 0), COALESCE(MAX(src), '')
		 FROM client_receptions WHERE rx_pubkey = $1`, testCompanionPK), &n, &keylen, &src)
	if n != 1 || keylen < 2 || src != "rxlog" {
		t.Fatalf("expected 1 rxlog reception (multibyte last hop), got n=%d keylen=%d src=%q", n, keylen, src)
	}

	// No GPS: no row.
	const companion2 = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3"
	handleClientPacket(ctx, s, &Config{}, "test", companion2,
		map[string]interface{}{"raw": advertHex, "direction": "rx"}, nil, nil)
	if n2 := countWhere(t, s, "client_receptions", "rx_pubkey = $1", companion2); n2 != 0 {
		t.Fatalf("packet without gps must be dropped, got %d rows", n2)
	}
}

// TestHandleClientPacketZeroHopAdvertWritesReception covers the #9 gap: the
// advert fixture used above is a RELAYED advert (non-empty path), so it exercises
// the rxlog last-hop branch, not the 0-hop src='advert' branch. Here we rebuild
// the same advert with zero hops — header (FLOOD ADVERT) + "00" (0 hops) + the
// same advert payload — so handleClientPacket stores the advertiser by its full
// pubkey with src='advert', and we assert gps/snr were captured too.
func TestHandleClientPacketZeroHopAdvertWritesReception(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	relayed := "11451000D818206D3AAC152C8A91F89957E6D30CA51F36E28790228971C473B755F244F718754CF5EE4A2FD58D944466E42CDED140C66D0CC590183E32BAF40F112BE8F3F2BDF6012B4B2793C52F1D36F69EE054D9A05593286F78453E56C0EC4A3EB95DDA2A7543FCCC00B939CACC009278603902FC12BCF84B706120526F6F6620536F6C6172"
	// relayed = header(2) + path-descriptor(2) + 5*2-byte hops(20) + payload.
	payload := relayed[24:]
	zeroHop := "1100" + payload
	advertPubkey := strings.ToLower(payload[:64])

	msg := map[string]interface{}{
		"raw": zeroHop, "direction": "rx", "timestamp": recentTS(time.Minute),
		"origin": "MyMob", "SNR": -7.0, "RSSI": -92.0,
		"gps": map[string]interface{}{"lat": 51.05, "lon": 3.72, "acc_m": 8.0},
	}
	handleClientPacket(ctx, s, &Config{}, "test", testCompanionPK, msg, nil, nil)

	var heardKey, src string
	var keylen int32
	var snr *float64
	var lat, lon float64
	mustScan(t, testRow(t, s,
		`SELECT heard_key, heard_keylen, src, snr, lat, lon FROM client_receptions WHERE rx_pubkey = $1`,
		testCompanionPK), &heardKey, &keylen, &src, &snr, &lat, &lon)

	if src != "advert" || keylen != 32 || heardKey != advertPubkey {
		t.Fatalf("0-hop advert: want advert/32/%s, got %s/%d/%s", advertPubkey, src, keylen, heardKey)
	}
	if snr == nil || *snr != -7 || lat != 51.05 || lon != 3.72 {
		t.Fatalf("gps/snr not captured: snr=%v lat=%f lon=%f", snr, lat, lon)
	}
}

func cfgWithObservations() *Config {
	return &Config{ClientRxObservations: &ClientRxObservationsConfig{Enabled: true}}
}

// TestClientPacketWritesObservationNotCoverage verifies a DIRECT-route packet
// (not attributable per deriveHeardKey) still produces one diagnostic
// observation row while writing ZERO coverage rows — the two paths are
// independent.
func TestClientPacketWritesObservationNotCoverage(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	// header 0x16 = route_type 2 (DIRECT), payload_type 5 (GRP_TXT). DIRECT
	// carries no transport codes. path byte 0x41 = hash_size 2, hop_count 1,
	// then the 2-byte hop, then the payload.
	// deriveHeardKey refuses a DIRECT route, so this must produce an
	// observation row and ZERO coverage rows.
	raw := "16" + "41" + "1a2b" + strings.Repeat("33", 16)
	msg := map[string]interface{}{
		"raw": raw, "direction": "rx", "SNR": 4.5, "RSSI": -101.0,
		"timestamp": "2026-08-17T10:00:00.123Z",
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4, "acc_m": 8.0},
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", msg, nil, nil)

	obs := countRows(t, s, "client_rx_observations")
	cov := countRows(t, s, "client_receptions")
	if obs != 1 {
		t.Errorf("observations = %d, want 1", obs)
	}
	if cov != 0 {
		t.Errorf("coverage rows = %d, want 0 — the invariant must hold", cov)
	}

	var hash string
	var hopCount, hashSize int32
	var fwd *string
	mustScan(t, testRow(t, s,
		`SELECT pkt_hash, hop_count, hash_size, forwarder
		 FROM client_rx_observations WHERE rx_pubkey = $1`, "aa11"),
		&hash, &hopCount, &hashSize, &fwd)
	if hash != decoder.ComputeContentHash(raw) {
		t.Errorf("pkt_hash = %q, want ComputeContentHash value %q", hash, decoder.ComputeContentHash(raw))
	}
	if hopCount != 1 || hashSize != 2 {
		t.Errorf("hop_count/hash_size = %d/%d, want 1/2", hopCount, hashSize)
	}
	// path[last] on a DIRECT route is the route's far end, not who transmitted
	// (deriveHeardKey's "path[last] is flood-only" rule) — a production bug
	// once got this wrong, so pin it here: forwarder must stay NULL, not the
	// path's last hop.
	if fwd != nil {
		t.Errorf("forwarder = %q, want NULL for a DIRECT route (path[last] is flood-only)", *fwd)
	}
}

// TestClientPacketFloodWritesBoth verifies a FLOOD-route packet (attributable)
// writes both an observation row AND a coverage row, and that the observation's
// forwarder is path[last] — the immediate RF transmitter.
func TestClientPacketFloodWritesBoth(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	// header 0x15 = route_type 1 (FLOOD), payload_type 5. 2-byte hash, one hop
	// → path[last] is the transmitter, so this IS attributable.
	raw := "15" + "41" + "1a2b" + strings.Repeat("33", 16)
	msg := map[string]interface{}{
		"raw": raw, "direction": "rx", "SNR": 4.5, "RSSI": -101.0,
		"timestamp": recentTS(time.Minute),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", msg, nil, nil)

	obs := countRows(t, s, "client_rx_observations")
	cov := countRows(t, s, "client_receptions")
	if obs != 1 || cov != 1 {
		t.Errorf("observations/coverage = %d/%d, want 1/1", obs, cov)
	}

	var fwd, pathJSON *string
	mustScan(t, testRow(t, s,
		`SELECT forwarder, path_json FROM client_rx_observations WHERE rx_pubkey = $1`, "aa11"),
		&fwd, &pathJSON)
	if fwd == nil || *fwd != "1a2b" {
		t.Errorf("forwarder = %v, want 1a2b", fwd)
	}
	// The decoder emits hops uppercase; path_json must be lowercased to match
	// forwarder (and every other identifier in this table), or a join between
	// the two silently returns zero rows instead of erroring.
	if pathJSON == nil || *pathJSON != `["1a2b"]` {
		t.Errorf("path_json = %v, want [\"1a2b\"] (lowercase, matching forwarder's case)", pathJSON)
	}
}

// TestClientObservationsGateOff verifies that with clientRxObservations
// disabled, a client packet writes zero observation rows.
func TestClientObservationsGateOff(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	raw := "16" + "41" + "1a2b" + strings.Repeat("33", 16)
	msg := map[string]interface{}{
		"raw": raw, "direction": "rx",
		"timestamp": recentTS(time.Minute),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
	}
	handleClientPacket(ctx, s, &Config{}, "test", "aa11", msg, nil, nil)

	if n := countRows(t, s, "client_rx_observations"); n != 0 {
		t.Errorf("rows = %d, want 0 when the gate is off", n)
	}
}

// TestClientObservationsTxDirectionSkipped verifies a companion's own
// outgoing transmission (direction "tx") writes zero observation rows, even
// though this FLOOD packet would otherwise be attributable (as
// TestClientPacketFloodWritesBoth proves for the same shape with "rx"). The
// observations gate sits above deriveHeardKey's own "rx" check, which runs
// only on the coverage path — without a matching check here, a companion's
// own transmissions would be recorded as observed RF and inflate the table's
// headline per-flood forwarder-multiplicity signal.
func TestClientObservationsTxDirectionSkipped(t *testing.T) {
	s := newTestStore(t)
	raw := "15" + "41" + "1a2b" + strings.Repeat("33", 16)
	msg := map[string]interface{}{
		"raw": raw, "direction": "tx",
		"timestamp": recentTS(time.Minute),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
	}
	handleClientPacket(t.Context(), s, cfgWithObservations(), "test", "aa11", msg, nil, nil)

	if obs := countRows(t, s, "client_rx_observations"); obs != 0 {
		t.Errorf("observations = %d, want 0 for a tx (self-transmission) packet", obs)
	}
	if cov := countRows(t, s, "client_receptions"); cov != 0 {
		t.Errorf("coverage rows = %d, want 0 for a tx packet", cov)
	}
}

// TestClientObservationsMillisecondPrecisionAllowsDistinctRows is the
// regression test for the second-resolution rx_at bug: two client packets
// with the same raw and rx_pubkey, timestamps 40ms apart in the same second,
// must produce TWO observation rows, not one collapsed by
// UNIQUE(rx_pubkey, pkt_hash, rx_at) + ON CONFLICT DO NOTHING. resolveRxTime's
// RFC3339 (second-resolution) formatting would collapse these; the dedicated
// millisecond-precision rx_at for this table must not.
func TestClientObservationsMillisecondPrecisionAllowsDistinctRows(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	raw := "16" + "41" + "1a2b" + strings.Repeat("33", 16)
	// Derived from time.Now() (not hardcoded) so this test never crosses
	// resolveRxTimeCore's >30d-stale reject, which would replace rx_at with
	// ingest time and silently stop exercising the millisecond-distinctness
	// this test exists to pin.
	t1 := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	t2 := t1.Add(40 * time.Millisecond)

	mk := func(at time.Time) map[string]interface{} {
		return map[string]interface{}{
			"raw": raw, "direction": "rx",
			"timestamp": at.Format(time.RFC3339Nano),
			"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
		}
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", mk(t1), nil, nil)
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", mk(t2), nil, nil)

	if n := countRows(t, s, "client_rx_observations"); n != 2 {
		t.Fatalf("observations = %d, want 2 — sub-second rx_at must keep both forwarder copies distinct", n)
	}
	rxAts := testCollect(t, s,
		`SELECT rx_at FROM client_rx_observations ORDER BY rx_at`, pgx.RowTo[time.Time])
	if len(rxAts) != 2 || !rxAts[0].Equal(t1) || !rxAts[1].Equal(t2) {
		t.Errorf("rx_at values = %v, want millisecond-distinct %v / %v", rxAts, t1, t2)
	}
}

// TestClientObservationScopeNameFromTransportCode covers code1/code2/scope_name
// — the field this whole table exists for ("which repeater forwards which
// scope"). Uses the same precomputed matchScope vector as main_test.go's
// TestMatchScope (key = SHA256("#test")[:16], payloadType=5, payload="hello"
// -> code1 2AB5) rather than inventing a vector, so a wrong HMAC algorithm
// cannot pass.
func TestClientObservationScopeNameFromTransportCode(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	testKey, err := hex.DecodeString("9cd8fcf22a47333b591d96a2b848b73f") // SHA256("#test")[:16]
	if err != nil {
		t.Fatal(err)
	}
	regionKeys := map[string][]byte{"#test": testKey}
	helloHex := hex.EncodeToString([]byte("hello"))

	// Derived from time.Now() (not hardcoded) so this test never crosses
	// resolveRxTimeCore's >30d-stale reject, which would replace rx_at with
	// ingest time and break the WHERE rx_at = $1 lookups below. Truncated to
	// the millisecond because that is the resolution the handler stores.
	t1 := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	t2 := t1.Add(time.Second)

	// header 0x14 = route_type 0 (TRANSPORT_FLOOD), payload_type 5 (GRP_TXT).
	// Transport routes (0,3) carry code1/code2 right after the header, raw
	// wire hex uppercased with no byte swap: bytes 2A B5 -> "2AB5", AA BB ->
	// "AABB". path byte 0x00 = hash_size 1, hop_count 0 (no hops). Payload
	// bytes are "hello" to match the precomputed matchScope vector.
	raw := "14" + "2ab5" + "aabb" + "00" + helloHex
	msg := map[string]interface{}{
		"raw": raw, "direction": "rx",
		"timestamp": t1.Format(time.RFC3339Nano),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", msg, nil, regionKeySetFromKeys(regionKeys))

	// ComputeContentHash deliberately excludes the transport-code bytes (so the
	// same content dedups across scopes), so raw and raw2 below share one
	// pkt_hash — distinguish the two inserted rows by rx_at, not pkt_hash.
	var code1, code2, scopeName *string
	mustScan(t, testRow(t, s,
		`SELECT code1, code2, scope_name FROM client_rx_observations WHERE rx_at = $1`, t1),
		&code1, &code2, &scopeName)
	if code1 == nil || *code1 != "2AB5" {
		t.Errorf("code1 = %v, want raw wire hex uppercased 2AB5 (bytes 2A B5, no byte swap)", code1)
	}
	if code2 == nil || *code2 != "AABB" {
		t.Errorf("code2 = %v, want raw wire hex uppercased AABB (bytes AA BB, no byte swap)", code2)
	}
	if scopeName == nil || *scopeName != "#test" {
		t.Errorf("scope_name = %v, want matched region #test", scopeName)
	}

	// Second packet: code1 "0000" is the real, non-scoped value — stored
	// literally, and the != "0000" guard must leave scope_name NULL rather
	// than attempt (and fail) a match.
	raw2 := "14" + "0000" + "aabb" + "00" + helloHex
	msg2 := map[string]interface{}{
		"raw": raw2, "direction": "rx",
		"timestamp": t2.Format(time.RFC3339Nano),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4},
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", msg2, nil, regionKeySetFromKeys(regionKeys))

	var code1b, scopeNameB *string
	mustScan(t, testRow(t, s,
		`SELECT code1, scope_name FROM client_rx_observations WHERE rx_at = $1`, t2),
		&code1b, &scopeNameB)
	if code1b == nil || *code1b != "0000" {
		t.Errorf("code1 = %v, want literal 0000 (a real value, not NULL)", code1b)
	}
	if scopeNameB != nil {
		t.Errorf("scope_name = %q, want NULL when code1=0000", *scopeNameB)
	}
}

// TestHandleClientPacketFloodTraceWritesNoCoverage is the CRITICAL-1
// regression test: a FLOOD-routed TRACE (header 0x25 = payload_type 9 << 2 |
// route_type 1) repurposes the header path bytes as per-hop SNR values, not
// node hashes (decoder.go). Before the packetpath.PathBytesAreHops guard was
// wired into the client path, this shape yielded heardKey=<SNR bytes>,
// src="rxlog" and a phantom client_receptions row. It must now produce ZERO
// coverage rows, and the surviving diagnostic observation must carry NULL
// forwarder/path_json (the route type, codes and signal are still valid
// diagnostics, so the row itself is kept).
func TestHandleClientPacketFloodTraceWritesNoCoverage(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	// header 0x25 = route_type 1 (FLOOD), payload_type 9 (TRACE).
	// pathByte 0x41 = hash_size 2, hash_count 1 -> path bytes "1a2b" (SNR, not a hop hash).
	// payload: tag(4) + authCode(4) + flags(1) = 9 bytes, so decodeTrace succeeds cleanly.
	raw := "25" + "41" + "1a2b" + "aabbccdd1122334400"
	decoded, err := decoder.DecodePacket(raw, nil, false)
	if err != nil {
		t.Fatalf("fixture must decode: %v", err)
	}
	if decoded.Header.PayloadTypeName != "TRACE" || decoded.Header.RouteType != types.RouteFlood {
		t.Fatalf("fixture sanity check failed: payloadType=%s routeType=%v", decoded.Header.PayloadTypeName, decoded.Header.RouteType)
	}
	if len(decoded.Path.Hops) != 1 || decoded.Path.Hops[0] != "1A2B" {
		t.Fatalf("fixture sanity check failed: hops=%v, want a 2-byte SNR entry decoded as a hop", decoded.Path.Hops)
	}

	msg := map[string]interface{}{
		"raw": raw, "direction": "rx", "SNR": 4.5, "RSSI": -101.0,
		"timestamp": recentTS(time.Minute),
		"gps":       map[string]interface{}{"lat": 51.2, "lon": 4.4, "acc_m": 8.0},
	}
	handleClientPacket(ctx, s, cfgWithObservations(), "test", "aa11", msg, nil, nil)

	if cov := countRows(t, s, "client_receptions"); cov != 0 {
		t.Fatalf("coverage rows = %d, want 0 — a FLOOD-routed TRACE must never be attributable", cov)
	}
	if obs := countRows(t, s, "client_rx_observations"); obs != 1 {
		t.Fatalf("observations = %d, want 1 (the packet was genuinely heard; only forwarder/path_json must be NULL)", obs)
	}

	var fwd, pathJSON *string
	mustScan(t, testRow(t, s,
		`SELECT forwarder, path_json FROM client_rx_observations WHERE rx_pubkey = $1`, "aa11"),
		&fwd, &pathJSON)
	if fwd != nil {
		t.Errorf("forwarder = %q, want NULL for a TRACE packet (path bytes are SNR values, not a node hash)", *fwd)
	}
	if pathJSON != nil {
		t.Errorf("path_json = %q, want NULL for a TRACE packet (path bytes are SNR values, not a hop chain)", *pathJSON)
	}
}

// TestHandleClientPacketRejectsNonHexPubkey verifies the #2 fix: a companion
// pubkey from the topic that isn't lowercase hex (a no-ACL broker could publish
// meshcore/client/!@#$/packets) writes nothing to either coverage table. Without
// the clientPubkeyRe guard this fixture would insert a polluting row.
// cfgWithObservations returns a Config with the observations gate on.
func TestHandleClientPacketRejectsNonHexPubkey(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	advertHex := "11451000D818206D3AAC152C8A91F89957E6D30CA51F36E28790228971C473B755F244F718754CF5EE4A2FD58D944466E42CDED140C66D0CC590183E32BAF40F112BE8F3F2BDF6012B4B2793C52F1D36F69EE054D9A05593286F78453E56C0EC4A3EB95DDA2A7543FCCC00B939CACC009278603902FC12BCF84B706120526F6F6620536F6C6172" // unchanged
	msgFor := func() map[string]interface{} {
		return map[string]interface{}{
			"raw": advertHex, "direction": "rx", "timestamp": recentTS(time.Minute),
			"origin": "Spoof", "SNR": -7.0, "RSSI": -92.0,
			"gps": map[string]interface{}{"lat": 51.05, "lon": 3.72, "acc_m": 8.0},
		}
	}

	for _, bad := range []string{"!@#$", "companionpk", "", "g0g0", "xyz"} {
		handleClientPacket(ctx, s, &Config{}, "test", bad, msgFor(), nil, nil)
	}
	if n := countRows(t, s, "client_receptions"); n != 0 {
		t.Fatalf("non-hex pubkey must write nothing, got %d receptions", n)
	}
	if n := countRows(t, s, "client_observers"); n != 0 {
		t.Fatalf("non-hex pubkey must write nothing, got %d observers", n)
	}

	// Positive control: the identical message with a valid pubkey does write,
	// so the zero counts above can only come from the pubkey check.
	handleClientPacket(ctx, s, &Config{}, "test", testCompanionPK, msgFor(), nil, nil)
	if n := countRows(t, s, "client_receptions"); n != 1 {
		t.Fatalf("valid pubkey control wrote %d receptions, want 1 — the fixture is not exercising the check", n)
	}
}
