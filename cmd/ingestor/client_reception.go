package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// clientPubkeyRe validates the companion pubkey taken from the MQTT topic
// (meshcore/client/<PUBLIC_KEY>/packets). A no-ACL broker would let a client
// publish under an arbitrary topic segment (e.g. "!@#$"), so we reject anything
// that is not lowercase hex before it reaches client_receptions/client_observers.
// Mirrors the server-side hexPrefixRe (cmd/server/node_resolve.go).
var clientPubkeyRe = regexp.MustCompile(`^[0-9a-f]{2,64}$`)

// handleClientPacket processes a packet from the mobile client RX topic
// (meshcore/client/{PUBLIC_KEY}/packets). Unlike observer packets, a roaming
// companion reports WHERE it directly heard a node, so we write a
// client_receptions row and never touch the observers/observations tables.
// rxPubkey is the companion pubkey from the topic (ACL-bound by the broker).
func handleClientPacket(ctx context.Context, store *Store, cfg *Config, tag, rxPubkey string, msg map[string]interface{}, channelKeys map[string]string, regionSet *regionKeySet) {
	// The companion identity IS the (ACL-bound) topic pubkey. Reject non-hex
	// topic segments so a no-ACL broker can't pollute the coverage tables, and
	// never fall back to a payload-supplied id (that would defeat the ACL trust
	// model — see docs/client-rx-coverage.md).
	rxPubkey = strings.ToLower(strings.TrimSpace(rxPubkey))
	if !clientPubkeyRe.MatchString(rxPubkey) {
		log.Printf("MQTT [%s] client: invalid pubkey %.8q, dropping", tag, rxPubkey)
		return
	}
	rawHex, _ := msg["raw"].(string)
	if rawHex == "" {
		return
	}
	gps, ok := msg["gps"].(map[string]interface{})
	if !ok {
		return // a client packet without a GPS fix is not coverage; drop
	}
	lat, latOK := toFloat64(gps["lat"])
	lon, lonOK := toFloat64(gps["lon"])
	if !latOK || !lonOK {
		return
	}
	var accPtr *float64
	if acc, ok := toFloat64(gps["acc_m"]); ok {
		accPtr = &acc
	}

	decoded, err := decoder.DecodePacket(rawHex, channelKeys, false)
	if err != nil {
		log.Printf("MQTT [%s] client decode error: %v", tag, err)
		return
	}

	direction := ""
	if v, ok := msg["direction"].(string); ok {
		direction = v
	} else if v, ok := msg["Direction"].(string); ok {
		direction = v
	}

	var snrPtr *float64
	if f, ok := toFloat64(firstPresent(msg, "SNR", "snr")); ok {
		snrPtr = &f
	}
	var rssiPtr *int
	if f, ok := toFloat64(firstPresent(msg, "RSSI", "rssi")); ok {
		v := int(f)
		rssiPtr = &v
	}

	// The envelope time is resolved once and truncated two ways: to the second
	// for client_receptions, to the millisecond for client_rx_observations. One
	// resolution (not two parses) keeps both rows for the SAME packet on the same
	// instant, so they can't straddle a second boundary on a fallback path.
	rxTime, _ := resolveRxTimeCore(msg, tag)
	ingestedAt := time.Now().UTC()
	rxAt := rxTime.Truncate(time.Second)            // client_receptions key stays second-resolution
	rxAtMillis := rxTime.Truncate(time.Millisecond) // keeps distinct forwarder copies apart
	isAdvert := decoded.Header.PayloadTypeName == "ADVERT"

	if cfg.ClientRxObservationsEnabled() {
		if obs := buildClientRxObservation(direction, rxPubkey, rawHex, rxAtMillis, ingestedAt, decoded, regionSet, snrPtr, rssiPtr, lat, lon, accPtr); obs != nil {
			if _, err := store.InsertClientRxObservation(ctx, obs); err != nil {
				log.Printf("MQTT [%s] client observation insert: %v", tag, err)
			}
		}
	}

	rec, ok := buildClientReception(
		rxPubkey,
		direction, decoded.Header.RouteType, decoded.Header.PayloadType, decoded.Path.Hops, decoded.Payload.PubKey, isAdvert,
		snrPtr, rssiPtr, lat, lon, accPtr, rxAt, ingestedAt,
	)
	if !ok {
		return
	}
	if _, err := store.InsertClientReception(ctx, rec); err != nil {
		log.Printf("MQTT [%s] client_reception insert: %v", tag, err)
	}
	// Remember the companion's self-reported name (sent as "origin") so the
	// leaderboard can show a name even if this companion never advertised.
	if name := stringField(msg, "origin"); name != "" {
		if err := store.UpsertClientObserver(ctx, rec.RxPubkey, name, ingestedAt); err != nil {
			log.Printf("MQTT [%s] client_observer upsert: %v", tag, err)
		}
	}
}

// UpsertClientObserver records/updates a mobile client's self-reported name.
// All writes live in the ingestor (read/write invariant #1283).
func (s *Store) UpsertClientObserver(ctx context.Context, pubkey string, name string, ts time.Time) error {
	if pubkey == "" || name == "" {
		return nil
	}
	_, err := s.db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{
		Pubkey:   strings.ToLower(pubkey),
		Name:     name,
		LastSeen: ts,
	})
	return err
}

// firstPresent returns the first present value among the given keys.
func firstPresent(msg map[string]interface{}, keys ...string) interface{} {
	for _, k := range keys {
		if v, ok := msg[k]; ok {
			return v
		}
	}
	return nil
}

// stringField returns msg[key] as a string, or "" if absent/not a string.
func stringField(msg map[string]interface{}, key string) string {
	if v, ok := msg[key].(string); ok {
		return v
	}
	return ""
}

// deriveHeardKey applies the RX capture HARD RULE: record only what the
// companion heard itself and directly.
//   - direction must be "rx".
//   - payload type must not repurpose the header path bytes (TRACE stores
//     per-hop SNR there, not node hashes — packetpath.PathBytesAreHops).
//   - hops present AND a FLOOD route → the directly-heard node is the LAST hop
//     (path[len-1] = the forwarder that just transmitted; each FLOOD forwarder
//     appends its hash to the end). 1-byte (2 hex char) prefixes are rejected.
//   - hops present on a DIRECT route → NOT attributable: direct forwarders
//     consume the next hop from the FRONT (firmware Mesh.cpp removeSelfFromPath),
//     so path[len-1] is the route's destination-side end, not who was heard.
//   - hops empty + isAdvert → the 0-hop advertiser, by its full pubkey.
//   - otherwise → not attributable (ok=false).
//
// Returns (heardKey lowercased, keylenBytes, src, ok).
func deriveHeardKey(direction string, routeType types.RouteType, payloadType types.PayloadType, hops []string, advertPubkey string, isAdvert bool) (string, int, string, bool) {
	if !strings.EqualFold(direction, "rx") {
		return "", 0, "", false
	}
	if !packetpath.PathBytesAreHops(payloadType) {
		return "", 0, "", false
	}
	if len(hops) > 0 {
		// FLOOD routes (TRANSPORT_FLOOD 0, FLOOD 1) APPEND each forwarder's hash to
		// the END of the path, so path[last] is the immediate RF transmitter. DIRECT
		// routes (2, 3) consume the next hop from the FRONT, so path[last] is the
		// route's destination-side end, NOT who was heard.
		if routeType != types.RouteTransportFlood && routeType != types.RouteFlood { // direct route: path[last] is not the transmitter
			return "", 0, "", false
		}
		last := strings.ToLower(strings.TrimSpace(hops[len(hops)-1]))
		keylen := len(last) / 2
		if keylen < 2 { // exclude 1-byte (collision-prone), matching Reach
			return "", 0, "", false
		}
		return last, keylen, "rxlog", true
	}
	if isAdvert && advertPubkey != "" {
		pk := strings.ToLower(strings.TrimSpace(advertPubkey))
		return pk, len(pk) / 2, "advert", true
	}
	return "", 0, "", false
}

// buildClientReception validates inputs and assembles a ClientReception, or
// returns ok=false when the packet is not attributable / out of range.
func buildClientReception(
	rxPubkey, direction string, routeType types.RouteType, payloadType types.PayloadType, hops []string, advertPubkey string, isAdvert bool,
	snr *float64, rssi *int, lat, lon float64, posAccM *float64, rxAt time.Time, ingestedAt time.Time,
) (*types.ClientReception, bool) {
	if rxPubkey == "" || rxAt.IsZero() {
		return nil, false
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return nil, false
	}
	heardKey, keylen, src, ok := deriveHeardKey(direction, routeType, payloadType, hops, advertPubkey, isAdvert)
	if !ok {
		return nil, false
	}
	return &types.ClientReception{
		RxPubkey: strings.ToLower(rxPubkey), HeardKey: heardKey, HeardKeyLen: keylen,
		RSSI: rssi, SNR: snr, Lat: lat, Lon: lon, PosAccM: posAccM,
		RxAt: rxAt, IngestedAt: ingestedAt, Src: src,
	}, true
}

// InsertClientReception writes one coverage row. Idempotent via the
// UNIQUE(rx_pubkey, heard_key, rx_at) constraint; returns ins=false when the
// row already existed. All writes live in the ingestor (read/write invariant #1283).
func (s *Store) InsertClientReception(ctx context.Context, r *types.ClientReception) (bool, error) {
	return s.db.InsertClientReception(ctx, *r)
}

// InsertClientRfSample writes one RF environment sample. Idempotent via
// UNIQUE(rx_pubkey, sampled_at). Every counter is stored as an absolute; the
// server derives deltas, so a lost or reordered sample costs one interval
// rather than corrupting a running total.
func (s *Store) InsertClientRfSample(ctx context.Context, o *types.ClientRfSample) (bool, error) {
	return s.db.InsertClientRfSample(ctx, o)

}

// buildClientRxObservation assembles a ClientRxObservation from a decoded
// client packet, regardless of whether it is attributable to a directly-heard
// node (deriveHeardKey/buildClientReception decide that separately, and this
// function has no opinion on it). It does, however, require direction "rx"
// (case-insensitively, matching deriveHeardKey's own check on the coverage
// path) and returns nil otherwise: a companion's own outgoing transmission is
// not RF it observed, and recording it would inflate this table's headline
// signal — per-flood row multiplicity meant to measure forwarder
// amplification of traffic actually heard over the air.
func buildClientRxObservation(
	direction string, rxPubkey string, rawHex string, rxAt time.Time, ingestedAt time.Time, decoded *decoder.DecodedPacket, regionSet *regionKeySet,
	snr *float64, rssi *int, lat, lon float64, posAccM *float64,
) *types.ClientRxObservation {
	if !strings.EqualFold(direction, "rx") {
		return nil
	}
	obs := &types.ClientRxObservation{
		RxPubkey:    rxPubkey,
		RxAt:        rxAt,
		IngestedAt:  ingestedAt,
		PktHash:     decoder.ComputeContentHash(rawHex),
		RouteType:   decoded.Header.RouteType,
		PayloadType: decoded.Header.PayloadType,
		HashSize:    int32(decoded.Path.HashSize),
		HopCount:    int32(decoded.Path.HashCount), // declared count; len(Hops) can be short on a truncated path
		SNR:         snr,
		RSSI:        rssi,
		Lat:         lat,
		Lon:         lon,
		PosAccM:     posAccM,
	}
	if tc := decoded.TransportCodes; tc != nil {
		obs.Code1 = &tc.Code1
		obs.Code2 = &tc.Code2
		if decoded.IsTransportScoped() {
			sn := regionSet.matchScopeName(decoded.Header.PayloadType, decoded.PayloadRaw, tc.Code1)
			obs.ScopeName = &sn
		}
	}
	// TRACE repurposes the header path bytes as per-hop SNR values, not node
	// hashes (decoder.go), so they must not be recorded as a forwarder or a hop
	// chain — same guard as deriveHeardKey. The row is still written (route
	// type, transport codes and signal are all still valid diagnostics); only
	// forwarder/path_json stay NULL.
	if len(decoded.Path.Hops) > 0 && packetpath.PathBytesAreHops(decoded.Header.PayloadType) {
		// The decoder emits hops as uppercase hex (decoder.go strings.ToUpper),
		// but every other identifier in this schema is lowercase (rxPubkey,
		// heard_key, ComputeContentHash output). Lowercase every hop here so
		// path_json and forwarder agree in case within this table and stay
		// joinable against them — otherwise a case-sensitive comparison (e.g.
		// json_extract(path_json,'$[n]') = forwarder) silently matches zero
		// rows instead of erroring.
		lowerHops := make([]string, len(decoded.Path.Hops))
		for i, h := range decoded.Path.Hops {
			lowerHops[i] = strings.ToLower(h)
		}
		if b, err := json.Marshal(lowerHops); err == nil {
			j := string(b)
			obs.PathJSON = &j
		}
		// FLOOD routes accumulate forwarders at the END of the path, so
		// path[last] is the node that actually transmitted. DIRECT routes
		// consume from the FRONT, so their path[last] is the route's far
		// end — never the transmitter. Same rule as deriveHeardKey.
		if decoded.Header.RouteType == types.RouteTransportFlood || decoded.Header.RouteType == types.RouteFlood {
			f := lowerHops[len(lowerHops)-1]
			obs.Forwarder = &f
		}
	}
	return obs
}

// InsertClientRxObservation writes one diagnostic observation. Idempotent via
// UNIQUE(rx_pubkey, pkt_hash, rx_at); returns ins=false when the row existed.
// Several rows per pkt_hash are EXPECTED — each is one forwarder's copy of the
// same flood, and that multiplicity is the flood-amplification signal.
func (s *Store) InsertClientRxObservation(ctx context.Context, o *types.ClientRxObservation) (bool, error) {
	res, err := s.db.InsertClientRxObservation(ctx, o)
	if err != nil {
		return false, err
	}
	return res, nil
}

// InsertClientDeclaredRegions writes one declared-region observation.
// Idempotent via UNIQUE(target, rx_pubkey, observed_at); returns ins=false
// when the row already existed.
func (s *Store) InsertClientDeclaredRegions(ctx context.Context, o *types.ClientDeclaredRegions) (bool, error) {
	res, err := s.db.InsertClientDeclaredRegions(ctx, o)
	if err != nil {
		return false, err
	}
	return res, nil
}

// CurrentDeclaredRegions returns the most recent declared-region observation
// for a target, or nil if none exist. "Most recent" is the greatest
// observed_at, NOT the greatest ingested_at: a drive buffered offline can
// arrive days late, and ordering by arrival would let that stale reading
// overwrite a fresher one.
func (s *Store) CurrentDeclaredRegions(ctx context.Context, target string) (*types.ClientDeclaredRegions, error) {
	target = strings.ToLower(strings.TrimSpace(target))
	return s.db.GetCurrentDeclaredRegions(ctx, target)
}

// PruneOldClientDeclaredRegions deletes declared-region observation rows
// (node_declared_regions) older than `days` (by observed_at). Independent of
// clientRxDays/clientRxObsDays/clientRfDays. 0 disables.
func (s *Store) PruneOldClientReceptions(ctx context.Context, days int) (int64, error) {
	if days <= 0 {
		return 0, nil // disabled
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	n, err := s.db.PruneClientReceptions(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		log.Printf("[prune] deleted %d client_receptions older than %d days", n, days)
	}
	return n, nil
}

// PruneOldClientRxObservations deletes diagnostic client_rx_observations rows
// older than `days` (by rx_at). Unlike client_receptions this table is
// diagnostic, not archival, so it gets its own (typically shorter) window.
// 0 disables.
func (s *Store) PruneOldClientRxObservations(ctx context.Context, days int) (int64, error) {
	if days <= 0 {
		return 0, nil // disabled
	}
	n, err := s.pruneOldClientRxObservationsAt(ctx, time.Now().UTC().AddDate(0, 0, -days))
	if err != nil {
		return 0, err
	}
	if n > 0 {
		log.Printf("[prune] deleted %d client_rx_observations older than %d days", n, days)
	}
	return n, nil
}

// pruneOldClientRxObservationsAt is the cutoff-instant seam behind
// PruneOldClientRxObservations, so tests can place a row on a specific side of
// the cutoff without racing time.Now().
func (s *Store) pruneOldClientRxObservationsAt(ctx context.Context, cutoff time.Time) (int64, error) {
	n, err := s.db.PruneClientRxObservations(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune client_rx_observations: %w", err)
	}
	return n, nil
}

// PruneOldClientDeclaredRegions deletes declared-region observation rows
// (node_declared_regions) older than `days` (by observed_at). Independent of
// clientRxDays/clientRxObsDays/clientRfDays. 0 disables. Owned by the
// ingestor writer (#1283). observed_at on this table is stored at
// millisecond precision via rxTimeMillisLayout, not RFC3339 — the cutoff
// must be formatted the same way so lexicographic comparison against
// stored values stays correct.
func (s *Store) PruneOldClientDeclaredRegions(ctx context.Context, days int) (int64, error) {
	if days <= 0 {
		return 0, nil
	}
	n, err := s.pruneOldClientDeclaredRegionsAt(ctx, time.Now().UTC().AddDate(0, 0, -days))
	if err != nil {
		return 0, err
	}
	if n > 0 {
		log.Printf("[prune] deleted %d node_declared_regions older than %d days", n, days)
	}
	return n, nil
}

// pruneOldClientDeclaredRegionsAt is the cutoff-instant seam behind
// PruneOldClientDeclaredRegions: it exists so tests can pin the cutoff
// deterministically instead of racing time.Now() to place a fixture row on a
// specific side of it.
func (s *Store) pruneOldClientDeclaredRegionsAt(ctx context.Context, cutoffInstant time.Time) (int64, error) {
	n, err := s.db.PruneDeclaredRegions(ctx, cutoffInstant)
	if err != nil {
		return 0, fmt.Errorf("prune node_declared_regions: %w", err)
	}
	return n, nil
}
