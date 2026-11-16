package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

// observationBatch is how many observations are held before they are copied.
// Frames reference their observation, so each batch inserts the observations
// first and their frames second; no table is ever held whole in memory.
const observationBatch = 20000

// progressEvery is how many source rows pass between progress lines. The
// observations table is by far the largest, so a long silence would look hung.
const progressEvery = 1_000_000

// observationsQuery reads in transmission order, so duplicates are always
// adjacent and the dedup state is one transmission wide. Within a transmission
// the lowest id comes first and wins.
const observationsQuery = `SELECT o.id, o.transmission_id, o.observer_idx, o.direction, o.snr, o.rssi, o.score,
	o.path_json, o.timestamp, o.resolved_path, o.raw_hex, t.raw_hex
FROM observations o
LEFT JOIN transmissions t ON t.id = o.transmission_id
ORDER BY o.transmission_id, o.id`

var observationColumns = []string{"id", "transmission_id", "observer_idx", "direction", "snr", "rssi", "score",
	"path_hash", "timestamp", "hop_prefixes", "hop_nodes"}

var frameColumns = []string{"observation_id", "frame"}

// hopPattern is what observations_hop_prefixes_hex_lowercase accepts for one hop.
var hopPattern = regexp.MustCompile(`^[0-9a-f]+$`)

// dedupKey is the observations_dedup unique index: the same transmission seen
// by the same observer over the same path. observer is -1 for a NULL observer.
type dedupKey struct {
	observer int64
	pathHash string
}

// parseHops reads a path_json blob into lowercase hops. ok is false when the
// blob is not a JSON array of hex strings.
func parseHops(pathJSON string) (hops types.HopList, ok bool) {
	var raw []string
	if err := json.Unmarshal([]byte(pathJSON), &raw); err != nil {
		return nil, false
	}
	hops = make(types.HopList, 0, len(raw))
	for _, hop := range raw {
		hop = strings.ToLower(strings.TrimSpace(hop))
		if !hopPattern.MatchString(hop) {
			return nil, false
		}
		hops = append(hops, hop)
	}
	return hops, true
}

// hopNodeIDs maps resolved_path, which holds one pubkey (or null) per hop, to
// nodes.id with 0 for a hop that did not resolve to a loaded node. It returns
// nil when nothing resolved, which is how the schema stores "no resolution".
// problem names why the value was unusable, "" when it was fine.
func hopNodeIDs(resolvedPath string, hopCount int, nodes *LoadedNodes) (ids []int32, problem string) {
	if strings.TrimSpace(resolvedPath) == "" {
		return nil, ""
	}
	var resolved []*string
	if err := json.Unmarshal([]byte(resolvedPath), &resolved); err != nil {
		return nil, "resolved_path is not a JSON array, hop_nodes left empty"
	}
	if len(resolved) != hopCount {
		return nil, "resolved_path length differs from the path, hop_nodes left empty"
	}
	ids = make([]int32, len(resolved))
	resolvedAny := false
	for i, key := range resolved {
		if key == nil {
			continue
		}
		if id, found := nodes.IDs[strings.ToLower(strings.TrimSpace(*key))]; found {
			ids[i] = id
			resolvedAny = true
		}
	}
	if !resolvedAny {
		return nil, ""
	}
	return ids, ""
}

func unixTimestamp(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}

// loadObservations copies every observation whose transmission and timestamp
// are usable. The dedup key is recomputed from the normalised path, so two rows
// that differed only by hop case now collide; the lowest id is kept and each
// drop is counted.
func loadObservations(ctx context.Context, src *Source, tx pgx.Tx, nodes *LoadedNodes, observers *LoadedObservers,
	transmissions *LoadedTransmissions, report *Report) error {

	rows, err := src.db.QueryContext(ctx, observationsQuery)
	if err != nil {
		return fmt.Errorf("read observations: %w", err)
	}
	defer rows.Close()

	var (
		observationRows [][]any
		frameRows       [][]any
		read            int64
		currentTx       int64 = -1
		txFrame         types.RawFrame
		seen            = map[dedupKey]int64{}
	)

	flush := func() error {
		if len(observationRows) == 0 {
			return nil
		}
		if err := copyRows(ctx, tx, "observations", observationColumns, observationRows); err != nil {
			return err
		}
		if len(frameRows) > 0 {
			if err := copyRows(ctx, tx, "observation_frames", frameColumns, frameRows); err != nil {
				return err
			}
		}
		observationRows = observationRows[:0]
		frameRows = frameRows[:0]
		return nil
	}

	for rows.Next() {
		var (
			id, transmissionID, timestamp         int64
			observerIdx                           sql.NullInt64
			direction, pathJSON, resolved, obsRaw sql.NullString
			txRaw                                 sql.NullString
			snr, rssi, score                      sql.NullFloat64
		)
		if err := rows.Scan(&id, &transmissionID, &observerIdx, &direction, &snr, &rssi, &score,
			&pathJSON, &timestamp, &resolved, &obsRaw, &txRaw); err != nil {
			return fmt.Errorf("scan observations: %w", err)
		}
		read++
		if read%progressEvery == 0 {
			log.Printf("observations: read %d, loaded %d", read, report.Count("observations: loaded"))
		}

		if !transmissions.Has(transmissionID) || !txRaw.Valid {
			report.Problem("observations: skipped, transmission missing", fmt.Sprintf("observation %d transmission %d", id, transmissionID))
			continue
		}
		if timestamp <= 0 {
			report.Problem("observations: skipped, no usable timestamp", fmt.Sprintf("observation %d timestamp %d", id, timestamp))
			continue
		}

		if transmissionID != currentTx {
			currentTx = transmissionID
			clear(seen)
			frame, err := types.ParseRawFrame(txRaw.String)
			if err != nil {
				return fmt.Errorf("transmission %d raw_hex: %w", transmissionID, err)
			}
			txFrame = frame
		}

		var observer *int64
		observerKey := int64(-1)
		if observerIdx.Valid {
			if _, known := observers.Pubkeys[observerIdx.Int64]; known {
				observer = &observerIdx.Int64
				observerKey = observerIdx.Int64
			} else {
				report.Problem("observations: observer_idx matches no observer, set NULL",
					fmt.Sprintf("observation %d observer_idx %d", id, observerIdx.Int64))
			}
		}

		hops := types.HopList{}
		switch path := strings.TrimSpace(pathJSON.String); {
		case !pathJSON.Valid || path == "" || path == "[]":
			frameHex := obsRaw.String
			if frameHex == "" {
				frameHex = txRaw.String
			}
			if derived, err := packetpath.DecodePathFromRawHex(frameHex); err == nil && len(derived) > 0 {
				for _, hop := range derived {
					hops = append(hops, strings.ToLower(hop))
				}
				report.Add("observations: path derived from raw_hex", 1)
			}
		default:
			parsed, ok := parseHops(path)
			if !ok {
				report.Problem("observations: path_json unusable, loaded with no hops", fmt.Sprintf("observation %d path_json %.60s", id, path))
			} else {
				hops = parsed
			}
		}

		hopNodes, problem := hopNodeIDs(resolved.String, len(hops), nodes)
		if problem != "" {
			report.Problem("observations: "+problem, fmt.Sprintf("observation %d", id))
		}
		if hopNodes == nil {
			hopNodes = []int32{}
		}

		pathHash := hops.Hash()
		key := dedupKey{observer: observerKey, pathHash: pathHash}
		if keptID, duplicate := seen[key]; duplicate {
			report.Problem("observations: duplicate of a lower id after normalising, dropped",
				fmt.Sprintf("observation %d repeats %d (transmission %d)", id, keptID, transmissionID))
			continue
		}
		seen[key] = id

		var hash *string
		if pathHash != "" {
			hash = &pathHash
		}
		observationRows = append(observationRows, []any{
			id, transmissionID, observer,
			ptr(direction.String, direction.Valid),
			ptr(snr.Float64, snr.Valid), ptr(rssi.Float64, rssi.Valid), ptr(score.Float64, score.Valid),
			hash, unixTimestamp(timestamp), []string(hops), hopNodes,
		})
		report.Add("observations: loaded", 1)

		if obsRaw.Valid && obsRaw.String != "" {
			frame, err := types.ParseRawFrame(obsRaw.String)
			switch {
			case err != nil:
				report.Problem("observations: raw_hex unparseable, no frame stored", fmt.Sprintf("observation %d", id))
			case !bytes.Equal(frame, txFrame):
				frameRows = append(frameRows, []any{id, []byte(frame)})
				report.Add("observations: frame stored (differs from the transmission)", 1)
			}
		}

		if len(observationRows) >= observationBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read observations: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}
	return resetSequence(ctx, tx, "observations")
}
