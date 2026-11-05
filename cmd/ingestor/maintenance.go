package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// pruneBatchTransmissions bounds how many transmissions (and their child
// observations) one prune transaction deletes before it commits.
//
// Batching keeps each transaction small: it bounds the WAL written per commit,
// how long the deleted rows stay locked, and how much dead space autovacuum has
// to clean up at once, so a retention-day-sized delete is never one long
// transaction.
//
// The bound is on transmissions, but the work scales with the rows actually
// deleted, and each transmission carries an unbounded number of observations.
// At ~16 observations per transmission a batch is ~4k row deletes; an instance
// with a denser observation ratio gets a proportionally larger transaction from
// the same batch size. 250 also keeps the commit count low (a 16k-transmission
// day is 64 transactions, not 16k).
const pruneBatchTransmissions = 250

// PruneNeighborEdges deletes rows older than maxAgeDays from neighbor_edges.
// Owned by the ingestor per #1287 (was in cmd/server). Returns DB rows deleted.
func (s *Store) PruneNeighborEdges(ctx context.Context, maxAgeDays int) (int64, error) {
	if maxAgeDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -maxAgeDays)
	n, err := s.db.PruneNeighborEdges(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune neighbor_edges: %w", err)
	}
	if n > 0 {
		log.Printf("[neighbor-prune] removed %d DB rows older than %d days", n, maxAgeDays)
	}
	return n, nil
}

// ─── from_pubkey backfill (#1143) ──────────────────────────────────────────
//
// Moved from cmd/server/from_pubkey_migration.go in #1287. Runs from the
// ingestor's maintenance loop. Populates transmissions.from_pubkey for
// ADVERT rows whose value is still NULL, by parsing decoded_json.pubKey.

// FromPubkeyBackfillStats holds progress for /api/healthz exposure.
// The ingestor exposes these via stats_file.go so the server can read
// them without writing.
type FromPubkeyBackfillStats struct {
	Total     int64 `json:"total"`
	Processed int64 `json:"processed"`
	Done      bool  `json:"done"`
}

// BackfillFromPubkey scans transmissions where from_pubkey IS NULL and
// payload_type = 4 (ADVERT) and populates from_pubkey from decoded_json.
// Chunked + yields between batches. Safe to call repeatedly; once a row
// is set to either "" or hex it never matches the WHERE clause again.

// TODO: GOES INTO ETL
// func (s *Store) BackfillFromPubkey(chunkSize int, yieldDuration time.Duration, progress func(total, processed int64, done bool)) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			log.Printf("[backfill] from_pubkey panic recovered: %v", r)
// 		}
// 		if progress != nil {
// 			progress(0, 0, true) // signal done; values overwritten below if collected
// 		}
// 	}()
// 	if chunkSize <= 0 {
// 		chunkSize = 5000
// 	}

// 	var total int64
// 	if err := s.db.QueryRow(
// 		"SELECT COUNT(*) FROM transmissions WHERE from_pubkey IS NULL AND payload_type = 4",
// 	).Scan(&total); err != nil {
// 		log.Printf("[backfill] from_pubkey count error: %v", err)
// 		return
// 	}
// 	if total == 0 {
// 		log.Println("[backfill] from_pubkey: nothing to do")
// 		if progress != nil {
// 			progress(0, 0, true)
// 		}
// 		return
// 	}
// 	if progress != nil {
// 		progress(total, 0, false)
// 	}
// 	log.Printf("[backfill] from_pubkey starting: %d ADVERT rows", total)

// 	stmt, err := s.db.Prepare("UPDATE transmissions SET from_pubkey = ? WHERE id = ?")
// 	if err != nil {
// 		log.Printf("[backfill] from_pubkey prepare: %v", err)
// 		return
// 	}
// 	defer stmt.Close()

// 	var processed int64
// 	for {
// 		rows, err := s.db.Query(
// 			"SELECT id, decoded_json FROM transmissions WHERE from_pubkey IS NULL AND payload_type = 4 LIMIT ?",
// 			chunkSize)
// 		if err != nil {
// 			log.Printf("[backfill] from_pubkey select: %v", err)
// 			return
// 		}
// 		type row struct {
// 			id int64
// 			pk string
// 		}
// 		batch := make([]row, 0, chunkSize)
// 		for rows.Next() {
// 			var id int64
// 			var dj sql.NullString
// 			if err := rows.Scan(&id, &dj); err != nil {
// 				continue
// 			}
// 			batch = append(batch, row{id: id, pk: extractPubkeyFromAdvertJSON(dj.String)})
// 		}
// 		rows.Close()
// 		if len(batch) == 0 {
// 			break
// 		}

// 		tx, err := s.db.Begin()
// 		if err != nil {
// 			log.Printf("[backfill] from_pubkey begin tx: %v", err)
// 			return
// 		}
// 		txStmt := tx.Stmt(stmt)
// 		for _, b := range batch {
// 			// Sentinel: "" = scanned-no-pubkey (so the WHERE clause
// 			// won't keep rescanning this row). hex = real pubkey.
// 			var val interface{} = ""
// 			if b.pk != "" {
// 				val = b.pk
// 			}
// 			if _, err := txStmt.Exec(val, b.id); err != nil {
// 				log.Printf("[backfill] from_pubkey update id=%d: %v", b.id, err)
// 			}
// 		}
// 		if err := tx.Commit(); err != nil {
// 			log.Printf("[backfill] from_pubkey commit: %v", err)
// 			return
// 		}
// 		processed += int64(len(batch))
// 		if progress != nil {
// 			progress(total, processed, false)
// 		}
// 		if len(batch) < chunkSize {
// 			break
// 		}
// 		if yieldDuration > 0 {
// 			time.Sleep(yieldDuration)
// 		}
// 	}
// 	log.Printf("[backfill] from_pubkey complete: %d rows processed", processed)
// 	if progress != nil {
// 		progress(total, processed, true)
// 	}
// }

// extractPubkeyFromAdvertJSON parses an ADVERT decoded_json blob and
// returns the pubKey field, or "" if absent/invalid.
func extractPubkeyFromAdvertJSON(s string) string {
	if s == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return ""
	}
	if v, ok := m["pubKey"].(string); ok {
		return v
	}
	return ""
}

// extractPubkeyFromAnonReqJSON parses an ANON_REQ decoded_json blob and
// returns the ephemeralPubKey field, or "" if absent/invalid (#1777).
// ANON_REQ carries the sender's full Ed25519 ephemeral pubkey — the same
// trust level as ADVERT's pubKey — unlike REQ/RESP/PATH/TXT, which only
// carry a 1-byte truncated hash of the originator.
func extractPubkeyFromAnonReqJSON(s string) string {
	if s == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return ""
	}
	if v, ok := m["ephemeralPubKey"].(string); ok {
		return v
	}
	return ""
}
