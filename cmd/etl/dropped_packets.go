package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// blankToNil turns NULL and the empty string into a NULL, which is how the
// target says "not recorded".
func blankToNil(s sql.NullString) *string {
	if !s.Valid || s.String == "" {
		return nil
	}
	return &s.String
}

// lowerOrNil is blankToNil for a pubkey, lowercased to match observers and nodes.
func lowerOrNil(s sql.NullString) *string {
	value := blankToNil(s)
	if value == nil {
		return nil
	}
	lowered := strings.ToLower(strings.TrimSpace(*value))
	if lowered == "" {
		return nil
	}
	return &lowered
}

func loadDroppedPackets(ctx context.Context, src *Source, tx pgx.Tx, report *Report) error {
	rows, err := src.db.QueryContext(ctx, `SELECT id, hash, raw_hex, reason, observer_id, observer_name,
		node_pubkey, node_name, dropped_at FROM dropped_packets ORDER BY id`)
	if err != nil {
		return fmt.Errorf("read dropped_packets: %w", err)
	}
	defer rows.Close()

	var out [][]any
	for rows.Next() {
		var (
			id                                                           int64
			hash, rawHex, observerID, observerName, nodePubkey, nodeName sql.NullString
			reason                                                       sql.NullString
			droppedAt                                                    any
		)
		if err := rows.Scan(&id, &hash, &rawHex, &reason, &observerID, &observerName, &nodePubkey, &nodeName, &droppedAt); err != nil {
			return fmt.Errorf("scan dropped_packets: %w", err)
		}
		if !reason.Valid {
			return fmt.Errorf("dropped_packets %d has no reason, which the target requires", id)
		}
		when := epoch
		if seen := report.Time("dropped_packets.dropped_at", droppedAt); seen != nil {
			when = *seen
		} else {
			report.Add("dropped_packets: no dropped_at, set to the epoch", 1)
		}
		row := fmt.Sprintf("dropped_packets %d", id)
		out = append(out, []any{id,
			cleanText(report, "dropped_packets.hash", row, blankToNil(hash)),
			cleanText(report, "dropped_packets.raw_hex", row, blankToNil(rawHex)),
			*cleanText(report, "dropped_packets.reason", row, &reason.String),
			lowerOrNil(observerID),
			cleanText(report, "dropped_packets.observer_name", row, blankToNil(observerName)),
			lowerOrNil(nodePubkey),
			cleanText(report, "dropped_packets.node_name", row, blankToNil(nodeName)), when})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read dropped_packets: %w", err)
	}
	report.Add("dropped_packets: loaded", int64(len(out)))
	columns := []string{"id", "hash", "raw_hex", "reason", "observer_id", "observer_name", "node_pubkey", "node_name", "dropped_at"}
	if err := copyRows(ctx, tx, "dropped_packets", columns, out); err != nil {
		return err
	}
	return resetSequence(ctx, tx, "dropped_packets")
}
