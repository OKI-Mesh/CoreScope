package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

const (
	defaultDroppedPacketLimit = 100
	maxDroppedPacketLimit     = 500
)

// DroppedPacket is a packet rejected during ingest. On insert, empty strings are
// stored as NULL and ID and DroppedAt are ignored (the database assigns them).
// On read, NULL comes back as "".
type DroppedPacket struct {
	ID           int64
	Hash         string
	RawHex       string
	Reason       string
	ObserverID   string
	ObserverName string
	NodePubkey   string
	NodeName     string
	DroppedAt    time.Time // UTC
}

func (db *DB) InsertDroppedPacket(ctx context.Context, dp DroppedPacket) error {
	return db.q.InsertDroppedPacket(ctx, pgdb.InsertDroppedPacketParams{
		Hash:         toPgTextIfNotEmpty(dp.Hash),
		RawHex:       toPgTextIfNotEmpty(dp.RawHex),
		Reason:       dp.Reason,
		ObserverID:   toPgTextIfNotEmpty(dp.ObserverID),
		ObserverName: toPgTextIfNotEmpty(dp.ObserverName),
		NodePubkey:   toPgTextIfNotEmpty(dp.NodePubkey),
		NodeName:     toPgTextIfNotEmpty(dp.NodeName),
	})
}

// PruneDroppedPackets deletes packets dropped before cutoff and returns how
// many were removed.
func (db *DB) PruneDroppedPackets(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneDroppedPackets(ctx, cutoff)
}

// ListDroppedPackets returns dropped packets, newest first (ties broken by
// newest id). An empty observerID or nodePubkey means no filter. A limit of
// zero or less becomes 100; a limit above 500 becomes 500.
func (db *DB) ListDroppedPackets(ctx context.Context, limit int, observerID, nodePubkey string) ([]DroppedPacket, error) {
	switch {
	case limit <= 0:
		limit = defaultDroppedPacketLimit
	case limit > maxDroppedPacketLimit:
		limit = maxDroppedPacketLimit
	}
	rows, err := db.q.ListDroppedPackets(ctx, pgdb.ListDroppedPacketsParams{
		ObserverID: toPgTextIfNotEmpty(observerID),
		NodePubkey: toPgTextIfNotEmpty(nodePubkey),
		MaxRows:    int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]DroppedPacket, 0, len(rows))
	for _, r := range rows {
		out = append(out, DroppedPacket{
			ID:           r.ID,
			Hash:         r.Hash.String,
			RawHex:       r.RawHex.String,
			Reason:       r.Reason,
			ObserverID:   r.ObserverID.String,
			ObserverName: r.ObserverName.String,
			NodePubkey:   r.NodePubkey.String,
			NodeName:     r.NodeName.String,
			DroppedAt:    r.DroppedAt.UTC(),
		})
	}
	return out, nil
}

// CountDroppedPackets returns the total number of dropped packets.
func (db *DB) CountDroppedPackets(ctx context.Context) (int64, error) {
	return db.q.CountDroppedPackets(ctx)
}
