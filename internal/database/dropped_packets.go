package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func (db *DB) InsertDroppedPacket(ctx context.Context, dp types.DroppedPacket) error {
	return db.q.InsertDroppedPacket(ctx, pgdb.InsertDroppedPacketParams{
		Hash:         toPgTextIfNotEmpty(dp.Hash),
		RawHex:       toPgTextIfNotEmpty(dp.RawHex),
		Reason:       dp.Reason,
		ObserverID:   toPgTextIfNotEmpty(dp.ObserverID),
		ObserverName: toPgTextIfNotEmpty(dp.ObserverName),
		NodePubkey:   toPgTextIfNotEmpty(dp.NodePubKey),
		NodeName:     toPgTextIfNotEmpty(dp.NodeName),
	})
}

func (db *DB) PruneDroppedPackets(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneDroppedPackets(ctx, cutoff)
}
