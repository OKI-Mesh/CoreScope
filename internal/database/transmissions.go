package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

type TransmissionInput struct {
	RawHex         string
	Hash           string
	RouteType      types.RouteType
	PayloadType    types.PayloadType
	PayloadVersion int
	DecodedJSON    string
	ChannelHash    string
	ScopeName      *string
	FromPubkey     string
	Code1          string
	Code2          string
	Advert         *NodeInput
}

func (db *DB) CreateOrUpdateTransmission(ctx context.Context, data TransmissionInput, rxTime time.Time) (txID int64, isNew bool, err error) {
	err = db.inTx(ctx, func(tx *DB) error {
		var from pgtype.Text // NULL unless there is a node to point at
		if data.Advert != nil {
			n := *data.Advert
			n.PublicKey = strings.ToLower(n.PublicKey)
			n.IngestNow = time.Now().UTC()
			if err := tx.CreateOrUpdateNode(ctx, n); err != nil {
				return fmt.Errorf("upsert advert node: %w", err)
			}
			from = pgtype.Text{String: n.PublicKey, Valid: true}
		}
		row, err := tx.q.InsertTransmission(ctx, pgdb.InsertTransmissionParams{
			RawHex:         data.RawHex,
			Hash:           data.Hash,
			FirstSeen:      rxTime,
			RouteType:      data.RouteType,
			PayloadType:    data.PayloadType,
			PayloadVersion: toPgInt4(data.PayloadVersion),
			DecodedJson:    toPgText(data.DecodedJSON),
			ChannelHash:    toPgTextIfNotEmpty(data.ChannelHash),
			ScopeName:      toPgTextPtr(data.ScopeName),
			FromPubkey:     from,
			LastSeen:       pgtype.Timestamptz{Time: rxTime, Valid: true},
			Code1:          toPgTextIfNotEmpty(data.Code1),
			Code2:          toPgTextIfNotEmpty(data.Code2),
		})
		if err != nil {
			return fmt.Errorf("insert transmission: %w", err)
		}
		txID, isNew = row.ID, row.Inserted
		return nil
	})
	return txID, isNew, err
}

func (db *DB) BumpTransmissionLastSeen(ctx context.Context, txID int64, rxTime time.Time) error {
	return db.q.BumpTxLastSeen(ctx, pgdb.BumpTxLastSeenParams{
		LastSeen: pgtype.Timestamptz{Time: rxTime, Valid: true},
		ID:       txID,
	})
}

func (db *DB) PruneTransmissionsBatch(ctx context.Context, cutoff time.Time, batchSize int32) (int64, error) {
	return db.q.PruneTransmissionsBatch(ctx, pgdb.PruneTransmissionsBatchParams{
		Cutoff: cutoff, BatchSize: batchSize,
	})
}
