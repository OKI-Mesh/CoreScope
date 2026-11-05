package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// GroupTextTransmission is one GRP_TXT transmission as the offline channel
// decrypt tool reads it: the raw frame plus the identifiers needed to report it.
type GroupTextTransmission struct {
	ID    int64
	Hash  string
	Frame types.RawFrame
}

// ObserverReception is one observer's receipt of a transmission.
type ObserverReception struct {
	TransmissionID int64
	ObserverName   *string // nil when the observer is unknown or never sent a name
	SNR, RSSI      *float64
	ReceivedAt     time.Time
}

// ListGroupTextTransmissions returns up to limit GRP_TXT transmissions with an
// id greater than afterID, oldest id first. Page by passing the last id back.
func (db *DB) ListGroupTextTransmissions(ctx context.Context, afterID int64, limit int) ([]GroupTextTransmission, error) {
	rows, err := db.q.ListGroupTextTransmissions(ctx, pgdb.ListGroupTextTransmissionsParams{
		PayloadType: types.PayloadGRP_TXT,
		AfterID:     afterID,
		RowLimit:    int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]GroupTextTransmission, 0, len(rows))
	for _, r := range rows {
		out = append(out, GroupTextTransmission{
			ID:    int64(r.ID),
			Hash:  r.Hash,
			Frame: types.RawFrame(r.RawHex),
		})
	}
	return out, nil
}

// ListObserverReceptions groups every observation of txIDs by transmission,
// oldest first, with the observer's name when it has one.
func (db *DB) ListObserverReceptions(ctx context.Context, txIDs []int64) (map[int64][]ObserverReception, error) {
	rows, err := db.q.ListObserverReceptions(ctx, txIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[int64][]ObserverReception, len(txIDs))
	for _, r := range rows {
		txID := int64(r.TransmissionID)
		out[txID] = append(out[txID], ObserverReception{
			TransmissionID: txID,
			ObserverName:   fromPgText(r.ObserverName),
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			ReceivedAt:     r.Timestamp.UTC(),
		})
	}
	return out, nil
}
