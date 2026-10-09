package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func (db *DB) ListClientRfSamplesWithPrev(ctx context.Context, rxPubkey string, start, end time.Time) ([]types.RfSampleWithPrev, error) {
	rows, err := db.q.ListClientRfSamplesWithPrev(ctx, pgdb.ListClientRfSamplesWithPrevParams{
		RxPubkey: rxPubkey, StartAt: start, EndAt: end,
	})
	if err != nil {
		return nil, err
	}
	out := make([]types.RfSampleWithPrev, len(rows))
	for i, r := range rows {
		out[i] = types.RfSampleWithPrev{
			SampledAt: r.SampledAt.UTC(), Lat: r.Lat, Lon: r.Lon,
			Stationary: r.Stationary, UptimeSecs: r.UptimeSecs,
			RxAirSecs: fromPgInt8(r.RxAirSecs), TxAirSecs: fromPgInt8(r.TxAirSecs),
			Recv: fromPgInt8(r.Recv), RecvErrors: fromPgInt8(r.RecvErrors),
			PrevAt: fromAnyTime(r.PrevAt), PrevUptime: fromAnyInt64(r.PrevUptime),
			PrevRx: fromAnyInt64(r.PrevRx), PrevTx: fromAnyInt64(r.PrevTx),
			PrevRecv: fromAnyInt64(r.PrevRecv), PrevErrs: fromAnyInt64(r.PrevErrs),
		}
	}
	return out, nil
}
