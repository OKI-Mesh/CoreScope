package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (db *DB) InsertClientReception(ctx context.Context, r types.ClientReception) (bool, error) {
	n, err := db.q.InsertClientReception(ctx, pgdb.InsertClientReceptionParams{
		RxPubkey: r.RxPubkey, HeardKey: r.HeardKey, HeardKeylen: int32(r.HeardKeyLen),
		Rssi: toPgInt4Ptr(r.RSSI), Snr: toPgFloat8Ptr(r.SNR),
		Lat: r.Lat, Lon: r.Lon, PosAccM: toPgFloat8Ptr(r.PosAccM),
		RxAt: r.RxAt, IngestedAt: r.IngestedAt, Src: r.Src,
	})
	return n > 0, err
}

func (db *DB) CreateOrUpdateClientObserver(ctx context.Context, r types.ClientObserver) (bool, error) {
	if r.LastSeen.IsZero() {
		return false, errors.New("CreateOrUpdateClientObserver: LastSeen is the zero time")
	}
	err := db.q.CreateOrUpdateClientObserver(ctx, pgdb.CreateOrUpdateClientObserverParams{
		Pubkey:   r.Pubkey,
		Name:     pgtype.Text{String: r.Name, Valid: true},
		LastSeen: r.LastSeen,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) InsertClientRfSample(ctx context.Context, r *types.ClientRfSample) (bool, error) {
	n, err := db.q.InsertClientRfSample(ctx, pgdb.InsertClientRfSampleParams{
		RxPubkey:   r.RxPubkey,
		SampledAt:  r.SampledAt,
		IngestedAt: r.IngestedAt,
		Lat:        r.Lat,
		Lon:        r.Lon,
		PosAccM:    toPgFloat8Ptr(r.PosAccM),
		Stationary: r.Stationary,
		UptimeSecs: r.UptimeSecs,
		BatteryMv:  toPgInt4Ptr(r.BatteryMV),
		QueueLen:   toPgInt4Ptr(r.QueueLen),
		Errors:     toPgInt4Ptr(r.Errors),
		NoiseFloor: toPgInt4Ptr(r.NoiseFloor),
		LastRssi:   toPgInt4Ptr(r.LastRSSI),
		LastSnr:    toPgFloat8Ptr(r.LastSNR),
		TxAirSecs:  toPgInt8Ptr(r.TxAirSecs),
		RxAirSecs:  toPgInt8Ptr(r.RxAirSecs),
		Recv:       toPgInt8Ptr(r.Recv),
		Sent:       toPgInt8Ptr(r.Sent),
		FloodRx:    toPgInt8Ptr(r.FloodRx),
		DirectRx:   toPgInt8Ptr(r.DirectRx),
		FloodTx:    toPgInt8Ptr(r.FloodTx),
		DirectTx:   toPgInt8Ptr(r.DirectTx),
		RecvErrors: toPgInt8Ptr(r.RecvErrors),
	})
	return n > 0, err
}

func (db *DB) InsertClientRxObservation(ctx context.Context, o *types.ClientRxObservation) (bool, error) {
	n, err := db.q.InsertClientRxObservation(ctx, pgdb.InsertClientRxObservationParams{
		RxPubkey:    o.RxPubkey,
		RxAt:        o.RxAt,
		IngestedAt:  o.IngestedAt,
		PktHash:     o.PktHash,
		RouteType:   o.RouteType,
		PayloadType: o.PayloadType,
		Code1:       toPgTextPtr(o.Code1),
		Code2:       toPgTextPtr(o.Code2),
		ScopeName:   toPgTextPtr(o.ScopeName),
		HashSize:    o.HashSize,
		HopCount:    o.HopCount,
		PathJson:    toPgTextPtr(o.PathJSON),
		Forwarder:   toPgTextPtr(o.Forwarder),
		Snr:         toPgFloat8Ptr(o.SNR),
		Rssi:        toPgInt4Ptr(o.RSSI),
		Lat:         o.Lat,
		Lon:         o.Lon,
		PosAccM:     toPgFloat8Ptr(o.PosAccM),
	})
	return n > 0, err
}

func (db *DB) InsertClientDeclaredRegions(ctx context.Context, o *types.ClientDeclaredRegions) (bool, error) {
	n, err := db.q.InsertClientDeclaredRegions(ctx, pgdb.InsertClientDeclaredRegionsParams{
		Target:        o.Target,
		RxPubkey:      o.RxPubkey,
		ObservedAt:    o.ObservedAt,
		IngestedAt:    o.IngestedAt,
		RegionsCsv:    o.RegionsCSV,
		Truncated:     o.Truncated,
		Lat:           toPgFloat8Ptr(o.Lat),
		Lon:           toPgFloat8Ptr(o.Lon),
		PosAccM:       toPgFloat8Ptr(o.PosAccM),
		RepeaterClock: toPgInt8Ptr(o.RepeaterClock),
	})
	return n > 0, err
}

func (db *DB) GetCurrentDeclaredRegions(ctx context.Context, target string) (*types.ClientDeclaredRegions, error) {
	row, err := db.q.GetCurrentDeclaredRegions(ctx, target)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no declared-regions observation yet is not an error
	}
	if err != nil {
		return nil, err
	}
	return &types.ClientDeclaredRegions{
		Target:        row.Target,
		RxPubkey:      row.RxPubkey,
		ObservedAt:    row.ObservedAt,
		IngestedAt:    row.IngestedAt,
		RegionsCSV:    row.RegionsCsv,
		Truncated:     row.Truncated,
		Lat:           fromPgFloat8(row.Lat),
		Lon:           fromPgFloat8(row.Lon),
		PosAccM:       fromPgFloat8(row.PosAccM),
		RepeaterClock: fromPgInt8(row.RepeaterClock),
	}, nil
}

// PruneClientReceptions deletes coverage rows with rx_at before cutoff, and
// companion-name rows not refreshed since cutoff, in one transaction.
// Returns the number of client_receptions rows deleted.
func (db *DB) PruneClientReceptions(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	err := pgx.BeginFunc(ctx, db.pool, func(tx pgx.Tx) error {
		qtx := db.q.WithTx(tx)
		var err error
		if n, err = qtx.PruneClientReceptions(ctx, cutoff); err != nil {
			return fmt.Errorf("prune client_receptions: %w", err)
		}
		if _, err = qtx.PruneClientObservers(ctx, cutoff); err != nil {
			return fmt.Errorf("prune client_observers: %w", err)
		}
		return nil
	})
	return n, err
}

func (db *DB) PruneClientRxObservations(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneClientRxObservations(ctx, cutoff)
}

func (db *DB) PruneClientRfSamples(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneClientRfSamples(ctx, cutoff)
}

func (db *DB) PruneDeclaredRegions(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneDeclaredRegions(ctx, cutoff)
}

func (db *DB) GetClientRxObservationsCount(ctx context.Context) (int64, error) {
	return db.q.GetClientRxObservationsCount(ctx)
}
