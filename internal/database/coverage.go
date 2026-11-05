package database

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

type CoverageQuery struct {
	HeardKeys                      []string
	RxPubkey                       string
	Since                          *time.Time
	MinLat, MaxLat, MinLon, MaxLon float64
}

type CoveragePoint struct {
	Lat, Lon float64
	SNR      *float64
	RSSI     *int
	HeardKey string
	RxAt     time.Time
}

func (db *DB) ListCoverageRows(ctx context.Context, q CoverageQuery) ([]CoveragePoint, error) {
	rxPubkey := pgtype.Text{String: q.RxPubkey, Valid: q.RxPubkey != ""}
	since := pgtype.Timestamptz{}
	if q.Since != nil {
		since = pgtype.Timestamptz{Time: q.Since.UTC(), Valid: true}
	}

	if len(q.HeardKeys) == 0 {
		rows, err := db.q.ListCoverageRowsAnyNode(ctx, pgdb.ListCoverageRowsAnyNodeParams{
			MinLat:   q.MinLat,
			MaxLat:   q.MaxLat,
			MinLon:   q.MinLon,
			MaxLon:   q.MaxLon,
			RxPubkey: rxPubkey,
			Since:    since,
		})
		if err != nil {
			return nil, err
		}
		out := make([]CoveragePoint, 0, len(rows))
		for _, r := range rows {
			out = append(out, CoveragePoint{
				Lat:      r.Lat,
				Lon:      r.Lon,
				SNR:      fromPgFloat8(r.Snr),
				RSSI:     fromPgInt4(r.Rssi),
				HeardKey: strings.ToLower(r.HeardKey),
				RxAt:     r.RxAt.UTC(),
			})
		}
		return out, nil
	}

	rows, err := db.q.ListCoverageRows(ctx, pgdb.ListCoverageRowsParams{
		HeardKeys: q.HeardKeys,
		MinLat:    q.MinLat,
		MaxLat:    q.MaxLat,
		MinLon:    q.MinLon,
		MaxLon:    q.MaxLon,
		RxPubkey:  rxPubkey,
		Since:     since,
	})
	if err != nil {
		return nil, err
	}
	out := make([]CoveragePoint, 0, len(rows))
	for _, r := range rows {
		out = append(out, CoveragePoint{
			Lat:      r.Lat,
			Lon:      r.Lon,
			SNR:      fromPgFloat8(r.Snr),
			RSSI:     fromPgInt4(r.Rssi),
			HeardKey: strings.ToLower(r.HeardKey),
			RxAt:     r.RxAt.UTC(),
		})
	}
	return out, nil
}

func (db *DB) CountMobileReceptions(ctx context.Context, heardKeys []string) (receptions, clients int, err error) {
	r, err := db.q.CountMobileReceptions(ctx, heardKeys)
	if err != nil {
		return 0, 0, err
	}
	return int(r.Receptions), int(r.Clients), nil
}
