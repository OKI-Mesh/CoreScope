package database

import (
	"context"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

// LeaderboardSample is one reception with the best available receiver name.
type LeaderboardSample struct {
	RxPubkey string
	Name     string // node name, else companion name, else ""
	Lat, Lon float64
	HeardKey string
}

// ListLeaderboardSamples returns the most recent receptions at or after since,
// newest first (ties by receiver, then heard key), at most limit. A limit of
// zero returns none and a negative limit is an error.
func (db *DB) ListLeaderboardSamples(ctx context.Context, since time.Time, limit int) ([]LeaderboardSample, error) {
	if limit < 0 {
		return nil, fmt.Errorf("leaderboard sample limit must not be negative, got %d", limit)
	}
	rows, err := db.q.ListLeaderboardSamples(ctx, pgdb.ListLeaderboardSamplesParams{
		Since:   since,
		MaxRows: int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]LeaderboardSample, 0, len(rows))
	for _, r := range rows {
		out = append(out, LeaderboardSample{
			RxPubkey: r.RxPubkey,
			Name:     r.Name,
			Lat:      r.Lat,
			Lon:      r.Lon,
			HeardKey: r.HeardKey,
		})
	}
	return out, nil
}
