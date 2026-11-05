package database

import (
	"context"
	"time"
)

// LeaderboardSample is one reception with the best available receiver name.
type LeaderboardSample struct {
	RxPubkey string
	Name     string // node name, else companion name, else ""
	Lat, Lon float64
	HeardKey string
}

// EachLeaderboardSample streams the most recent receptions at or after since
// (newest first, at most limit) to fn, without holding them all in memory.
// It holds one pooled connection until the scan ends, so fn must not call the
// database. A non-nil error from fn stops the scan and is returned.
func (db *DB) EachLeaderboardSample(ctx context.Context, since time.Time, limit int, fn func(LeaderboardSample) error) error {
	rows, err := db.pool.Query(ctx, `
		SELECT cr.rx_pubkey,
		       COALESCE(NULLIF(n.name, ''), NULLIF(co.name, ''), '')::text,
		       cr.lat, cr.lon, cr.heard_key
		FROM client_receptions cr
		LEFT JOIN nodes n ON n.public_key = cr.rx_pubkey
		LEFT JOIN client_observers co ON co.pubkey = cr.rx_pubkey
		WHERE cr.rx_at >= $1
		ORDER BY cr.rx_at DESC
		LIMIT $2`, since, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s LeaderboardSample
		if err := rows.Scan(&s.RxPubkey, &s.Name, &s.Lat, &s.Lon, &s.HeardKey); err != nil {
			return err
		}
		if err := fn(s); err != nil {
			return err
		}
	}
	return rows.Err()
}
