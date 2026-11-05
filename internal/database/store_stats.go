package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

// EntityCounts are the node and observer totals in the store stats.
type EntityCounts struct {
	ActiveNodes int
	AllNodes    int
	Observers   int
}

// GetEntityCounts counts nodes heard after activeSince, all nodes, and
// observers that are not inactive.
func (db *DB) GetEntityCounts(ctx context.Context, activeSince time.Time) (EntityCounts, error) {
	r, err := db.q.GetEntityCounts(ctx, activeSince)
	if err != nil {
		return EntityCounts{}, err
	}
	return EntityCounts{
		ActiveNodes: int(r.ActiveNodes),
		AllNodes:    int(r.AllNodes),
		Observers:   int(r.Observers),
	}, nil
}

// RecentObservationCounts are the observation totals for the last hour and day.
type RecentObservationCounts struct {
	LastHour int
	Last24h  int
}

// CountRecentObservations counts observations in the hour and the day before now.
func (db *DB) CountRecentObservations(ctx context.Context, now time.Time) (RecentObservationCounts, error) {
	r, err := db.q.CountRecentObservations(ctx, pgdb.CountRecentObservationsParams{
		HourAgo: now.Add(-time.Hour),
		DayAgo:  now.Add(-24 * time.Hour),
	})
	if err != nil {
		return RecentObservationCounts{}, err
	}
	return RecentObservationCounts{LastHour: int(r.LastHour), Last24h: int(r.LastDay)}, nil
}
