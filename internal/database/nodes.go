package database

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgtype"
)

type NodeInput struct {
	PublicKey string
	Name      string
	Role      string
	Lat, Lon  *float64
	RxTime    time.Time // envelope/receive time
	IngestNow time.Time // wall clock; caps how far last_seen may advance
}

func (db *DB) CreateOrUpdateNode(ctx context.Context, n NodeInput) error {

	if n.IngestNow.IsZero() {
		n.IngestNow = time.Now().UTC()
	}
	rx := n.RxTime
	if rx.IsZero() {
		rx = n.IngestNow // zero means "no envelope time": use ingest time
	}

	n.PublicKey = strings.ToLower(strings.TrimSpace(n.PublicKey))
	if n.PublicKey == "" {
		return errors.New("node public key is empty")
	}

	return db.q.CreateOrUpdateNode(ctx, pgdb.CreateOrUpdateNodeParams{
		PublicKey: n.PublicKey,
		Name:      toPgTextIfNotEmpty(n.Name),
		Role:      toPgTextIfNotEmpty(n.Role),
		Lat:       toPgFloat8Ptr(n.Lat),
		Lon:       toPgFloat8Ptr(n.Lon),
		RxTime:    rx,
		IngestNow: n.IngestNow,
	})
}

func (db *DB) IncrementNodeAdvertCount(ctx context.Context, pubKey string) error {
	err := db.q.IncrementNodeAdvertCount(ctx, pubKey)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) TouchNodeLastSeen(ctx context.Context, pubkey string, rxTime time.Time) (int64, error) {
	return db.q.UpdateNodeLastSeen(ctx, pgdb.UpdateNodeLastSeenParams{
		LastSeen:  pgtype.Timestamptz{Time: rxTime, Valid: true},
		PublicKey: pubkey,
	})
}

// database — pure persistence, plain-Go in, pgtype internal
func (db *DB) MarkStaleNodesInactive(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.MarkStaleNodesInactive(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

func (db *DB) UpdateBatteryVoltageAndTempC(ctx context.Context, pubKey string, bv *int, tc *float64) error {
	err := db.q.UpdateBatteryVoltageAndTempC(ctx, pgdb.UpdateBatteryVoltageAndTempCParams{
		BatteryMv:    toPgInt4Ptr(bv),
		TemperatureC: toPgFloat8Ptr(tc),
		PublicKey:    pubKey,
	})
	return err
}

func (db *DB) SetForeignAdvert(ctx context.Context, pubKey string) error {
	return db.q.SetForeignAdvert(ctx, pubKey)
}

func (db *DB) UpdateNodeConfiguredScope(ctx context.Context, pubkey, scope string, reportedAt *time.Time) error {
	var at pgtype.Timestamptz
	if reportedAt != nil {
		at = pgtype.Timestamptz{Time: *reportedAt, Valid: true}
	}
	return db.q.UpdateNodeConfiguredScope(ctx, pgdb.UpdateNodeConfiguredScopeParams{
		PublicKey:  pubkey,
		Scope:      scope,
		ReportedAt: at,
	})
}

func (db *DB) UpdateNodeDefaultScope(ctx context.Context, pubkey, scope string) error {
	return db.q.UpdateNodeDefaultScope(ctx, pgdb.UpdateNodeDefaultScopeParams{
		PublicKey: pubkey,
		Scope:     scope,
	})
}

// NodeMultibyteCap is one node's multibyte-capability result, as computed by
// the analytics cycle. Used only by UpdateNodeMultibyteCaps.
type NodeMultibyteCap struct {
	PublicKey string
	Sup       int32 // 1 = suspected, 2 = confirmed; 0 (unknown) is never persisted
	Evidence  string
}

// UpdateNodeMultibyteCaps applies the capabilities in one statement and returns
// how many rows actually changed. A node that doesn't exist is skipped, and a
// node whose stored value already matches is left untouched.
func (db *DB) UpdateNodeMultibyteCaps(ctx context.Context, caps []NodeMultibyteCap) (int64, error) {
	if len(caps) == 0 {
		return 0, nil
	}
	keys := make([]string, len(caps))
	sups := make([]int32, len(caps))
	evid := make([]string, len(caps))
	for i, c := range caps {
		keys[i], sups[i], evid[i] = c.PublicKey, c.Sup, c.Evidence
	}
	return db.q.UpdateNodeMultibyteCaps(ctx, pgdb.UpdateNodeMultibyteCapsParams{
		Pubkeys:  keys,
		Sups:     sups,
		Evidence: evid,
	})
}

func (db *DB) DeleteNodesByPubkeys(ctx context.Context, pubkeys []string) (int64, error) {
	if len(pubkeys) == 0 {
		return 0, nil
	}
	return db.q.DeleteNodesByPubkeys(ctx, pubkeys)
}
