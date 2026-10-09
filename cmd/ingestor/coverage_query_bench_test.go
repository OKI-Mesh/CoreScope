package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// coverageBenchSQL is the dominant per-node coverage query (mirrors
// cmd/server queryCoverageRows): a bbox range plus a full-key/2-3-byte-prefix
// match on the heard node. $5 is the heard node's full pubkey, used twice.
// The substr() prefix test is deliberately non-sargable: it is the baseline
// the IN-list rewrite is measured against, so don't "improve" it.
const coverageBenchSQL = `SELECT lat, lon, snr, rssi, heard_key, rx_at
	FROM client_receptions
	WHERE lat BETWEEN $1 AND $2 AND lon BETWEEN $3 AND $4
	  AND ( (heard_keylen = 32 AND heard_key = $5)
	     OR (heard_keylen IN (2,3) AND substr($5::text, 1, heard_keylen*2) = heard_key) )`

// coverageInListSQL is the sargable shape: the heard node's candidate keys are
// exactly the full pubkey and its 2/3-byte prefixes, so heard_key = ANY(...)
// can seek the heard_key-leading composite index instead of scanning the bbox.
const coverageInListSQL = `SELECT lat, lon, snr, rssi, heard_key, rx_at
	FROM client_receptions
	WHERE heard_key = ANY($1::text[]) AND lat BETWEEN $2 AND $3 AND lon BETWEEN $4 AND $5`

func BenchmarkCoverageQuery(b *testing.B) {
	const n = 1_000_000
	const prefixPool = 2000 // distinct 3-byte heard_key prefixes

	s := newTestStore(b) // the helper must take testing.TB, not *testing.T
	ctx := b.Context()

	// Everything runs in one transaction that is rolled back at the end.
	// Postgres DDL is transactional, so the index drops for the table-scan
	// baseline are undone too, and a b.Fatal can't leave 1M rows behind.
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { tx.Rollback(context.Background()) }) // b.Context() is already cancelled by now

	rng := rand.New(rand.NewSource(1))
	base := time.Now().UTC().Add(-14 * 24 * time.Hour).Truncate(time.Second)
	loadRows := pgx.CopyFromSlice(n, func(i int) ([]any, error) {
		hk := fmt.Sprintf("%06x", rng.Intn(prefixPool))
		lat := 51.0 + rng.Float64()*0.4 // ~44 km metro span
		lon := 3.5 + rng.Float64()*0.4
		rxpk := fmt.Sprintf("%064x", rng.Intn(500))
		// rx_at carries i so (rx_pubkey, heard_key, rx_at) stays unique.
		at := base.Add(time.Duration(i) * time.Second)
		return []any{rxpk, hk, int32(3), -6.0, lat, lon, at, at, "rxlog"}, nil
	})
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"client_receptions"},
		[]string{"rx_pubkey", "heard_key", "heard_keylen", "snr", "lat", "lon", "rx_at", "ingested_at", "src"},
		loadRows); err != nil {
		b.Fatal(err)
	}
	// A freshly bulk-loaded table has no statistics, and the planner needs them
	// to choose as it will in production.
	if _, err := tx.Exec(ctx, "ANALYZE client_receptions"); err != nil {
		b.Fatal(err)
	}

	// Target node whose 3-byte prefix (0003e8 = 1000) is in the pool, queried
	// over a sub-bbox of the metro area.
	target := "0003e8" + strings.Repeat("ab", 29) // 6 + 58 = 64 hex
	candidates := []string{target, target[:4], target[:6]}

	drain := func(b *testing.B, query string, args ...any) {
		b.Helper()
		r, err := tx.Query(ctx, query, args...)
		if err != nil {
			b.Fatal(err)
		}
		for r.Next() {
		}
		r.Close()
		if err := r.Err(); err != nil {
			b.Fatal(err)
		}
	}
	runOR := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			drain(b, coverageBenchSQL, 51.1, 51.3, 3.6, 3.8, target)
		}
	}
	runIN := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			drain(b, coverageInListSQL, candidates, 51.1, 51.3, 3.6, 3.8)
		}
	}

	b.Run("or_query_indexed", runOR)
	b.Run("inlist_query_indexed", runIN)

	// Drop the coverage indexes to measure the full-scan baseline.
	if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS
		idx_client_recept_heard_geo, idx_client_recept_latlon, idx_client_recept_rxpk`); err != nil {
		b.Fatal(err)
	}
	b.Run("or_query_table_scan", runOR)
	b.Run("inlist_query_table_scan", runIN)
}
