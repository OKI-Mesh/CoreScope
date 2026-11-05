package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgconn"
)

func upsertNode(ctx context.Context, s *Store, pubKey, name string, role types.NodeRole, lat, lon *float64, lastSeen time.Time) error {
	return s.db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubKey,
		Name:      name,
		Role:      role,
		Lat:       lat,
		Lon:       lon,
		RxTime:    lastSeen,
		IngestNow: time.Now().UTC(),
	})
}

func TestDeleteNodesByPubkeysRemovesNodesAndTheirEdges(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	const a, b, c = "aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"
	for _, pk := range []string{a, b, c} {
		seedNeighborNode(t, s, pk)
	}
	testExec(t, s, `INSERT INTO neighbor_edges (node_a, node_b, count, last_seen)
		VALUES ($1, $2, 1, $4), ($2, $3, 1, $4)`, a, b, c, time.Now().UTC())

	// One uppercase key (matched after lowercasing) and one unknown key.
	n, err := s.DeleteNodesByPubkeys(ctx, []string{"AAAAAAAAAA", "dddddddddd"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1 (the unknown key matches nothing)", n)
	}
	if countWhere(t, s, "nodes", "public_key = $1", a) != 0 || countRows(t, s, "nodes") != 2 {
		t.Error("only node a should be gone")
	}
	// a|b went with node a. b|c survives.
	if got := countRows(t, s, "neighbor_edges"); got != 1 {
		t.Errorf("neighbor_edges = %d, want 1", got)
	}

	if n, err := s.DeleteNodesByPubkeys(ctx, nil); err != nil || n != 0 {
		t.Errorf("empty input: n=%d err=%v, want 0 and nil", n, err)
	}
}

// #1483: the server's key lookup relies on stored public_key being lowercase.
// There are no legacy rows to repair on Postgres: UpsertNode lowercases on
// write, the ETL lowercases on load, and a CHECK constraint backs both.
func TestUpsertNodeStoresLowercaseKey(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	const upper = "AABBCCDDEEFF11223344"
	lower := strings.ToLower(upper)

	if err := upsertNode(ctx, s, upper, "mixed-case-node", "companion", nil, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if n := countWhere(t, s, "nodes", "public_key = $1", lower); n != 1 {
		t.Fatalf("lowercase rows = %d, want 1", n)
	}
	if n := countWhere(t, s, "nodes", "public_key = $1", upper); n != 0 {
		t.Fatalf("uppercase rows = %d, want 0", n)
	}
	// The same key in the other case is the same node, not a duplicate.
	if err := upsertNode(ctx, s, lower, "renamed", "companion", nil, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, s, "nodes"); n != 1 {
		t.Fatalf("nodes = %d, want 1", n)
	}
}

func TestNodesRejectUppercasePublicKey(t *testing.T) {
	s := newTestStore(t)
	// Bypasses UpsertNode, as the old test did, to prove the schema holds
	// even when a writer forgets to lowercase.
	_, err := s.db.Pool().Exec(testCtx(t),
		`INSERT INTO nodes (public_key, name, role, last_seen, first_seen)
		 VALUES ($1, 'mixed-case-node', 'companion', $2, $2)`,
		"AABBCCDDEEFF11223344", time.Now().UTC())

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("err = %v, want a check violation (23514)", err)
	}
}
