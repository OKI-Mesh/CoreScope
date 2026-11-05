package database

import (
	"context"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func (db *DB) PruneNeighborEdges(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneNeighborEdges(ctx, cutoff)
}

// NeighborEdge is one persisted first-hop neighbor edge. NodeA and NodeB are in
// canonical order.
type NeighborEdge struct {
	NodeA    string
	NodeB    string
	Count    int64
	LastSeen time.Time // UTC
}

// NeighborObservation is one observation as the neighbor builder needs it.
type NeighborObservation struct {
	PayloadType types.PayloadType
	DecodedJSON string // only for the requested payload types, and only when from_pubkey is NULL
	FromPubkey  string
	Observer    string // observers.pubkey, "" if the observation has no observer row
	Timestamp   time.Time
	FirstHop    *string // nil when the path has no hops
	LastHop     *string
}

// NeighborEdge is an aggregated upsert: Hits observations of the pair, newest at LastSeen.
type NeighborEdgeInput struct {
	NodeA, NodeB string // canonical: NodeA <= NodeB
	Hits         int32
	LastSeen     time.Time
}

type NodeDegree struct {
	Pubkey string
	Degree int
}

func (db *DB) ListNodePublicKeys(ctx context.Context) ([]string, error) {
	return db.q.ListNodePublicKeys(ctx)
}

// LatestNeighborEdgeTime returns the newest last_seen, or the zero time when the table is empty.
func (db *DB) LatestNeighborEdgeTime(ctx context.Context) (time.Time, error) {
	t, err := db.q.LatestNeighborEdgeTime(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if t.Unix() == 0 { // the query's 'epoch' stand-in for "no rows"
		return time.Time{}, nil
	}
	return t.UTC(), nil
}

func (db *DB) ScanNeighborObservations(ctx context.Context, since time.Time, limit int32,
	jsonTypes []types.PayloadType) ([]NeighborObservation, error) {
	jt := make([]int32, len(jsonTypes))
	for i, p := range jsonTypes {
		jt[i] = int32(p)
	}
	rows, err := db.q.ScanNeighborObservations(ctx, pgdb.ScanNeighborObservationsParams{
		Since: since, BatchSize: limit, JsonTypes: jt,
	})
	if err != nil {
		return nil, err
	}
	nonEmpty := func(s string) *string { // "" is the query's way of saying "no hop"
		if s == "" {
			return nil
		}
		return &s
	}
	out := make([]NeighborObservation, len(rows))
	for i, r := range rows {
		out[i] = NeighborObservation{
			PayloadType: types.PayloadType(r.PayloadType), // no-op if the column override applied
			DecodedJSON: r.DecodedJson,
			FromPubkey:  r.FromPubkey,
			Observer:    r.ObserverPubkey,
			Timestamp:   r.Timestamp.UTC(),
			FirstHop:    nonEmpty(r.FirstHop),
			LastHop:     nonEmpty(r.LastHop),
		}
	}
	return out, nil
}

func (db *DB) UpsertNeighborEdges(ctx context.Context, edges []NeighborEdgeInput) (int64, error) {
	if len(edges) == 0 {
		return 0, nil
	}
	a, b := make([]string, len(edges)), make([]string, len(edges))
	hits, seen := make([]int32, len(edges)), make([]time.Time, len(edges))
	for i, e := range edges {
		a[i], b[i] = strings.ToLower(e.NodeA), strings.ToLower(e.NodeB) // the invariant: keys are stored lowercase
		hits[i], seen[i] = e.Hits, e.LastSeen
	}
	return db.q.UpsertNeighborEdges(ctx, pgdb.UpsertNeighborEdgesParams{
		NodeA: a, NodeB: b, Hits: hits, SeenAt: seen,
	})
}

// NeighborPair is the two endpoints of a neighbor_edges row.
type NeighborPair struct{ A, B string }

func (db *DB) ListNeighborPairs(ctx context.Context) ([]NeighborPair, error) {
	rows, err := db.q.ListNeighborPairs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NeighborPair, len(rows))
	for i, r := range rows {
		out[i] = NeighborPair{A: r.NodeA, B: r.NodeB}
	}
	return out, nil
}

func (db *DB) ListNeighborEdges(ctx context.Context) ([]NeighborEdge, error) {
	rows, err := db.q.ListNeighborEdges(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NeighborEdge, 0, len(rows))
	for _, r := range rows {
		out = append(out, NeighborEdge{
			NodeA:    r.NodeA,
			NodeB:    r.NodeB,
			Count:    int64(r.Count),
			LastSeen: r.LastSeen.UTC(),
		})
	}
	return out, nil
}

// ListNodeDegrees returns the degree of every node that appears in neighbor_edges.
func (d *DB) ListNodeDegrees(ctx context.Context) ([]NodeDegree, error) {
	rows, err := d.q.ListNodeDegrees(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NodeDegree, len(rows))
	for i, r := range rows {
		out[i] = NodeDegree{Pubkey: r.Pubkey, Degree: int(r.Degree)}
	}
	return out, nil
}

// HasNeighborEdges reports whether neighbor_edges has any rows.
func (db *DB) HasNeighborEdges(ctx context.Context) (bool, error) {
	return db.q.HasNeighborEdges(ctx)
}
