package database

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// NeighborCursor is a position in the observation scan: the scan returns
// observations strictly after (Timestamp, ID). A cursor with ID 0 therefore
// includes every observation at exactly Timestamp.
type NeighborCursor struct {
	Timestamp time.Time
	ID        int64
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
	ID          int64
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

func hopOrNil(s string) *string { // "" is the query's way of saying "no hop"
	if s == "" {
		return nil
	}
	return &s
}

// CursorAfterTime is the cursor just past every observation at or before t, so
// a scan from it returns only observations strictly after t.
func CursorAfterTime(t time.Time) NeighborCursor {
	return NeighborCursor{Timestamp: t, ID: math.MaxInt64}
}

// Cursor is the position just after this observation.
func (o NeighborObservation) Cursor() NeighborCursor {
	return NeighborCursor{Timestamp: o.Timestamp, ID: o.ID}
}

func (db *DB) PruneNeighborEdges(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneNeighborEdges(ctx, cutoff)
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

// ScanNeighborObservations returns up to limit observations after the cursor,
// oldest first. DecodedJSON is filled only for the payload types in jsonTypes
// and only when the transmission has no from_pubkey. To page, pass the last
// observation's Cursor() back in. A negative limit is an error; zero returns
// nothing.
func (db *DB) ScanNeighborObservations(ctx context.Context, after NeighborCursor, limit int,
	jsonTypes []types.PayloadType) ([]NeighborObservation, error) {
	if limit < 0 {
		return nil, fmt.Errorf("neighbor scan limit must not be negative, got %d", limit)
	}
	jt := make([]int32, len(jsonTypes))
	for i, p := range jsonTypes {
		jt[i] = int32(p)
	}
	rows, err := db.q.ScanNeighborObservations(ctx, pgdb.ScanNeighborObservationsParams{
		AfterTimestamp: after.Timestamp,
		AfterID:        after.ID,
		BatchSize:      int32(limit),
		JsonTypes:      jt,
	})
	if err != nil {
		return nil, err
	}
	out := make([]NeighborObservation, len(rows))
	for i, r := range rows {
		out[i] = NeighborObservation{
			ID:          r.ID,
			PayloadType: types.PayloadType(r.PayloadType),
			DecodedJSON: r.DecodedJson,
			FromPubkey:  r.FromPubkey,
			Observer:    r.ObserverPubkey,
			Timestamp:   r.Timestamp.UTC(),
			FirstHop:    hopOrNil(r.FirstHop),
			LastHop:     hopOrNil(r.LastHop),
		}
	}
	return out, nil
}

// UpsertNeighborEdges adds each pair's hits to its stored count and moves its
// last_seen forward. Keys are lowercased and each pair is put in canonical
// order (NodeA <= NodeB); duplicate pairs within one call are merged first. It
// returns the number of distinct pairs written.
func (db *DB) UpsertNeighborEdges(ctx context.Context, edges []NeighborEdgeInput) (int64, error) {
	if len(edges) == 0 {
		return 0, nil
	}
	type pair struct{ a, b string }
	index := make(map[pair]int, len(edges))
	var a, b []string
	var hits []int32
	var seen []time.Time
	for _, e := range edges {
		p := pair{normalizeKey(e.NodeA), normalizeKey(e.NodeB)}
		if p.a > p.b {
			p.a, p.b = p.b, p.a
		}
		if i, ok := index[p]; ok {
			hits[i] += e.Hits
			if e.LastSeen.After(seen[i]) {
				seen[i] = e.LastSeen
			}
			continue
		}
		index[p] = len(a)
		a = append(a, p.a)
		b = append(b, p.b)
		hits = append(hits, e.Hits)
		seen = append(seen, e.LastSeen)
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
