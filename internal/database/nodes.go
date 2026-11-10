package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/geofilter"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

type NodeSort string

const (
	NodeSortLastSeen    NodeSort = "lastSeen" // default
	NodeSortName        NodeSort = "name"
	NodeSortPacketCount NodeSort = "packetCount"
)

type NodeListParams struct {
	Limit               int // <= 0 means 50
	Offset              int
	Role                string     // "" = any
	Search              string     // "" = none; case-insensitive substring of name
	Before              *time.Time // first_seen <= Before
	HeardSince          *time.Time // last_seen > HeardSince
	Regions             []string   // normalized upper-case IATA codes; empty = any region
	Sort                NodeSort   // unknown values fall back to last_seen
	ExcludeKeys         []string   // lowercase public keys to hide (blacklist)
	ExcludeNamePrefixes []string   // hide nodes whose name starts with any of these (#1181)
}

type NodeInput struct {
	PublicKey string
	Name      string
	Role      types.NodeRole
	Lat, Lon  *float64
	RxTime    time.Time // envelope/receive time
	IngestNow time.Time // wall clock; caps how far last_seen may advance
}

type NodeLocation struct {
	PublicKey string
	Lat, Lon  float64
}

type RoleCounts struct {
	Repeaters  int
	Rooms      int
	Companions int
	Sensors    int
}

// Node is a row from the nodes table.
type Node struct {
	PublicKey         string
	Name              *string
	Role              *types.NodeRole
	Lat               *float64
	Lon               *float64
	LastSeen          time.Time
	FirstSeen         time.Time
	AdvertCount       int
	BatteryMv         *int
	TemperatureC      *float64
	Foreign           bool // foreign_advert: ingestor's geo_filter rejected the advert (#730)
	DefaultScope      *string
	ConfiguredScope   *string
	ConfiguredScopeAt *time.Time // verify the column type (string in SQLite)
}

type MultibyteCapNode struct {
	Pubkey   string
	Name     string
	Role     types.NodeRole
	LastSeen time.Time
	Sup      int
	Evidence string
}

// GeoPruneNode holds the minimal fields needed for geo-filter pruning.
type GeoPruneNode struct {
	Pubkey string
	Name   string // "" when the node has no name
	Lat    *float64
	Lon    *float64
}

type MultiByteCap struct {
	Status      types.MultibyteSupport
	Evidence    *types.MultibyteEvidence // nil = no evidence
	MaxHashSize int
}

// NodeIdentity is the display identity of a node. Name and Role are nil when
// the column is NULL.
type NodeIdentity struct {
	Name *string
	Role *types.NodeRole
}

// normalizeKey is the stored form of a public key.
func normalizeKey(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// normalizeKeys returns the stored form of each public key.
// normalizeKeys returns the stored form of each public key; nil stays nil.
func normalizeKeys(keys []string) []string {
	if keys == nil {
		return nil
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = normalizeKey(k)
	}
	return out
}

// CreateOrUpdateNode inserts a node or updates its name, role, position and
// last_seen. last_seen never advances past IngestNow.
func (db *DB) CreateOrUpdateNode(ctx context.Context, n NodeInput) error {
	if n.IngestNow.IsZero() {
		n.IngestNow = time.Now().UTC()
	}
	rx := n.RxTime
	if rx.IsZero() {
		rx = n.IngestNow // zero means "no envelope time": use ingest time
	}

	n.PublicKey = normalizeKey(n.PublicKey)
	if n.PublicKey == "" {
		return errors.New("node public key is empty")
	}
	var role *types.NodeRole
	if n.Role != "" {
		role = &n.Role
	}

	return db.q.CreateOrUpdateNode(ctx, pgdb.CreateOrUpdateNodeParams{
		PublicKey: n.PublicKey,
		Name:      toPgTextIfNotEmpty(n.Name),
		Role:      role,
		Lat:       toPgFloat8Ptr(n.Lat),
		Lon:       toPgFloat8Ptr(n.Lon),
		RxTime:    rx,
		IngestNow: n.IngestNow,
	})
}

func (db *DB) IncrementNodeAdvertCount(ctx context.Context, pubKey string) error {
	return db.q.IncrementNodeAdvertCount(ctx, normalizeKey(pubKey))
}

func (db *DB) TouchNodeLastSeen(ctx context.Context, pubkey string, rxTime time.Time) (int64, error) {
	return db.q.UpdateNodeLastSeen(ctx, pgdb.UpdateNodeLastSeenParams{
		LastSeen:  rxTime,
		PublicKey: normalizeKey(pubkey),
	})
}

// database — pure persistence, plain-Go in, pgtype internal
func (db *DB) MarkStaleNodesInactive(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.MarkStaleNodesInactive(ctx, cutoff)
}

func (db *DB) UpdateBatteryVoltageAndTempC(ctx context.Context, pubKey string, bv *int, tc *float64) error {
	return db.q.UpdateBatteryVoltageAndTempC(ctx, pgdb.UpdateBatteryVoltageAndTempCParams{
		BatteryMv:    toPgInt4Ptr(bv),
		TemperatureC: toPgFloat8Ptr(tc),
		PublicKey:    normalizeKey(pubKey),
	})
}

func (db *DB) SetForeignAdvert(ctx context.Context, pubKey string) error {
	return db.q.SetForeignAdvert(ctx, normalizeKey(pubKey))
}

func (db *DB) UpdateNodeConfiguredScope(ctx context.Context, pubkey, scope string, reportedAt *time.Time) error {
	var at pgtype.Timestamptz
	if reportedAt != nil {
		at = pgtype.Timestamptz{Time: *reportedAt, Valid: true}
	}
	return db.q.UpdateNodeConfiguredScope(ctx, pgdb.UpdateNodeConfiguredScopeParams{
		PublicKey:  normalizeKey(pubkey),
		Scope:      scope,
		ReportedAt: at,
	})
}

func (db *DB) UpdateNodeDefaultScope(ctx context.Context, pubkey, scope string) error {
	return db.q.UpdateNodeDefaultScope(ctx, pgdb.UpdateNodeDefaultScopeParams{
		PublicKey: normalizeKey(pubkey),
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
		keys[i], sups[i], evid[i] = normalizeKey(c.PublicKey), c.Sup, c.Evidence
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
	return db.q.DeleteNodesByPubkeys(ctx, normalizeKeys(pubkeys))
}

// ListLocatedNodes returns every node that has GPS coordinates.
func (db *DB) ListLocatedNodes(ctx context.Context) ([]NodeLocation, error) {
	rows, err := db.q.ListLocatedNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list located nodes: %w", err)
	}
	out := make([]NodeLocation, len(rows))
	for i, r := range rows {
		out[i] = NodeLocation{PublicKey: r.PublicKey, Lat: r.Lat, Lon: r.Lon}
	}
	return out, nil
}

func (db *DB) ListMultibyteCapNodes(ctx context.Context) ([]MultibyteCapNode, error) {
	rows, err := db.q.ListMultibyteCapNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MultibyteCapNode, 0, len(rows))
	for _, r := range rows {
		n := MultibyteCapNode{
			Pubkey:   r.PublicKey,
			Name:     r.Name,
			Sup:      int(r.MultibyteSup),
			Evidence: r.MultibyteEvidence,
			LastSeen: r.LastSeen.UTC(),
		}
		if r.Role != nil {
			n.Role = *r.Role
		}
		out = append(out, n)
	}
	return out, nil
}

type NodeLocationWithRole struct {
	Pubkey string
	Lat    *float64 // nil when the node has no coordinates
	Lon    *float64
	Role   *types.NodeRole // nil when the role is unset
}

// GetNodeLocationsByKeys returns location data only for the given public keys.
// This avoids fetching ALL nodes when only a few keys need to be matched.
// Nodes without coordinates are included (with nil Lat/Lon); unknown keys are absent.
func (db *DB) GetNodeLocationsByKeys(ctx context.Context, keys []string) (map[string]NodeLocationWithRole, error) {
	result := make(map[string]NodeLocationWithRole)
	if len(keys) == 0 {
		return result, nil
	}

	rows, err := db.q.ListNodeLocationsByKeys(ctx, normalizeKeys(keys))
	if err != nil {
		return nil, fmt.Errorf("node locations by keys: %w", err)
	}
	for _, r := range rows {
		result[r.PublicKey] = NodeLocationWithRole{
			Pubkey: r.PublicKey,
			Lat:    fromPgFloat8(r.Lat),
			Lon:    fromPgFloat8(r.Lon),
			Role:   r.Role,
		}
	}
	return result, nil
}

// GetRoleCounts returns the node count per role. With since == nil it counts
// every node (matching Node.js /api/nodes); otherwise only nodes with
// last_seen after *since (the 7-day active window used by /api/stats).
// GetRoleCounts counts nodes seen in the 7 days before now (matches /api/stats).
func (d *DB) GetRoleCounts(ctx context.Context, now time.Time) (RoleCounts, error) {
	since := now.Add(-7 * 24 * time.Hour)
	return d.roleCounts(ctx, &since)
}

// GetAllRoleCounts counts every node, no time filter (matches /api/nodes).
func (d *DB) GetAllRoleCounts(ctx context.Context) (RoleCounts, error) {
	return d.roleCounts(ctx, nil)
}

func (d *DB) roleCounts(ctx context.Context, activeSince *time.Time) (RoleCounts, error) {
	var since pgtype.Timestamptz // zero value is Valid=false, sent as SQL NULL
	if activeSince != nil {
		since = pgtype.Timestamptz{Time: *activeSince, Valid: true}
	}
	rows, err := d.q.CountNodesByRole(ctx, since)
	if err != nil {
		return RoleCounts{}, fmt.Errorf("count nodes by role: %w", err)
	}
	var rc RoleCounts
	for _, r := range rows {
		switch r.Role {
		case "repeater":
			rc.Repeaters = int(r.N)
		case "room":
			rc.Rooms = int(r.N)
		case "companion":
			rc.Companions = int(r.N)
		case "sensor":
			rc.Sensors = int(r.N)
		}
	}
	return rc, nil
}

// ListNodes returns a filtered, sorted page of nodes and the total number of
// nodes matching the filters (ignoring Limit and Offset).
func (db *DB) ListNodes(ctx context.Context, p NodeListParams) ([]Node, int64, error) {
	if p.Limit <= 0 {
		p.Limit = 50
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	excludeKeys := normalizeKeys(p.ExcludeKeys)
	sort := string(NodeSortLastSeen)
	switch p.Sort {
	case NodeSortName, NodeSortPacketCount:
		sort = string(p.Sort)
	}

	var role, search *string
	if p.Role != "" {
		role = &p.Role
	}
	if p.Search != "" {
		search = &p.Search
	}

	hiddenPrefixes := make([]string, 0, len(p.ExcludeNamePrefixes))
	for _, prefix := range p.ExcludeNamePrefixes {
		if trimmed := strings.TrimSpace(prefix); trimmed != "" {
			hiddenPrefixes = append(hiddenPrefixes, trimmed)
		}
	}

	total, err := db.q.CountNodes(ctx, pgdb.CountNodesParams{
		Role:           toPgTextPtr(role),
		Search:         toPgTextPtr(search),
		Before:         toPgTimestamptzPtr(p.Before),
		HeardSince:     toPgTimestamptzPtr(p.HeardSince),
		Regions:        p.Regions,
		ExcludeKeys:    excludeKeys,
		HiddenPrefixes: hiddenPrefixes,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("count nodes: %w", err)
	}

	rows, err := db.q.ListNodes(ctx, pgdb.ListNodesParams{
		Role:           toPgTextPtr(role),
		Search:         toPgTextPtr(search),
		Before:         toPgTimestamptzPtr(p.Before),
		HeardSince:     toPgTimestamptzPtr(p.HeardSince),
		Regions:        p.Regions,
		ExcludeKeys:    excludeKeys,
		HiddenPrefixes: hiddenPrefixes,
		Sort:           sort,
		RowLimit:       int32(p.Limit),
		RowOffset:      int32(p.Offset),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list nodes: %w", err)
	}

	nodes := make([]Node, 0, len(rows))
	for _, r := range rows {
		nodes = append(nodes, nodeFromRow(r))
	}
	return nodes, total, nil
}

func nodeFromRow(r pgdb.Node) Node {
	return Node{
		PublicKey:         r.PublicKey,
		Name:              fromPgText(r.Name),
		Role:              r.Role,
		Lat:               fromPgFloat8(r.Lat),
		Lon:               fromPgFloat8(r.Lon),
		LastSeen:          r.LastSeen.UTC(),
		FirstSeen:         r.FirstSeen.UTC(),
		AdvertCount:       int(r.AdvertCount),
		BatteryMv:         fromPgInt4(r.BatteryMv),
		TemperatureC:      fromPgFloat8(r.TemperatureC),
		Foreign:           r.ForeignAdvert, // adjust to the column type
		DefaultScope:      fromPgText(r.DefaultScope),
		ConfiguredScope:   fromPgText(r.ConfiguredScope),
		ConfiguredScopeAt: utcPtr(fromPgTimestamptz(r.ConfiguredScopeAt)),
	}
}

type NodePosition struct {
	PublicKey string
	Lat, Lon  float64
}

// ListNodePositions returns every node that has a real GPS position.
func (db *DB) ListNodePositions(ctx context.Context) ([]NodePosition, error) {
	rows, err := db.q.ListNodeCoordinates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node positions: %w", err)
	}
	out := make([]NodePosition, 0, len(rows))
	for _, r := range rows {
		out = append(out, NodePosition{
			PublicKey: r.PublicKey,
			Lat:       r.Lat,
			Lon:       r.Lon,
		})
	}
	return out, nil
}

// GetNodePubkeysInArea returns public keys of nodes whose GPS coordinates pass the
// given geo filter (a polygon or a bounding box).
func (d *DB) GetNodePubkeysInArea(ctx context.Context, gf *geofilter.Config) ([]string, error) {
	rows, err := d.q.ListNodeCoordinates(ctx)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, r := range rows {
		if geofilter.PassesFilter(r.Lat, r.Lon, gf) {
			result = append(result, r.PublicKey)
		}
	}
	return result, nil
}

// SearchNodes searches nodes by name substring or public-key prefix,
// most recently seen first. limit <= 0 means 10.
func (db *DB) SearchNodes(ctx context.Context, query string, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := db.q.SearchNodes(ctx, pgdb.SearchNodesParams{
		Query:    query,
		RowLimit: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("search nodes: %w", err)
	}
	nodes := make([]Node, 0, len(rows))
	for _, r := range rows {
		nodes = append(nodes, nodeFromRow(r))
	}
	return nodes, nil
}

// GetNodeByPrefix resolves a hex prefix (>= 8 chars) to a unique node.
// It returns (node, ambiguous, err): when several nodes share the prefix it
// returns (nil, true, nil); when none do, or the prefix is too short or not
// hex, (nil, false, nil). Used by the short-URL feature (issue #772).
//
// Trade-off vs an opaque ID lookup table: prefixes are stable across
// restarts, self-describing (no allocator needed), and resolve to the
// authoritative pubkey on the server. Cost: ambiguity grows with the node
// directory; we mitigate with a hard 8-hex-char (32-bit) minimum and surface
// 409 Conflict when collisions occur.
func (db *DB) GetNodeByPrefix(ctx context.Context, prefix string, minHexLen int) (*Node, bool, error) {
	if len(prefix) < minHexLen {
		return nil, false, nil
	}
	for _, c := range prefix {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return nil, false, nil
		}
	}
	rows, err := db.q.FindNodesByKeyPrefix(ctx, normalizeKey(prefix))
	if err != nil {
		return nil, false, fmt.Errorf("find node by prefix: %w", err)
	}
	switch len(rows) {
	case 0:
		return nil, false, nil
	case 1:
		n := nodeFromRow(rows[0])
		return &n, false, nil
	default:
		return nil, true, nil
	}
}

// GetNodeByPubkey returns a single node, or ErrNotFound when there is none.
func (db *DB) GetNodeByPubkey(ctx context.Context, pubkey string) (*Node, error) {
	row, err := db.q.GetNodeByPublicKey(ctx, normalizeKey(pubkey))
	if err != nil {
		if err = notFound(err); errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("get node %s: %w", pubkey, err)
	}
	n := nodeFromRow(row)
	return &n, nil
}

// NodeHealth is the minimum a node contributes to the network-health summary.
type NodeHealth struct {
	Role     types.NodeRole // "unknown" when the node has no role
	LastSeen time.Time
}

func (db *DB) ListNodeHealth(ctx context.Context) ([]NodeHealth, error) {
	rows, err := db.q.ListNodeHealth(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node health: %w", err)
	}
	out := make([]NodeHealth, len(rows))
	for i, r := range rows {
		role := types.RoleUnknown
		if r.Role != nil {
			role = *r.Role
		}
		out[i] = NodeHealth{Role: role, LastSeen: r.LastSeen.UTC()}
	}
	return out, nil
}

// ListNodesForGeoPrune returns every node with its coordinates for geo-filter evaluation.
func (db *DB) ListNodesForGeoPrune(ctx context.Context) ([]GeoPruneNode, error) {
	rows, err := db.q.ListNodesForGeoPrune(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]GeoPruneNode, 0, len(rows))
	for _, r := range rows {
		out = append(out, GeoPruneNode{
			Pubkey: r.PublicKey,
			Name:   r.Name,
			Lat:    fromPgFloat8(r.Lat),
			Lon:    fromPgFloat8(r.Lon),
		})
	}
	return out, nil
}

// NodeMatch summarizes the nodes whose public key starts with a prefix.
// Pubkey and Name are only meaningful when Matches == 1.
type NodeMatch struct {
	Pubkey  string
	Name    string
	Matches int
}

// MatchNodesByPrefix maps each lowercase hex prefix to its matches. Prefixes
// with no node are absent from the map.
func (db *DB) MatchNodesByPrefix(ctx context.Context, prefixes []string) (map[string]NodeMatch, error) {
	prefixes = normalizeKeys(prefixes)
	rows, err := db.q.MatchNodesByPrefix(ctx, prefixes)
	if err != nil {
		return nil, err
	}
	out := make(map[string]NodeMatch, len(rows))
	for _, r := range rows {
		out[r.Prefix] = NodeMatch{Pubkey: r.PublicKey, Name: r.Name, Matches: int(r.Matches)}
	}
	return out, nil
}

// ListNodeIdentities resolves name and role for pubkeys in one query, keyed by
// lowercase pubkey. A pubkey with no nodes row (deleted, pruned) is absent from
// the map; callers treat a missing entry as the zero NodeIdentity.
func (db *DB) ListNodeIdentities(ctx context.Context, pubkeys []string) (map[string]NodeIdentity, error) {
	out := make(map[string]NodeIdentity, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := db.q.ListNodeIdentities(ctx, normalizeKeys(pubkeys))
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[normalizeKey(r.PublicKey)] = NodeIdentity{
			Name: fromPgText(r.Name),
			Role: r.Role,
		}
	}
	return out, nil
}

// ResolveNodePubkeys maps each pubkey-or-name in inputs to a node's public key.
// An input that matches no node maps to itself, so callers can look up by
// whatever the client sent.
func (db *DB) ResolveNodePubkeys(ctx context.Context, inputs []string) (map[string]string, error) {
	out := make(map[string]string, len(inputs))
	for _, in := range inputs {
		out[in] = in
	}
	if len(inputs) == 0 {
		return out, nil
	}
	rows, err := db.q.ResolveNodePubkeys(ctx, inputs)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.Input] = r.PublicKey
	}
	return out, nil
}

// ResolveNodePubkey is ResolveNodePubkeys for one input.
func (db *DB) ResolveNodePubkey(ctx context.Context, input string) (string, error) {
	m, err := db.ResolveNodePubkeys(ctx, []string{input})
	if err != nil {
		return "", err
	}
	return m[input], nil
}

// ListAllNodes returns every node, ordered by public key.
func (db *DB) ListAllNodes(ctx context.Context) ([]Node, error) {
	rows, err := db.q.ListAllNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(rows))
	for _, r := range rows {
		nodes = append(nodes, nodeFromRow(r))
	}
	return nodes, nil
}
