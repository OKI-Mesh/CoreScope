package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

// typeNRole matches the roles older code wrote for advert types MeshCore does
// not define (type-7 and friends). Those nodes are spam and are not loaded.
var typeNRole = regexp.MustCompile(`^type-[0-9]+$`)

const nodeColumns = `public_key, name, role, lat, lon, last_seen, first_seen, advert_count,
	battery_mv, temperature_c, foreign_advert, default_scope, multibyte_sup,
	multibyte_evidence, configured_scope, configured_scope_at`

type node struct {
	PublicKey         string
	Name              *string
	Role              *string
	Lat               *float64
	Lon               *float64
	LastSeen          *time.Time
	FirstSeen         *time.Time
	AdvertCount       int64
	BatteryMv         *int64
	TemperatureC      *float64
	ForeignAdvert     bool
	DefaultScope      *string
	MultibyteSup      int64
	MultibyteEvidence *string
	ConfiguredScope   *string
	ConfiguredScopeAt *time.Time
	Inactive          bool
}

// MergedWithInactive is the node a key becomes when it sits in both nodes and
// inactive_nodes: the active row wins and is active, keeps the earlier
// first_seen of the two, and keeps the larger advert_count.
func (n node) MergedWithInactive(inactive node) node {
	merged := n
	merged.Inactive = false
	merged.FirstSeen = earlier(n.FirstSeen, inactive.FirstSeen)
	if inactive.AdvertCount > merged.AdvertCount {
		merged.AdvertCount = inactive.AdvertCount
	}
	return merged
}

// LoadedNodes is what later stages need to know about the nodes load.
type LoadedNodes struct {
	// IDs maps a lowercase public key to its nodes.id.
	IDs map[string]int32
	// TypeN holds the keys skipped for a type-N role. Transmissions that
	// pointed at them get a NULL from_pubkey, like any other orphan.
	TypeN map[string]bool
}

func readNodes(ctx context.Context, src *Source, table string, inactive bool, report *Report) ([]node, error) {
	rows, err := src.db.QueryContext(ctx, "SELECT "+nodeColumns+" FROM "+table+" ORDER BY public_key")
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", table, err)
	}
	defer rows.Close()

	var nodes []node
	for rows.Next() {
		var (
			publicKey                                           string
			name, role, defaultScope, evidence, configuredScope sql.NullString
			lat, lon, temperature                               sql.NullFloat64
			lastSeen, firstSeen, configuredScopeAt              any
			advertCount, battery, foreign, multibyteSup         sql.NullInt64
		)
		if err := rows.Scan(&publicKey, &name, &role, &lat, &lon, &lastSeen, &firstSeen, &advertCount,
			&battery, &temperature, &foreign, &defaultScope, &multibyteSup,
			&evidence, &configuredScope, &configuredScopeAt); err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		nodes = append(nodes, node{
			PublicKey:         strings.ToLower(publicKey),
			Name:              cleanText(report, table+".name", publicKey, ptr(name.String, name.Valid)),
			Role:              ptr(role.String, role.Valid),
			Lat:               ptr(lat.Float64, lat.Valid),
			Lon:               ptr(lon.Float64, lon.Valid),
			LastSeen:          report.Time(table+".last_seen", lastSeen),
			FirstSeen:         report.Time(table+".first_seen", firstSeen),
			AdvertCount:       advertCount.Int64,
			BatteryMv:         ptr(battery.Int64, battery.Valid),
			TemperatureC:      ptr(temperature.Float64, temperature.Valid),
			ForeignAdvert:     foreign.Int64 != 0,
			DefaultScope:      cleanText(report, table+".default_scope", publicKey, ptr(defaultScope.String, defaultScope.Valid)),
			MultibyteSup:      multibyteSup.Int64,
			MultibyteEvidence: cleanText(report, table+".multibyte_evidence", publicKey, ptr(evidence.String, evidence.Valid)),
			ConfiguredScope:   cleanText(report, table+".configured_scope", publicKey, ptr(configuredScope.String, configuredScope.Valid)),
			ConfiguredScopeAt: report.Time(table+".configured_scope_at", configuredScopeAt),
			Inactive:          inactive,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", table, err)
	}
	return nodes, nil
}

// admit decides whether a node row is loaded. A type-N role is skipped and
// counted; any other role the table's CHECK would reject stops the load.
func admit(n node, table string, typeN map[string]bool, report *Report) (bool, error) {
	if n.Role == nil {
		return true, nil
	}
	if typeNRole.MatchString(*n.Role) {
		typeN[n.PublicKey] = true
		report.Add(fmt.Sprintf("%s: skipped, type-N role", table), 1)
		return false, nil
	}
	if !types.NodeRole(*n.Role).Valid() {
		return false, fmt.Errorf("%s: node %s has role %q, which nodes_role_valid rejects", table, n.PublicKey, *n.Role)
	}
	return true, nil
}

func loadNodes(ctx context.Context, src *Source, tx pgx.Tx, report *Report) (*LoadedNodes, error) {
	inactive, err := readNodes(ctx, src, "inactive_nodes", true, report)
	if err != nil {
		return nil, err
	}
	active, err := readNodes(ctx, src, "nodes", false, report)
	if err != nil {
		return nil, err
	}

	typeN := map[string]bool{}
	byKey := make(map[string]node, len(active)+len(inactive))

	for _, n := range inactive {
		keep, err := admit(n, "inactive_nodes", typeN, report)
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		if _, dup := byKey[n.PublicKey]; dup {
			return nil, fmt.Errorf("inactive_nodes: public key %s appears twice after lowercasing", n.PublicKey)
		}
		byKey[n.PublicKey] = n
	}

	seenActive := make(map[string]bool, len(active))
	for _, n := range active {
		keep, err := admit(n, "nodes", typeN, report)
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		if seenActive[n.PublicKey] {
			return nil, fmt.Errorf("nodes: public key %s appears twice after lowercasing", n.PublicKey)
		}
		seenActive[n.PublicKey] = true
		if other, both := byKey[n.PublicKey]; both {
			byKey[n.PublicKey] = n.MergedWithInactive(other)
			report.Add("nodes: key in both nodes and inactive_nodes, nodes row wins", 1)
			continue
		}
		byKey[n.PublicKey] = n
	}

	for key := range typeN {
		if _, loaded := byKey[key]; loaded {
			delete(typeN, key)
		}
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	ids := make(map[string]int32, len(keys))
	rows := make([][]any, 0, len(keys))
	for i, key := range keys {
		id := int32(i + 1)
		ids[key] = id
		n := byKey[key]
		rows = append(rows, []any{
			id, n.PublicKey, n.Name, n.Role, n.Lat, n.Lon, n.LastSeen, n.FirstSeen, n.AdvertCount,
			n.BatteryMv, n.TemperatureC, n.ForeignAdvert, n.DefaultScope, n.MultibyteSup,
			n.MultibyteEvidence, n.ConfiguredScope, n.ConfiguredScopeAt, n.Inactive,
		})
		if n.Inactive {
			report.Add("nodes: loaded inactive", 1)
		} else {
			report.Add("nodes: loaded active", 1)
		}
	}

	columns := []string{"id", "public_key", "name", "role", "lat", "lon", "last_seen", "first_seen", "advert_count",
		"battery_mv", "temperature_c", "foreign_advert", "default_scope", "multibyte_sup",
		"multibyte_evidence", "configured_scope", "configured_scope_at", "inactive"}
	if err := copyRows(ctx, tx, "nodes", columns, rows); err != nil {
		return nil, err
	}
	if err := resetSequence(ctx, tx, "nodes"); err != nil {
		return nil, err
	}
	return &LoadedNodes{IDs: ids, TypeN: typeN}, nil
}

// Reference resolves a transmission's from_pubkey against the loaded nodes. It
// returns the lowercase key to store, or nil with the reason when the value has
// to be NULL: the node was skipped as type-N, or no node row exists for it.
func (l *LoadedNodes) Reference(key string) (reference *string, droppedBecause string) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return nil, ""
	}
	if l.TypeN[key] {
		return nil, "from_pubkey set NULL, node skipped (type-N role)"
	}
	if _, found := l.IDs[key]; !found {
		return nil, "from_pubkey set NULL, no node row"
	}
	return &key, ""
}
