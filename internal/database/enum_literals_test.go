package database_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// The schema holds the wire values of RouteType and PayloadType as SQL
// literals, which Go constants cannot reach. These tests tie the two together:
// if a constant changes, or a literal does, they fail.
func TestEnumConstantsMatchSQLLiterals_DB(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seq := 0
	insert := func(route types.RouteType, scope *string) error {
		seq++
		_, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			RawHex:      "AABB",
			Hash:        fmt.Sprintf("enumliteral%04d", seq),
			RouteType:   route,
			PayloadType: types.PayloadGRP_TXT,
			ScopeName:   scope,
		}, at)
		return err
	}

	t.Run("route_type CHECK accepts exactly the RouteType constants", func(t *testing.T) {
		for _, route := range []types.RouteType{
			types.RouteUnknown, types.RouteTransportFlood, types.RouteFlood, types.RouteDirect, types.RouteTransportDirect,
		} {
			if err := insert(route, nil); err != nil {
				t.Errorf("route_type %d (%s) rejected: %v", route, route, err)
			}
		}
		for _, route := range []types.RouteType{-2, 4} {
			if err := insert(route, nil); err == nil {
				t.Errorf("route_type %d accepted, want it rejected", route)
			}
		}
	})

	t.Run("scope_name is accepted on exactly the transport routes", func(t *testing.T) {
		scope := "#region"
		for _, route := range []types.RouteType{types.RouteTransportFlood, types.RouteTransportDirect} {
			if !route.IsTransport() {
				t.Fatalf("%s: IsTransport = false", route)
			}
			if err := insert(route, &scope); err != nil {
				t.Errorf("scope_name on %s rejected: %v", route, err)
			}
		}
		for _, route := range []types.RouteType{types.RouteUnknown, types.RouteFlood, types.RouteDirect} {
			if route.IsTransport() {
				t.Fatalf("%s: IsTransport = true", route)
			}
			if err := insert(route, &scope); err == nil {
				t.Errorf("scope_name on %s accepted, want it rejected", route)
			}
		}
	})

	t.Run("idx_tx_channel predicate uses the GRP_TXT value", func(t *testing.T) {
		var def string
		if err := db.Pool().QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_tx_channel'`).Scan(&def); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("payload_type = %d", int(types.PayloadGRP_TXT))
		if !strings.Contains(def, want) {
			t.Errorf("idx_tx_channel = %q, want it to contain %q", def, want)
		}
	})
}
