package database_test

import (
	"fmt"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func TestListDroppedPackets_LimitDefaultsAndClamps_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	for i := range 501 {
		dp := database.DroppedPacket{Hash: fmt.Sprintf("h%03d", i), Reason: "bad_hash"}
		if err := db.InsertDroppedPacket(ctx, dp); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name  string
		limit int
		want  int
	}{
		{"zero uses the default", 0, 100},
		{"negative uses the default", -1, 100},
		{"within range is honored", 250, 250},
		{"maximum is honored", 500, 500},
		{"above the maximum clamps to it", 1000, 500},
	}
	for _, c := range cases {
		got, err := db.ListDroppedPackets(ctx, c.limit, "", "")
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(got) != c.want {
			t.Errorf("%s: want %d rows, got %d", c.name, c.want, len(got))
		}
	}
}
