package database

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBuildTransmissionWhere_ClauseAndArgShape(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		query       PacketQuery
		wantClauses int
		wantArgs    []any
	}{
		{"obs since", PacketQuery{ObsSince: &ts}, 1, []any{ts}},
		{"obs until", PacketQuery{ObsUntil: &ts}, 1, []any{ts}},
		{"first_seen after", PacketQuery{FirstSeenAfter: &ts}, 1, []any{ts}},
		{"first_seen before", PacketQuery{FirstSeenBefore: &ts}, 1, []any{ts}},
		{"observers", PacketQuery{Observers: []string{"obs1", "obs2"}}, 1, []any{[]string{"obs1", "obs2"}}},
		{"region", PacketQuery{Region: "SJC"}, 1, []any{"SJC"}},
		{"hash is lowercased", PacketQuery{Hash: "ABC123"}, 1, []any{"abc123"}},
		{"channel plain", PacketQuery{Channel: "ab12"}, 2, []any{"ab12", false}},
		{"channel encrypted", PacketQuery{Channel: "ab12", ChannelEncrypted: true}, 2, []any{"ab12", true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			where, args := buildTransmissionWhere(tc.query, "")
			if len(where) != tc.wantClauses {
				t.Fatalf("clauses = %d, want %d: %v", len(where), tc.wantClauses, where)
			}
			if len(args) != len(tc.wantArgs) {
				t.Fatalf("args = %d, want %d: %v", len(args), len(tc.wantArgs), args)
			}
			for i := range args {
				if !argEqual(args[i], tc.wantArgs[i]) {
					t.Errorf("arg %d = %v (%T), want %v (%T)", i, args[i], args[i], tc.wantArgs[i], tc.wantArgs[i])
				}
			}
		})
	}
}

func TestBuildTransmissionWhere_NumbersPlaceholdersInOrder(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	typ := 4
	where, _ := buildTransmissionWhere(PacketQuery{
		Type: &typ, Hash: "ABC", ObsSince: &ts, Observers: []string{"obs1"}, Region: "SJC",
	}, "")

	for i, w := range where {
		want := "$" + strconv.Itoa(i+1)
		if !strings.Contains(w, want) {
			t.Errorf("clause %d = %q, want placeholder %s", i, w, want)
		}
		if strings.Contains(w, "?") {
			t.Errorf("clause %d = %q still contains a ? placeholder", i, w)
		}
	}
}

// argEqual compares arguments, using Equal for times and slices.Equal for string slices.
func argEqual(got, want any) bool {
	switch w := want.(type) {
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(w)
	case []string:
		g, ok := got.([]string)
		return ok && slices.Equal(g, w)
	default:
		return got == want
	}
}
