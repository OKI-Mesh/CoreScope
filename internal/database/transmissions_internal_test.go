package database

import (
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestBuildTransmissionWhere_ClauseAndArgShape(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	typ, route := 4, 1

	tests := []struct {
		name        string
		query       PacketQuery
		nodePubkey  string
		wantClauses int
		wantArgs    []any
	}{
		{"no filters", PacketQuery{}, "", 0, nil},
		{"payload type", PacketQuery{Type: &typ}, "", 1, []any{4}},
		{"route type", PacketQuery{Route: &route}, "", 1, []any{1}},
		{"obs since", PacketQuery{ObsSince: &ts}, "", 1, []any{ts}},
		{"obs until", PacketQuery{ObsUntil: &ts}, "", 1, []any{ts}},
		{"first_seen after", PacketQuery{FirstSeenAfter: &ts}, "", 1, []any{ts}},
		{"first_seen before", PacketQuery{FirstSeenBefore: &ts}, "", 1, []any{ts}},
		{"node uses the resolved pubkey", PacketQuery{Node: "Some Name"}, "aabb", 1, []any{"aabb"}},
		{"observers", PacketQuery{Observers: []string{"obs1", "obs2"}}, "", 1, []any{[]string{"obs1", "obs2"}}},
		{"observers are lowercased and trimmed", PacketQuery{Observers: []string{" OBS1", "Obs2 "}}, "", 1, []any{[]string{"obs1", "obs2"}}},
		{"region", PacketQuery{Region: "SJC"}, "", 1, []any{"SJC"}},
		{"hash is lowercased", PacketQuery{Hash: "ABC123"}, "", 1, []any{"abc123"}},
		{"hash is trimmed", PacketQuery{Hash: "  abc123 "}, "", 1, []any{"abc123"}},
		{"channel plain", PacketQuery{Channel: "ab12"}, "", 2, []any{"ab12", false}},
		{"channel encrypted", PacketQuery{Channel: "ab12", ChannelEncrypted: true}, "", 2, []any{"ab12", true}},
		{"encrypted flag alone adds nothing", PacketQuery{ChannelEncrypted: true}, "", 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			where, args := buildTransmissionWhere(tc.query, tc.nodePubkey)
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

func TestBuildTransmissionWhere_NumbersEveryPlaceholderOnceInOrder(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	typ, route := 4, 1
	where, args := buildTransmissionWhere(PacketQuery{
		Type: &typ, Route: &route, Hash: "ABC", Node: "n",
		ObsSince: &ts, ObsUntil: &ts, FirstSeenAfter: &ts, FirstSeenBefore: &ts,
		Channel: "ab12", ChannelEncrypted: true,
		Observers: []string{"obs1"}, Region: "SJC",
	}, "aabb")

	placeholder := regexp.MustCompile(`\$(\d+)`)
	next := 1
	for i, w := range where {
		found := placeholder.FindAllStringSubmatch(w, -1)
		if len(found) != 1 || found[0][1] != strconv.Itoa(next) {
			t.Errorf("clause %d = %q, want exactly the placeholder $%d", i, w, next)
		}
		if regexp.MustCompile(`\?`).MatchString(w) {
			t.Errorf("clause %d = %q still contains a ? placeholder", i, w)
		}
		next++
	}
	if len(args) != len(where) {
		t.Errorf("args = %d, want one per clause (%d)", len(args), len(where))
	}
	if next-1 != len(args) {
		t.Errorf("placeholders run to $%d but there are %d args", next-1, len(args))
	}
}

func TestPacketQuery_Normalize(t *testing.T) {
	good := []struct {
		name      string
		in        PacketQuery
		wantOrder string
		wantLimit int
	}{
		{"defaults", PacketQuery{}, "DESC", 50},
		{"ascending, any case", PacketQuery{Order: "asc", Limit: 7}, "ASC", 7},
		{"descending", PacketQuery{Order: "DESC", Limit: 7}, "DESC", 7},
		{"anything else is descending", PacketQuery{Order: "id; DROP TABLE x", Limit: 7}, "DESC", 7},
		{"negative limit gets the default", PacketQuery{Limit: -3}, "DESC", 50},
	}
	for _, tc := range good {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.in
			order, err := q.normalize()
			if err != nil {
				t.Fatal(err)
			}
			if order != tc.wantOrder || q.Limit != tc.wantLimit {
				t.Errorf("want order %s and limit %d, got %s and %d", tc.wantOrder, tc.wantLimit, order, q.Limit)
			}
		})
	}

	for _, q := range []PacketQuery{{Offset: -1}, {Limit: math.MaxInt32 + 1}, {Offset: math.MaxInt32 + 1}} {
		if _, err := q.normalize(); err == nil {
			t.Errorf("limit %d offset %d: want an error", q.Limit, q.Offset)
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
		return reflect.DeepEqual(got, want)
	}
}
