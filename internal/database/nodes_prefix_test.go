package database_test

import "testing"

func TestMatchNodesByPrefix_IsCaseInsensitiveAndKeyedLowercase(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "Alpha", "repeater", nodesNow),
		nodeAt("aa02", "Bravo", "room", nodesNow),
		nodeAt("bb03", "Charlie", "repeater", nodesNow),
	)

	matches, err := db.MatchNodesByPrefix(t.Context(), []string{"AA01", "Aa", "BB03"})
	if err != nil {
		t.Fatal(err)
	}
	if m := matches["aa01"]; m.Matches != 1 || m.Pubkey != "aa01" || m.Name != "Alpha" {
		t.Errorf("aa01: want one match, Alpha, got %+v", m)
	}
	if m := matches["aa"]; m.Matches != 2 {
		t.Errorf("aa: want 2 matches, got %+v", m)
	}
	if m := matches["bb03"]; m.Matches != 1 || m.Pubkey != "bb03" || m.Name != "Charlie" {
		t.Errorf("bb03: want one match, Charlie, got %+v", m)
	}
	if len(matches) != 3 {
		t.Errorf("want 3 entries keyed by lowercase prefix, got %d: %+v", len(matches), matches)
	}
	for _, upper := range []string{"AA01", "Aa", "BB03"} {
		if _, ok := matches[upper]; ok {
			t.Errorf("result must be keyed by the lowercase prefix, found key %q", upper)
		}
	}
}
