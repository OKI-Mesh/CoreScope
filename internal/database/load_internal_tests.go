package database

import "testing"

func TestPathHops(t *testing.T) {
	if got := pathHops(nil, []string{"x"}); got != nil {
		t.Errorf("no prefixes: want nil, got %+v", got)
	}

	resolved := "bbbbbbbb"
	got := pathHops([]string{"aa", "bb", "cc"}, []string{"", resolved})
	if len(got) != 3 {
		t.Fatalf("want 3 hops, got %+v", got)
	}
	if got[0].Prefix != "aa" || got[0].ResolvedPubkey != nil {
		t.Errorf("hop 0: want unresolved aa (empty string means unresolved), got %+v", got[0])
	}
	if got[1].Prefix != "bb" || got[1].ResolvedPubkey == nil || *got[1].ResolvedPubkey != resolved {
		t.Errorf("hop 1: want bb resolved to %s, got %+v", resolved, got[1])
	}
	if got[2].Prefix != "cc" || got[2].ResolvedPubkey != nil {
		t.Errorf("hop 2: want unresolved cc (the resolved list is shorter), got %+v", got[2])
	}
}
