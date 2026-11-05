package database_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func TestChannelMessages_OrderedByLatestSeen_DB(t *testing.T) {
	db := newDB(t)

	now := time.Now().UTC()
	tOld := now.Add(-24 * time.Hour)
	tMid := now.Add(-1 * time.Hour)
	tNewest := now.Add(-30 * time.Minute)
	tFresh := now.Add(-1 * time.Minute)

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obsD := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, tOld)
	obsE := dbtest.Observer(t, db, "obse", "ObsE", "SJC", epoch, tOld)

	// tx-A: FirstSeen 24h ago, observations at T-24h and T-1m (LatestSeen
	// = T-1m, the FRESHEST). Despite the freshest LatestSeen, a
	// FirstSeen-DESC selection would push it OFF a small page.
	a := dbtest.ChannelTx(t, db, "order_db_hash_a", "#ordb",
		`{"type":"CHAN","channel":"#ordb","text":"Alpha: hb","sender":"Alpha"}`, tOld)
	dbtest.Observation(t, db, a, obsD, tOld, 10.0, -90, "aa")
	dbtest.Observation(t, db, a, obsE, tFresh, 11.0, -88, "aa")

	// tx-B: FirstSeen 1h ago, LatestSeen 1h ago. OLDEST LatestSeen.
	b := dbtest.ChannelTx(t, db, "order_db_hash_b", "#ordb",
		`{"type":"CHAN","channel":"#ordb","text":"Bravo: msg","sender":"Bravo"}`, tMid)
	dbtest.Observation(t, db, b, obsD, tMid, 9.0, -91, "bb")

	// tx-C: FirstSeen 30m ago, LatestSeen 30m ago. Middle LatestSeen.
	// With FirstSeen-DESC selection + limit=2, page = [tx-C, tx-B] and
	// tx-A is EXCLUDED — that's the selection bug the test gates.
	c := dbtest.ChannelTx(t, db, "order_db_hash_c", "#ordb",
		`{"type":"CHAN","channel":"#ordb","text":"Charlie: msg","sender":"Charlie"}`, tNewest)
	dbtest.Observation(t, db, c, obsD, tNewest, 9.0, -91, "cc")
	ctx := t.Context()

	total, err := db.CountChannelMessages(ctx, "#ordb", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("DB: want total=3, got %d", total)
	}

	msgs, err := db.ListChannelMessages(ctx, "#ordb", false, nil, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("DB: want 2 msgs in page (limit=2), got %d", len(msgs))
	}
	// Selection (fix #2): the page MUST include tx-A (Alpha) because its
	// LatestSeen is the newest — even though its FirstSeen is the OLDEST.
	// With limit=2 + LatestSeen-DESC selection, page = [Alpha, Charlie].
	// Returned ASC by LatestSeen (newest LAST, fix #3) = [Charlie, Alpha].
	// Selection (fix #2): the page MUST include tx-A (Alpha) because its
	// LatestSeen is the newest — even though its FirstSeen is the OLDEST.
	// With limit=2 + LatestSeen-DESC selection, page = [Alpha, Charlie].
	// Returned ASC by LatestSeen (newest LAST, fix #3) = [Charlie, Alpha].
	hash0, hash1 := msgs[0].Hash, msgs[1].Hash
	if hash0 != "order_db_hash_c" || hash1 != "order_db_hash_a" {
		t.Errorf("DB: want order [order_db_hash_c, order_db_hash_a] (page selected by LatestSeen DESC, returned ASC, fix #2+#3), got [%q, %q]",
			hash0, hash1)
	}
	if !slices.ContainsFunc(msgs, func(m database.ChannelMessage) bool { return m.Hash == "order_db_hash_a" }) {
		t.Errorf("DB: tx-A (Alpha) excluded from page — FirstSeen-based selection bug (fix #2 reverted?)")
	}

	// Also exercise large-page case (limit > total): ordering-only check.
	msgsAll, err := db.ListChannelMessages(ctx, "#ordb", false, nil, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsAll) != 3 {
		t.Fatalf("DB: want all 3 msgs at limit=10, got len=%d", len(msgsAll))
	}
	// Expected ASC by LatestSeen: Bravo (T-1h), Charlie (T-30m), Alpha (T-1m).
	wantOrder := []string{"order_db_hash_b", "order_db_hash_c", "order_db_hash_a"}
	for i, want := range wantOrder {
		if got := msgsAll[i].Hash; got != want {
			t.Errorf("DB: msg[%d] want hash=%q, got %q (full order: must be LatestSeen ASC, fix #3)", i, want, got)
		}
	}
}

func TestListChannelSummaries_NoUnknownBucket_1373(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seen := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)

	// 5 encrypted GRP_TXT rows with no channel hash (server had no PSK).
	for i := 0; i < 5; i++ {
		dbtest.ChannelTx(t, db, sqlHashFor(i), "",
			`{"type":"CHAN","channel":"","text":"","sender":""}`, seen)
	}
	// 3 decrypted GRP_TXT rows on "#real".
	for i := 0; i < 3; i++ {
		dbtest.ChannelTx(t, db, sqlHashFor(100+i), "#real",
			`{"type":"CHAN","channel":"#real","text":"Alice: hi","sender":"Alice"}`, seen)
	}

	summaries, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatalf("ListChannelSummaries: %v", err)
	}

	var got []string
	for _, s := range summaries {
		got = append(got, s.ChannelHash)
		if s.ChannelHash == "" || s.ChannelHash == "unknown" {
			t.Errorf("emitted ghost bucket %q (issue #1373): %+v", s.ChannelHash, s)
		}
	}
	if len(summaries) != 1 {
		t.Fatalf("expected exactly 1 channel (#real), got %d: %v", len(summaries), got)
	}
	if summaries[0].ChannelHash != "#real" {
		t.Errorf("expected channel %q, got %q", "#real", summaries[0].ChannelHash)
	}
	if summaries[0].MessageCount != 3 {
		t.Errorf("expected MessageCount=3 for #real, got %d", summaries[0].MessageCount)
	}
}

// sqlHashFor returns a unique 16-char hex string per index for the
// UNIQUE hash column in transmissions.
func sqlHashFor(i int) string {
	return fmt.Sprintf("%016x", uint64(0x1373_0000_0000_0000)+uint64(i))
}
