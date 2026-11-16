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

func hashesOf(msgs []database.ChannelMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Hash
	}
	return out
}

func TestListChannelMessages_OffsetPagesBackThroughTime_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obs := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, base)

	for i := 0; i < 5; i++ {
		seen := base.Add(time.Duration(i) * time.Minute)
		tx := dbtest.ChannelTx(t, db, fmt.Sprintf("pg_msg_%d", i), "#pg",
			fmt.Sprintf(`{"type":"CHAN","channel":"#pg","text":"S%d: hi","sender":"S%d"}`, i, i), seen)
		dbtest.Observation(t, db, tx, obs, seen, 10.0, -90, "aa")
	}

	pages := []struct {
		offset int
		want   []string
	}{
		{0, []string{"pg_msg_3", "pg_msg_4"}},
		{2, []string{"pg_msg_1", "pg_msg_2"}},
		{4, []string{"pg_msg_0"}},
		{6, []string{}},
	}
	for _, p := range pages {
		got, err := db.ListChannelMessages(ctx, "#pg", false, nil, 2, p.offset)
		if err != nil {
			t.Fatalf("offset %d: %v", p.offset, err)
		}
		if !slices.Equal(hashesOf(got), p.want) {
			t.Errorf("offset %d: want %v (newest page first, each page oldest-first), got %v",
				p.offset, p.want, hashesOf(got))
		}
	}
}

func TestListChannelSummaries_OrderedByLastActivityThenHash_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	channels := []struct {
		name string
		at   time.Time
	}{
		{"#tieb", base.Add(1 * time.Hour)},
		{"#old", base},
		{"#new", base.Add(2 * time.Hour)},
		{"#tiea", base.Add(1 * time.Hour)},
	}
	for i, c := range channels {
		dbtest.ChannelTx(t, db, sqlHashFor(200+i), c.name,
			fmt.Sprintf(`{"type":"CHAN","channel":%q,"text":"Alice: hi","sender":"Alice"}`, c.name), c.at)
	}

	got, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := []struct {
		name string
		at   time.Time
	}{
		{"#new", base.Add(2 * time.Hour)},
		{"#tiea", base.Add(1 * time.Hour)},
		{"#tieb", base.Add(1 * time.Hour)},
		{"#old", base},
	}
	if len(got) != len(want) {
		t.Fatalf("want %d channels, got %d: %+v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i].ChannelHash != w.name {
			t.Errorf("summaries[%d]: want %s, got %s", i, w.name, got[i].ChannelHash)
		}
		if !got[i].LastActivity.Equal(w.at) {
			t.Errorf("summaries[%d] %s: want LastActivity %v, got %v", i, w.name, w.at, got[i].LastActivity)
		}
		if got[i].LastActivity.Location() != time.UTC {
			t.Errorf("summaries[%d] %s: want LastActivity in UTC, got %v", i, w.name, got[i].LastActivity.Location())
		}
	}
}

func TestListChannelSummaries_SampleIsNewestMessage_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	older := `{"type":"CHAN","channel":"#sample","text":"Alice: older","sender":"Alice"}`
	newer := `{"type":"CHAN","channel":"#sample","text":"Bob: newer","sender":"Bob"}`
	dbtest.ChannelTx(t, db, sqlHashFor(300), "#sample", older, base)
	dbtest.ChannelTx(t, db, sqlHashFor(301), "#sample", newer, base.Add(time.Minute))

	got, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 channel, got %+v", got)
	}
	if got[0].MessageCount != 2 {
		t.Errorf("want MessageCount 2, got %d", got[0].MessageCount)
	}
	if got[0].SampleJSON == nil || *got[0].SampleJSON != newer {
		t.Errorf("want SampleJSON %s, got %v", newer, got[0].SampleJSON)
	}
}

func TestListChannelSummaries_RegionFilter_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	sjc := dbtest.Observer(t, db, "obssjc", "ObsSJC", "SJC", epoch, base)
	pdx := dbtest.Observer(t, db, "obspdx", "ObsPDX", "PDX", epoch, base)

	west := dbtest.ChannelTx(t, db, sqlHashFor(400), "#west",
		`{"type":"CHAN","channel":"#west","text":"Alice: hi","sender":"Alice"}`, base)
	dbtest.Observation(t, db, west, sjc, base, 10.0, -90, "aa")
	north := dbtest.ChannelTx(t, db, sqlHashFor(401), "#north",
		`{"type":"CHAN","channel":"#north","text":"Bob: hi","sender":"Bob"}`, base)
	dbtest.Observation(t, db, north, pdx, base, 10.0, -90, "bb")

	got, err := db.ListChannelSummaries(ctx, []string{"SJC"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ChannelHash != "#west" {
		t.Fatalf("region SJC: want only #west, got %+v", got)
	}

	all, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("no region filter: want both channels, got %+v", all)
	}
}

func TestListChannelMessages_FieldsFromFirstObservation_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obsD := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, base)
	obsE := dbtest.Observer(t, db, "obse", "ObsE", "SJC", epoch, base)

	seen := dbtest.ChannelTx(t, db, "fld_hash_seen", "#fld",
		`{"type":"CHAN","channel":"#fld","text":"Alpha: hi","sender":"Alpha"}`, base)
	dbtest.Observation(t, db, seen, obsD, base.Add(2*time.Minute), 10.0, -90, "aa", "bb")
	dbtest.Observation(t, db, seen, obsE, base.Add(1*time.Minute), 11.0, -88, "aa")

	dbtest.ChannelTx(t, db, "fld_hash_unseen", "#fld",
		`{"type":"CHAN","channel":"#fld","text":"Bravo: hi","sender":"Bravo"}`, base)

	msgs, err := db.ListChannelMessages(ctx, "#fld", false, nil, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want only the observed transmission, got %+v", msgs)
	}
	m := msgs[0]
	if m.TransmissionID != seen || m.Hash != "fld_hash_seen" {
		t.Errorf("want transmission %d fld_hash_seen, got %d %s", seen, m.TransmissionID, m.Hash)
	}
	if m.Repeats != 2 {
		t.Errorf("want Repeats 2, got %d", m.Repeats)
	}
	if !m.FirstSeen.Equal(base) {
		t.Errorf("want FirstSeen %v, got %v", base, m.FirstSeen)
	}
	if !m.LatestSeen.Equal(base.Add(2 * time.Minute)) {
		t.Errorf("want LatestSeen %v (newest observation), got %v", base.Add(2*time.Minute), m.LatestSeen)
	}
	if m.SNR == nil || *m.SNR != 10.0 {
		t.Errorf("want SNR 10 from the first observation inserted, got %v", m.SNR)
	}
	if m.ObserverPubkey == nil || *m.ObserverPubkey != "obsd" {
		t.Errorf("want ObserverPubkey obsd, got %v", m.ObserverPubkey)
	}
	if m.ObserverName == nil || *m.ObserverName != "Obsd" {
		t.Errorf("want ObserverName Obsd, got %v", m.ObserverName)
	}
	if m.HopCount != 2 {
		t.Errorf("want HopCount 2, got %d", m.HopCount)
	}
	if m.ScopeName != nil {
		t.Errorf("want nil ScopeName, got %q", *m.ScopeName)
	}

	total, err := db.CountChannelMessages(ctx, "#fld", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if total != len(msgs) {
		t.Errorf("CountChannelMessages = %d but ListChannelMessages returned %d", total, len(msgs))
	}
}

func TestChannelMessages_EncryptedFlagSelectsChannelKind_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obs := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, base)

	tx := dbtest.EncryptedChannelTx(t, db, "enc_msg_hash", "3A", "AABB", base)
	dbtest.Observation(t, db, tx, obs, base, 10.0, -90, "aa")

	dbtest.EncryptedChannelTx(t, db, sqlHashFor(500), "3A", "AABB", base)
	dbtest.EncryptedChannelTx(t, db, sqlHashFor(501), "3A", "AABB", base.Add(time.Minute))
	dbtest.EncryptedChannelTx(t, db, sqlHashFor(502), "7F", "AABB", base.Add(2*time.Hour))

	plain, err := db.ListChannelMessages(ctx, "3A", false, nil, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != 0 {
		t.Fatalf("encrypted=false: want none, got %+v", plain)
	}

	countEncrypted, err := db.CountChannelMessages(ctx, "3A", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	countPlain, err := db.CountChannelMessages(ctx, "3A", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if countEncrypted != 1 || countPlain != 0 {
		t.Errorf("want counts 1 (encrypted) and 0 (plain), got %d and %d", countEncrypted, countPlain)
	}
}

func TestListEncryptedChannelSummaries_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	dbtest.EncryptedChannelTx(t, db, sqlHashFor(500), "3A", "AABB", base)
	dbtest.EncryptedChannelTx(t, db, sqlHashFor(501), "3A", "AABB", base.Add(time.Minute))
	dbtest.EncryptedChannelTx(t, db, sqlHashFor(502), "7F", "AABB", base.Add(2*time.Hour))
	dbtest.ChannelTx(t, db, sqlHashFor(503), "#plain",
		`{"type":"CHAN","channel":"#plain","text":"Alice: hi","sender":"Alice"}`, base.Add(3*time.Hour))

	got, err := db.ListEncryptedChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 encrypted channels, got %+v", got)
	}
	if got[0].ChannelHash != "7F" || got[0].MessageCount != 1 || !got[0].LastActivity.Equal(base.Add(2*time.Hour)) {
		t.Errorf("summaries[0]: want 7F, 1 message, %v; got %+v", base.Add(2*time.Hour), got[0])
	}
	if got[1].ChannelHash != "3A" || got[1].MessageCount != 2 || !got[1].LastActivity.Equal(base.Add(time.Minute)) {
		t.Errorf("summaries[1]: want 3A, 2 messages, %v; got %+v", base.Add(time.Minute), got[1])
	}

	plain, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != 1 || plain[0].ChannelHash != "#plain" {
		t.Errorf("plain summaries must not include encrypted channels, got %+v", plain)
	}
}
