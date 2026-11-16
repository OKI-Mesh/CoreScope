package main

import (
	"database/sql"
	"testing"
)

// Issue #1373: /api/channels emits a ghost "unknown" bucket for encrypted GRP_TXT
// packets whose decoded JSON sets channel="" (server has no PSK to decrypt).
// Fix A (cosmetic): drop the "unknown" bucket from the response so users only
// see real channels. Encrypted-no-key packets are still observable via the
// encrypted-channels analytics, just not as a fake "unknown" channel.
//
// This test seeds 5 GRP_TXT with Channel="" (encrypted-no-key) + 3 with
// Channel="#real" and asserts GetChannels returns exactly one entry, #real —
// no "unknown" bucket.

func TestGetChannels_NoUnknownBucket_1373(t *testing.T) {
	ctx := t.Context()
	packets := []*StoreTx{
		makeGrpTx(129, "", "", ""),
		makeGrpTx(129, "", "", ""),
		makeGrpTx(129, "", "", ""),
		makeGrpTx(129, "", "", ""),
		makeGrpTx(129, "", "", ""),
		makeGrpTx(72, "#real", "hello", "alice"),
		makeGrpTx(72, "#real", "world", "bob"),
		makeGrpTx(72, "#real", "third", "carol"),
	}
	store := newChannelTestStore(packets)

	channels := store.GetChannels(ctx, "")

	var gotNames []string
	for _, ch := range channels {
		name, _ := ch["name"].(string)
		gotNames = append(gotNames, name)
		if name == "unknown" {
			t.Errorf("GetChannels emitted ghost 'unknown' bucket (issue #1373): %+v", ch)
		}
	}
	if len(channels) != 1 {
		t.Fatalf("expected exactly 1 channel (#real), got %d: %v", len(channels), gotNames)
	}
	if name, _ := channels[0]["name"].(string); name != "#real" {
		t.Errorf("expected channel name '#real', got %q", name)
	}
	if mc, _ := channels[0]["messageCount"].(int); mc != 3 {
		t.Errorf("expected messageCount=3 for #real, got %v", channels[0]["messageCount"])
	}
}

// silence unused-import warning when the file is reduced.
var _ = sql.ErrNoRows
