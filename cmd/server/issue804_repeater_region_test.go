package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestIssue804_AnalyticsAttributesByRepeaterRegion verifies that analytics
// (specifically GetAnalyticsHashSizes) attribute multi-byte nodes to the
// REPEATER's home region, not the observer that happened to hear the relay.
//
// Scenario from #804:
//   - PDX-Repeater is a multi-byte (hashSize=2) repeater whose ZERO-HOP direct
//     adverts are only heard by obs-PDX (a PDX observer). That zero-hop direct
//     advert is the most reliable home-region signal — it cannot have been
//     relayed.
//   - A flood advert from PDX-Repeater (hashSize=2) propagates and is heard by
//     obs-SJC (a SJC observer) via a multi-hop relay path.
//   - When the user asks for region=SJC analytics, the PDX-Repeater MUST NOT
//     pollute SJC's multiByteNodes — it lives in PDX.
//   - The result should also expose attributionMethod="repeater" so the API
//     consumer knows which method was used.
//
// Pre-fix behavior: PDX-Repeater appears in SJC's multiByteNodes because the
// filter is observer-based. This test fails on the pre-fix code at the
// "want PDX-Repeater EXCLUDED" assertion.
func TestIssue804_AnalyticsAttributesByRepeaterRegion(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)

	// Observers: one in PDX, one in SJC
	observerPDX := mustCreateTestObserver(t, ctx, db, "obs-pdx", "Obs PDX", "PDX", recent)
	observerSJC := mustCreateTestObserver(t, ctx, db, "obs-sjc", "Obs SJC", "SJC", recent)

	// PDX-Repeater node (lives in Portland)
	pdxPK := "pdx0000000000001"
	mustCreateTestNode(t, ctx, db, database.NodeInput{PublicKey: pdxPK, Name: "PDX-Repeater", Role: "repeater"})

	// SJC-Repeater node (lives in San Jose) — sanity baseline
	sjcPK := "sjc0000000000001"
	mustCreateTestNode(t, ctx, db, database.NodeInput{PublicKey: sjcPK, Name: "SJC-Repeater", Role: "repeater"})

	pdxDecoded := `{"pubKey":"` + pdxPK + `","name":"PDX-Repeater","type":"ADVERT","flags":{"isRepeater":true}}`
	sjcDecoded := `{"pubKey":"` + sjcPK + `","name":"SJC-Repeater","type":"ADVERT","flags":{"isRepeater":true}}`

	// 1) PDX-Repeater zero-hop DIRECT advert heard only by obs-PDX.
	//    Establishes PDX as the repeater's home region.
	//    raw_hex header 0x12 = route_type 2 (direct), payload_type 4
	//    pathByte 0x40 (hashSize bits=01 → 2, hop_count=0)
	pdxDirectID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"1240aabbccdd", "pdx_zh_direct", pdxDecoded, types.RouteDirect, types.PayloadADVERT, "",
	), recent)
	dbtest.Observation(t, db, pdxDirectID, observerPDX, recent, 12, -85, nil...)

	// 2) PDX-Repeater FLOOD advert with hashSize=2 (reliable).
	//    Heard ONLY by obs-SJC via a relay path (this is the polluting case).
	//    raw_hex header 0x11 = route_type 1 (flood), payload_type 4
	//    pathByte 0x41 (hashSize bits=01 → 2, hop_count=1)
	pdxFloodID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"1141aabbccdd", "pdx_flood", pdxDecoded, types.RouteFlood, types.PayloadADVERT, "",
	), recent)
	dbtest.Observation(t, db, pdxFloodID, observerSJC, recent, 8, -95, []string{"aa11"}...)

	// 3) SJC-Repeater zero-hop DIRECT advert heard only by obs-SJC.
	//    Establishes SJC as the repeater's home region.
	sjcDirectID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"1240ccddeeff", "sjc_zh_direct", sjcDecoded, types.RouteDirect, types.PayloadADVERT, "",
	), recent)
	dbtest.Observation(t, db, sjcDirectID, observerSJC, recent, 14, -82, nil...)

	// 4) SJC-Repeater FLOOD advert with hashSize=2, heard by obs-SJC.
	sjcFloodID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"1141ccddeeff", "sjc_flood", sjcDecoded, types.RouteFlood, types.PayloadADVERT, "",
	), recent)
	dbtest.Observation(t, db, sjcFloodID, observerSJC, recent, 11, -88, []string{"cc22"}...)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	t.Run("region=SJC excludes PDX-Repeater (heard but not home)", func(t *testing.T) {
		ctx := t.Context()
		result := store.GetAnalyticsHashSizes(ctx, "SJC", "")

		var foundPDX, foundSJC bool
		for _, n := range result.MultiByteNodes {
			pk := n.Pubkey
			if pk == pdxPK {
				foundPDX = true
			}
			if pk == sjcPK {
				foundSJC = true
			}
		}

		if foundPDX {
			t.Errorf("PDX-Repeater leaked into SJC analytics — region attribution still observer-based (#804 not fixed)")
		}
		if !foundSJC {
			t.Errorf("SJC-Repeater missing from SJC analytics — fix over-filtered")
		}
	})

	t.Run("API exposes attributionMethod", func(t *testing.T) {
		ctx := t.Context()
		result := store.GetAnalyticsHashSizes(ctx, "SJC", "")
		method := result.AttributionMethod
		if method != "repeater" {
			t.Errorf("attributionMethod = %q, want %q", method, "repeater")
		}
	})

	t.Run("region=PDX excludes SJC-Repeater", func(t *testing.T) {
		ctx := t.Context()
		result := store.GetAnalyticsHashSizes(ctx, "PDX", "")

		var foundPDX, foundSJC bool
		for _, n := range result.MultiByteNodes {
			pk := n.Pubkey
			if pk == pdxPK {
				foundPDX = true
			}
			if pk == sjcPK {
				foundSJC = true
			}
		}
		if !foundPDX {
			t.Errorf("PDX-Repeater missing from PDX analytics")
		}
		if foundSJC {
			t.Errorf("SJC-Repeater leaked into PDX analytics")
		}
	})
}
