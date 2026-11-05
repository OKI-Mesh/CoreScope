package main

// Regression tests for #1285:
//
// Bug A: per-hash evidence's MedianCorrectedSkewSec includes 700-day RTC-reset
//        outliers, dragging the displayed median into "−704d 18h" garbage even
//        though every recent sample is small (< 30s).
//
// Bug B: RecentBadSampleCount counts samples outside the *displayed* recent
//        window (or counts against raw skew, not corrected) → "3 of last 5
//        adverts had nonsense timestamps" warning fires on healthy nodes.
//
// Both must pass without disturbing the existing bimodal / no-clock logic.

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Synthesizes the repro from issue #1285:
//
//	30 healthy adverts (skew ~−20s) + 1 historical advert with an RTC reset
//	(advertTS = 2024-06-13, observed today → −705d skew).
//
// Returns the populated store.
func seedIssue1285Repro(t *testing.T) *PacketStore {
	t.Helper()
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadADVERT

	const pubkey = "RTCRESET"
	const skew = -20 * time.Second // node clock is 20s BEHIND wall-clock

	baseObs := time.Unix(1779000000, 0).UTC() // ~mid-2026
	rtcResetAdv := int64(1718281640)          // 2024-06-13 (from the issue repro); payload value, stays unix seconds

	var txs []*StoreTx

	// 30 healthy adverts spanning the older end of the recent window.
	for i := 0; i < 30; i++ {
		obsTime := baseObs.Add(time.Duration(i) * time.Minute)
		advTS := obsTime.Add(skew).Unix()
		tx := &StoreTx{
			Hash:        "healthy-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"RTCRESET"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		txs = append(txs, tx)
	}

	// One RTC-reset packet observed MOST RECENTLY (so it sits in the
	// per-hash evidence list AND is included in the recent-window count
	// on master). Its advertTS is from 2024 → corrected skew ≈ -60M sec.
	rtcResetObs := baseObs.Add(31 * time.Minute) // 30*60 + 60 seconds after base
	rtcTx := &StoreTx{
		Hash:        "rtc-reset-0001",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":` + formatInt64(rtcResetAdv) + `},"pubKey":"RTCRESET"}`,
		Observations: []*StoreObs{
			{ObserverID: "obs1", Timestamp: rtcResetObs},
		},
	}
	txs = append(txs, rtcTx)

	ps.mu.Lock()
	ps.byNode[pubkey] = txs
	for _, tx := range txs {
		ps.byPayloadType[4] = append(ps.byPayloadType[4], tx)
	}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()
	return ps
}

// Bug A — per-hash evidence median must EXCLUDE the 705-day RTC-reset outlier.
// On master this asserts on the RTC-reset hash's MedianCorrectedSkewSec being
// ≈ −60M sec ("−704d 18h"); after the fix the field is suppressed (0) or
// otherwise marked insufficient, never displayed as the garbage value.
func TestIssue1285_HashEvidence_OutlierExcludedFromMedian(t *testing.T) {
	ps := seedIssue1285Repro(t)
	r := ps.GetNodeClockSkew("RTCRESET")
	if r == nil {
		t.Fatal("expected clock skew result")
	}

	// The recent-hash evidence list is the source of the "median corrected:
	// −704d 18h" string in the UI. After the fix, NO entry in this list
	// should report a |median| above the 24h sanity threshold — the fix is
	// to drop outlier samples (or flag the hash as insufficient-data) before
	// publishing the median.
	const maxSaneAbsSec = float64(24 * 3600)
	for _, ev := range r.RecentHashEvidence {
		if abs(ev.MedianCorrectedSkewSec) > maxSaneAbsSec {
			t.Errorf("hash %s exposes outlier-dominated median %.0fs (~%.1fd); "+
				"expected entry to be filtered out or marked insufficient (|median| <= %.0fs)",
				ev.Hash, ev.MedianCorrectedSkewSec,
				ev.MedianCorrectedSkewSec/86400, maxSaneAbsSec)
		}
	}
}

// Bug B — RecentBadSampleCount must be 0 when every sample in the recent
// window is healthy (<30s |corrected skew|). On master this fires because
// "recent" is computed over the wrong set (or against raw skew).
func TestIssue1285_RecentBadCount_NotPollutedByOldOutlier(t *testing.T) {
	ps := seedIssue1285Repro(t)
	r := ps.GetNodeClockSkew("RTCRESET")
	if r == nil {
		t.Fatal("expected clock skew result")
	}
	if r.RecentBadSampleCount != 0 {
		t.Errorf("RecentBadSampleCount = %d, want 0 — recent samples are all "+
			"~−20s (healthy); the historical RTC-reset outlier is outside the "+
			"recent window and must not be counted", r.RecentBadSampleCount)
	}
	if r.Severity == SkewBimodalClock || r.Severity == SkewNoClock {
		t.Errorf("severity = %v, want ok/warning — recent samples are all "+
			"healthy (~−20s skew), one historical outlier must not flip the node "+
			"to bimodal/no-clock", r.Severity)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
