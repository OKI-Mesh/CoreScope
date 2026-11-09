package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
)

func TestNodeClockSkewResponseMatchesSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}

	full := &NodeClockSkew{
		Pubkey:               "aabb",
		MeanSkewSec:          -11.8,
		MedianSkewSec:        -11.8,
		LastSkewSec:          -14.8,
		RecentMedianSkewSec:  -11.8,
		DriftPerDaySec:       0.5,
		Severity:             types.SkewBimodalClock,
		SampleCount:          94,
		Calibrated:           true,
		LastAdvertTime:       time.Unix(1794020249, 0).UTC(),
		LastObservedTime:     time.Unix(1794020267, 0).UTC(),
		Samples:              []SkewSample{{Time: time.Unix(1794020000, 0).UTC(), SkewSec: -12}},
		GoodFraction:         0.5,
		RecentBadSampleCount: 1,
		RecentBadSamples:     []BadSample{{Hash: "344dff63e1f1566a", AdvertTime: time.Unix(1794081212, 0).UTC(), SkewSec: -31420.6}},
		RecentSampleCount:    2,
		RecentHashEvidence: []HashEvidence{{
			Hash: "204850b4bee8dcb6",
			Observers: []HashEvidenceObserver{{
				ObserverID: "AC9A", ObserverName: "obs", RawSkewSec: -1.9, CorrectedSkewSec: 1, ObserverOffsetSec: 3, Calibrated: true,
			}},
			MedianCorrectedSkewSec: 1,
			Time:                   time.Unix(1794186064, 0).UTC(),
		}},
		CalibrationSummary: &CalibrationSummary{TotalSamples: 13, CalibratedSamples: 13},
		NodeName:           "CamoCrabby RPTR-E",
		NodeRole:           types.RoleRepeater,
	}
	got := validateAgainstSchema(t, doc, "NodeClockSkew", nodeClockSkewResponse(full))
	for _, key := range []string{"lastObservedTS", "samples", "recentBadSamples", "recentHashEvidence", "calibrationSummary", "nodeName", "nodeRole"} {
		if _, ok := got[key]; !ok {
			t.Errorf("full entry is missing %q", key)
		}
	}
	if _, ok := got["lastObservedTime"]; ok {
		t.Error("lastObservedTime must not be sent; production and the frontend use lastObservedTS")
	}

	t.Run("epoch seconds survive the conversion", func(t *testing.T) {
		if got := nodeClockSkewResponse(full); got.LastObservedTS != 1794020267 || got.LastAdvertTS != 1794020249 || got.Samples[0].Ts != 1794020000 || got.RecentBadSamples[0].AdvertTS != 1794081212 || got.RecentHashEvidence[0].Timestamp != 1794186064 {
			t.Errorf("epoch seconds changed in conversion: %+v", got)
		}
	})

	t.Run("nil result stays nil", func(t *testing.T) {
		if nodeClockSkewResponsePtr(nil) != nil {
			t.Error("nil engine result must convert to nil")
		}
	})

	t.Run("fleet entry omits detail groups", func(t *testing.T) {
		fleet := &NodeClockSkew{Pubkey: "ccdd", Severity: types.SkewOK, Calibrated: true}
		got := validateAgainstSchema(t, doc, "NodeClockSkew", nodeClockSkewResponse(fleet))
		for _, key := range []string{"samples", "recentBadSamples", "recentHashEvidence", "calibrationSummary", "nodeName", "nodeRole"} {
			if _, present := got[key]; present {
				t.Errorf("%q must be absent when empty", key)
			}
		}
	})
}

func TestObserverCalibrationsResponse(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	rows := observerCalibrationsResponse([]ObserverCalibration{{ObserverID: "6C22", OffsetSec: 8719, Samples: 1}})
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	validateAgainstSchema(t, doc, "ObserverCalibration", rows[0])

	raw, err := json.Marshal(observerCalibrationsResponse(nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "[]" {
		t.Errorf("no observers must encode as [], got %s", raw)
	}
}
