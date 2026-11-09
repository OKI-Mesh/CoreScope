package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
)

// epochSeconds is the one place engine times become the integer Unix seconds
// the clock-skew wire format uses. A zero time encodes as 0.
func epochSeconds(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// nodeClockSkewResponsePtr is nodeClockSkewResponse for an optional result.
func nodeClockSkewResponsePtr(cs *NodeClockSkew) *api.NodeClockSkew {
	if cs == nil {
		return nil
	}
	out := nodeClockSkewResponse(cs)
	return &out
}

// nodeClockSkewResponse converts the engine's per-node result into the wire
// type. The engine struct is internal; this is the only place that decides what
// reaches the client. Empty collections and the fleet-only name and role are
// omitted rather than sent as null or empty strings.
func nodeClockSkewResponse(cs *NodeClockSkew) api.NodeClockSkew {
	out := api.NodeClockSkew{
		Pubkey:               cs.Pubkey,
		MeanSkewSec:          cs.MeanSkewSec,
		MedianSkewSec:        cs.MedianSkewSec,
		LastSkewSec:          cs.LastSkewSec,
		RecentMedianSkewSec:  cs.RecentMedianSkewSec,
		DriftPerDaySec:       cs.DriftPerDaySec,
		Severity:             cs.Severity,
		SampleCount:          cs.SampleCount,
		Calibrated:           cs.Calibrated,
		LastAdvertTS:         epochSeconds(cs.LastAdvertTime),
		LastObservedTS:       epochSeconds(cs.LastObservedTime),
		GoodFraction:         cs.GoodFraction,
		RecentBadSampleCount: cs.RecentBadSampleCount,
		RecentSampleCount:    cs.RecentSampleCount,
	}
	if len(cs.Samples) > 0 {
		out.Samples = make([]api.SkewSample, 0, len(cs.Samples))
		for _, sm := range cs.Samples {
			out.Samples = append(out.Samples, api.SkewSample{Ts: epochSeconds(sm.Time), Skew: sm.SkewSec})
		}
	}
	if len(cs.RecentBadSamples) > 0 {
		out.RecentBadSamples = make([]api.BadSample, 0, len(cs.RecentBadSamples))
		for _, bs := range cs.RecentBadSamples {
			out.RecentBadSamples = append(out.RecentBadSamples, api.BadSample{Hash: bs.Hash, AdvertTS: epochSeconds(bs.AdvertTime), SkewSec: bs.SkewSec})
		}
	}
	if len(cs.RecentHashEvidence) > 0 {
		out.RecentHashEvidence = make([]api.HashEvidence, 0, len(cs.RecentHashEvidence))
		for _, ev := range cs.RecentHashEvidence {
			observers := make([]api.HashEvidenceObserver, 0, len(ev.Observers))
			for _, o := range ev.Observers {
				observers = append(observers, api.HashEvidenceObserver{
					ObserverID:        o.ObserverID,
					ObserverName:      o.ObserverName,
					RawSkewSec:        o.RawSkewSec,
					CorrectedSkewSec:  o.CorrectedSkewSec,
					ObserverOffsetSec: o.ObserverOffsetSec,
					Calibrated:        o.Calibrated,
				})
			}
			out.RecentHashEvidence = append(out.RecentHashEvidence, api.HashEvidence{
				Hash:                   ev.Hash,
				Observers:              observers,
				MedianCorrectedSkewSec: ev.MedianCorrectedSkewSec,
				Timestamp:              epochSeconds(ev.Time),
			})
		}
	}
	if cs.CalibrationSummary != nil {
		out.CalibrationSummary = &api.CalibrationSummary{
			TotalSamples:        cs.CalibrationSummary.TotalSamples,
			CalibratedSamples:   cs.CalibrationSummary.CalibratedSamples,
			UncalibratedSamples: cs.CalibrationSummary.UncalibratedSamples,
		}
	}
	if cs.NodeName != "" {
		out.NodeName = ptr(cs.NodeName)
	}
	if cs.NodeRole != "" {
		out.NodeRole = ptr(cs.NodeRole)
	}
	return out
}

func observerCalibrationsResponse(rows []ObserverCalibration) []api.ObserverCalibration {
	out := make([]api.ObserverCalibration, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.ObserverCalibration{ObserverID: r.ObserverID, OffsetSec: r.OffsetSec, Samples: r.Samples})
	}
	return out
}
