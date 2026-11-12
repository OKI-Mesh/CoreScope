package ingeststats_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/ingeststats"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func wireTime(t time.Time) *types.WireTimeSeconds { return &types.WireTimeSeconds{Time: t} }

func fullStats() ingeststats.Stats {
	at := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	return ingeststats.Stats{
		SampledAt:          wireTime(at),
		TxInserted:         1,
		ObsInserted:        2,
		DuplicateTx:        3,
		NodeUpserts:        4,
		ObserverUpserts:    5,
		WriteErrors:        6,
		SignatureDrops:     7,
		WALCommits:         8,
		GroupCommitFlushes: 9,
		BackfillUpdates:    map[string]int64{"path_a": 10},
		ProcIO: &ingeststats.IOSample{
			ReadBytesPerSec:           1.5,
			WriteBytesPerSec:          2.5,
			CancelledWriteBytesPerSec: 3.5,
			SyscallsRead:              4.5,
			SyscallsWrite:             5.5,
			SampledAt:                 wireTime(at),
		},
		SourceLiveness: map[string]ingeststats.SourceLiveness{
			"east": {LastReceiptUnix: 1752841790, LastMessageUnix: 1752841780},
		},
		SourceStatuses: []ingeststats.SourceStatus{{
			Name: "east", Broker: "mqtts://u:p@broker:8883", Connected: true,
			LastConnectUnix: 1, LastDisconnectUnix: 2, LastPacketUnix: 3,
			ConnectCount: 4, DisconnectCount: 5, PacketsTotal: 6, PacketsLast5m: 7,
			LastError: "dial tcp: refused",
		}},
		WatchdogLastTickUnix: 1752841800,
		WatchdogPanicCount:   11,
		WatchdogLogDropCount: 413,
	}
}

func keysOf(t *testing.T, v any) []string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal keys: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func TestStats_RoundTrip(t *testing.T) {
	want := fullStats()
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	got, err := ingeststats.Decode(b)
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("round trip changed the stats\n got: %+v\nwant: %+v", *got, want)
	}
}

// The keys are what the ingestor has always written. Renaming one breaks the
// server reading an older file during an upgrade, so this list is the contract.
func TestStats_WireKeysAreStable(t *testing.T) {
	want := []string{
		"backfillUpdates", "groupCommitFlushes", "node_upserts", "obs_inserted",
		"observer_upserts", "procIO", "sampledAt", "sig_drops", "source_liveness",
		"source_statuses", "tx_dupes", "tx_inserted", "walCommits",
		"watchdogLastTickUnix", "watchdogLogDropCount", "watchdogPanicCount",
		"write_errors",
	}
	if got := keysOf(t, fullStats()); !slices.Equal(got, want) {
		t.Errorf("stats keys\n got: %v\nwant: %v", got, want)
	}

	wantIO := []string{
		"cancelledWriteBytesPerSec", "readBytesPerSec", "sampledAt",
		"syscallsRead", "syscallsWrite", "writeBytesPerSec",
	}
	if got := keysOf(t, fullStats().ProcIO); !slices.Equal(got, wantIO) {
		t.Errorf("procIO keys\n got: %v\nwant: %v", got, wantIO)
	}

	wantStatus := []string{
		"broker", "connectCount", "connected", "disconnectCount", "lastConnectUnix",
		"lastDisconnectUnix", "lastError", "lastPacketUnix", "name", "packetsLast5m",
		"packetsTotal",
	}
	if got := keysOf(t, fullStats().SourceStatuses[0]); !slices.Equal(got, wantStatus) {
		t.Errorf("source status keys\n got: %v\nwant: %v", got, wantStatus)
	}

	wantLiveness := []string{"lastMessageUnix", "lastReceiptUnix"}
	if got := keysOf(t, fullStats().SourceLiveness["east"]); !slices.Equal(got, wantLiveness) {
		t.Errorf("source liveness keys\n got: %v\nwant: %v", got, wantLiveness)
	}
}

func TestIOView_KeysAreASubsetOfStats(t *testing.T) {
	stats := keysOf(t, fullStats())
	view := keysOf(t, ingeststats.IOView{SampledAt: wireTime(time.Now()), ProcIO: fullStats().ProcIO})
	for _, k := range view {
		if !slices.Contains(stats, k) {
			t.Errorf("IOView key %q is not a Stats key", k)
		}
	}
}

func TestStats_SampledAtIsWholeSecondUTC(t *testing.T) {
	s := fullStats()
	s.SampledAt = wireTime(time.Date(2026, 7, 18, 14, 30, 0, 900_000_000, time.FixedZone("UTC+2", 2*60*60)))
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	if !strings.Contains(string(b), `"sampledAt":"2026-07-18T12:30:00Z"`) {
		t.Errorf("sampledAt is not whole-second UTC in %s", b)
	}
}

func TestDecode_SampledAt(t *testing.T) {
	cases := []struct {
		name    string
		json    string
		wantNil bool
		wantErr bool
	}{
		{"whole second UTC", `{"sampledAt":"2026-07-18T12:30:00Z"}`, false, false},
		{"absent", `{}`, true, false},
		{"null", `{"sampledAt":null}`, true, false},
		{"offset", `{"sampledAt":"2026-07-18T12:30:00+01:00"}`, false, true},
		{"fractional seconds are accepted", `{"sampledAt":"2026-07-18T12:30:00.5Z"}`, false, false},
		{"not a time", `{"sampledAt":"yesterday"}`, false, true},
		{"empty string", `{"sampledAt":""}`, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ingeststats.Decode([]byte(c.json))
			if (err != nil) != c.wantErr {
				t.Fatalf("Decode(%s) error = %v, want error %v", c.json, err, c.wantErr)
			}
			if err == nil && (got.SampledAt == nil) != c.wantNil {
				t.Errorf("SampledAt = %v, want nil %v", got.SampledAt, c.wantNil)
			}
		})
	}
}

func TestIOView_SampleTime(t *testing.T) {
	top := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	inner := time.Date(2026, 7, 18, 12, 29, 0, 0, time.UTC)

	cases := []struct {
		name   string
		view   ingeststats.IOView
		want   time.Time
		wantOK bool
	}{
		{"file stamp wins", ingeststats.IOView{SampledAt: wireTime(top), ProcIO: &ingeststats.IOSample{SampledAt: wireTime(inner)}}, top, true},
		{"falls back to procIO", ingeststats.IOView{ProcIO: &ingeststats.IOSample{SampledAt: wireTime(inner)}}, inner, true},
		{"procIO without a stamp", ingeststats.IOView{ProcIO: &ingeststats.IOSample{}}, time.Time{}, false},
		{"nothing", ingeststats.IOView{}, time.Time{}, false},
	}
	for _, c := range cases {
		got, ok := c.view.SampleTime()
		if ok != c.wantOK || !got.Equal(c.want) {
			t.Errorf("%s: SampleTime() = %v, %v; want %v, %v", c.name, got, ok, c.want, c.wantOK)
		}
	}
}

func TestDecodeIO_SkipsTheRest(t *testing.T) {
	b, err := json.Marshal(fullStats())
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	v, err := ingeststats.DecodeIO(b)
	if err != nil {
		t.Fatalf("DecodeIO(): %v", err)
	}
	if v.SampledAt == nil || v.ProcIO == nil || v.ProcIO.ReadBytesPerSec != 1.5 {
		t.Errorf("DecodeIO() = %+v, want the sample time and procIO", v)
	}
}

func TestPath(t *testing.T) {
	t.Setenv(ingeststats.PathEnv, "")
	if got := ingeststats.Path(); got != ingeststats.DefaultPath {
		t.Errorf("Path() with no override = %q, want %q", got, ingeststats.DefaultPath)
	}
	t.Setenv(ingeststats.PathEnv, "/var/lib/corescope/stats.json")
	if got := ingeststats.Path(); got != "/var/lib/corescope/stats.json" {
		t.Errorf("Path() with override = %q", got)
	}
}
