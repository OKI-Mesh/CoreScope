// Package ingeststats is the contract of the ingestor's stats file: the JSON
// document the ingestor rewrites about once a second and the server reads for
// /api/perf/write-sources, /api/perf/io, /api/mqtt/status and /api/healthz.
//
// Both binaries import these types, so a field added, renamed or retyped on one
// side cannot drift from the other. The wire format is unchanged: every key
// below is the key the ingestor has always written.
//
// Times in the file are whole-second UTC (types.WireTimeSeconds), the one
// parser and formatter for them. The generated API types in package api are
// separate from this contract: they describe what the server sends to clients,
// this describes what the ingestor leaves on disk.
package ingeststats

import (
	"encoding/json"
	"os"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

const (
	// PathEnv names the environment variable that moves the stats file, for
	// tests and non-default deployments.
	PathEnv = "CORESCOPE_INGESTOR_STATS"

	// DefaultPath is where the stats file lives when PathEnv is not set.
	//
	// SECURITY: this is under /tmp, which is world-writable. The ingestor writes
	// with O_NOFOLLOW and mode 0o600 so a pre-planted symlink cannot be used to
	// clobber another file. Operators who want a stronger guarantee should point
	// PathEnv at a private directory such as /var/lib/corescope/.
	DefaultPath = "/tmp/corescope-ingestor-stats.json"
)

// Path returns the stats file path: PathEnv if set, otherwise DefaultPath. The
// ingestor and the server both call it, so they cannot disagree.
func Path() string {
	if p := os.Getenv(PathEnv); p != "" {
		return p
	}
	return DefaultPath
}

// IOSample is the per-process I/O rate sample the ingestor publishes under
// "procIO" and the server surfaces under /api/perf/io's ingestor block. Field
// names and JSON tags are the on-disk contract: adding or renaming one is a
// breaking change.
type IOSample struct {
	ReadBytesPerSec           float64                `json:"readBytesPerSec"`
	WriteBytesPerSec          float64                `json:"writeBytesPerSec"`
	CancelledWriteBytesPerSec float64                `json:"cancelledWriteBytesPerSec"`
	SyscallsRead              float64                `json:"syscallsRead"`
	SyscallsWrite             float64                `json:"syscallsWrite"`
	SampledAt                 *types.WireTimeSeconds `json:"sampledAt,omitempty"`
}

// SourceLiveness is the per-MQTT-source two-clock view exposed for /api/healthz.
// Both fields are unix seconds; 0 means "never".
type SourceLiveness struct {
	LastReceiptUnix int64 `json:"lastReceiptUnix"`
	LastMessageUnix int64 `json:"lastMessageUnix"`
}

// SourceStatus is the per-MQTT-source connection state and counter view,
// written under "source_statuses" and served by /api/mqtt/status (#1043).
//
// All times are unix seconds (0 = "never"). PacketsLast5m is a sliding
// five-minute count derived from a per-second ring buffer. Broker may carry
// credentials; the server masks them before sending.
type SourceStatus struct {
	Name               string `json:"name"`
	Broker             string `json:"broker"`
	Connected          bool   `json:"connected"`
	LastConnectUnix    int64  `json:"lastConnectUnix"`
	LastDisconnectUnix int64  `json:"lastDisconnectUnix"`
	LastPacketUnix     int64  `json:"lastPacketUnix"`
	ConnectCount       int64  `json:"connectCount"`
	DisconnectCount    int64  `json:"disconnectCount"`
	PacketsTotal       int64  `json:"packetsTotal"`
	PacketsLast5m      int64  `json:"packetsLast5m"`
	LastError          string `json:"lastError,omitempty"`
}

// Stats is the whole stats file.
//
// Each counter is read with its own atomic load, so a Stats is eventually
// consistent: invariants such as walCommits >= tx_inserted may be briefly
// violated within one sample. Readers must not derive ratios on the assumption
// that the counters were captured at the same instant; treat each as an
// independent, monotonically increasing counter and compare deltas across
// samples.
type Stats struct {
	// SampledAt is when the ingestor took this sample. The server drops a sample
	// that is too old, so a wedged writer shows up as missing data.
	SampledAt          *types.WireTimeSeconds `json:"sampledAt"`
	TxInserted         int64                  `json:"tx_inserted"`
	ObsInserted        int64                  `json:"obs_inserted"`
	DuplicateTx        int64                  `json:"tx_dupes"`
	NodeUpserts        int64                  `json:"node_upserts"`
	ObserverUpserts    int64                  `json:"observer_upserts"`
	WriteErrors        int64                  `json:"write_errors"`
	SignatureDrops     int64                  `json:"sig_drops"`
	WALCommits         int64                  `json:"walCommits"`
	GroupCommitFlushes int64                  `json:"groupCommitFlushes"` // always 0: group commit was reverted (refs #1129)
	BackfillUpdates    map[string]int64       `json:"backfillUpdates"`

	// ProcIO is the ingestor's own /proc/self/io rates since its previous
	// sample (#1120). Absent on hosts without /proc/self/io and in older builds.
	ProcIO *IOSample `json:"procIO,omitempty"`

	// SourceLiveness (PR #1609 M1) is the per-MQTT-source receipt-versus-write
	// liveness, keyed by source tag. /api/healthz shows it as ingest_liveness so
	// operators can tell "broker alive, write path stuck" (recent receipt, stale
	// message) from "everything stalled" (both stale).
	SourceLiveness map[string]SourceLiveness `json:"source_liveness,omitempty"`

	// SourceStatuses (#1043) is the per-MQTT-source connection state for
	// /api/mqtt/status.
	SourceStatuses []SourceStatus `json:"source_statuses,omitempty"`

	// WatchdogLastTickUnix (#1749) is the unix time of the liveness watchdog's
	// last tick. A value older than about twice the scan interval (60s) means the
	// watchdog itself wedged or panicked. 0 means it has not ticked yet.
	WatchdogLastTickUnix int64 `json:"watchdogLastTickUnix,omitempty"`

	// WatchdogPanicCount (#1810) is the running total of panics recovered inside
	// the watchdog's per-source work. The tick clock is stamped before that work,
	// so a loop that panics on every source still looks alive by tick alone; a
	// climbing count shows it is not healthy.
	WatchdogPanicCount int64 `json:"watchdogPanicCount,omitempty"`

	// WatchdogLogDropCount (#1749) is the running total of watchdog log lines
	// dropped because the async writer behind them could not keep up, usually
	// because its own write() is stuck. It tells "watchdog dead" (stale tick)
	// from "watchdog alive but its log sink is stuck" (ticking, count climbing).
	WatchdogLogDropCount int64 `json:"watchdogLogDropCount,omitempty"`
}

// IOView is the part of Stats that /api/perf/io needs. Decoding into it instead
// of Stats avoids allocating BackfillUpdates and the other unused fields on
// every request. Its keys must stay a subset of Stats' keys; a test enforces it.
type IOView struct {
	SampledAt *types.WireTimeSeconds `json:"sampledAt"`
	ProcIO    *IOSample              `json:"procIO,omitempty"`
}

// SampleTime returns when the sample was taken: the file's own sampledAt, or the
// procIO block's when the file has none. ok is false when neither is present.
func (v IOView) SampleTime() (at time.Time, ok bool) {
	switch {
	case v.SampledAt != nil:
		return v.SampledAt.Time, true
	case v.ProcIO != nil && v.ProcIO.SampledAt != nil:
		return v.ProcIO.SampledAt.Time, true
	}
	return time.Time{}, false
}

// Decode parses the stats file. A sampledAt that is not whole-second UTC makes
// the whole file unreadable, as any other malformed JSON does.
func Decode(data []byte) (*Stats, error) {
	var s Stats
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// DecodeIO parses only the I/O part of the stats file.
func DecodeIO(data []byte) (*IOView, error) {
	var v IOView
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
