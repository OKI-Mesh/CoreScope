package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/ingeststats"
	"github.com/OKI-Mesh/CoreScope/internal/perfio"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// statsFilePath returns the writable path the ingestor will publish stats to:
// ingeststats.Path(), so the server reads the same file. See ingeststats for the
// CORESCOPE_INGESTOR_STATS override and the /tmp security note; the writer below
// uses O_NOFOLLOW and 0o600 for that reason.
func statsFilePath() string {
	return ingeststats.Path()
}

// writeStatsAtomic writes b to path via a tmp-then-rename, refusing to follow
// symlinks on the tmp file. Returns nil on success, an error otherwise.
//
// Symlink semantics (refs #1170):
//
//   - tmp side (path+".tmp"): protected by O_NOFOLLOW below. If tmp is a
//     pre-planted symlink, openat fails with ELOOP instead of writing
//     through it. This is the defensive-coding path that matters when the
//     default stats path lives under world-writable /tmp.
//
//   - rename side (path): NOT protected by O_NOFOLLOW. Instead, os.Rename's
//     semantics are relied upon — rename atomically replaces any existing
//     entry at path (including a symlink) with the new regular file. The
//     symlink's target is NEVER written through, because all writes happened
//     to the unrelated tmp file before rename. Post-rename, path is a
//     regular file (not a symlink) and any prior symlink target's contents
//     are unchanged. The regression guardrail
//     TestWriteStatsAtomic_SymlinkAtDestIsReplaced pins this behavior so a
//     future refactor that swaps os.Rename for a destination-symlink-
//     following primitive (e.g. an open(path, O_WRONLY) without O_NOFOLLOW)
//     fails loudly.
func writeStatsAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	// O_NOFOLLOW: if tmp is a pre-existing symlink, openat fails with ELOOP
	// instead of clobbering the symlink target. O_TRUNC zeroes existing
	// regular-file content. 0o600 — no need for world-readable.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|oNoFollow, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// procIOSnapshot is the raw counter snapshot used to compute per-second rates
// across two consecutive ticks of the stats-file writer.
type procIOSnapshot struct {
	at             time.Time
	readBytes      int64
	writeBytes     int64
	cancelledWrite int64
	syscR          int64
	syscW          int64
	ok             bool
}

// readProcSelfIOFn is the package-level hook the writer loop uses to read
// /proc/self/io. Defaults to readProcSelfIO; tests override it to inject
// deterministic counter snapshots without depending on a Linux kernel
// that exposes /proc/self/io (CONFIG_TASK_IO_ACCOUNTING).
var readProcSelfIOFn = readProcSelfIO

// readProcSelfIO parses /proc/self/io. Returns ok=false on non-Linux hosts or
// any read/parse failure (caller skips the procIO block in that case).
func readProcSelfIO() procIOSnapshot {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return procIOSnapshot{}
	}
	defer f.Close()
	out := procIOSnapshot{at: time.Now()}
	parseProcSelfIOInto(bufio.NewScanner(f), &out)
	return out
}

// parseProcSelfIOInto reads /proc/self/io-shaped key:value lines from sc and
// populates the byte/syscall fields on out. Sets out.ok=true only if at
// least one expected key was successfully parsed (#1167 must-fix #3).
//
// Implementation delegates to perfio.ParseProcIO so the ingestor and the
// server share exactly one parser (Carmack must-fix #7).
func parseProcSelfIOInto(sc *bufio.Scanner, out *procIOSnapshot) {
	var c perfio.Counters
	out.ok = perfio.ParseProcIO(sc, &c)
	out.readBytes = c.ReadBytes
	out.writeBytes = c.WriteBytes
	out.cancelledWrite = c.CancelledWriteBytes
	out.syscR = c.SyscR
	out.syscW = c.SyscW
}

// procIORate computes a per-second rate sample between two procIOSnapshots,
// stamped with sampledAt (Carmack must-fix #5: the writer captures time.Now()
// once per tick and passes the same stamp down, so the snapshot's SampledAt and
// the inner procIO SampledAt cannot drift).
// Returns nil if either snapshot is invalid or the interval is zero.
func procIORate(prev, cur procIOSnapshot, sampledAt *types.WireTimeSeconds) *ingeststats.IOSample {
	if !prev.ok || !cur.ok {
		return nil
	}
	dt := cur.at.Sub(prev.at).Seconds()
	if dt < 0.001 {
		return nil
	}
	return &ingeststats.IOSample{
		ReadBytesPerSec:           float64(cur.readBytes-prev.readBytes) / dt,
		WriteBytesPerSec:          float64(cur.writeBytes-prev.writeBytes) / dt,
		CancelledWriteBytesPerSec: float64(cur.cancelledWrite-prev.cancelledWrite) / dt,
		SyscallsRead:              float64(cur.syscR-prev.syscR) / dt,
		SyscallsWrite:             float64(cur.syscW-prev.syscW) / dt,
		SampledAt:                 sampledAt,
	}
}

// StartStatsFileWriter writes the current stats snapshot to disk every
// `interval` so the server can serve them at /api/perf/write-sources.
// Failures are logged once-per-interval and never fatal.
//
// The stats file path is resolved via statsFilePath() once at writer-loop
// start; the env var (CORESCOPE_INGESTOR_STATS) is only re-read on process
// restart, not per tick.
// The returned stop function ends the writer goroutine and returns once it has
// exited. Production ignores it and runs for the process lifetime; tests must
// call it, because this goroutine reads package-level hooks (readProcSelfIOFn)
// that a later test may replace, and a writer left running past its own test
// races with whoever runs next. It is safe to call more than once.
func StartStatsFileWriter(s *Store, interval time.Duration) (stop func()) {
	if interval <= 0 {
		interval = time.Second
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		t := time.NewTicker(interval)
		defer t.Stop()
		path := statsFilePath()
		// Track previous procIO sample so we can compute per-second deltas
		// across ticks (#1120 follow-up: ingestor /proc/self/io exposure).
		prevIO := readProcSelfIOFn()
		// Reuse a single bytes.Buffer + json.Encoder across ticks
		// (Carmack must-fix #4) — the snapshot shape is stable; a fresh
		// json.Marshal allocation per second × forever is pure GC waste.
		// The buffer grows once and stays.
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			// Capture time.Now() ONCE per tick (Carmack must-fix #5).
			// Both snapshot.SampledAt and procIO.SampledAt MUST share the
			// same stamp so the freshness guard isn't validating one
			// timestamp while the consumer renders another.
			tickAt := time.Now().UTC()
			stamp := &types.WireTimeSeconds{Time: tickAt}
			curIO := readProcSelfIOFn()
			ioRate := procIORate(prevIO, curIO, stamp)
			prevIO = curIO
			snap := ingeststats.Stats{
				SampledAt:            stamp,
				TxInserted:           s.Stats.TransmissionsInserted.Load(),
				ObsInserted:          s.Stats.ObservationsInserted.Load(),
				DuplicateTx:          s.Stats.DuplicateTransmissions.Load(),
				NodeUpserts:          s.Stats.NodeUpserts.Load(),
				ObserverUpserts:      s.Stats.ObserverUpserts.Load(),
				WriteErrors:          s.Stats.WriteErrors.Load(),
				SignatureDrops:       s.Stats.SignatureDrops.Load(),
				WALCommits:           s.Stats.WALCommits.Load(),
				GroupCommitFlushes:   0, // group commit reverted (refs #1129)
				BackfillUpdates:      s.Stats.SnapshotBackfills(),
				ProcIO:               ioRate,
				SourceLiveness:       SnapshotLivenessClocks(),
				SourceStatuses:       SnapshotSourceStatuses(tickAt),
				WatchdogLastTickUnix: WatchdogLastTickUnix(),
				WatchdogPanicCount:   WatchdogPanicCount(),
				WatchdogLogDropCount: WatchdogLogDropCount(),
			}
			buf.Reset()
			if err := enc.Encode(&snap); err != nil {
				log.Printf("[stats-file] encode: %v", err)
				continue
			}
			// json.Encoder.Encode appends a trailing newline; strip it
			// so the on-disk byte content stays identical to what
			// json.Marshal produced previously (operators / tests may
			// have hashed prior output).
			b := buf.Bytes()
			if n := len(b); n > 0 && b[n-1] == '\n' {
				b = b[:n-1]
			}
			if err := writeStatsAtomic(path, b); err != nil {
				log.Printf("[stats-file] write %s: %v", path, err)
			}
		}
	}()
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}
