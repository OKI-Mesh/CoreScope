package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/ingeststats"
)

// TestStatsFileWriter_ServerReadsWhatTheWriterWrote runs the real writer and
// reads its file back with ingeststats.Decode, the function the server uses, so
// the two sides cannot disagree about the file.
func TestStatsFileWriter_ServerReadsWhatTheWriterWrote(t *testing.T) {
	statsPath := filepath.Join(t.TempDir(), "ingestor-stats.json")
	t.Setenv(ingeststats.PathEnv, statsPath)

	store := newTestStore(t)
	defer store.Close()
	store.Stats.TransmissionsInserted.Add(3)
	store.Stats.SignatureDrops.Add(7)

	origFn := readProcSelfIOFn
	t.Cleanup(func() { readProcSelfIOFn = origFn })
	var calls int64
	readProcSelfIOFn = func() procIOSnapshot {
		calls++
		return procIOSnapshot{
			at:         time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(calls) * time.Second),
			readBytes:  1000 * calls,
			writeBytes: 2000 * calls,
			syscR:      10 * calls,
			syscW:      20 * calls,
			ok:         true,
		}
	}

	t.Cleanup(StartStatsFileWriter(store, 50*time.Millisecond))

	var got *ingeststats.Stats
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(75 * time.Millisecond)
		b, err := os.ReadFile(statsPath)
		if err != nil {
			continue
		}
		s, err := ingeststats.Decode(b)
		if err != nil {
			t.Fatalf("Decode() of the writer's own file: %v\n%s", err, b)
		}
		if s.ProcIO != nil {
			got = s
			break
		}
	}
	if got == nil {
		t.Fatal("the stats file never appeared with a procIO block")
	}

	if got.TxInserted != 3 || got.SignatureDrops != 7 {
		t.Errorf("tx_inserted = %d, sig_drops = %d; want 3 and 7", got.TxInserted, got.SignatureDrops)
	}
	if got.SampledAt == nil {
		t.Fatal("sampledAt is missing")
	}
	if age := time.Since(got.SampledAt.Time); age < -2*time.Second || age > 5*time.Second {
		t.Errorf("sampledAt = %v, want a time within the last few seconds", got.SampledAt.Time)
	}
	if got.ProcIO.SampledAt == nil || !got.ProcIO.SampledAt.Equal(got.SampledAt.Time) {
		t.Errorf("procIO.sampledAt = %v, want the file's sampledAt %v", got.ProcIO.SampledAt, got.SampledAt.Time)
	}
	if got.ProcIO.ReadBytesPerSec != 1000 || got.ProcIO.SyscallsWrite != 20 {
		t.Errorf("procIO = %+v, want 1000 read bytes/s and 20 write syscalls/s", got.ProcIO)
	}

	// The cheap decode the I/O endpoint uses sees the same sample.
	b, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatalf("ReadFile(): %v", err)
	}
	view, err := ingeststats.DecodeIO(b)
	if err != nil {
		t.Fatalf("DecodeIO(): %v", err)
	}
	if at, ok := view.SampleTime(); !ok || time.Since(at) > 5*time.Second {
		t.Errorf("SampleTime() = %v, %v; want a recent time", at, ok)
	}
}
