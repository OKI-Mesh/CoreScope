package main

import "testing"

// #44: fail-closed packet-store bounding policy, applied at the composition
// root (see main.go) via applyFailClosedDefaults.
//
// The store CONSTRUCTOR keeps its original contract (0 = unlimited); the POLICY
// of substituting a safe bound when nothing is configured lives here, so it
// protects the production path without surprising the many tests that build a
// raw store with a nil config.
//
// Contract for RetentionHours / MaxMemoryMB:
//   >0  explicit bound (unchanged)
//   0   unset/omitted -> safe fail-closed default
//   <0  explicit opt-out -> genuinely unlimited (normalized to 0)
// Fail-closed only triggers when BOTH axes are unset — a single configured
// bound already caps growth.

func TestApplyFailClosedDefaults_NilConfig(t *testing.T) {
	out := applyFailClosedDefaults(nil)
	if out == nil {
		t.Fatal("must return a non-nil config")
	}
	if out.RetentionHours <= 0 || out.MaxMemoryMB <= 0 {
		t.Fatalf("nil config must fail closed; got retentionHours=%g maxMemoryMB=%d",
			out.RetentionHours, out.MaxMemoryMB)
	}
}

func TestApplyFailClosedDefaults_EmptyConfig(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{})
	if out.RetentionHours <= 0 || out.MaxMemoryMB <= 0 {
		t.Fatalf("empty config must fail closed; got retentionHours=%g maxMemoryMB=%d",
			out.RetentionHours, out.MaxMemoryMB)
	}
}

func TestApplyFailClosedDefaults_NegativeSentinelIsUnlimited(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: -1, MaxMemoryMB: -1})
	if out.RetentionHours != 0 || out.MaxMemoryMB != 0 {
		t.Fatalf("negative sentinel must normalize to unlimited (0); got retentionHours=%g maxMemoryMB=%d",
			out.RetentionHours, out.MaxMemoryMB)
	}
}

// A single explicit bound already caps growth — the other axis is left alone.
func TestApplyFailClosedDefaults_MemoryOnlyNotOverridden(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{MaxMemoryMB: 512})
	if out.MaxMemoryMB != 512 {
		t.Fatalf("expected maxMemoryMB=512, got %d", out.MaxMemoryMB)
	}
	if out.RetentionHours != 0 {
		t.Fatalf("expected retentionHours left at 0 when memory is bounded, got %g", out.RetentionHours)
	}
}

func TestApplyFailClosedDefaults_RetentionOnlyNotOverridden(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: 24})
	if out.RetentionHours != 24 {
		t.Fatalf("expected retentionHours=24, got %g", out.RetentionHours)
	}
	if out.MaxMemoryMB != 0 {
		t.Fatalf("expected maxMemoryMB left at 0 when retention is bounded, got %d", out.MaxMemoryMB)
	}
}

// The resolver must not mutate the caller's config — main.go reads the original
// for the GOMEMLIMIT derivation.
func TestApplyFailClosedDefaults_DoesNotMutateInput(t *testing.T) {
	in := &PacketStoreConfig{}
	_ = applyFailClosedDefaults(in)
	if in.RetentionHours != 0 || in.MaxMemoryMB != 0 {
		t.Fatalf("input config was mutated: retentionHours=%g maxMemoryMB=%d",
			in.RetentionHours, in.MaxMemoryMB)
	}
}

// A negative "unlimited" opt-out on ONE axis must NOT disable the fail-closed
// default on the OTHER, unconfigured axis — otherwise {retentionHours:-1,
// maxMemoryMB:0} silently runs fully unbounded (the exact OOM we prevent).
func TestApplyFailClosedDefaults_UnlimitedRetentionStillBoundsMemory(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: -1, MaxMemoryMB: 0})
	if out.RetentionHours != 0 {
		t.Fatalf("expected retention normalized to unlimited (0), got %g", out.RetentionHours)
	}
	if out.MaxMemoryMB <= 0 {
		t.Fatalf("expected unset memory to get a fail-closed bound, got %d", out.MaxMemoryMB)
	}
}

func TestApplyFailClosedDefaults_UnlimitedMemoryStillBoundsRetention(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: 0, MaxMemoryMB: -1})
	if out.MaxMemoryMB != 0 {
		t.Fatalf("expected memory normalized to unlimited (0), got %d", out.MaxMemoryMB)
	}
	if out.RetentionHours <= 0 {
		t.Fatalf("expected unset retention to get a fail-closed bound, got %g", out.RetentionHours)
	}
}

// The cgroup-derived memory bound must never truncate to 0 (NewPacketStore
// reads 0 as "unlimited"), and must fall back to the static default when no
// cgroup limit is readable.
func TestFailClosedMemoryMB(t *testing.T) {
	orig := readCgroupMemoryMBFn
	defer func() { readCgroupMemoryMBFn = orig }()

	readCgroupMemoryMBFn = func() int64 { return 0 } // no cgroup -> static fallback
	if got := failClosedMemoryMB(); got != failClosedStaticMemoryMB {
		t.Errorf("cg=0: expected static %d, got %d", failClosedStaticMemoryMB, got)
	}
	readCgroupMemoryMBFn = func() int64 { return 3000 } // 2/3 = 2000
	if got := failClosedMemoryMB(); got != 2000 {
		t.Errorf("cg=3000: expected 2000, got %d", got)
	}
	readCgroupMemoryMBFn = func() int64 { return 1 } // pathologically tiny — must never be 0
	if got := failClosedMemoryMB(); got <= 0 {
		t.Errorf("cg=1: expected non-zero floored bound, got %d", got)
	}
}
