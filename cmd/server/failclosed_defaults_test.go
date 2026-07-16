package main

import "testing"

// #44: fail-closed packet-store MEMORY default, applied at the composition root
// (see main.go) via applyFailClosedDefaults.
//
// ONLY MaxMemoryMB is defaulted. RetentionHours is an age policy, not a memory
// guarantee (24h of a busy mesh can still exhaust RAM), so defaulting it would
// silently delete data WITHOUT preventing the OOM. It is left exactly as
// configured.
//
// Contract for MaxMemoryMB:
//   >0  explicit bound (unchanged)
//   0   unset/omitted -> fail-closed default (see failClosedMemoryMB)
//   <0  explicit opt-out -> genuinely unlimited (normalized to 0)

func TestApplyFailClosedDefaults_NilConfigBoundsMemoryOnly(t *testing.T) {
	out := applyFailClosedDefaults(nil)
	if out == nil {
		t.Fatal("must return a non-nil config")
	}
	if out.MaxMemoryMB <= 0 {
		t.Fatalf("nil config must fail closed on memory; got maxMemoryMB=%d", out.MaxMemoryMB)
	}
	if out.RetentionHours != 0 {
		t.Fatalf("retention must be left untouched (0), got %g", out.RetentionHours)
	}
}

func TestApplyFailClosedDefaults_EmptyConfigBoundsMemoryOnly(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{})
	if out.MaxMemoryMB <= 0 {
		t.Fatalf("empty config must fail closed on memory; got maxMemoryMB=%d", out.MaxMemoryMB)
	}
	if out.RetentionHours != 0 {
		t.Fatalf("retention must be left untouched (0), got %g", out.RetentionHours)
	}
}

func TestApplyFailClosedDefaults_ExplicitMemoryBoundRespected(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{MaxMemoryMB: 512})
	if out.MaxMemoryMB != 512 {
		t.Fatalf("expected maxMemoryMB=512, got %d", out.MaxMemoryMB)
	}
}

func TestApplyFailClosedDefaults_NegativeMemoryIsExplicitUnlimited(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{MaxMemoryMB: -1})
	if out.MaxMemoryMB != 0 {
		t.Fatalf("negative sentinel must normalize to unlimited (0), got %d", out.MaxMemoryMB)
	}
}

// An age bound is NOT a memory guarantee: 24h of a busy mesh can still exhaust
// RAM. A configured retention must therefore not suppress the memory default.
func TestApplyFailClosedDefaults_RetentionBoundStillBoundsMemory(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: 24})
	if out.RetentionHours != 24 {
		t.Fatalf("expected retentionHours=24 preserved, got %g", out.RetentionHours)
	}
	if out.MaxMemoryMB <= 0 {
		t.Fatalf("an age bound must not suppress the memory default; got maxMemoryMB=%d", out.MaxMemoryMB)
	}
}

// Retention must NEVER be defaulted: a config-less server must not silently
// start deleting data by age. (Imposing 168h is what evicted the E2E fixture's
// seeded row on #46.)
func TestApplyFailClosedDefaults_NeverImposesRetention(t *testing.T) {
	cases := []*PacketStoreConfig{
		nil,
		{},
		{MaxMemoryMB: 512},
		{MaxMemoryMB: -1},
	}
	for _, cfg := range cases {
		out := applyFailClosedDefaults(cfg)
		if out.RetentionHours != 0 {
			t.Fatalf("retention must never be defaulted; got %g for cfg %+v", out.RetentionHours, cfg)
		}
	}
}

func TestApplyFailClosedDefaults_NegativeRetentionNormalized(t *testing.T) {
	out := applyFailClosedDefaults(&PacketStoreConfig{RetentionHours: -1, MaxMemoryMB: 512})
	if out.RetentionHours != 0 {
		t.Fatalf("negative retention must normalize to 0, got %g", out.RetentionHours)
	}
}

// The resolver must not mutate the caller's config.
func TestApplyFailClosedDefaults_DoesNotMutateInput(t *testing.T) {
	in := &PacketStoreConfig{}
	_ = applyFailClosedDefaults(in)
	if in.RetentionHours != 0 || in.MaxMemoryMB != 0 {
		t.Fatalf("input config was mutated: retentionHours=%g maxMemoryMB=%d",
			in.RetentionHours, in.MaxMemoryMB)
	}
}

// The cgroup-derived memory bound must never truncate to 0 (NewPacketStore
// reads 0 as "unlimited"), must honour the minFailClosedMemoryMB floor at its
// exact boundary, and must fall back to the static default when no cgroup limit
// is readable.
func TestFailClosedMemoryMB(t *testing.T) {
	orig := readCgroupMemoryMBFn
	defer func() { readCgroupMemoryMBFn = orig }()

	cases := []struct {
		name     string
		cgroupMB int64
		want     int
	}{
		{"no cgroup readable -> static fallback", 0, failClosedStaticMemoryMB},
		{"normal cgroup -> two thirds", 3000, 2000},
		{"pathologically tiny -> floor, never 0", 1, minFailClosedMemoryMB},
		{"derives just below floor (47*2/3=31)", 47, minFailClosedMemoryMB},
		{"derives exactly at floor (48*2/3=32)", 48, minFailClosedMemoryMB},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			readCgroupMemoryMBFn = func() int64 { return tc.cgroupMB }
			if got := failClosedMemoryMB(); got != tc.want {
				t.Errorf("cg=%d: want %d, got %d", tc.cgroupMB, tc.want, got)
			}
		})
	}
}

// End-to-end wiring: a cgroup limit must actually flow through the resolver and
// bound an unconfigured store (the resolver tests above otherwise only exercise
// the static-fallback path, since dev machines have no cgroup).
func TestApplyFailClosedDefaults_BoundsMemoryFromCgroup(t *testing.T) {
	orig := readCgroupMemoryMBFn
	defer func() { readCgroupMemoryMBFn = orig }()
	readCgroupMemoryMBFn = func() int64 { return 3000 } // -> 2000

	if out := applyFailClosedDefaults(nil); out.MaxMemoryMB != 2000 {
		t.Errorf("nil config: expected cgroup-derived 2000, got %d", out.MaxMemoryMB)
	}
	if out := applyFailClosedDefaults(&PacketStoreConfig{}); out.MaxMemoryMB != 2000 {
		t.Errorf("empty config: expected cgroup-derived 2000, got %d", out.MaxMemoryMB)
	}
	// An explicit bound must still win over the cgroup derivation.
	if out := applyFailClosedDefaults(&PacketStoreConfig{MaxMemoryMB: 512}); out.MaxMemoryMB != 512 {
		t.Errorf("explicit bound must beat cgroup derivation, got %d", out.MaxMemoryMB)
	}
}
