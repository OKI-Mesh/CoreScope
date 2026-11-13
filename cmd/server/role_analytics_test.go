package main

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestComputeRoleAnalytics_Distribution verifies that computeRoleAnalytics
// groups nodes by role, normalises empty/case-different roles, and sorts the
// output largest-population first. Asserts on the public RoleAnalyticsResponse
// shape so the bar is "behaviour", not "compiles".
func TestComputeRoleAnalytics_Distribution(t *testing.T) {
	nodes := map[string]types.NodeRole{
		"pk_a": types.RoleRepeater,
		"pk_b": types.RoleRepeater,
		"pk_c": types.RoleCompanion,
		"pk_d": "",
		"pk_e": types.RoleRoom,
	}
	got := computeRoleAnalytics(nodes, nil)

	if got.TotalNodes != 5 {
		t.Fatalf("TotalNodes = %d, want 5", got.TotalNodes)
	}
	if len(got.Roles) != 4 {
		t.Fatalf("len(Roles) = %d, want 4 (repeater, companion, room, unknown), got %+v", len(got.Roles), got.Roles)
	}
	if got.Roles[0].Role != types.RoleRepeater || got.Roles[0].NodeCount != 2 {
		t.Errorf("Roles[0] = %+v, want {repeater,2}", got.Roles[0])
	}

	counts := map[types.NodeRole]int{}
	for _, r := range got.Roles {
		counts[r.Role] = r.NodeCount
	}
	want := map[types.NodeRole]int{
		types.RoleRepeater:  2,
		types.RoleCompanion: 1,
		types.RoleRoom:      1,
		types.RoleUnknown:   1,
	}
	for role, n := range want {
		if counts[role] != n {
			t.Errorf("bucket %q = %d, want %d (all buckets: %+v)", role, counts[role], n, got.Roles)
		}
	}
}

// TestComputeRoleAnalytics_SkewAggregation verifies per-role clock-skew
// aggregation: counts by severity, mean and median absolute skew.
func TestComputeRoleAnalytics_SkewAggregation(t *testing.T) {
	nodes := map[string]types.NodeRole{
		"pk_1": "repeater",
		"pk_2": "repeater",
		"pk_3": "repeater",
	}
	skews := map[string]*NodeClockSkew{
		"pk_1": {Pubkey: "pk_1", RecentMedianSkewSec: 10, Severity: types.SkewOK},
		"pk_2": {Pubkey: "pk_2", RecentMedianSkewSec: -400, Severity: types.SkewWarning},
		"pk_3": {Pubkey: "pk_3", RecentMedianSkewSec: 7200, Severity: types.SkewCritical},
	}
	got := computeRoleAnalytics(nodes, skews)
	if len(got.Roles) != 1 {
		t.Fatalf("len(Roles) = %d, want 1; got %+v", len(got.Roles), got.Roles)
	}
	r := got.Roles[0]
	if r.WithSkew != 3 {
		t.Errorf("WithSkew = %d, want 3", r.WithSkew)
	}
	if r.OkCount != 1 || r.WarningCount != 1 || r.CriticalCount != 1 {
		t.Errorf("severity counts = ok %d, warn %d, crit %d; want 1/1/1", r.OkCount, r.WarningCount, r.CriticalCount)
	}
	// mean(|10|, |−400|, |7200|) = 7610/3 ≈ 2536.67
	if r.MeanAbsSkewSec < 2536 || r.MeanAbsSkewSec > 2537 {
		t.Errorf("MeanAbsSkewSec = %v, want ~2536.67", r.MeanAbsSkewSec)
	}
	// median(10, 400, 7200) = 400
	if r.MedianAbsSkewSec != 400 {
		t.Errorf("MedianAbsSkewSec = %v, want 400", r.MedianAbsSkewSec)
	}
}
