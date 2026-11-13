package types

import (
	"testing"
)

func TestNodeRoleAdvertTypeRoundTrip(t *testing.T) {
	for _, r := range []NodeRole{RoleCompanion, RoleRepeater, RoleRoom, RoleSensor} {
		advertType, ok := r.AdvertType()
		if !ok {
			t.Fatalf("%s has no advert type", r)
		}
		got, ok := NodeRoleFromAdvertType(advertType)
		if !ok || got != r {
			t.Errorf("advert type %d -> %q (ok=%v), want %q", advertType, got, ok, r)
		}
	}
}

func TestNodeRoleFromAdvertTypeRejectsNonRoles(t *testing.T) {
	for _, advertType := range []int{0, 5, 255} {
		if got, ok := NodeRoleFromAdvertType(advertType); ok {
			t.Errorf("advert type %d resolved to %q, want no role", advertType, got)
		}
	}
	if _, ok := RoleUnknown.AdvertType(); ok {
		t.Error("RoleUnknown must not have an advert type")
	}
}

func TestNodeRoleRelaysTraffic(t *testing.T) {
	cases := []struct {
		role NodeRole
		want bool
	}{
		{RoleRepeater, true},
		{RoleRoom, true},
		{RoleCompanion, false},
		{RoleSensor, false},
		{RoleUnknown, false},
		{"", false},
	}
	for _, tc := range cases {
		if got := tc.role.RelaysTraffic(); got != tc.want {
			t.Errorf("%q.RelaysTraffic() = %v, want %v", tc.role, got, tc.want)
		}
	}
}

func TestNodeRoleFilterable(t *testing.T) {
	for _, r := range []NodeRole{RoleRepeater, RoleCompanion, RoleRoom, RoleSensor, RoleObserver} {
		if !r.Filterable() {
			t.Errorf("%q should be filterable", r)
		}
	}
	for _, r := range []NodeRole{"", RoleUnknown, "room_server", "Repeater", "client"} {
		if r.Filterable() {
			t.Errorf("%q should not be filterable", r)
		}
	}
}

func TestObserverIsNotAStoredRole(t *testing.T) {
	if RoleObserver.Valid() {
		t.Error("observer is an API-only pseudo-role and must not be a valid stored role")
	}
	if _, ok := RoleObserver.AdvertType(); ok {
		t.Error("observer has no advert type")
	}
	if RoleObserver.RelaysTraffic() {
		t.Error("observer must not relay traffic")
	}
}
