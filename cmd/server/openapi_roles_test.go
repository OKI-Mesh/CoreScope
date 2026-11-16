package main

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
)

// TestSpecFilterableRoleEnumMatchesFilterable fails when the FilterableRole enum
// in the spec and types.NodeRole.Filterable() disagree.
func TestSpecFilterableRoleEnumMatchesFilterable(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	schema, ok := doc.Components.Schemas["FilterableRole"]
	if !ok {
		t.Fatal("FilterableRole is not in the spec")
	}

	inSpec := make(map[types.NodeRole]bool)
	for _, v := range schema.Value.Enum {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("FilterableRole enum value %v is not a string", v)
		}
		inSpec[types.NodeRole(s)] = true
	}

	all := []types.NodeRole{
		types.RoleRepeater, types.RoleCompanion, types.RoleRoom,
		types.RoleSensor, types.RoleObserver, types.RoleUnknown,
	}
	for _, role := range all {
		if role.Filterable() != inSpec[role] {
			t.Errorf("role %q: Filterable()=%v but in spec enum=%v", role, role.Filterable(), inSpec[role])
		}
	}
}
