package main

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
)

// TestSpecRoleEnumMatchesNodeRole fails when a role is added to or removed from
// types.NodeRole without the same change to the NodeRole enum in openapi.yaml.
func TestSpecRoleEnumMatchesNodeRole(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	schema, ok := doc.Components.Schemas["NodeRole"]
	if !ok {
		t.Fatal("NodeRole is not in the spec")
	}

	inSpec := make(map[types.NodeRole]bool)
	for _, v := range schema.Value.Enum {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("NodeRole enum value %v is not a string", v)
		}
		inSpec[types.NodeRole(s)] = true
	}

	inCode := []types.NodeRole{
		types.RoleRepeater,
		types.RoleCompanion,
		types.RoleRoom,
		types.RoleSensor,
		types.RoleObserver,
		types.RoleUnknown,
	}

	if len(inSpec) != len(inCode) {
		t.Errorf("spec lists %d roles, code defines %d", len(inSpec), len(inCode))
	}
	for _, role := range inCode {
		if !inSpec[role] {
			t.Errorf("role %q is defined in code but missing from the spec enum", role)
		}
	}
}
