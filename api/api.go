// Package api holds the CoreScope HTTP contract: the OpenAPI document and the
// response types generated from it. Edit openapi.yaml, then run go generate.
package api

import _ "embed"

//go:generate go tool oapi-codegen -config oapi-codegen.yaml openapi.yaml

// Spec is the OpenAPI document served at /api/spec and used by contract tests.
//
//go:embed openapi.yaml
var Spec []byte
