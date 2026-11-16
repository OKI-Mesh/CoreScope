package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

// TestServerSourceHasNoWriterCallSites enforces issue #1287: after the
// follow-up to #1283, cmd/server/ must contain ZERO writer call sites.
// Specifically, no `OpenReadWrite(`, no raw DML against the node directory
// and no multibyte-persist helpers in non-test source files. All schema
// migrations, backfills, and
// neighbor-edge persistence must live in cmd/ingestor or a shared
// package — the server is the read path.
func TestServerSourceHasNoWriterCallSites(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd/server dir: %v", err)
	}
	// Patterns that indicate write-side DB usage on the server.
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\bOpenReadWrite\s*\(`),
		// The node directory is ingestor-owned (#1283/#1287); the
		// server opens mode=ro since #1289 and may never write it.
		//
		// This deliberately matches ANY column rather than naming
		// them. The column-specific form is what let #1598 through:
		// #1324 added `UPDATE nodes SET multibyte_` after relocating
		// PR #903's writer, then touchRelayLastSeen introduced a
		// second write shape (`SET last_seen`) that the grep did not
		// cover. It failed on every call for months with the error
		// discarded at the call site, so nodes.last_seen silently
		// degraded into an advert-age proxy. Enumerating shapes does
		// not scale — forbid the table instead.
		//
		// Writers live in cmd/ingestor: Store.TouchRelayNodes (#1598)
		// and RunMultibyteCapPersist (#1324, fed by a snapshot the
		// server publishes via internal/mbcapqueue).
		// Shapes are normalised before matching (see nodeTableWritePattern):
		// optional OR-conflict clause, optional quoting, optional alias.
		nodeTableWritePattern(`UPDATE(\s+OR\s+\w+)?`, `SET`),
		nodeTableWritePattern(`INSERT\s+(OR\s+\w+\s+)?INTO`, ``),
		nodeTableWritePattern(`REPLACE\s+INTO`, ``),
		nodeTableWritePattern(`DELETE\s+FROM`, ``),
		regexp.MustCompile(`\bpersistMultibyteCapability\s*\(`),
		regexp.MustCompile(`\bmaybePersistMultibyteCapability\s*\(`),
	}
	violations := []string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, p := range patterns {
			if loc := p.FindIndex(b); loc != nil {
				// Get line number
				line := 1 + strings.Count(string(b[:loc[0]]), "\n")
				violations = append(violations, fmt.Sprintf("%s:%d: %s", name, line, p.String()))
			}
		}
	}
	if len(violations) > 0 {
		t.Errorf("cmd/server/ contains forbidden writer call sites (#1287):\n  %s",
			strings.Join(violations, "\n  "))
	}
}

// TestPacketStoreHasNoMultibytePersistMethods enforces the #1324 follow-up:
// PR #903 wired persistMultibyteCapability + maybePersistMultibyteCapability
// onto *PacketStore in cmd/server. Both executed UPDATEs on
// nodes/inactive_nodes from a mode=ro DB handle — impossible since #1289.
// After relocation the persistence lives in cmd/ingestor/*Store; the
// server only publishes a snapshot via internal/mbcapqueue. This test
// fails if a future change re-introduces these methods on *PacketStore.
func TestPacketStoreHasNoMultibytePersistMethods(t *testing.T) {
	forbidden := []string{
		"persistMultibyteCapability",
		"maybePersistMultibyteCapability",
	}
	typ := reflect.TypeOf((*PacketStore)(nil))
	for _, name := range forbidden {
		if _, ok := typ.MethodByName(name); ok {
			t.Errorf("server *PacketStore exposes forbidden write method %q — must be relocated to ingestor (#1324)", name)
		}
	}
}

// nodeTableWritePattern builds a matcher for DML against the
// ingestor-owned node directory. verb is the leading keyword(s); trailer
// is what must follow the table name (e.g. SET for UPDATE), or empty.
//
// Covers the shapes a plain `UPDATE nodes SET` regex misses:
//
//	UPDATE OR REPLACE nodes SET ...
//	UPDATE "nodes" SET ... / `nodes` / [nodes]
//	UPDATE nodes AS n SET ...
//	REPLACE INTO nodes ...
//
// Residual gap, stated rather than papered over: SQL assembled at
// runtime (fmt.Sprintf("UPDATE %s SET ...", tbl)) cannot be caught by
// source grepping. That is what TestOpenDBIsReadOnly is for — Postgres
// refuses writes on the server's handle. This test is the
// cheap first line that names the offending file and line; those two are
// the structural backstop.
func nodeTableWritePattern(verb, trailer string) *regexp.Regexp {
	const table = "[\"`\\[]?(nodes|inactive_nodes)[\"`\\]]?"
	const alias = `(\s+(AS\s+)?[a-z]\w*)?`
	expr := `(?i)` + verb + `\s+` + table + alias
	if trailer != "" {
		expr += `\s+` + trailer
	} else {
		expr += `\b`
	}
	return regexp.MustCompile(expr)
}

// TestOpenDBIsReadOnly pins the guarantee the server relies on: a write
// through the handle OpenDB returns is refused by Postgres itself, so a bug
// elsewhere in cmd/server cannot modify data.
func TestOpenDBIsReadOnly(t *testing.T) {
	ctx := t.Context()
	db, err := OpenDB(ctx, pgtest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	now := time.Now().UTC()
	_, err = db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "0f0f0f0f",
		Name:      ptr("should not be written"),
		IATA:      "CVG",
		LastSeen:  now,
		IngestNow: now,
	})
	if err == nil {
		t.Fatal("a write through the server's database handle succeeded; the handle is not read-only")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("write failed, but not because the handle is read-only: %v", err)
	}
}
