package database

import "testing"

func TestObjectExists(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT)`)
	execRaw(t, db, `CREATE VIEW nodes_v AS SELECT * FROM nodes`)
	execRaw(t, db, `CREATE INDEX idx_nodes_pk ON nodes(public_key)`)

	tests := []struct {
		objType string
		name    string
		want    bool
	}{
		{"table", "nodes", true},
		{"table", "ghost", false},
		{"view", "nodes_v", true},
		{"view", "ghost_v", false},
		{"index", "idx_nodes_pk", true},
		{"index", "idx_ghost", false},
	}
	for _, tc := range tests {
		got, err := ObjectExists(db, tc.objType, tc.name)
		if err != nil {
			t.Errorf("ObjectExists(%q, %q): unexpected error: %v", tc.objType, tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ObjectExists(%q, %q) = %v, want %v", tc.objType, tc.name, got, tc.want)
		}
	}
}

func TestTableExists(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT)`)

	got, err := TableExists(db, "nodes")
	if err != nil || !got {
		t.Fatalf("expected nodes to exist, got %v err=%v", got, err)
	}
	got, err = TableExists(db, "ghost")
	if err != nil || got {
		t.Fatalf("expected ghost to not exist, got %v err=%v", got, err)
	}
}

func TestViewExists(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT)`)
	execRaw(t, db, `CREATE VIEW packets_v AS SELECT * FROM nodes`)

	got, err := ViewExists(db, "packets_v")
	if err != nil || !got {
		t.Fatalf("expected packets_v to exist, got %v err=%v", got, err)
	}
}

func TestIndexExists(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (last_seen TEXT)`)
	execRaw(t, db, `CREATE INDEX idx_nodes_last_seen ON nodes(last_seen)`)

	got, err := IndexExists(db, "idx_nodes_last_seen")
	if err != nil || !got {
		t.Fatalf("expected index to exist, got %v err=%v", got, err)
	}
}

func TestTableColumns(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (
		public_key TEXT PRIMARY KEY,
		lat REAL,
		advert_count INTEGER DEFAULT 0
	)`)

	cols, err := TableColumns(db, "nodes")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		"public_key":   "TEXT",
		"lat":          "REAL",
		"advert_count": "INTEGER",
	}
	for col, wantType := range want {
		gotType, ok := cols[col]
		if !ok {
			t.Errorf("missing column %q", col)
			continue
		}
		if gotType != wantType {
			t.Errorf("column %q: got type %q, want %q", col, gotType, wantType)
		}
	}
}

func TestTableColumns_NonexistentTable(t *testing.T) {
	db := newMemoryDB(t)
	cols, err := TableColumns(db, "ghost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("expected empty map for nonexistent table, got %v", cols)
	}
}
