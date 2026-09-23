package database

import (
	"testing"
)

func TestCurrentSchemaVersion_FreshDB(t *testing.T) {
	db := newMemoryDB(t)
	ver, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ver != 0 {
		t.Errorf("expected version 0 for fresh db, got %d", ver)
	}
}

func TestCurrentSchemaVersion_AfterStamp(t *testing.T) {
	db := newMemoryDB(t)
	if err := StampVersions(db, []int64{1, 2, 5}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ver, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ver != 5 {
		t.Errorf("expected max version 5, got %d", ver)
	}
}
