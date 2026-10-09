package main

import (
	"testing"
)

func TestLoadChannelKeys_NormalizesKnownDisplayNames(t *testing.T) {
	// Verify that known channel keys with wrong casing get normalized
	cfg := &Config{
		ChannelKeys: map[string]string{
			"public": "8b3387e9c5cdea6ac9e5edbaa115cd72",
		},
	}

	keys := loadChannelKeys(cfg, "/dev/null")

	// Should have "Public" (normalized) not "public" (raw)
	if _, ok := keys["public"]; ok {
		t.Error("Expected 'public' to be normalized to 'Public'")
	}
	if _, ok := keys["Public"]; !ok {
		t.Error("Expected 'Public' key to exist in loaded channel keys")
	}
}

func TestLoadChannelKeys_LeavesCustomNamesUntouched(t *testing.T) {
	// Verify that custom channel names are NOT normalized
	cfg := &Config{
		ChannelKeys: map[string]string{
			"myCustomChannel": "deadbeef12345678",
		},
	}

	keys := loadChannelKeys(cfg, "/dev/null")

	// Should keep "myCustomChannel" as-is
	if _, ok := keys["myCustomChannel"]; !ok {
		t.Error("Expected 'myCustomChannel' to be left untouched")
	}
	// Should NOT have "MyCustomChannel"
	if _, ok := keys["MyCustomChannel"]; ok {
		t.Error("Custom channel names should NOT be auto-capitalized")
	}
}

func TestLoadChannelKeys_DuplicateCasingLogsWarning(t *testing.T) {
	// Verify that config with both "public" and "Public" resolves deterministically:
	// the canonical (already-normalized) form should win.
	cfg := &Config{
		ChannelKeys: map[string]string{
			"public": "8b3387e9c5cdea6ac9e5edbaa115cd72",
			"Public": "differentkey1234567",
		},
	}

	keys := loadChannelKeys(cfg, "/dev/null")

	// After normalization, only one key should exist: "Public"
	// The canonical form ("Public") should win over the lowercase form ("public")
	if _, ok := keys["public"]; ok {
		t.Error("Expected 'public' to be normalized away")
	}
	if _, ok := keys["Public"]; !ok {
		t.Error("Expected 'Public' key to exist")
	}
	// Assert the canonical form's value won, not just any value
	if keys["Public"] != "differentkey1234567" {
		t.Errorf("Expected canonical 'Public' value to win, got %q", keys["Public"])
	}
}
