package main

import (
	"testing"
)

func storedObserverName(t *testing.T, store *Store, pubkey string) *string {
	t.Helper()
	var name *string
	mustScan(t, testRow(t, store, `SELECT name FROM observers WHERE pubkey = $1`, pubkey), &name)
	return name
}

func TestHandleMessageStatus_ObserverNamePresence(t *testing.T) {
	store := newTestStore(t)
	source := MQTTSource{Name: "test"}
	ctx := t.Context()
	send := func(observerID, payload string) {
		t.Helper()
		msg := &mockMessage{topic: "meshcore/SJC/" + observerID + "/status", payload: []byte(payload)}
		handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})
	}

	t.Run("no origin key stores NULL", func(t *testing.T) {
		send("obs-noorigin", `{"status":"online"}`)
		if got := storedObserverName(t, store, "obs-noorigin"); got != nil {
			t.Fatalf("want NULL name, got %q", *got)
		}
	})

	t.Run("a blank origin stores a blank name, not NULL", func(t *testing.T) {
		send("obs-blankorigin", `{"status":"online","origin":""}`)
		got := storedObserverName(t, store, "obs-blankorigin")
		if got == nil || *got != "" {
			t.Fatalf("want a blank name, got %v", got)
		}
	})

	t.Run("a later message without origin keeps the stored name", func(t *testing.T) {
		send("obs-sequence", `{"status":"online","origin":"Hilltop"}`)
		if got := storedObserverName(t, store, "obs-sequence"); got == nil || *got != "Hilltop" {
			t.Fatalf("want Hilltop after the named message, got %v", got)
		}
		send("obs-sequence", `{"status":"online"}`)
		if got := storedObserverName(t, store, "obs-sequence"); got == nil || *got != "Hilltop" {
			t.Fatalf("a message without origin must keep the name: want Hilltop, got %v", got)
		}
	})

	t.Run("a later blank origin overwrites the stored name", func(t *testing.T) {
		send("obs-sequence", `{"status":"online","origin":""}`)
		got := storedObserverName(t, store, "obs-sequence")
		if got == nil || *got != "" {
			t.Fatalf("a blank origin must overwrite the name: want blank, got %v", got)
		}
	})
}
