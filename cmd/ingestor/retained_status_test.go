package main

import (
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// A retained MQTT status message is a snapshot the broker replays on every
// subscribe — it says what the observer last published, not that the observer
// is alive now. Treating the replay as liveness makes dead observers immortal:
// last_seen jumps to the ingestor's restart time, RemoveStaleObservers can
// never age them out, and any row it did mark gets reactivated on the next
// restart. Observed on live 2026-08-11: 23 observers, all stamped 15s after
// container start, 18 of them with no real packet in over a month.

func statusTopic(id string) string {
	return "meshcore/LAX/" + id + "/status"
}

func retainedStatusMsg(topic string, payload string) *mockMessage {
	return &mockMessage{topic: topic, payload: []byte(payload), retained: true}
}

// seedObserver inserts an observer and forces last_seen to ageDays ago.
func seedObserver(t *testing.T, s *Store, id string, ageDays int) time.Time {
	t.Helper()
	ctx := t.Context()
	old := time.Now().UTC().AddDate(0, 0, -ageDays).Truncate(time.Second)
	if _, err := s.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        id,
		Name:      id,
		IATA:      "LAX",
		Meta:      nil,
		LastSeen:  old,
		IngestNow: old,
	}); err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	return old
}

func TestRetainedStatusDoesNotReactivateInactiveObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0b", 32)
	seedObserver(t, store, observerID, 60)
	if err := store.db.MarkObserverInactive(ctx, observerID); err != nil {
		t.Fatalf("MarkObserverInactive: %v", err)
	}

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(statusTopic(observerID), `{"origin":"`+observerID+`","noise_floor":-95.5}`),
		nil, nil, &Config{})

	var inactive int
	mustScan(t, testRow(t, store, `SELECT inactive::int FROM observers WHERE pubkey = $1`, observerID), &inactive)
	if inactive != 1 {
		t.Errorf("inactive = %d, want 1 (retained replay must not resurrect a soft-deleted observer)", inactive)
	}
}

// A retained snapshot from an observer the analyzer has never heard from live
// describes a past that may be months old. Creating a row for it puts a
// permanently dead observer in the list.
func TestRetainedStatusDoesNotCreateUnknownObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0c", 32)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(statusTopic(observerID), `{"origin":"Ghost","noise_floor":-95.5}`),
		nil, nil, &Config{})

	count := countWhere(t, store, "observers", "pubkey = $1", observerID)
	if count != 0 {
		t.Errorf("observers row count = %d, want 0 (retained-only observer must not be created)", count)
	}
}

// The metrics sample is stamped with ingest time, so a replay would file a
// months-old reading as a present-tense measurement.
func TestRetainedStatusDoesNotInsertMetricsSample(t *testing.T) {
	store := newTestStore(t)
	observerID := strings.Repeat("0b", 32)
	seedObserver(t, store, observerID, 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(statusTopic(observerID), `{"origin":"`+observerID+`","noise_floor":-95.5,"battery_mv":3900}`),
		nil, nil, &Config{})

	count := countWhere(t, store, "observer_metrics", "observer_id = $1", observerID)
	if count != 0 {
		t.Errorf("observer_metrics rows = %d, want 0 (retained replay is not a new sample)", count)
	}
}

// The retained snapshot is still the observer's last known state, so its
// metadata is worth keeping — only the liveness signal is suppressed.
func TestRetainedStatusStillUpdatesMetadata(t *testing.T) {
	store := newTestStore(t)
	observerID := strings.Repeat("0b", 32)
	seedObserver(t, store, observerID, 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(statusTopic(observerID), `{"origin":"`+observerID+`","firmware":"v1.2.3","noise_floor":-95.5}`),
		nil, nil, &Config{})

	var firmware string
	var noiseFloor float64
	mustScan(t, testRow(t, store, `SELECT firmware, noise_floor FROM observers WHERE pubkey = $1`, observerID), &firmware, &noiseFloor)
	if firmware != "v1.2.3" {
		t.Errorf("firmware = %q, want %q", firmware, "v1.2.3")
	}
	if noiseFloor != -95.5 {
		t.Errorf("noise_floor = %v, want -95.5", noiseFloor)
	}
}

// Regression guard for #1465: a live status message must still stamp last_seen
// with ingest time. Only the retained flag changes the behaviour.
func TestLiveStatusStillAdvancesLastSeen(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0d", 32)
	before := seedObserver(t, store, observerID, 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		&mockMessage{topic: statusTopic(observerID), payload: []byte(`{"origin":"` + observerID + `","noise_floor":-95.5}`)},
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if !got.After(before) {
		t.Errorf("last_seen = %s, want it advanced past %s (live status is liveness)", got, before)
	}
	if err != nil {
		t.Errorf("Unable to find Observer for pubkey: %s", observerID)
	}
}

func TestLiveStatusStillCreatesUnknownObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0e", 32)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		&mockMessage{topic: statusTopic(observerID), payload: []byte(`{"origin":"Fresh","noise_floor":-95.5}`)},
		nil, nil, &Config{})

	count := countWhere(t, store, "observers", "pubkey = $1", observerID)
	if count != 1 {
		t.Errorf("observers row count = %d, want 1 (live status creates the observer)", count)
	}
}
