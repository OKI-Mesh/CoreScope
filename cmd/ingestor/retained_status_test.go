package main

import (
	"testing"
	"time"
)

// A retained MQTT status message is a snapshot the broker replays on every
// subscribe — it says what the observer last published, not that the observer
// is alive now. Treating the replay as liveness makes dead observers immortal:
// last_seen jumps to the ingestor's restart time, RemoveStaleObservers can
// never age them out, and any row it did mark gets reactivated on the next
// restart. Observed on live 2026-08-11: 23 observers, all stamped 15s after
// container start, 18 of them with no real packet in over a month.

const retainedStatusTopic = "meshcore/LAX/obs-retained/status"

func retainedStatusMsg(topic string, payload string) *mockMessage {
	return &mockMessage{topic: topic, payload: []byte(payload), retained: true}
}

// seedObserver inserts an observer and forces last_seen to ageDays ago.
func seedObserver(t *testing.T, s *Store, id string, ageDays int) time.Time {
	ctx := t.Context()
	t.Helper()
	if err := s.UpsertObserver(ctx, id, id, "LAX", nil); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -ageDays)
	testExec(t, s, `UPDATE observers SET last_seen = $1 WHERE id = $2`, old, id)
	return old
}

func observerLastSeen(t *testing.T, s *Store, id string) time.Time {
	t.Helper()
	var got time.Time
	mustScan(t, testRow(t, s, `SELECT last_seen FROM observers WHERE id = $1`, id), &got)
	return got
}

func TestRetainedStatusDoesNotAdvanceLastSeen(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	want := seedObserver(t, store, "obs-retained", 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(retainedStatusTopic, `{"origin":"obs-retained","noise_floor":-95.5}`),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-retained"); !got.Equal(want) {
		t.Errorf("last_seen = %s, want %s (retained replay is not liveness)", got, want)
	}
}

func TestRetainedStatusDoesNotReactivateInactiveObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	seedObserver(t, store, "obs-retained", 60)
	testExec(t, store, `UPDATE observers SET inactive = 1 WHERE id = $1`, "obs-retained")

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(retainedStatusTopic, `{"origin":"obs-retained","noise_floor":-95.5}`),
		nil, nil, &Config{})

	var inactive int
	mustScan(t, testRow(t, store, `SELECT inactive FROM observers WHERE id = $1`, "obs-retained"), &inactive)
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

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg("meshcore/LAX/obs-never-seen/status", `{"origin":"Ghost","noise_floor":-95.5}`),
		nil, nil, &Config{})

	var count int
	count = countWhere(t, store, "observers", "id = $1", "obs-never-seen")
	if count != 0 {
		t.Errorf("observers row count = %d, want 0 (retained-only observer must not be created)", count)
	}
}

// The metrics sample is stamped with ingest time, so a replay would file a
// months-old reading as a present-tense measurement.
func TestRetainedStatusDoesNotInsertMetricsSample(t *testing.T) {
	store := newTestStore(t)
	seedObserver(t, store, "obs-retained", 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(retainedStatusTopic, `{"origin":"obs-retained","noise_floor":-95.5,"battery_mv":3900}`),
		nil, nil, &Config{})

	var count int
	count = countWhere(t, store, "observer_metrics", "observer_id = $1", "obs-retained")
	if count != 0 {
		t.Errorf("observer_metrics rows = %d, want 0 (retained replay is not a new sample)", count)
	}
}

// The retained snapshot is still the observer's last known state, so its
// metadata is worth keeping — only the liveness signal is suppressed.
func TestRetainedStatusStillUpdatesMetadata(t *testing.T) {
	store := newTestStore(t)
	seedObserver(t, store, "obs-retained", 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(retainedStatusTopic, `{"origin":"obs-retained","firmware":"v1.2.3","noise_floor":-95.5}`),
		nil, nil, &Config{})

	var firmware string
	var noiseFloor float64
	mustScan(t, testRow(t, store, `SELECT firmware, noise_floor FROM observers WHERE id = $1`, "obs-retained"), &firmware, &noiseFloor)
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
	before := seedObserver(t, store, "obs-live", 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		&mockMessage{topic: "meshcore/LAX/obs-live/status", payload: []byte(`{"origin":"obs-live","noise_floor":-95.5}`)},
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-live"); got == before {
		t.Errorf("last_seen = %s, want it advanced past %s (live status is liveness)", got, before)
	}
}

func TestLiveStatusStillCreatesUnknownObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		&mockMessage{topic: "meshcore/LAX/obs-fresh/status", payload: []byte(`{"origin":"Fresh","noise_floor":-95.5}`)},
		nil, nil, &Config{})

	var count int
	count = countWhere(t, store, "observers", "id = $1", "obs-fresh")
	if count != 1 {
		t.Errorf("observers row count = %d, want 1 (live status creates the observer)", count)
	}
}
