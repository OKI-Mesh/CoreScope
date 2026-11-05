package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The retain flag alone does not identify a replay. Our live topology puts a
// mosquitto bridge between EMQX and the ingestor, and MQTT only sets RETAIN on
// messages delivered in response to a *new* subscription. When the bridge
// reconnects (it runs cleansession, so every reconnect resubscribes), EMQX
// replays its whole retained set to the bridge, which republishes it into its
// own broker — and the ingestor, already subscribed, receives those months-old
// snapshots with RETAIN cleared, indistinguishable from live traffic.
//
// Observed on live 2026-08-13: EMQX restarted at 13:09 UTC and 17 long-dead
// observers were recreated inside two seconds, among them BE-BGS-RRY120-RES
// whose retained payload was published 2026-04-02 and says "offline".
//
// So judge the payload, not the transport: a status message is evidence of
// life only if it claims to be online and carries a timestamp from roughly now.

func liveStatusMsg(topic, payload string) *mockMessage {
	return &mockMessage{topic: topic, payload: []byte(payload), retained: false}
}

func statusPayload(origin, status string, ts time.Time) string {
	return fmt.Sprintf(`{"origin":%q,"status":%q,"timestamp":%q,"noise_floor":-95.5}`,
		origin, status, ts.Format("2006-01-02T15:04:05.000000"))
}

func TestRetainedStatusDoesNotAdvanceLastSeen(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0b", 32)
	want := seedObserver(t, store, observerID, 60)

	topic := "meshcore/LAX/" + observerID + "/status"
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		retainedStatusMsg(topic, `{"origin":"`+observerID+`","noise_floor":-95.5}`),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if !got.Equal(want) {
		t.Errorf("last_seen = %s, want %s (retained replay is not liveness)", got, want)
	}
	if err != nil {
		t.Errorf("Unable to retreive Observer for: %s", observerID)
	}
}

// The exact live case: an observer purged from the DB must not come back when
// the bridge replays its months-old retained snapshot with RETAIN cleared.
func TestStaleStatusPayloadDoesNotCreateObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("0f", 32)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/OST/"+observerID+"/status",
			statusPayload("BE-BGS-RRY120-RES", "offline", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	count := countWhere(t, store, "observers", "pubkey = $1", observerID)
	if count != 0 {
		t.Errorf("observers row count = %d, want 0 (replayed snapshot must not resurrect a purged observer)", count)
	}
}

// An offline notice is the observer's own death certificate — the broker
// replaying it, or the LWT firing, is never proof of life.
func TestOfflineStatusIsNotLiveness(t *testing.T) {
	store := newTestStore(t)
	observerID := strings.Repeat("1a", 32)
	want := seedObserver(t, store, observerID, 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			statusPayload(observerID, "offline", time.Now().UTC())),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if !got.Equal(want) {
		t.Errorf("last_seen = %s, want %s (an offline status is not liveness)", got, want)
	}
	if err != nil {
		t.Errorf("Unable to get observer: %s", observerID)
	}
}

func TestStaleStatusPayloadDoesNotReactivateInactiveObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("1b", 32)
	seedObserver(t, store, observerID, 60)
	if err := store.db.MarkObserverInactive(ctx, observerID); err != nil {
		t.Fatalf("MarkObserverInactive: %v", err)
	}
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			statusPayload(observerID, "online", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	var inactive int
	mustScan(t, testRow(t, store, `SELECT inactive::int FROM observers WHERE pubkey = $1`, observerID), &inactive)
	if inactive != 1 {
		t.Errorf("inactive = %d, want 1 (a replay must not undo the soft-delete)", inactive)
	}
}

// ─── The guard must not swallow real observers ──────────────────────────────

func TestFreshStatusStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	observerID := strings.Repeat("1c", 32)
	stale := seedObserver(t, store, observerID, 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			statusPayload(observerID, "online", time.Now().UTC())),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if !got.After(stale) {
		t.Errorf("last_seen = %s, want it advanced past %s (a fresh online status is liveness)", got, stale)
	}
	if err != nil {
		t.Errorf("Unable to get observer: %s", observerID)
	}
}

// Observer clocks are naive and can sit a whole timezone off (#1478). A UTC-12
// observer must not be mistaken for a replay.
func TestNaiveClockSkewStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("1d", 32)
	stale := seedObserver(t, store, observerID, 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			statusPayload(observerID, "online", time.Now().UTC().Add(-12*time.Hour))),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if err != nil {
		t.Fatalf("GetObserverLastSeen: %v", err)
	}
	if !got.After(stale) {
		t.Errorf("last_seen = %s, want it advanced past %s (a fresh online status is liveness)", got, stale)
	}
}

func TestStatusWithoutTimestampStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("1e", 32)
	stale := seedObserver(t, store, observerID, 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status", `{"origin":"`+observerID+`","noise_floor":-95.5}`),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if err != nil {
		t.Fatalf("GetObserverLastSeen: %v", err)
	}
	if !got.After(stale) {
		t.Errorf("last_seen = %s, want it advanced past %s (a fresh online status is liveness)", got, stale)
	}
}

func TestStatusWithUnparseableTimestampStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("2a", 32)
	stale := seedObserver(t, store, observerID, 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			`{"origin":"`+observerID+`","status":"online","timestamp":"not-a-time","noise_floor":-95.5}`),
		nil, nil, &Config{})

	got, err := store.db.GetObserverLastSeen(ctx, observerID)
	if err != nil {
		t.Fatalf("GetObserverLastSeen: %v", err)
	}
	if !got.After(stale) {
		t.Errorf("last_seen = %s, want it advanced past %s (a fresh online status is liveness)", got, stale)
	}
}

// A replayed snapshot is still the observer's last known state: metadata is
// kept, only the liveness signal is suppressed.
func TestStaleStatusPayloadStillUpdatesMetadata(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	observerID := strings.Repeat("1f", 32)
	seedObserver(t, store, observerID, 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/"+observerID+"/status",
			fmt.Sprintf(`{"origin":%q,"status":"online","timestamp":%q,"firmware":"v1.14.1","noise_floor":-95.5}`,
				observerID,
				time.Now().UTC().AddDate(0, -4, 0).Format("2006-01-02T15:04:05.000000"))),
		nil, nil, &Config{})

	var firmware string
	mustScan(t, testRow(t, store, `SELECT firmware FROM observers WHERE pubkey = $1`, observerID), &firmware)

	if firmware != "v1.14.1" {
		t.Errorf("firmware = %q, want %q (metadata from a replay is still the last known state)", firmware, "v1.14.1")
	}
}

// No metrics sample either — a months-old reading filed at ingest time reads
// as a present-tense measurement.
func TestStaleStatusPayloadDoesNotInsertMetricsSample(t *testing.T) {
	store := newTestStore(t)
	seedObserver(t, store, "obs-zombie", 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-zombie/status",
			statusPayload("obs-zombie", "online", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	var count int
	count = countWhere(t, store, "observer_metrics", "observer_id = $1", "obs-zombie")
	if count != 0 {
		t.Errorf("observer_metrics rows = %d, want 0 (a replayed reading is not a new sample)", count)
	}
}
