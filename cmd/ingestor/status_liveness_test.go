package main

import (
	"fmt"
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

func TestStaleStatusPayloadDoesNotAdvanceLastSeen(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	want := seedObserver(t, store, "obs-zombie", 60)

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-zombie/status",
			statusPayload("obs-zombie", "online", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-zombie"); got != want {
		t.Errorf("last_seen = %s, want %s (a 4-month-old payload is a replay, not liveness)", got, want)
	}
}

// The exact live case: an observer purged from the DB must not come back when
// the bridge replays its months-old retained snapshot with RETAIN cleared.
func TestStaleStatusPayloadDoesNotCreateObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/OST/obs-purged/status",
			statusPayload("BE-BGS-RRY120-RES", "offline", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	var count int
	count = countWhere(t, store, "observers", "id = $1", "obs-purged")
	if count != 0 {
		t.Errorf("observers row count = %d, want 0 (replayed snapshot must not resurrect a purged observer)", count)
	}
}

// An offline notice is the observer's own death certificate — the broker
// replaying it, or the LWT firing, is never proof of life.
func TestOfflineStatusIsNotLiveness(t *testing.T) {
	store := newTestStore(t)
	want := seedObserver(t, store, "obs-offline", 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-offline/status",
			statusPayload("obs-offline", "offline", time.Now().UTC())),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-offline"); got != want {
		t.Errorf("last_seen = %s, want %s (an offline status is not liveness)", got, want)
	}
}

func TestStaleStatusPayloadDoesNotReactivateInactiveObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	seedObserver(t, store, "obs-zombie", 60)
	testExec(t, store, `UPDATE observers SET inactive = 1 WHERE id = ?`, "obs-zombie")
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-zombie/status",
			statusPayload("obs-zombie", "online", time.Now().UTC().AddDate(0, -4, 0))),
		nil, nil, &Config{})

	var inactive int
	mustScan(t, testRow(t, store, `SELECT inactive FROM observers WHERE id = $1`, "obs-zombie"), &inactive)
	if inactive != 1 {
		t.Errorf("inactive = %d, want 1 (a replay must not undo the soft-delete)", inactive)
	}
}

// ─── The guard must not swallow real observers ──────────────────────────────

func TestFreshStatusStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	stale := seedObserver(t, store, "obs-live", 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-live/status",
			statusPayload("obs-live", "online", time.Now().UTC())),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-live"); got == stale {
		t.Errorf("last_seen = %s, want it advanced (a fresh online status is liveness)", got)
	}
}

// Observer clocks are naive and can sit a whole timezone off (#1478). A UTC-12
// observer must not be mistaken for a replay.
func TestNaiveClockSkewStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	stale := seedObserver(t, store, "obs-skewed", 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-skewed/status",
			statusPayload("obs-skewed", "online", time.Now().UTC().Add(-12*time.Hour))),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-skewed"); got == stale {
		t.Errorf("last_seen = %s, want it advanced (12h naive skew is a timezone, not a replay)", got)
	}
}

func TestStatusWithoutTimestampStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	stale := seedObserver(t, store, "obs-nots", 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-nots/status", `{"origin":"obs-nots","noise_floor":-95.5}`),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-nots"); got == stale {
		t.Errorf("last_seen = %s, want it advanced (no timestamp to judge — keep the old behaviour)", got)
	}
}

func TestStatusWithUnparseableTimestampStillCountsAsLiveness(t *testing.T) {
	store := newTestStore(t)
	stale := seedObserver(t, store, "obs-badts", 60)
	ctx := t.Context()

	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-badts/status",
			`{"origin":"obs-badts","status":"online","timestamp":"not-a-time","noise_floor":-95.5}`),
		nil, nil, &Config{})

	if got := observerLastSeen(t, store, "obs-badts"); got == stale {
		t.Errorf("last_seen = %s, want it advanced (unjudgeable timestamp — keep the old behaviour)", got)
	}
}

// A replayed snapshot is still the observer's last known state: metadata is
// kept, only the liveness signal is suppressed.
func TestStaleStatusPayloadStillUpdatesMetadata(t *testing.T) {
	store := newTestStore(t)
	seedObserver(t, store, "obs-zombie", 60)
	ctx := t.Context()
	handleMessage(ctx, store, "test", MQTTSource{Name: "test"},
		liveStatusMsg("meshcore/LAX/obs-zombie/status",
			fmt.Sprintf(`{"origin":"obs-zombie","status":"online","timestamp":%q,"firmware":"v1.14.1","noise_floor":-95.5}`,
				time.Now().UTC().AddDate(0, -4, 0).Format("2006-01-02T15:04:05.000000"))),
		nil, nil, &Config{})

	var firmware string
	mustScan(t, testRow(t, store, `SELECT firmware FROM observers WHERE id = $1`, "obs-zombie"), &firmware)

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
