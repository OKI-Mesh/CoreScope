package main

import "testing"

// testCompanionPK is a valid lowercase-hex companion pubkey for coverage tests.
// The topic segment must be hex (clientPubkeyRe) or handleClientPacket drops it.
const testCompanionPK = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"

// clientCoverageMsg builds a valid mobile client-RX coverage message on the
// dedicated topic meshcore/client/<pubkey>/packets. The raw hex is a relayed
// advert with GPS, so handleClientPacket would write exactly one
// client_receptions row when the feature is enabled (see
// TestHandleClientPacketAdvertWritesReception).
func clientCoverageMsg() *mockMessage {
	advertHex := "11451000D818206D3AAC152C8A91F89957E6D30CA51F36E28790228971C473B755F244F718754CF5EE4A2FD58D944466E42CDED140C66D0CC590183E32BAF40F112BE8F3F2BDF6012B4B2793C52F1D36F69EE054D9A05593286F78453E56C0EC4A3EB95DDA2A7543FCCC00B939CACC009278603902FC12BCF84B706120526F6F6620536F6C6172"
	payload := []byte(`{"raw":"` + advertHex + `","direction":"rx","timestamp":"2026-06-09T12:00:00Z","origin":"MyMob","SNR":-7.0,"RSSI":-92.0,"gps":{"lat":51.05,"lon":3.72,"acc_m":8.0}}`)
	return &mockMessage{topic: "meshcore/client/" + testCompanionPK + "/packets", payload: payload}
}

func clientReceptionCount(t *testing.T, s *Store) int {
	t.Helper()
	return countRows(t, s, "client_receptions")
}

// TestClientRxCoverageEnabledDefault verifies the gate helper defaults OFF for
// nil/absent config and is only true when explicitly enabled.
func TestClientRxCoverageEnabledDefault(t *testing.T) {
	if (&Config{}).ClientRxCoverageEnabled() {
		t.Fatal("nil ClientRxCoverage must report disabled")
	}
	if (&Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: false}}).ClientRxCoverageEnabled() {
		t.Fatal("Enabled:false must report disabled")
	}
	if !(&Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}).ClientRxCoverageEnabled() {
		t.Fatal("Enabled:true must report enabled")
	}
}

// TestClientRxCoverageGateOff drives handleMessage with the feature OFF: the
// client-topic message must fall through and write no client_receptions rows.
func TestClientRxCoverageGateOff(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	source := MQTTSource{Name: "test"}
	cfg := &Config{} // ClientRxCoverage nil ⇒ disabled

	handleMessage(ctx, store, "test", source, clientCoverageMsg(), nil, nil, cfg)

	if n := clientReceptionCount(t, store); n != 0 {
		t.Fatalf("feature OFF: expected 0 client_receptions rows, got %d", n)
	}
}

// TestClientRxCoverageGateOn drives handleMessage with the feature ON: the
// client-topic message must be dispatched and write exactly one row.
func TestClientRxCoverageGateOn(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	source := MQTTSource{Name: "test"}
	cfg := &Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}

	handleMessage(ctx, store, "test", source, clientCoverageMsg(), nil, nil, cfg)

	if n := clientReceptionCount(t, store); n != 1 {
		t.Fatalf("feature ON: expected 1 client_receptions row, got %d", n)
	}
}

// TestClientRxCoverageGateOffDoesNotFallThroughToObserverPath is the
// CRITICAL-adjacent regression test: with the feature OFF, a
// meshcore/client/<pubkey>/packets message must be dropped outright, never
// fall through to the observer packet path below. Before the fix, the
// enable-gate lived INSIDE the topic match
// (cfg.ClientRxCoverageEnabled() && parts[1]=="client" && ...), so a disabled
// gate made the whole condition false and the message fell through: the
// observer path takes parts[1] ("client") as a region and parts[2] (the
// companion pubkey) as an observer id, creating a bogus "client" region and
// registering the phone as an observer — worse now that fullRfLog multiplies
// client-topic volume. Asserts zero client_receptions rows (already covered
// by TestClientRxCoverageGateOff) AND zero observer/region pollution.
func TestClientRxCoverageGateOffDoesNotFallThroughToObserverPath(t *testing.T) {
	store := newTestStore(t)
	source := MQTTSource{Name: "test"}
	cfg := &Config{} // ClientRxCoverage nil ⇒ disabled
	ctx := t.Context()

	handleMessage(ctx, store, "test", source, clientCoverageMsg(), nil, nil, cfg)

	if n := clientReceptionCount(t, store); n != 0 {
		t.Fatalf("feature OFF: expected 0 client_receptions rows, got %d", n)
	}
	observerRows := countRows(t, store, "observers")
	if observerRows != 0 {
		t.Fatalf("feature OFF: the companion pubkey must not be registered as an observer, got %d rows", observerRows)
	}
	clientRegionRows := countWhere(t, store, "observers", "iata = $1", "client")
	if clientRegionRows != 0 {
		t.Fatalf("feature OFF: no observer should be registered under a bogus 'client' region, got %d rows", clientRegionRows)
	}
	txRows := countRows(t, store, "transmissions")
	if txRows != 0 {
		t.Fatalf("feature OFF: the client-topic packet must not be ingested as an ordinary observer packet, got %d transmissions rows", txRows)
	}
}

// TestClientRxCoverageBlacklistedDropped verifies the #1 fix: a blacklisted
// operator cannot skirt the observer blacklist via the client topic. With the
// feature ON but the companion pubkey blacklisted, no row is written. Without
// the gate the client dispatch runs before the blacklist check and inserts.
func TestClientRxCoverageBlacklistedDropped(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	source := MQTTSource{Name: "test"}
	cfg := &Config{
		ClientRxCoverage:  &ClientRxCoverageConfig{Enabled: true},
		ObserverBlacklist: []string{testCompanionPK},
	}

	handleMessage(ctx, store, "test", source, clientCoverageMsg(), nil, nil, cfg)

	if n := clientReceptionCount(t, store); n != 0 {
		t.Fatalf("blacklisted companion: expected 0 client_receptions rows, got %d", n)
	}
}
