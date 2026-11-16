package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

const issue673NodePK = "7502f19f44cad6d7b626e1d811c00a914af452636182ccded3fd019803395ec9"

// setupIssue673Store builds an in-memory store with one repeater node having:
//   - one ADVERT packet (legitimately indexed in byNode)
//   - one GRP_TXT packet whose decoded text contains the node's pubkey (false-positive candidate)
func setupIssue673Store(t *testing.T) (*PacketStore, *database.DB) {
	t.Helper()
	db := setupTestDB(t)
	now := time.Now().UTC()
	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: issue673NodePK, Name: "Quail Hollow Park", Role: "repeater",
		RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatal(err)
	}

	ps := newPacketStoreForTest(t, db)

	pt4 := types.PayloadType(4) // ADVERT
	pt5 := types.PayloadType(5) // GRP_TXT

	advertDecoded, _ := json.Marshal(map[string]interface{}{"pubKey": issue673NodePK})
	advert := &StoreTx{
		ID:          1,
		Hash:        "advert_hash_673",
		PayloadType: &pt4,
		DecodedJSON: string(advertDecoded),
		FirstSeen:   now,
	}

	otherPK := "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"
	chatDecoded, _ := json.Marshal(map[string]interface{}{
		"srcPubKey": otherPK,
		"text":      "Check out node " + issue673NodePK + " on the analyzer",
	})
	chat := &StoreTx{
		ID:          2,
		Hash:        "chat_hash_673",
		PayloadType: &pt5,
		DecodedJSON: string(chatDecoded),
		FirstSeen:   now,
	}

	ps.mu.Lock()
	ps.packets = append(ps.packets, advert, chat)
	ps.byHash[advert.Hash] = advert
	ps.byHash[chat.Hash] = chat
	ps.byTxID[advert.ID] = advert
	ps.byTxID[chat.ID] = chat
	ps.byNode[issue673NodePK] = []*StoreTx{advert}
	ps.mu.Unlock()

	return ps, db
}

// TestGetNodeAnalytics_ExcludesGRPTXTWithPubkeyInText verifies that a GRP_TXT packet
// whose message text contains a node's pubkey is not counted in that node's analytics.
func TestGetNodeAnalytics_ExcludesGRPTXTWithPubkeyInText(t *testing.T) {
	ctx := t.Context()
	ps, db := setupIssue673Store(t)
	defer db.Close()

	analytics, err := ps.GetNodeAnalytics(ctx, issue673NodePK, 30)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if analytics == nil {
		t.Fatal("expected analytics, got nil")
	}

	for _, ptc := range analytics.PacketTypeBreakdown {
		if ptc.PayloadType == 5 {
			t.Errorf("GRP_TXT (type 5) should not appear in analytics for repeater node, got count=%d", ptc.Count)
		}
	}
}

// TestFilterPackets_NodeQueryDoesNotMatchChatText verifies that the slow path of
// filterPackets (node filter combined with Since) does not return a GRP_TXT packet
// whose pubkey appears only in message text, not in a structured pubkey field.
func TestFilterPackets_NodeQueryDoesNotMatchChatText(t *testing.T) {
	ctx := t.Context()
	ps, db := setupIssue673Store(t)
	defer db.Close()

	yesterday := time.Now().Add(-24 * time.Hour).UTC()
	result, err := ps.QueryPackets(ctx, database.PacketQuery{Node: issue673NodePK, FirstSeenAfter: &yesterday, Limit: 50})
	if err != nil {
		t.Fatalf("QueryPackets(): %v", err)
	}

	if result.Total != 1 {
		t.Errorf("expected 1 packet for node (ADVERT only), got %d", result.Total)
	}
	for _, pkt := range result.Packets {
		if pkt.Hash != nil && *pkt.Hash == "chat_hash_673" {
			t.Errorf("GRP_TXT with pubkey in message text was incorrectly returned for node query")
		}
	}
}
