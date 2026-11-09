package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/mux"
)

// decodeSample is one packet captured from production with the response the
// production server gave for POST /api/decode. bucket is the audio-lab payload
// type the packet was taken from.
type decodeSample struct {
	Bucket   string          `json:"bucket"`
	Hex      string          `json:"hex"`
	Response json.RawMessage `json:"response"`
}

func readDecodeSamples(t *testing.T) []decodeSample {
	t.Helper()
	raw, err := os.ReadFile("testdata/decode_samples_prod.json")
	if err != nil {
		t.Fatal(err)
	}
	var samples []decodeSample
	decodeInto(t, raw, &samples)
	if len(samples) == 0 {
		t.Fatal("no decode samples")
	}
	return samples
}

func postRecorded(router *mux.Router, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestMiscGetEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for _, path := range []string{
		"/api/iata-coords",
		"/api/audio-lab/buckets",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

func TestAudioLabBucketsDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	var resp api.AudioLabBucketsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/audio-lab/buckets", http.StatusOK).Body.Bytes(), &resp)
	if len(resp.Buckets) == 0 {
		t.Fatal("the seeded packets should produce at least one bucket")
	}
	for name, packets := range resp.Buckets {
		if len(packets) == 0 || len(packets) > 8 {
			t.Errorf("bucket %s has %d packets, want 1 to 8", name, len(packets))
		}
		for _, p := range packets {
			if p.RawHex == "" || p.ObservationCount < 1 || p.Timestamp.IsZero() {
				t.Errorf("bucket %s: incomplete packet %+v", name, p)
			}
		}
	}
}

func TestIataCoordsDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	var resp api.IataCoordsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/iata-coords", http.StatusOK).Body.Bytes(), &resp)
	if len(resp.Coords) != len(iataCoords) {
		t.Errorf("got %d codes, the table has %d", len(resp.Coords), len(iataCoords))
	}
}

// missingFrom reports the first place where got does not carry what want has:
// an object key that is absent or has a different value, or a differing array
// or scalar. Keys that only got has are allowed, so a decoder that has learned
// to report more than the production build still passes. It returns "" when
// got covers want.
func missingFrom(got, want any, path string) string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return path + " is not an object"
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok {
				return path + "." + k + " is missing"
			}
			if d := missingFrom(gv, wv, path+"."+k); d != "" {
				return d
			}
		}
		return ""
	default:
		if !reflect.DeepEqual(got, want) {
			return path + " differs"
		}
		return ""
	}
}

// The branch decoder reports channelHashHex and decryptionStatus on group
// payloads where the production build sent neither; both are optional in the
// spec, so only what production sends is compared.
func TestDecodeMatchesSpecAndProduction(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for _, s := range readDecodeSamples(t) {
		t.Run(s.Bucket, func(t *testing.T) {
			body, err := json.Marshal(api.DecodeRequest{Hex: s.Hex})
			if err != nil {
				t.Fatal(err)
			}
			checker.checkPost(t, router, "/api/decode", string(body), http.StatusOK)

			rec := postRecorded(router, "/api/decode", string(body))
			var got, want any
			decodeInto(t, rec.Body.Bytes(), &got)
			decodeInto(t, s.Response, &want)
			if diff := missingFrom(got, want, ""); diff != "" {
				t.Errorf("response lacks or changes what production sends: %s\n got: %s\nwant: %s", diff, rec.Body.String(), s.Response)
			}
		})
	}
}

func TestDecodeResponseDecodesAsSpecTypes(t *testing.T) {
	_, router := setupTestServer(t)

	ack := readDecodeSamples(t)
	var hexACK string
	for _, s := range ack {
		if s.Bucket == "ACK" {
			hexACK = s.Hex
		}
	}
	if hexACK == "" {
		t.Fatal("no ACK sample")
	}
	var resp api.DecodeResponse
	decodeInto(t, postRecorded(router, "/api/decode", `{"hex":"`+hexACK+`"}`).Body.Bytes(), &resp)
	if resp.Decoded.Header.PayloadTypeName != "ACK" || resp.Decoded.Payload.Type != "ACK" {
		t.Errorf("decoded = %+v", resp.Decoded)
	}
	if resp.Decoded.Payload.AckLen == nil || *resp.Decoded.Payload.AckLen != 4 {
		t.Errorf("ackLen = %v, want 4", resp.Decoded.Payload.AckLen)
	}
}

func TestDecodeErrorsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for name, body := range map[string]string{
		"not json":  `{`,
		"empty hex": `{"hex":"  "}`,
		"not hex":   `{"hex":"zz"}`,
	} {
		t.Run(name, func(t *testing.T) {
			checker.checkPost(t, router, "/api/decode", body, http.StatusBadRequest)
		})
	}
}

func inspectRouter(t *testing.T) *mux.Router {
	t.Helper()
	srv := newTestServerForInspect(t)
	now := time.Now().UTC()
	insertInspectNodes(t, srv, []database.NodeInput{
		{PublicKey: inspectKey("aabb1234"), Name: "NodeA", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: inspectKey("aabb5678"), Name: "NodeA2", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: inspectKey("ccdd5678"), Name: "NodeB", Role: "repeater", RxTime: now, IngestNow: now},
	})
	if err := srv.store.RefreshNodeCache(t.Context()); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	router := mux.NewRouter()
	router.HandleFunc("/api/paths/inspect", srv.handlePathInspect).Methods("POST")
	return router
}

func TestPathInspectMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	router := inspectRouter(t)

	checker.checkPost(t, router, "/api/paths/inspect", `{"prefixes":["aa","cc"]}`, http.StatusOK)
	checker.checkPost(t, router, "/api/paths/inspect", `{"prefixes":["AA"," cc "],"limit":1,"context":{"observerId":"o","since":"a","until":"b"}}`, http.StatusOK)

	for name, body := range map[string]string{
		"not json":      `{`,
		"no prefixes":   `{"prefixes":[]}`,
		"odd length":    `{"prefixes":["a"]}`,
		"not hex":       `{"prefixes":["zz"]}`,
		"too long":      `{"prefixes":["aabbccdd"]}`,
		"mixed lengths": `{"prefixes":["aa","aabb"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			checker.checkPost(t, router, "/api/paths/inspect", body, http.StatusBadRequest)
		})
	}
}

func TestPathInspectResponseDecodesAsSpecTypes(t *testing.T) {
	router := inspectRouter(t)

	rec := postRecorded(router, "/api/paths/inspect", `{"prefixes":["aa","cc"],"limit":2}`)
	var resp api.PathInspectResponse
	decodeInto(t, rec.Body.Bytes(), &resp)
	if len(resp.Candidates) == 0 || len(resp.Candidates) > 2 {
		t.Fatalf("candidates = %d, want 1 or 2", len(resp.Candidates))
	}
	if resp.Input.Hops != 2 || !reflect.DeepEqual(resp.Input.Prefixes, []string{"aa", "cc"}) {
		t.Errorf("input = %+v", resp.Input)
	}
	if resp.Stats.BeamWidth != beamWidth || resp.Stats.ExpansionsRun != 2*beamWidth {
		t.Errorf("stats = %+v", resp.Stats)
	}
	c := resp.Candidates[0]
	if len(c.Path) != 2 || len(c.Names) != 2 || len(c.Evidence.PerHop) != 2 {
		t.Errorf("candidate = %+v", c)
	}
}

func TestPathInspectColdStartBodyMatchesSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(api.PathInspectColdStart{Retry: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"retry":true}` {
		t.Fatalf("body = %s, production sends {\"retry\":true}", body)
	}
	var decoded any
	decodeInto(t, body, &decoded)
	if err := doc.Components.Schemas["PathInspectColdStart"].Value.VisitJSON(decoded); err != nil {
		t.Error(err)
	}
}

func TestProdMiscFixturesMatchSpec(t *testing.T) {
	doc := loadSpec(t)
	read := func(t *testing.T, file string) any {
		t.Helper()
		raw, err := os.ReadFile("testdata/" + file)
		if err != nil {
			t.Fatal(err)
		}
		var v any
		decodeInto(t, raw, &v)
		return v
	}
	validate := func(t *testing.T, schema string, v any) {
		t.Helper()
		s, ok := doc.Components.Schemas[schema]
		if !ok {
			t.Fatalf("%s is not in the spec", schema)
		}
		if err := s.Value.VisitJSON(v); err != nil {
			t.Errorf("production response does not match %s: %v", schema, err)
		}
	}

	t.Run("iata_coords_prod.json", func(t *testing.T) {
		validate(t, "IataCoordsResponse", read(t, "iata_coords_prod.json"))
	})
	t.Run("paths_inspect_prod.json", func(t *testing.T) {
		validate(t, "PathInspectResponse", read(t, "paths_inspect_prod.json"))
	})
	t.Run("audio_lab_buckets_prod.json", func(t *testing.T) {
		v := read(t, "audio_lab_buckets_prod.json")
		validate(t, "AudioLabBucketsResponse", v)

		// decoded_json carries a payload; every one must fit DecodedPayload.
		for name, packets := range v.(map[string]any)["buckets"].(map[string]any) {
			for _, p := range packets.([]any) {
				var payload any
				decodeInto(t, []byte(p.(map[string]any)["decoded_json"].(string)), &payload)
				t.Run(name, func(t *testing.T) {
					validate(t, "DecodedPayload", payload)
				})
			}
		}
	})
	t.Run("decode_samples_prod.json", func(t *testing.T) {
		for _, s := range readDecodeSamples(t) {
			var v any
			decodeInto(t, s.Response, &v)
			validate(t, "DecodeResponse", v)
		}
	})
}
