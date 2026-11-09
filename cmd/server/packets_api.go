package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

// registerPacketRoutes registers the packet endpoints. The literal routes come
// before /api/packets/{id}, which would otherwise match "observations" and
// "timestamps".
func (s *Server) registerPacketRoutes(r *mux.Router) {
	r.HandleFunc("/api/packets/observations", s.handleBatchObservations).Methods("POST")
	r.HandleFunc("/api/packets/timestamps", s.handlePacketTimestamps).Methods("GET")
	r.HandleFunc("/api/packets/{id}", s.handlePacketDetail).Methods("GET")
	r.HandleFunc("/api/packets", s.handlePackets).Methods("GET")
}

func (s *Server) handlePackets(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	since, err := parsePacketTimeBound(r.URL.Query().Get("since"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	until, err := parsePacketTimeBound(r.URL.Query().Get("until"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	lim := queryLimit(r, 50, s.cfg.ListLimits.PacketsMax)
	offset := queryInt(r, "offset", 0)

	// Multi-node filter: comma-separated pubkeys (Node.js parity)
	if nodesParam := qs.Get("nodes"); nodesParam != "" {
		var cleaned []string
		for _, pk := range strings.Split(nodesParam, ",") {
			if pk = strings.TrimSpace(pk); pk != "" {
				cleaned = append(cleaned, pk)
			}
		}
		order := "DESC"
		if qs.Get("order") == "asc" {
			order = "ASC"
		}
		result, err := s.store.QueryMultiNodePackets(r.Context(), cleaned, lim, offset, order, since, until)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		result.Limit = lim
		result.Offset = offset
		writeJSON(w, result)
		return
	}

	q := database.PacketQuery{
		Limit:              lim,
		Offset:             offset,
		Hash:               qs.Get("hash"),
		Region:             qs.Get("region"),
		Node:               qs.Get("node"),
		Area:               qs.Get("area"),
		Observers:          splitObserverIDs(qs.Get("observer")),
		Channel:            qs.Get("channel"),
		ChannelEncrypted:   qs.Get("encrypted") == "true",
		Order:              "DESC",
		ExpandObservations: qs.Get("expand") == "observations",
	}

	if !since.IsZero() {
		q.ObsSince = &since
	}
	if !until.IsZero() {
		q.ObsUntil = &until
	}
	if qs.Get("order") == "asc" {
		q.Order = "ASC"
	}
	if v := qs.Get("type"); v != "" {
		t, _ := strconv.Atoi(v)
		q.Type = &t
	}
	if v := qs.Get("route"); v != "" {
		t, _ := strconv.Atoi(v)
		q.Route = &t
	}

	if qs.Get("groupByHash") == "true" {
		result, err := s.store.QueryGroupedPackets(r.Context(), q)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		result.Limit = q.Limit
		result.Offset = q.Offset
		writeJSON(w, result)
		return
	}

	result, err := s.store.QueryPackets(r.Context(), q)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	result.Limit = q.Limit
	result.Offset = q.Offset
	writeJSON(w, result)
}

func (s *Server) handlePacketTimestamps(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		writeError(w, 400, "since required")
		return
	}
	since, err := parsePacketTimeBound(raw)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, s.store.GetTimestamps(since))
}

var hashPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// handleBatchObservations returns observations for multiple hashes in a single request.
// POST /api/packets/observations with JSON body: {"hashes": ["abc123", "def456", ...]}
// Response: {"results": {"abc123": [...observations...], "def456": [...], ...}}
// Limited to 200 hashes per request to prevent abuse.
func (s *Server) handleBatchObservations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		Hashes []string `json:"hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	const maxHashes = 200
	if len(body.Hashes) > maxHashes {
		writeError(w, 400, fmt.Sprintf("too many hashes (max %d)", maxHashes))
		return
	}

	results := make(map[string][]api.ObservationDetail, len(body.Hashes))
	if s.store != nil {
		for _, hash := range body.Hashes {
			obs, _ := s.store.GetObservationsForHash(ctx, hash)
			if obs == nil {
				obs = []api.ObservationDetail{}
			}
			results[hash] = obs
		}
	}
	writeJSON(w, api.ObservationBatchResponse{Results: results})
}

func (s *Server) handlePacketDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	param := mux.Vars(r)["id"]
	isHash := hashPattern.MatchString(strings.ToLower(param))
	id, idErr := strconv.Atoi(param)

	var (
		packet    *api.Packet            // a transmission, by hash or id
		obsPacket *api.ObservationPacket // an observation, by observation id
		fromDB    bool
		err       error
	)
	if isHash {
		packet, err = s.store.GetPacketByHash(ctx, param)
	}
	if err == nil && packet == nil && idErr == nil {
		packet, err = s.store.GetTransmissionByID(ctx, id)
		if err == nil && packet == nil {
			obsPacket, err = s.store.GetPacketByID(ctx, id)
		}
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	// DB fallback: the in-memory store prunes old entries, the database keeps
	// them. Without this, links from node-detail pages 404 once the packet ages out.
	if packet == nil && obsPacket == nil {
		var row *database.Packet
		if isHash {
			row, err = s.db.GetPacketByHash(ctx, param)
			if errors.Is(err, database.ErrNotFound) {
				row, err = nil, nil
			}
		}
		if err == nil && row == nil && idErr == nil {
			row, err = s.db.GetPacketByID(ctx, int64(id))
			if errors.Is(err, database.ErrNotFound) {
				row, err = nil, nil
			}
		}
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		if row != nil {
			p := dbPacket(*row)
			packet = &p
			fromDB = true
		}
	}
	if packet == nil && obsPacket == nil {
		writeError(w, 404, "Not found")
		return
	}

	var hash, canonicalHex, pathJSON string
	if packet != nil {
		hash, canonicalHex, pathJSON = deref(packet.Hash), deref(packet.RawHex), deref(packet.PathJson)
	} else {
		hash, canonicalHex, pathJSON = deref(obsPacket.Hash), deref(obsPacket.RawHex), deref(obsPacket.PathJson)
	}

	observations, err := s.store.GetObservationsForHash(ctx, hash)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if len(observations) == 0 && fromDB && hash != "" {
		rows, err := s.db.GetObservationsForHash(ctx, hash)
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		observations = make([]api.ObservationDetail, len(rows))
		for i, o := range rows {
			observations[i] = dbObservationDetail(o)
		}
	}

	// #1999: give each observation its OWN wire bytes. Neither the store nor
	// the DB observation query carries them — both deliberately drop
	// observations.raw_hex (#881, ~98MB on a 1.7M-observation store) on the
	// assumption that one content hash means one frame. The firmware hashes
	// payload and type independently of the relay path, so that is false: on a
	// production DB, 1844 of the 3000 most recent transmissions with multiple
	// observations hold genuinely different frames, up to 51 of them for a
	// single packet. Without this the detail view showed the transmission's
	// canonical frame for every observation, which can contradict the
	// path_json shown beside it.
	//
	// Done here rather than when the observations are built so it is one query
	// for the whole request instead of one per observation, and after the store
	// lock is released.
	//
	// A stored per-observation frame WINS over whatever is already set. On the
	// store path observationDetail has already put the transmission's canonical
	// frame there, so skipping observations that "already have" raw_hex would
	// skip every one of them and leave the bug in place on the main path. Only
	// where no frame is stored does the canonical value stand: the store path
	// already falls back to tx.RawHex, but the DB-fallback path never has (its
	// observation query selects no raw_hex at all), so those observations came
	// back with the field missing entirely. Both paths end up consistent here.
	if hash != "" && len(observations) > 0 {
		byObsID, err := s.db.ObservationRawHexForHash(ctx, hash)
		if err != nil {
			// Non-fatal: the old code returned nil on error and the loop
			// fell back to the canonical frame. Keep that, but say so.
			log.Printf("[packets] per-observation raw_hex lookup failed for %.16s: %v", hash, err)
		}
		for i := range observations {
			if hx := byObsID[int64(observations[i].Id)]; hx != "" {
				observations[i].RawHex = &hx
				continue
			}
			if observations[i].RawHex != nil && *observations[i].RawHex != "" {
				continue
			}
			if canonicalHex != "" {
				hx := canonicalHex
				observations[i].RawHex = &hx
			}
		}
	}
	observationCount := len(observations)
	if observationCount == 0 {
		observationCount = 1
	}

	pathHops := []string{}
	if pathJSON != "" {
		var hops []string
		if json.Unmarshal([]byte(pathJSON), &hops) == nil && hops != nil {
			pathHops = hops
		}
	}

	resp := api.PacketDetailResponse{
		Path:             pathHops,
		ObservationCount: observationCount,
	}
	if len(observations) > 0 {
		resp.Observations = &observations
	}
	// The packet is one of two shapes. A later release gives the observation-id
	// lookup its own route, which removes this union.
	if packet != nil {
		err = resp.Packet.FromPacket(*packet)
	} else {
		err = resp.Packet.FromObservationPacket(*obsPacket)
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, resp)
}
