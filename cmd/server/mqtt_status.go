package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/OKI-Mesh/CoreScope/api"
)

// mqttBrokerSchemes is the set of broker URL schemes whose embedded
// `user:pass@host` credentials we want to redact. We URL-parse for these
// (defense vs. passwords containing `@`); other strings fall through to
// the legacy regex pass for embedded user:pass occurrences in free-form
// error strings.
var mqttBrokerSchemes = map[string]bool{
	"mqtt": true, "mqtts": true, "tcp": true, "ssl": true, "ws": true, "wss": true,
}

// mqttBrokerURLRe locates a broker URL (with credentials) embedded inside
// a larger free-form string — e.g. an error message that quotes the
// failing broker. Each match is fed through url.Parse + redaction. We
// match greedily up through the LAST `@` followed by a host-shaped token
// so passwords containing `@` are not truncated (#1682 adversarial r1).
//
// Go's RE2 has no lookahead; we capture the host tail and emit it
// unchanged in the replacement.
var mqttBrokerURLRe = regexp.MustCompile(`(?i)(?:mqtt|mqtts|tcp|ssl|ws|wss)://[^\s]*`)

// maskBrokerURL returns the broker URL with any inline password redacted.
// `mqtt://user:secret@host:1883` -> `mqtt://user:****@host:1883`.
// `mqtt://user:p@ss@host` -> `mqtt://user:****@host` (password with `@`).
// URLs without inline credentials are returned unchanged.
//
// Primary strategy: url.Parse — handles passwords with `@`, `:`, etc.
// Fallback: regex sweep for free-form strings (e.g. error messages that
// quote a URL fragment but aren't standalone-parseable).
func maskBrokerURL(s string) string {
	if s == "" {
		return s
	}
	// Fast path: the whole string is the broker URL.
	if masked, ok := redactBrokerURL(s); ok {
		return masked
	}
	// Fallback: free-form string (e.g. error message) containing a URL.
	// Find embedded broker URLs and redact each in-place.
	return mqttBrokerURLRe.ReplaceAllStringFunc(s, func(m string) string {
		if out, ok := redactBrokerURL(m); ok {
			return out
		}
		return m
	})
}

// redactBrokerURL parses s as a URL and, if it has an mqtt-family scheme
// with userinfo containing a password, returns the URL with the password
// replaced by `****`. Returns ok=false when s is not such a URL.
func redactBrokerURL(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.User == nil {
		return s, false
	}
	if !mqttBrokerSchemes[strings.ToLower(u.Scheme)] {
		return s, false
	}
	if _, hasPass := u.User.Password(); !hasPass {
		return s, false
	}
	// Re-assemble manually rather than via url.UserPassword + u.String()
	// because the latter percent-encodes the `*` mask token into `%2A`,
	// defeating the user-visible redaction marker. We only need to swap
	// the userinfo segment of the original string.
	hostAndAfter := s
	if idx := strings.LastIndex(s, "@"); idx >= 0 {
		hostAndAfter = s[idx+1:]
	}
	// Preserve original scheme casing (url.Parse lowercases u.Scheme).
	schemeEnd := strings.Index(s, "://")
	if schemeEnd < 0 {
		return s, false
	}
	return s[:schemeEnd] + "://" + u.User.Username() + ":****@" + hostAndAfter, true
}

// ingestorMqttSource is the per-MQTT-source status row as the ingestor writes
// it to its stats file (cmd/ingestor SourceStatusSnapshot). handleMqttStatus
// copies it into api.MqttSourceStatus with the broker URL credentials
// redacted — operators must not see the broker password in the API response
// (#1043 acceptance criterion).
type ingestorMqttSource struct {
	Name               string `json:"name"`
	Broker             string `json:"broker"`
	Connected          bool   `json:"connected"`
	LastConnectUnix    int64  `json:"lastConnectUnix"`
	LastDisconnectUnix int64  `json:"lastDisconnectUnix"`
	LastPacketUnix     int64  `json:"lastPacketUnix"`
	ConnectCount       int64  `json:"connectCount"`
	DisconnectCount    int64  `json:"disconnectCount"`
	PacketsTotal       int64  `json:"packetsTotal"`
	PacketsLast5m      int64  `json:"packetsLast5m"`
	LastError          string `json:"lastError,omitempty"`
}

// ingestorMqttStatusEnvelope is the partial shape the server decodes from
// the ingestor stats file (additive — older ingestors omit the field).
type ingestorMqttStatusEnvelope struct {
	SampledAt            string               `json:"sampledAt"`
	SourceStatuses       []ingestorMqttSource `json:"source_statuses"`
	WatchdogLastTickUnix int64                `json:"watchdogLastTickUnix"`
	WatchdogPanicCount   int64                `json:"watchdogPanicCount"`
	WatchdogLogDropCount int64                `json:"watchdogLogDropCount"`
}

// handleMqttStatus serves GET /api/mqtt/status. Reads the ingestor stats
// file, masks broker-URL passwords, and returns the per-source status
// list. Returns an empty list (200 OK) when the stats file is missing
// or unparseable — the UI panel renders a "no data yet" state.
func (s *Server) handleMqttStatus(w http.ResponseWriter, r *http.Request) {
	resp := api.MqttStatusResponse{Sources: []api.MqttSourceStatus{}}
	data, err := os.ReadFile(IngestorStatsPath())
	if err != nil {
		writeJSON(w, resp)
		return
	}
	var env ingestorMqttStatusEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		writeJSON(w, resp)
		return
	}
	resp.SampleAt = statsTime(env.SampledAt)
	resp.WatchdogLastTickUnix = nonZero(env.WatchdogLastTickUnix)
	resp.WatchdogPanicCount = nonZero(env.WatchdogPanicCount)
	resp.WatchdogLogDropCount = nonZero(env.WatchdogLogDropCount)
	for _, src := range env.SourceStatuses {
		status := api.MqttSourceStatus{
			Name:               src.Name,
			Broker:             maskBrokerURL(src.Broker),
			Connected:          src.Connected,
			LastConnectUnix:    src.LastConnectUnix,
			LastDisconnectUnix: src.LastDisconnectUnix,
			LastPacketUnix:     src.LastPacketUnix,
			ConnectCount:       src.ConnectCount,
			DisconnectCount:    src.DisconnectCount,
			PacketsTotal:       src.PacketsTotal,
			PacketsLast5m:      src.PacketsLast5m,
		}
		// Broker libraries occasionally quote the failing URL in the
		// error string — redact there too as defense-in-depth.
		if src.LastError != "" {
			masked := maskBrokerURL(src.LastError)
			status.LastError = &masked
		}
		resp.Sources = append(resp.Sources, status)
	}
	writeJSON(w, resp)
}

// nonZero returns a pointer to v, or nil when v is zero, for response fields
// that are left out when they are zero.
func nonZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}
