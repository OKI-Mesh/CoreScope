package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"golang.org/x/sync/singleflight"
)

const wireTimeLayout = "2006-01-02T15:04:05.000Z"

func formatTS(t time.Time) string {
	return t.UTC().Format(wireTimeLayout)
}

type UTCTime struct {
	time.Time
}

func (t UTCTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(formatTS(t.Time))
}

func (t *UTCTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(wireTimeLayout, s)
	if err != nil {
		return err
	}
	t.Time = parsed
	return nil
}

type ChannelMessagesResponse struct {
	Messages []ChannelMessageResponse `json:"messages"`
	Total    int                      `json:"total"`
}

type ChannelResponse struct {
	Hash         string  `json:"hash"`
	Name         string  `json:"name"`
	LastMessage  *string `json:"lastMessage"`
	LastSender   *string `json:"lastSender"`
	MessageCount int     `json:"messageCount"`
	LastActivity string  `json:"lastActivity"`
	Encrypted    bool    `json:"encrypted,omitempty"`
}

type flightCache[T any] struct { // 1. the type declares T
	cache     ttlCache[[]T]
	sf        singleflight.Group
	queryHook func()
}

type ChannelListResponse struct {
	Channels []ChannelResponse `json:"channels"`
}

// channelCaches holds the query-level caches for the channel endpoints.
// Declare it on Server as a value (not a pointer) so the zero Server works.
type channelCaches struct {
	msgs     ttlCache[msgPage]
	channels flightCache[ChannelResponse]
	enc      flightCache[ChannelResponse]
}

func (c *flightCache[T]) do(ctx context.Context, region string,
	load func(ctx context.Context) ([]T, error)) ([]T, error) {

	if cached, ok := c.cache.get(region); ok {
		return cached, nil
	}
	v, err, _ := c.sf.Do(region, func() (interface{}, error) {
		// Double-check inside the flight: a previous winner may have stored a
		// fresh result between this caller's read above and its arrival here.
		if cached, ok := c.cache.get(region); ok {
			return cached, nil
		}
		if c.queryHook != nil {
			c.queryHook()
		}
		// The flight is shared by every waiter, so it must not die with the
		// first caller's request context.
		qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		out, err := load(qctx)
		if err != nil {
			return nil, err
		}
		c.cache.set(region, out)
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]T), nil
}

func (s *Server) getChannels(ctx context.Context, region string) ([]ChannelResponse, error) {
	return s.channelCaches.channels.do(ctx, region, func(qctx context.Context) ([]ChannelResponse, error) {
		summaries, err := s.db.ListChannelSummaries(qctx, database.NormalizeRegionCodes(region))
		if err != nil {
			return nil, err
		}
		out := make([]ChannelResponse, len(summaries))
		for i, c := range summaries {
			out[i] = channelToResponse(c)
		}
		return out, nil
	})
}

func (s *Server) getEncryptedChannels(ctx context.Context, region string) ([]ChannelResponse, error) {
	return s.channelCaches.enc.do(ctx, region, func(qctx context.Context) ([]ChannelResponse, error) {
		summaries, err := s.db.ListEncryptedChannelSummaries(qctx, database.NormalizeRegionCodes(region))
		if err != nil {
			return nil, err
		}
		out := make([]ChannelResponse, len(summaries))
		for i, c := range summaries {
			out[i] = encryptedChannelToResponse(c)
		}
		return out, nil
	})
}

// ---------------------------------------------------------------------------
// Caches
// ---------------------------------------------------------------------------

// defaultChannelCacheTTL is the TTL of the query-level caches below. Set it to
// whatever the old DB-level channel caches used.
const defaultChannelCacheTTL = 15 * time.Second

const ttlCacheMaxEntries = 512

type ttlEntry[T any] struct {
	v  T
	at time.Time
}

// ttlCache is a small TTL map. The zero value is ready to use.
type ttlCache[T any] struct {
	mu  sync.RWMutex
	m   map[string]ttlEntry[T]
	ttl time.Duration // zero = defaultChannelCacheTTL
}

func (c *ttlCache[T]) effectiveTTL() time.Duration {
	if c.ttl > 0 {
		return c.ttl
	}
	return defaultChannelCacheTTL
}

func (c *ttlCache[T]) get(key string) (T, bool) {
	c.mu.RLock()
	e, ok := c.m[key]
	c.mu.RUnlock()
	if !ok || time.Since(e.at) >= c.effectiveTTL() {
		var zero T
		return zero, false
	}
	return e.v, true
}

func (c *ttlCache[T]) set(key string, v T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = make(map[string]ttlEntry[T])
	}
	if len(c.m) >= ttlCacheMaxEntries {
		ttl := c.effectiveTTL()
		for k, e := range c.m {
			if time.Since(e.at) >= ttl {
				delete(c.m, k)
			}
		}
		if len(c.m) >= ttlCacheMaxEntries {
			clear(c.m) // still full of live entries: start over
		}
	}
	c.m[key] = ttlEntry[T]{v: v, at: time.Now()}
}

// ---------------------------------------------------------------------------
// Channel messages
// ---------------------------------------------------------------------------

type ChannelMessageResponse struct {
	Sender          string          `json:"sender"`
	Text            string          `json:"text"`
	Timestamp       string          `json:"timestamp"`
	FirstSeen       string          `json:"first_seen"`
	SenderTimestamp json.RawMessage `json:"sender_timestamp"` // passed through as-is; absent -> null
	PacketID        int64           `json:"packetId"`
	PacketHash      string          `json:"packetHash"`
	Repeats         int             `json:"repeats"`
	Observers       []string        `json:"observers"`
	Hops            int             `json:"hops"`
	SNR             *float64        `json:"snr"`
	ScopeName       *string         `json:"scope_name"`
}

type msgPage struct {
	msgs  []ChannelMessageResponse
	total int
}

func (c *channelCaches) getMsg(key string) ([]ChannelMessageResponse, int, bool) {
	p, ok := c.msgs.get(key)
	return p.msgs, p.total, ok
}

func (c *channelCaches) setMsg(key string, msgs []ChannelMessageResponse, total int) {
	c.msgs.set(key, msgPage{msgs: msgs, total: total})
}

func (s *Server) getChannelMessages(ctx context.Context, channelHash string, encrypted bool, limit, offset int, region ...string) ([]ChannelMessageResponse, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	regionParam := ""
	if len(region) > 0 {
		regionParam = region[0]
	}

	cacheKey := fmt.Sprintf("%s|%t|%d|%d|%s", channelHash, encrypted, limit, offset, regionParam)
	if msgs, total, ok := s.channelCaches.getMsg(cacheKey); ok {
		return msgs, total, nil
	}

	regions := database.NormalizeRegionCodes(regionParam)

	total, err := s.db.CountChannelMessages(ctx, channelHash, encrypted, regions)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.db.ListChannelMessages(ctx, channelHash, encrypted, regions, limit, offset)
	if err != nil {
		return nil, 0, err
	}

	messages := make([]ChannelMessageResponse, 0, len(rows))
	for _, r := range rows {
		var decoded struct {
			Text            string          `json:"text"`
			Sender          string          `json:"sender"`
			SenderTimestamp json.RawMessage `json:"sender_timestamp"`
		}
		if json.Unmarshal([]byte(r.DecodedJSON), &decoded) != nil {
			continue // invalid decoded_json: skip silently, as before
		}
		displaySender, displayText := decoded.Sender, decoded.Text
		if decoded.Text != "" {
			if idx := strings.Index(decoded.Text, ": "); idx > 0 && idx < 50 {
				displaySender = decoded.Text[:idx]
				displayText = decoded.Text[idx+2:]
			}
		}

		observers := []string{}
		switch {
		case r.ObserverName != nil:
			observers = []string{*r.ObserverName}
		case r.ObserverPubkey != nil:
			observers = []string{*r.ObserverPubkey}
		}

		messages = append(messages, ChannelMessageResponse{
			Sender:          displaySender,
			Text:            displayText,
			Timestamp:       r.LatestSeen.UTC().Format(time.RFC3339), // #1366: max observation time
			FirstSeen:       formatTS(r.FirstSeen),
			SenderTimestamp: decoded.SenderTimestamp,
			PacketID:        r.TransmissionID,
			PacketHash:      r.Hash,
			Repeats:         r.Repeats,
			Observers:       observers,
			Hops:            r.HopCount,
			SNR:             r.SNR,
			ScopeName:       r.ScopeName,
		})
	}

	s.channelCaches.setMsg(cacheKey, messages, total)
	return messages, total, nil
}

// ---------------------------------------------------------------------------
// Channel lists
// ---------------------------------------------------------------------------

// The encrypted merge bypassed the query-level TTL, and concurrent cold misses
// stampeded the database. Mirrors observers_cache.go (#1481 / #1483).

const channelsCacheTTL = 90 * time.Second

func (s *Server) effectiveChannelsCacheTTL() time.Duration {
	if s.cfg != nil && s.cfg.ChannelsCache != nil && s.cfg.ChannelsCache.TTLSeconds > 0 {
		return time.Duration(s.cfg.ChannelsCache.TTLSeconds) * time.Second
	}
	return channelsCacheTTL
}

func channelsCacheKey(region string, includeEncrypted bool) string {
	enc := "0"
	if includeEncrypted {
		enc = "1"
	}
	return fmt.Sprintf("channels|%s|%s", region, enc)
}

type channelsCacheEntry struct {
	resp ChannelListResponse
	at   time.Time
}

type channelsCacheField struct {
	entries   sync.Map // string → *channelsCacheEntry
	sf        singleflight.Group
	fillCount atomic.Int64 // test-only: counts singleflight wins
}

func (s *Server) isChannelsListCacheStale(cachedAt time.Time) bool {
	if cachedAt.IsZero() {
		return true
	}
	return time.Since(cachedAt) >= s.effectiveChannelsCacheTTL()
}

func (s *Server) fetchChannelsList(ctx context.Context, region string, includeEncrypted bool) (ChannelListResponse, error) {
	channels, err := s.getChannels(ctx, region)
	if err != nil {
		return ChannelListResponse{}, err
	}
	if includeEncrypted {
		encrypted, err := s.getEncryptedChannels(ctx, region)
		if err != nil {
			log.Printf("WARN getEncryptedChannels: %v", err)
			return ChannelListResponse{}, err // don't cache a partial encrypted merge
		}
		channels = append(channels, encrypted...)
	}
	return ChannelListResponse{Channels: channels}, nil
}

func channelToResponse(c database.ChannelSummary) ChannelResponse {
	msg, sender := channelLastMessage(deref(c.SampleJSON))
	return ChannelResponse{
		Hash: c.ChannelHash, Name: c.ChannelHash,
		LastMessage: msg, LastSender: sender,
		MessageCount: c.MessageCount, LastActivity: formatTS(c.LastActivity),
	}
}

func encryptedChannelToResponse(c database.EncryptedChannelSummary) ChannelResponse {
	return ChannelResponse{
		Hash: c.ChannelHash, Name: "Encrypted (0x" + strings.TrimPrefix(c.ChannelHash, "enc_") + ")",
		MessageCount: c.MessageCount, LastActivity: formatTS(c.LastActivity),
		Encrypted: true,
	}
}

// channelLastMessage extracts "text" (minus a leading "sender: ") and "sender" from a
// GRP_TXT payload. Both are nil when the payload is empty, invalid or has no text.
func channelLastMessage(sampleJSON string) (message, sender *string) {
	if sampleJSON == "" {
		return nil, nil
	}
	var d struct {
		Text   string  `json:"text"`
		Sender *string `json:"sender"`
	}
	if json.Unmarshal([]byte(sampleJSON), &d) != nil || d.Text == "" {
		return nil, nil
	}
	text := d.Text
	if i := strings.Index(text, ": "); i > 0 {
		text = text[i+2:]
	}
	return &text, d.Sender
}
