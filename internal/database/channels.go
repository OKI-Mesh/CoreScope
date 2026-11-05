package database

import (
	"context"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

type ChannelMessage struct {
	TransmissionID int64
	Hash           string
	DecodedJSON    string
	FirstSeen      time.Time
	ScopeName      *string
	LatestSeen     time.Time
	Repeats        int
	SNR            *float64
	ObserverPubkey *string
	ObserverName   *string
	HopCount       int
}

type ChannelSummary struct {
	ChannelHash  string
	MessageCount int
	LastActivity time.Time
	SampleJSON   *string // decoded_json of the newest message in scope
}

func (db *DB) CountChannelMessages(ctx context.Context, channelHash string, encrypted bool, regions []string) (int, error) {
	n, err := db.q.CountChannelMessages(ctx, pgdb.CountChannelMessagesParams{
		ChannelHash: channelHash,
		Encrypted:   encrypted,
		Regions:     nonNil(regions),
	})
	if err != nil {
		return 0, fmt.Errorf("count channel messages: %w", err)
	}
	return int(n), nil
}

// ListChannelMessages returns one page, oldest-first by latest observation.
func (db *DB) ListChannelMessages(ctx context.Context, channelHash string, encrypted bool, regions []string, limit, offset int) ([]ChannelMessage, error) {
	rows, err := db.q.ListChannelMessages(ctx, pgdb.ListChannelMessagesParams{
		ChannelHash: channelHash,
		Regions:     nonNil(regions),
		PageLimit:   int32(limit),
		PageOffset:  int32(offset),
		Encrypted:   encrypted,
	})
	if err != nil {
		return nil, fmt.Errorf("list channel messages: %w", err)
	}
	out := make([]ChannelMessage, len(rows))
	for i, r := range rows {
		m := ChannelMessage{
			TransmissionID: r.TransmissionID,
			Hash:           r.Hash,
			DecodedJSON:    r.DecodedJson,
			FirstSeen:      r.FirstSeen.UTC(),
			Repeats:        int(r.Repeats),
			HopCount:       int(r.HopCount),
			LatestSeen:     r.LatestSeen.UTC(),
		}
		if r.ScopeName.Valid {
			m.ScopeName = &r.ScopeName.String
		}
		if r.Snr.Valid {
			m.SNR = &r.Snr.Float64
		}
		if r.ObserverPubkey.Valid {
			m.ObserverPubkey = &r.ObserverPubkey.String
		}
		if r.ObserverName.Valid {
			m.ObserverName = &r.ObserverName.String
		}
		out[i] = m
	}
	return out, nil
}

// nonNil keeps pgx from sending NULL for an empty filter. The SQL also
// COALESCEs, so this is belt and braces.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (db *DB) ListChannelSummaries(ctx context.Context, regions []string) ([]ChannelSummary, error) {
	rows, err := db.q.ListChannelSummaries(ctx, nonNil(regions))
	if err != nil {
		return nil, fmt.Errorf("list channel summaries: %w", err)
	}
	out := make([]ChannelSummary, len(rows))
	for i, r := range rows {
		out[i] = ChannelSummary{
			ChannelHash:  r.ChannelHash,
			MessageCount: int(r.MsgCount),
			LastActivity: r.LastActivity.UTC(),
			SampleJSON:   fromPgText(r.SampleJson),
		}
	}
	return out, nil
}

type EncryptedChannelSummary struct {
	ChannelHash  string // e.g. "enc_3A"
	MessageCount int
	LastActivity time.Time
}

func (db *DB) ListEncryptedChannelSummaries(ctx context.Context, regions []string) ([]EncryptedChannelSummary, error) {
	rows, err := db.q.ListEncryptedChannelSummaries(ctx, nonNil(regions))
	if err != nil {
		return nil, fmt.Errorf("list encrypted channel summaries: %w", err)
	}
	out := make([]EncryptedChannelSummary, len(rows))
	for i, r := range rows {
		out[i] = EncryptedChannelSummary{
			ChannelHash:  r.ChannelHash,
			MessageCount: int(r.MsgCount),
			LastActivity: r.LastActivity.UTC(),
		}
	}
	return out, nil
}
