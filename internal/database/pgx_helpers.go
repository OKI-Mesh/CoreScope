package database

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func fromPgText(v pgtype.Text) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}

// toPgText is always valid, including for the empty string.
func toPgText(v string) pgtype.Text {
	return pgtype.Text{String: v, Valid: true}
}

func toPgFloat8Ptr(v *float64) pgtype.Float8 {
	if v == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: *v, Valid: true}
}

// toPgInt4Ptr converts to a 32-bit column. The caller guarantees the value
// fits; int4Fits checks it where external data enters.
func toPgInt4Ptr(v *int) pgtype.Int4 {
	if v == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*v), Valid: true}
}

func toPgInt8Ptr(v *int64) pgtype.Int8 {
	if v == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *v, Valid: true}
}

func toPgBoolPtr(v *bool) pgtype.Bool {
	if v == nil {
		return pgtype.Bool{}
	}
	return pgtype.Bool{Bool: *v, Valid: true}
}

func toPgTextPtr(v *string) pgtype.Text {
	if v == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *v, Valid: true}
}

// toPgTextIfNotEmpty maps the empty string to NULL.
func toPgTextIfNotEmpty(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

func fromPgFloat8(v pgtype.Float8) *float64 {
	if !v.Valid {
		return nil
	}
	return &v.Float64
}

func fromPgInt8(v pgtype.Int8) *int64 {
	if !v.Valid {
		return nil
	}
	return &v.Int64
}

// fromPgTimestamptz is nil for NULL and for infinite timestamps; otherwise UTC.
func fromPgTimestamptz(v pgtype.Timestamptz) *time.Time {
	if !v.Valid || v.InfinityModifier != pgtype.Finite {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

// fromPgInt4 is nil for NULL.
func fromPgInt4(v pgtype.Int4) *int {
	if !v.Valid {
		return nil
	}
	n := int(v.Int32)
	return &n
}

func fromPgInt4To64(v pgtype.Int4) *int64 {
	if !v.Valid {
		return nil
	}
	x := int64(v.Int32)
	return &x
}

func utcPtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	u := v.UTC()
	return &u
}

func toPgTimestamptzPtr(p *time.Time) pgtype.Timestamptz {
	if p == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *p, Valid: true}
}
