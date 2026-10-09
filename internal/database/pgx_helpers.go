package database

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

func fromPgText(v pgtype.Text) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}

func toPgText(v interface{}) pgtype.Text {
	if v == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: v.(string), Valid: true}
}

func toPgFloat8Ptr(v *float64) pgtype.Float8 {
	if v == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: *v, Valid: true}
}

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
func toPgInt4(v interface{}) pgtype.Int4 {
	if v == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(v.(int)), Valid: true} // adjust int type to match meta.BatteryMv's actual type
}

func toPgInt8(v interface{}) pgtype.Int8 {
	if v == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: int64(v.(int)), Valid: true}
}

func toPgFloat8(v interface{}) pgtype.Float8 {
	if v == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: v.(float64), Valid: true}
}

func toPgTextPtr(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func toPgBool(v interface{}) pgtype.Bool {
	if v == nil {
		return pgtype.Bool{}
	}
	return pgtype.Bool{Bool: v.(int) == 1, Valid: true} // canRelay comes through as 1/0 per observerMetaColumns
}

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

func fromPgTimestamptz(v pgtype.Timestamptz) *time.Time {
	if !v.Valid || v.InfinityModifier != pgtype.Finite {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

func fromAnyTime(v interface{}) *time.Time {
	t, ok := v.(time.Time)
	if !ok {
		return nil // NULL, e.g. the first row, arrives as nil
	}
	u := t.UTC()
	return &u
}

func fromAnyInt64(v interface{}) *int64 {
	switch n := v.(type) {
	case int64:
		return &n
	case int32:
		x := int64(n)
		return &x
	default:
		return nil
	}
}

func metaToParams(meta *types.ObserverMeta) (model, firmware, clientVersion, radio pgtype.Text, batteryMv, uptimeSecs pgtype.Int4, noiseFloor pgtype.Float8, canRelay pgtype.Bool) {
	if meta == nil {
		return
	}
	if meta.Model != nil {
		model = pgtype.Text{String: *meta.Model, Valid: true}
	}
	if meta.Firmware != nil {
		firmware = pgtype.Text{String: *meta.Firmware, Valid: true}
	}
	if meta.ClientVersion != nil {
		clientVersion = pgtype.Text{String: *meta.ClientVersion, Valid: true}
	}
	if meta.Radio != nil {
		radio = pgtype.Text{String: *meta.Radio, Valid: true}
	}
	if meta.BatteryMv != nil {
		batteryMv = pgtype.Int4{Int32: int32(*meta.BatteryMv), Valid: true}
	}
	if meta.UptimeSecs != nil {
		uptimeSecs = pgtype.Int4{Int32: int32(*meta.UptimeSecs), Valid: true}
	}
	if meta.NoiseFloor != nil {
		noiseFloor = pgtype.Float8{Float64: *meta.NoiseFloor, Valid: true}
	}
	if meta.CanRelay != nil {
		canRelay = pgtype.Bool{Bool: *meta.CanRelay, Valid: true}
	}
	return
}
