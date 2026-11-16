package main

import (
	"fmt"
	"strconv"
	"time"
)

var textTimeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// parseTime is the one place SQLite timestamps become time.Time. SQLite holds
// them as RFC3339 or datetime() text, as epoch seconds, and, for columns
// declared DATETIME, as a value the driver has already parsed. ok is false for
// NULL and the empty string.
func parseTime(v any) (t time.Time, ok bool, err error) {
	switch x := v.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return x.UTC(), true, nil
	case int64:
		return time.Unix(x, 0).UTC(), true, nil
	case float64:
		whole := int64(x)
		return time.Unix(whole, int64((x-float64(whole))*1e9)).UTC(), true, nil
	case []byte:
		return parseTime(string(x))
	case string:
		if x == "" {
			return time.Time{}, false, nil
		}
		for _, layout := range textTimeLayouts {
			if parsed, perr := time.ParseInLocation(layout, x, time.UTC); perr == nil {
				return parsed.UTC(), true, nil
			}
		}
		if n, perr := strconv.ParseInt(x, 10, 64); perr == nil {
			return time.Unix(n, 0).UTC(), true, nil
		}
		return time.Time{}, false, fmt.Errorf("unrecognised timestamp %q", x)
	default:
		return time.Time{}, false, fmt.Errorf("unsupported timestamp type %T", v)
	}
}

func earlier(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if b.Before(*a) {
		return b
	}
	return a
}
