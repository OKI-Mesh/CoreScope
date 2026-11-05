package main

import (
	"fmt"
	"strings"
)

// cleanText makes a SQLite string acceptable to Postgres, which rejects NUL
// bytes and invalid UTF-8 where SQLite stores any bytes. NULs are removed and
// each invalid sequence (typically a character cut off mid-way) becomes U+FFFD.
// A change is counted per column and the first few rows are listed, so nothing
// is altered silently.
func cleanText(report *Report, column, row string, s *string) *string {
	if s == nil {
		return nil
	}
	cleaned := strings.ToValidUTF8(strings.ReplaceAll(*s, "\x00", ""), "�")
	if cleaned == *s {
		return s
	}
	report.Problem("text cleaned (NUL or invalid UTF-8): "+column, fmt.Sprintf("%s %q", row, *s))
	return &cleaned
}
