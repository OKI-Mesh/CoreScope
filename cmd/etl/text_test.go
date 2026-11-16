package main

import "testing"

func TestCleanText(t *testing.T) {
	cases := []struct {
		name    string
		input   *string
		want    *string
		changed bool
	}{
		{"nil", nil, nil, false},
		{"plain", text("Alpha"), text("Alpha"), false},
		{"valid multibyte", text("Café ☕"), text("Café ☕"), false},
		{"truncated character", text("Ab\xef\xbf"), text("Ab�"), true},
		{"NUL byte", text("a\x00b"), text("ab"), true},
		{"invalid byte inside", text("a\xffb"), text("a�b"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			report := newReport()
			got := cleanText(report, "t.c", "row", c.input)
			if (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
				t.Errorf("cleanText = %v, want %v", deref(got), deref(c.want))
			}
			if changed := report.Count("text cleaned (NUL or invalid UTF-8): t.c") == 1; changed != c.changed {
				t.Errorf("reported a change = %v, want %v", changed, c.changed)
			}
		})
	}
}
