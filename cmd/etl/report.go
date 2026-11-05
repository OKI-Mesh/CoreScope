package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const maxSamples = 5

// Report counts what the load did and keeps the first few examples of each
// problem, so a dry run shows both how much was affected and what it looked like.
type Report struct {
	counts  map[string]int64
	samples map[string][]string
}

func newReport() *Report {
	return &Report{counts: map[string]int64{}, samples: map[string][]string{}}
}

func (r *Report) Add(name string, n int64) {
	r.counts[name] += n
}

func (r *Report) Problem(name, detail string) {
	r.counts[name]++
	if len(r.samples[name]) < maxSamples {
		r.samples[name] = append(r.samples[name], detail)
	}
}

func (r *Report) Count(name string) int64 {
	return r.counts[name]
}

// Time converts a SQLite timestamp value for the named column. An unparseable
// value is counted as a problem and becomes NULL; NULL and "" are NULL without
// being a problem.
func (r *Report) Time(column string, v any) *time.Time {
	t, ok, err := parseTime(v)
	if err != nil {
		r.Problem(column+": unparseable timestamp, loaded as NULL", err.Error())
		return nil
	}
	if !ok {
		return nil
	}
	return &t
}

func (r *Report) String() string {
	names := make([]string, 0, len(r.counts))
	for name := range r.counts {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "%-64s %d\n", name, r.counts[name])
		for _, sample := range r.samples[name] {
			fmt.Fprintf(&b, "    e.g. %s\n", sample)
		}
	}
	return b.String()
}
