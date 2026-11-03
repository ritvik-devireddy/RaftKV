// Package benchstat holds the small pieces of arithmetic the benchmark tool
// needs: percentiles, summaries and flag parsing. It has no dependencies so it
// can be unit tested on its own.
package benchstat

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Summary describes a set of latency samples.
type Summary struct {
	Count               int
	Min, Mean, Max      time.Duration
	P50, P95, P99, P999 time.Duration
}

// Percentile returns the p-th percentile (0 < p <= 100) of sorted, using the
// nearest-rank method: the smallest sample such that at least p percent of the
// samples are at or below it. It returns 0 for an empty slice.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1]
}

// Summarize computes a Summary. It does not modify samples.
func Summarize(samples []time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	s := append([]time.Duration(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var total time.Duration
	for _, d := range s {
		total += d
	}
	return Summary{
		Count: len(s),
		Min:   s[0],
		Mean:  total / time.Duration(len(s)),
		Max:   s[len(s)-1],
		P50:   Percentile(s, 50),
		P95:   Percentile(s, 95),
		P99:   Percentile(s, 99),
		P999:  Percentile(s, 99.9),
	}
}

// Median returns the middle value of ds (the mean of the two middle values for
// an even count). It returns 0 for an empty slice and does not modify ds.
func Median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// MS formats d in milliseconds with two decimals, for tables.
func MS(d time.Duration) string {
	return fmt.Sprintf("%.2f", float64(d)/float64(time.Millisecond))
}

// ParseIntList parses a comma-separated list of positive integers, such as
// "1,3,5". Empty entries are an error.
func ParseIntList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%q is not a positive integer list (bad entry %q)", s, part)
		}
		out = append(out, n)
	}
	return out, nil
}
