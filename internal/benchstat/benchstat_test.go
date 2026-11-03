package benchstat

import (
	"slices"
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestPercentileNearestRank(t *testing.T) {
	var s []time.Duration
	for i := 1; i <= 100; i++ {
		s = append(s, ms(i))
	}
	for _, tc := range []struct {
		p    float64
		want time.Duration
	}{{50, ms(50)}, {95, ms(95)}, {99, ms(99)}, {99.9, ms(100)}, {100, ms(100)}, {1, ms(1)}, {0.1, ms(1)}} {
		if got := Percentile(s, tc.p); got != tc.want {
			t.Errorf("p%v = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestPercentileSmallInputs(t *testing.T) {
	if got := Percentile(nil, 99); got != 0 {
		t.Errorf("empty = %v, want 0", got)
	}
	one := []time.Duration{ms(7)}
	for _, p := range []float64{1, 50, 99, 100} {
		if got := Percentile(one, p); got != ms(7) {
			t.Errorf("single sample p%v = %v", p, got)
		}
	}
	// Four samples: p50 is the 2nd, p75 the 3rd, p76 the 4th.
	four := []time.Duration{ms(10), ms(20), ms(30), ms(40)}
	if Percentile(four, 50) != ms(20) || Percentile(four, 75) != ms(30) || Percentile(four, 76) != ms(40) {
		t.Errorf("four-sample percentiles wrong: %v %v %v", Percentile(four, 50), Percentile(four, 75), Percentile(four, 76))
	}
}

func TestSummarizeSortsACopy(t *testing.T) {
	in := []time.Duration{ms(30), ms(10), ms(20), ms(50), ms(40)}
	orig := slices.Clone(in)
	s := Summarize(in)
	if !slices.Equal(in, orig) {
		t.Fatalf("Summarize modified its input: %v", in)
	}
	if s.Count != 5 || s.Min != ms(10) || s.Max != ms(50) || s.Mean != ms(30) || s.P50 != ms(30) {
		t.Errorf("unexpected summary %+v", s)
	}
	if got := Summarize(nil); got != (Summary{}) {
		t.Errorf("empty summary = %+v", got)
	}
}

func TestMedian(t *testing.T) {
	if Median(nil) != 0 {
		t.Error("empty median not 0")
	}
	if got := Median([]time.Duration{ms(30), ms(10), ms(20)}); got != ms(20) {
		t.Errorf("odd median = %v", got)
	}
	if got := Median([]time.Duration{ms(40), ms(10), ms(20), ms(30)}); got != ms(25) {
		t.Errorf("even median = %v", got)
	}
}

func TestMS(t *testing.T) {
	if got := MS(1500 * time.Microsecond); got != "1.50" {
		t.Errorf("MS = %q", got)
	}
}

func TestParseIntList(t *testing.T) {
	got, err := ParseIntList("1, 3,5")
	if err != nil || !slices.Equal(got, []int{1, 3, 5}) {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "1,,3", "0", "-2", "a", "1,3,"} {
		if _, err := ParseIntList(bad); err == nil {
			t.Errorf("ParseIntList(%q) succeeded", bad)
		}
	}
}
