package snapshot

import (
	"math"
	"testing"
)

// covers asserts spans tile [lo,hi] exactly: contiguous, non-overlapping,
// last one inclusive.
func covers(t *testing.T, lo, hi int64, workers int) {
	t.Helper()
	spans := splitSpans(lo, hi, workers)
	if len(spans) == 0 || len(spans) > workers {
		t.Fatalf("lo=%d hi=%d workers=%d: got %d spans", lo, hi, workers, len(spans))
	}
	if spans[0].lo != lo {
		t.Fatalf("first span must start at lo: %+v", spans)
	}
	for i, s := range spans {
		last := i == len(spans)-1
		if s.incl != last {
			t.Fatalf("only the last span is inclusive: %+v", spans)
		}
		if !last {
			if s.hi <= s.lo {
				t.Fatalf("empty/inverted span: %+v", spans)
			}
			if spans[i+1].lo != s.hi {
				t.Fatalf("gap or overlap between spans: %+v", spans)
			}
		}
	}
	if spans[len(spans)-1].hi != hi {
		t.Fatalf("last span must end at hi: %+v", spans)
	}
}

func TestSplitSpans(t *testing.T) {
	covers(t, 1, 1, 4)    // single key
	covers(t, 1, 2, 4)    // fewer keys than workers
	covers(t, 1, 1000, 4) // typical
	covers(t, 0, 99, 7)
	covers(t, -500, 500, 3) // negative keys
	covers(t, math.MinInt64, math.MaxInt64, 8)
	covers(t, 5, 5, 1)
}

func TestSnapshotNameRegex(t *testing.T) {
	for _, ok := range []string{"00000003-00000002-1", "0000000A-0000001B-1"} {
		if !snapshotNameRe.MatchString(ok) {
			t.Fatalf("should accept %q", ok)
		}
	}
	for _, bad := range []string{"", "x'; DROP TABLE t;--", "1 2"} {
		if snapshotNameRe.MatchString(bad) {
			t.Fatalf("should reject %q", bad)
		}
	}
}
