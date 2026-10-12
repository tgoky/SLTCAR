package verify

import (
	"testing"
	"time"
)

func TestCanonicalAgreesAcrossSides(t *testing.T) {
	// Postgres-side decoded values vs ClickHouse-side scanned values.
	ts := time.Date(2024, 6, 1, 12, 30, 0, 123_456_000, time.UTC)   // microseconds
	tsCH := time.Date(2024, 6, 1, 12, 30, 0, 123_000_000, time.UTC) // sink keeps ms
	str := "hello"
	i64 := int64(42)
	pairs := []struct {
		name string
		oid  uint32
		pg   any
		ch   any
	}{
		{"bool", 16, true, uint8(1)},
		{"bool-false", 16, false, uint8(0)},
		{"int", 20, int64(42), int64(42)},
		{"nullable int ptr", 20, int64(42), &i64},
		{"nullable string ptr", 25, "hello", &str},
		{"null", 25, nil, (*string)(nil)},
		{"float", 701, 1.5, 1.5},
		{"timestamptz truncated to ms", 1184, ts, tsCH},
		{"date", 1082, time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, p := range pairs {
		a := canonical(p.oid, chNormalize(p.pg))
		b := canonical(p.oid, chNormalize(p.ch))
		if a != b {
			t.Errorf("%s: pg=%q ch=%q", p.name, a, b)
		}
	}
}

func TestRowHashDetectsDifferences(t *testing.T) {
	oids := []uint32{25, 25}
	if rowHash(oids, []any{"ab", "c"}) == rowHash(oids, []any{"a", "bc"}) {
		t.Fatal("length prefix should separate (ab,c) from (a,bc)")
	}
	if rowHash(oids, []any{"a", nil}) == rowHash(oids, []any{"a", ""}) {
		t.Fatal("NULL must differ from empty string")
	}
	// XOR aggregate is order independent.
	r1, r2 := rowHash(oids, []any{"x", "1"}), rowHash(oids, []any{"y", "2"})
	if r1^r2 != r2^r1 {
		t.Fatal("xor not commutative?!")
	}
}
