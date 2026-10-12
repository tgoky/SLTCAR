package decode

import (
	"math"
	"testing"
	"time"
)

func TestByOIDScalars(t *testing.T) {
	cases := []struct {
		oid  uint32
		in   string
		want any
	}{
		{16, "t", true},
		{16, "f", false},
		{20, "9007199254740993", int64(9007199254740993)}, // > 2^53: must stay exact
		{23, "42", int64(42)},
		{21, "-7", int64(-7)},
		{700, "1.5", float64(1.5)},
		{1700, "123.45", float64(123.45)},
		{25, "hello", "hello"},
		{2950, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	}
	for _, c := range cases {
		got, err := ByOID(c.oid, []byte(c.in))
		if err != nil {
			t.Fatalf("oid %d %q: %v", c.oid, c.in, err)
		}
		if got != c.want {
			t.Fatalf("oid %d %q: got %v (%T), want %v", c.oid, c.in, got, got, c.want)
		}
	}
}

func TestByOIDNaN(t *testing.T) {
	v, err := ByOID(1700, []byte("NaN"))
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(v.(float64)) {
		t.Fatalf("expected NaN, got %v", v)
	}
}

func TestByOIDTimes(t *testing.T) {
	want := time.Date(2024, 6, 1, 12, 30, 0, 123_000_000, time.UTC)
	for _, in := range []string{
		"2024-06-01 12:30:00.123+00",
		"2024-06-01 12:30:00.123+00:00",
		"2024-06-01 14:00:00.123+01:30", // half-hour zones print as +HH:MM
	} {
		got, err := ByOID(1184, []byte(in))
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if !got.(time.Time).Equal(want) {
			t.Fatalf("%q: got %v want %v", in, got, want)
		}
	}

	ts, err := ByOID(1114, []byte("2024-06-01 12:30:00"))
	if err != nil || !ts.(time.Time).Equal(time.Date(2024, 6, 1, 12, 30, 0, 0, time.UTC)) {
		t.Fatalf("timestamp: %v %v", ts, err)
	}

	d, err := ByOID(1082, []byte("2024-06-01"))
	if err != nil || d.(time.Time).Format("2006-01-02") != "2024-06-01" {
		t.Fatalf("date: %v %v", d, err)
	}

	inf, err := ByOID(1184, []byte("infinity"))
	if err != nil || inf != nil {
		t.Fatalf("infinity should be NULL, got %v %v", inf, err)
	}

	if _, err := ByOID(1184, []byte("garbage")); err == nil {
		t.Fatal("expected error for garbage timestamp")
	}
}
