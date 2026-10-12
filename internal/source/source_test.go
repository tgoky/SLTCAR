package source

import (
	"strings"
	"testing"

	"github.com/YOURNAME/slotcar/internal/pipeline"
	"github.com/jackc/pglogrepl"
)

func relFixture() *Rel {
	return &Rel{
		OID: 16384, Schema: "public", Table: "docs",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: colFlagKey},
			{Name: "note", DataType: 25},
		},
	}
}

func tuple(id, note string, noteType uint8) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte(id)},
		{DataType: noteType, Data: []byte(note)},
	}}
}

func TestToastReconstruction(t *testing.T) {
	rel := relFixture()
	tc := NewToastCache(4, 1<<20)
	big := strings.Repeat("x", 5000)

	// Full insert, then remember it (what emit() does for complete rows).
	ins := tuple("7", big, pglogrepl.TupleDataTypeText)
	row, complete, err := buildRow(rel, ins, nil, tc)
	if err != nil || !complete || row[1].Val != big {
		t.Fatalf("insert row: complete=%v err=%v", complete, err)
	}
	tc.Remember(rel.KeyString(ins), row)

	// Update re-sends everything except the toasted column ('u').
	row, complete, err = buildRow(rel, tuple("7", "", pglogrepl.TupleDataTypeToast), nil, tc)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("expected cache hit, row should be complete")
	}
	if row[1].Val != big {
		t.Fatalf("toast value not restored")
	}

	// Cold cache: value unrecoverable, flagged incomplete -> NULL.
	cold := NewToastCache(4, 1<<20)
	row, complete, err = buildRow(rel, tuple("7", "", pglogrepl.TupleDataTypeToast), nil, cold)
	if err != nil || complete || row[1].Val != nil {
		t.Fatalf("cold cache: %v %v %v", row, complete, err)
	}
}

func TestToastCacheKeysAreTableScoped(t *testing.T) {
	a, b := relFixture(), relFixture()
	b.OID = 99999
	tp := tuple("7", "", pglogrepl.TupleDataTypeText)
	if a.KeyString(tp) == b.KeyString(tp) {
		t.Fatal("same key value in different tables must not collide")
	}
}

func TestToastCacheSkipsSmallRows(t *testing.T) {
	tc := NewToastCache(4, 1<<20)
	tc.Remember("k", []pipeline.Value{{Name: "x", Val: "tiny"}})
	if _, ok := tc.Get("k"); ok {
		t.Fatal("small rows cannot contain TOASTed values and should not be cached")
	}
}

func TestToastCacheEviction(t *testing.T) {
	big := []pipeline.Value{{Name: "x", Val: strings.Repeat("y", 2000)}}
	tc := NewToastCache(2, 1<<20)
	tc.Remember("a", big)
	tc.Remember("b", big)
	tc.Get("a") // a is now most-recent
	tc.Remember("c", big)
	if _, ok := tc.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := tc.Get("a"); !ok {
		t.Fatal("a should survive (recently used)")
	}

	// Byte bound: two ~2KB rows cannot fit in 3KB.
	tb := NewToastCache(100, 3000)
	tb.Remember("a", big)
	tb.Remember("b", big)
	if _, ok := tb.Get("a"); ok {
		t.Fatal("a should be evicted by the byte bound")
	}
}

func TestDeleteRowCarriesKeys(t *testing.T) {
	rel := relFixture()
	old := &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
		{DataType: pglogrepl.TupleDataTypeNull},
	}}
	row, _, err := buildRow(rel, old, nil, NewToastCache(1, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if row[0].Val != int64(7) || !row[0].IsKey {
		t.Fatalf("key not decoded: %+v", row[0])
	}
	if row[1].Val != nil {
		t.Fatalf("non-key delete column should be nil: %+v", row[1])
	}
}

func TestColumnCountMismatch(t *testing.T) {
	short := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
	}}
	if _, _, err := buildRow(relFixture(), short, nil, NewToastCache(1, 1024)); err == nil {
		t.Fatal("expected error instead of an index panic")
	}
}
