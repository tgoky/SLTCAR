package sink

import (
	"strings"
	"testing"

	"github.com/YOURNAME/slotcar/internal/pipeline"
)

func TestBuildDDL(t *testing.T) {
	cols := []pipeline.Value{
		{Name: "id", OID: 20, IsKey: true},
		{Name: "note", OID: 25},
		{Name: "amount", OID: 1700},
	}
	ddl := BuildDDL("public", "orders", cols)
	for _, want := range []string{
		"`public`.`orders`", "`id` Int64", "`note` Nullable(String)",
		"`amount` Nullable(Float64)", "ReplacingMergeTree(`_lsn`)", "ORDER BY (`id`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("missing %q in %s", want, ddl)
		}
	}

	noKey := BuildDDL("public", "t", []pipeline.Value{{Name: "a", OID: 25}})
	if !strings.Contains(noKey, "ENGINE = MergeTree ORDER BY tuple()") || strings.Contains(noKey, "Replacing") {
		t.Fatalf("keyless table must be plain MergeTree (Replacing would collapse all rows): %s", noKey)
	}

	stmt := InsertStmt("public", "orders", cols)
	want := "INSERT INTO `public`.`orders` (`id`, `note`, `amount`, `_deleted`, `_lsn`, `_seen`)"
	if stmt != want {
		t.Fatalf("unexpected stmt: %s", stmt)
	}
}

func TestAlterStmtsSkipKeys(t *testing.T) {
	cols := []pipeline.Value{{Name: "id", OID: 20, IsKey: true}, {Name: "n", OID: 25}}
	got := AlterStmts("public", "t", cols)
	if len(got) != 1 || !strings.Contains(got[0], "ADD COLUMN IF NOT EXISTS `n` Nullable(String)") {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestSameSchema(t *testing.T) {
	a := []pipeline.Value{{Name: "id", OID: 20}, {Name: "n", OID: 25}}
	b := []pipeline.Value{{Name: "id", OID: 20}, {Name: "n", OID: 25}}
	c := append(append([]pipeline.Value{}, b...), pipeline.Value{Name: "extra", OID: 25})
	if !sameSchema(a, b) || sameSchema(a, c) {
		t.Fatal("sameSchema misjudged")
	}
}
