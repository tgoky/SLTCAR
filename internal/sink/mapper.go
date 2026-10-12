package sink

import (
	"fmt"
	"strings"

	"github.com/YOURNAME/slotcar/internal/pipeline"
)

// BaseType maps a Postgres type OID to a ClickHouse column type.
func BaseType(oid uint32) string {
	switch oid {
	case 16: // bool
		return "UInt8"
	case 20, 21, 23: // int8/int2/int4 -> Int64 (sign-safe superset)
		return "Int64"
	case 700, 701, 1700: // float4/float8/numeric
		return "Float64"
	case 1082: // date
		return "Date"
	case 1114, 1184: // timestamp / timestamptz
		return "DateTime64(3, 'UTC')"
	default: // text-family, json, uuid, bytea, unknown
		return "String"
	}
}

func qi(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func colDef(c pipeline.Value) string {
	t := BaseType(c.OID)
	if c.IsKey {
		return qi(c.Name) + " " + t
	}
	return qi(c.Name) + " Nullable(" + t + ")"
}

// BuildDDL creates the idempotent destination table. Key columns are NOT NULL
// and form the ReplacingMergeTree ORDER BY (the dedupe key); value columns are
// Nullable so TOAST-reconstruction misses and real NULLs can be expressed.
//
// Tables without a primary key cannot be deduplicated or updated (Postgres
// rejects UPDATE/DELETE on them under the default replica identity), so they
// become plain append-only MergeTree tables. ReplacingMergeTree with an empty
// sort key would collapse every row of the table into one.
func BuildDDL(schema, table string, cols []pipeline.Value) string {
	var defs, keys []string
	for _, c := range cols {
		defs = append(defs, colDef(c))
		if c.IsKey {
			keys = append(keys, qi(c.Name))
		}
	}
	engine, order := "MergeTree", "tuple()"
	if len(keys) > 0 {
		engine, order = "ReplacingMergeTree(`_lsn`)", "("+strings.Join(keys, ", ")+")"
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (%s, `_deleted` UInt8, `_lsn` UInt64, `_seen` DateTime64(3, 'UTC')) "+
			"ENGINE = %s ORDER BY %s",
		qi(schema), qi(table), strings.Join(defs, ", "), engine, order,
	)
}

// AlterStmts returns idempotent ADD COLUMN statements for every non-key
// column, so a column added in Postgres appears in ClickHouse on its next row.
func AlterStmts(schema, table string, cols []pipeline.Value) []string {
	var out []string
	for _, c := range cols {
		if c.IsKey {
			continue
		}
		out = append(out, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s",
			qi(schema), qi(table), colDef(c)))
	}
	return out
}

// InsertStmt pins column order explicitly, so destination schema never
// depends on positional luck. No VALUES clause: clickhouse-go's batch API
// appends FORMAT Native itself.
func InsertStmt(schema, table string, cols []pipeline.Value) string {
	names := make([]string, 0, len(cols)+3)
	for _, c := range cols {
		names = append(names, qi(c.Name))
	}
	names = append(names, qi("_deleted"), qi("_lsn"), qi("_seen"))
	return fmt.Sprintf("INSERT INTO %s.%s (%s)", qi(schema), qi(table), strings.Join(names, ", "))
}
