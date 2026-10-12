// Package inspect reads source schema metadata (tables, columns, primary keys)
// shared by the snapshot copier and the verifier.
package inspect

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Col struct {
	Name string
	OID  uint32
}

type Table struct{ Schema, Name string }

// Q quotes a Postgres identifier.
func Q(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// Qualified returns a safely quoted schema.table.
func (t Table) Qualified() string { return Q(t.Schema) + "." + Q(t.Name) }

func Tables(ctx context.Context, pool *pgxpool.Pool, match func(schema, table string) bool) ([]Table, error) {
	rows, err := pool.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND n.nspname NOT LIKE 'pg\_toast%'
		ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		if match == nil || match(t.Schema, t.Name) {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// Columns returns live columns in attnum order. The table is passed as a
// quoted regclass literal so mixed-case / odd names resolve correctly.
func Columns(ctx context.Context, pool *pgxpool.Pool, t Table) ([]Col, error) {
	rows, err := pool.Query(ctx, `
		SELECT a.attname, a.atttypid
		FROM pg_attribute a
		WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, t.Qualified())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Col
	for rows.Next() {
		var c Col
		if err := rows.Scan(&c.Name, &c.OID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// PrimaryKey returns PK column names in key order.
func PrimaryKey(ctx context.Context, pool *pgxpool.Pool, t Table) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY k.ord`, t.Qualified())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
