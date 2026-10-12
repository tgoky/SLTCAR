package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsurePrereqs verifies wal_level and idempotently creates the publication.
// FOR ALL TABLES auto-includes tables created later; the sink creates their
// ClickHouse side lazily on first event. (Creating it needs a superuser.)
func EnsurePrereqs(ctx context.Context, dsn, publication string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("admin connect: %w", err)
	}
	defer pool.Close()

	var lvl string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&lvl); err != nil {
		return err
	}
	if lvl != "logical" {
		return fmt.Errorf("wal_level is %q; start Postgres with wal_level=logical", lvl)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, publication).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(publication))); err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
	}
	return nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
