//go:build integration

// Runs against the compose stack: make compose-up && make integration
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/YOURNAME/slotcar/internal/sink"
	"github.com/YOURNAME/slotcar/internal/source"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	table = "orders_it"
	slot  = "slotcar_it"
	pub   = "slotcar_pub"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	pgDSN  = getenv("PG_DSN", "postgres://demo:demo@localhost:5432/demo?sslmode=disable")
	chAddr = getenv("CH_ADDR", "localhost:9000")
	chUser = getenv("CH_USER", "demo")
	chPass = getenv("CH_PASSWORD", "demo")
)

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func dropSlot(ctx context.Context, pool *pgxpool.Pool) {
	_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active_pid IS NOT NULL`, slot)
	for i := 0; i < 20; i++ {
		if _, err := pool.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, slot); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// chCount runs one aggregate over the FINAL view of the test table. Counts of
// live rows must use countIf(_deleted = 0): FINAL keeps tombstones as the
// newest version of a deleted key.
func chCount(ch driver.Conn, expr string) int {
	var n uint64
	q := fmt.Sprintf("SELECT %s FROM `public`.`%s` FINAL", expr, table)
	if err := ch.QueryRow(context.Background(), q).Scan(&n); err != nil {
		return -1 // table not created yet, etc.; waitUntil retries
	}
	return int(n)
}

func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ch, err := sink.OpenCH(chAddr, chUser, chPass)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	// Clean slate: unique table + slot, fresh ClickHouse database.
	dropSlot(ctx, pool)
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id bigserial PRIMARY KEY, user_id bigint NOT NULL,
		amount numeric(12,2) NOT NULL, status text NOT NULL,
		note text, created_at timestamptz NOT NULL DEFAULT now())`, table)); err != nil {
		t.Fatal(err)
	}
	if err := ch.Exec(ctx, "DROP DATABASE IF EXISTS `public`"); err != nil {
		t.Fatal(err)
	}
	if err := source.EnsurePrereqs(ctx, pgDSN, pub); err != nil {
		t.Fatal(err)
	}

	s, err := sink.New(sink.Options{Addr: chAddr, User: chUser, Password: chPass, BatchSize: 500, FlushEvery: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	go s.Run()
	defer s.Stop()

	onlyTable := func(schema, tbl string) bool { return schema == "public" && tbl == table }
	src := source.New(source.ReplDSN(pgDSN), slot, pub, onlyTable, s, slog.Default())
	done := make(chan struct{})
	go func() { _ = src.Run(ctx, 0); close(done) }()
	defer func() {
		cancel()
		<-done
		dropSlot(context.Background(), pool)
	}()

	// The slot is created asynchronously: inserting before it is streaming
	// would lose those rows (they precede the slot's start point).
	waitUntil(t, "slot streaming", func() bool {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&active)
		return err == nil && active
	})

	// 1) Inserts replicate.
	const n = 100
	b := &pgx.Batch{}
	for i := 0; i < n; i++ {
		b.Queue(fmt.Sprintf(`INSERT INTO %s (user_id, amount, status, note) VALUES ($1,$2,$3,$4)`, table),
			int64(i), "12.34", "new", "hello")
	}
	if err := pool.SendBatch(ctx, b).Close(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "100 rows in CH", func() bool { return chCount(ch, "countIf(_deleted = 0)") == n })

	// 2) Update + delete converge via ReplacingMergeTree.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET status='paid' WHERE id=$1`, table), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, table), 2); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "99 live rows", func() bool { return chCount(ch, "countIf(_deleted = 0)") == n-1 })
	waitUntil(t, "update applied", func() bool { return chCount(ch, "countIf(id = 1 AND status = 'paid')") == 1 })
	if got := chCount(ch, "sum(_deleted)"); got != 1 {
		t.Fatalf("tombstones = %d, want 1", got)
	}

	// 3) TOAST. A big, poorly compressible note (700 x md5 hex = 22400 chars)
	//    is stored out-of-line; updating another column then sends it as an
	//    unchanged-TOAST placeholder that must be restored from the cache.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET note=(SELECT string_agg(md5(random()::text), '') FROM generate_series(1,700)) WHERE id=$1`, table), 3); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "big note landed", func() bool { return chCount(ch, "countIf(id = 3 AND length(note) = 22400)") == 1 })
	if _, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET status='shipped' WHERE id=$1`, table), 3); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "status updated", func() bool { return chCount(ch, "countIf(id = 3 AND status = 'shipped')") == 1 })
	if got := chCount(ch, "countIf(id = 3 AND length(note) = 22400)"); got != 1 {
		t.Fatalf("TOAST value lost across unchanged-note update (rows with intact note: %d)", got)
	}

	// 4) Checkpoint discipline: sink confirmed real WAL positions.
	if s.Confirmed() == 0 {
		t.Fatal("confirmed LSN never advanced")
	}

	// 5) Postgres saw our acks: confirmed_flush_lsn moves past the slot's
	//    starting point only if the flush position is reported.
	waitUntil(t, "slot confirmed_flush_lsn advanced", func() bool {
		var adv bool
		err := pool.QueryRow(ctx,
			`SELECT confirmed_flush_lsn > restart_lsn FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&adv)
		return err == nil && adv
	})
}
