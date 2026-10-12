// Package verify reconciles source Postgres against the ClickHouse sink.
// Method: per-row canonical encoding -> FNV-64a -> XOR aggregate, on both
// sides, streamed (O(1) memory). XOR is order-independent, so no sorting is
// needed; row counts guard against gross drift. Limits: XOR cannot detect an
// even number of identical compensating differences (the count check catches
// the realistic cases), and rows whose TOAST value was lost to a cold cache
// after a restart will fail verification until they are rewritten.
package verify

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/YOURNAME/slotcar/internal/decode"
	"github.com/YOURNAME/slotcar/internal/inspect"
	"github.com/YOURNAME/slotcar/internal/sink"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	PGDsn      string
	CHAddr     string
	CHUser     string
	CHPassword string
	Slot       string
	Attempts   int
}

type result struct {
	Table     inspect.Table
	SrcN, ChN int64
	SrcX, ChX uint64
	Err       string
}

func (r *result) ok() bool { return r.Err == "" && r.SrcN == r.ChN && r.SrcX == r.ChX }

func Run(ctx context.Context, cfg Config, match func(schema, table string) bool, log *slog.Logger) error {
	return run(ctx, cfg, match, log, os.Stdout)
}

func run(ctx context.Context, cfg Config, match func(schema, table string) bool, log *slog.Logger, out io.Writer) error {
	if cfg.Attempts < 1 {
		cfg.Attempts = 3
	}
	pool, err := pgxpool.New(ctx, cfg.PGDsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	ch, err := sink.OpenCH(cfg.CHAddr, cfg.CHUser, cfg.CHPassword)
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := drain(ctx, pool, cfg.Slot, log); err != nil {
		return err
	}

	tables, err := inspect.Tables(ctx, pool, match)
	if err != nil {
		return err
	}

	// results holds the latest outcome for EVERY table; passing tables are
	// not re-checked on later attempts.
	results := make([]*result, len(tables))
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		failing := 0
		for i, t := range tables {
			if results[i] != nil && results[i].ok() {
				continue
			}
			results[i] = verifyTable(ctx, pool, ch, t)
			if !results[i].ok() {
				failing++
			}
		}
		if failing == 0 {
			break
		}
		if attempt == cfg.Attempts {
			break
		}
		log.Warn("verification mismatch; retrying after settle delay", "attempt", attempt, "failing", failing)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}

	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "TABLE\tSRC ROWS\tCH ROWS\tSRC XOR\tCH XOR\tRESULT\n")
	failed := 0
	for _, r := range results {
		status := "PASS"
		if !r.ok() {
			status = "FAIL"
			failed++
			if r.Err != "" {
				status += " (" + r.Err + ")"
			}
		}
		fmt.Fprintf(tw, "%s.%s\t%d\t%d\t%016x\t%016x\t%s\n",
			r.Table.Schema, r.Table.Name, r.SrcN, r.ChN, r.SrcX, r.ChX, status)
	}
	tw.Flush()

	if failed > 0 {
		return fmt.Errorf("verification failed for %d/%d table(s) after %d attempt(s)", failed, len(tables), cfg.Attempts)
	}
	log.Info("verification passed", "tables", len(tables))
	return nil
}

// drain waits until the slot has consumed the source WAL (or times out with
// a warning — verification on a lagging system still reports, just noisily).
// Note: WAL from unpublished tables also counts as lag, so on a busy database
// with other workloads this can time out; the verdict is still computed.
func drain(ctx context.Context, pool *pgxpool.Pool, slot string, log *slog.Logger) error {
	deadline := time.Now().Add(120 * time.Second)
	clean := 0
	for time.Now().Before(deadline) {
		var lag *float64
		err := pool.QueryRow(ctx, `
			SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::double precision
			FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&lag)
		if err == pgx.ErrNoRows {
			log.Warn("drain: slot not found; verifying without drain guarantee", "slot", slot)
			return nil
		}
		if err == nil && lag != nil && *lag <= 1<<20 { // within 1 MiB
			clean++
			if clean >= 2 {
				log.Info("drained", "lag_bytes", int64(*lag))
				return nil
			}
		} else {
			clean = 0
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	log.Warn("drain timeout; verifying anyway (expect possible mismatches under load)")
	return nil
}

func verifyTable(ctx context.Context, pool *pgxpool.Pool, ch driver.Conn, t inspect.Table) *result {
	r := &result{Table: t}
	src, err := inspect.Columns(ctx, pool, t)
	if err != nil {
		r.Err = err.Error()
		return r
	}

	// ClickHouse column list (minus bookkeeping columns), matched by name.
	chCols := map[string]bool{}
	crs, err := ch.Query(ctx, `SELECT name FROM system.columns WHERE database = ? AND table = ?`, t.Schema, t.Name)
	if err != nil {
		r.Err = fmt.Sprintf("ch schema: %v", err)
		return r
	}
	for crs.Next() {
		var n string
		if err := crs.Scan(&n); err != nil {
			crs.Close()
			r.Err = err.Error()
			return r
		}
		chCols[n] = true
	}
	crs.Close()
	for _, skip := range []string{"_deleted", "_lsn", "_seen"} {
		delete(chCols, skip)
	}

	// Verify only columns present on both sides, in PG order.
	var common []inspect.Col
	for _, c := range src {
		if chCols[c.Name] {
			common = append(common, c)
		}
	}
	if len(common) == 0 {
		r.Err = "table missing in ClickHouse"
		return r
	}
	oids := make([]uint32, len(common))
	for i, c := range common {
		oids[i] = c.OID
	}

	// FINAL only exists to collapse versions on Replacing tables; keyless
	// tables are plain MergeTree and are read as-is.
	var engine string
	if err := ch.QueryRow(ctx, `SELECT engine FROM system.tables WHERE database = ? AND name = ?`,
		t.Schema, t.Name).Scan(&engine); err != nil {
		r.Err = fmt.Sprintf("ch engine: %v", err)
		return r
	}
	final := ""
	if strings.Contains(engine, "Replacing") {
		final = " FINAL"
	}

	// Source side: stream, decode, hash. Both sides are read concurrently
	// inside different systems, but sequentially here for simplicity.
	sel := make([]string, len(common))
	for i, c := range common {
		sel[i] = inspect.Q(c.Name) + "::text"
	}
	srows, err := pool.Query(ctx, fmt.Sprintf("SELECT %s FROM %s", strings.Join(sel, ", "), t.Qualified()))
	if err != nil {
		r.Err = err.Error()
		return r
	}
	ptrs := make([]*string, len(common))
	scans := make([]any, len(common))
	for i := range ptrs {
		scans[i] = &ptrs[i]
	}
	for srows.Next() {
		if err := srows.Scan(scans...); err != nil {
			srows.Close()
			r.Err = err.Error()
			return r
		}
		vals := make([]any, len(common))
		for i, c := range common {
			if ptrs[i] == nil {
				continue
			}
			v, err := decode.ByOID(c.OID, []byte(*ptrs[i]))
			if err != nil {
				srows.Close()
				r.Err = fmt.Sprintf("decode %s: %v", c.Name, err)
				return r
			}
			vals[i] = v
		}
		r.SrcX ^= rowHash(oids, vals)
		r.SrcN++
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		r.Err = err.Error()
		return r
	}

	// ClickHouse side: live rows only, streamed. Destinations are allocated
	// from the driver's own column scan types, which is the supported way to
	// scan arbitrary columns.
	csel := make([]string, len(common))
	for i, c := range common {
		csel[i] = "`" + strings.ReplaceAll(c.Name, "`", "``") + "`"
	}
	crows, err := ch.Query(ctx, fmt.Sprintf("SELECT %s FROM `%s`.`%s`%s WHERE _deleted = 0",
		strings.Join(csel, ", "), strings.ReplaceAll(t.Schema, "`", "``"), strings.ReplaceAll(t.Name, "`", "``"), final))
	if err != nil {
		r.Err = fmt.Sprintf("ch query: %v", err)
		return r
	}
	defer crows.Close()
	cts := crows.ColumnTypes()
	dest := make([]any, len(cts))
	holders := make([]reflect.Value, len(cts))
	for i, ct := range cts {
		holders[i] = reflect.New(ct.ScanType())
		dest[i] = holders[i].Interface()
	}
	for crows.Next() {
		if err := crows.Scan(dest...); err != nil {
			r.Err = fmt.Sprintf("ch scan: %v", err)
			return r
		}
		vals := make([]any, len(common))
		for i := range holders {
			vals[i] = chNormalize(holders[i].Elem().Interface())
		}
		r.ChX ^= rowHash(oids, vals)
		r.ChN++
	}
	if err := crows.Err(); err != nil {
		r.Err = err.Error()
	}
	return r
}
