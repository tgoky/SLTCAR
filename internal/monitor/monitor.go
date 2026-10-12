// Package monitor polls Postgres for slot health and exports it as metrics.
// This catches the CDC failure mode that hurts most: a stalled consumer
// silently filling the source's disk with retained WAL.
package monitor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/YOURNAME/slotcar/internal/metrics"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, dsn, slot string, log *slog.Logger) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Error("monitor: connect failed", "err", err)
		return
	}
	defer pool.Close()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		poll(ctx, pool, slot, log)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func poll(ctx context.Context, pool *pgxpool.Pool, slot string, log *slog.Logger) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var curS string
	if err := pool.QueryRow(cctx, "SELECT pg_current_wal_lsn()::text").Scan(&curS); err != nil {
		log.Debug("monitor: current lsn", "err", err)
		return
	}
	if cur, err := pglogrepl.ParseLSN(curS); err == nil {
		metrics.PGCurrentLSN.Set(float64(cur))
	}

	var confS *string
	var active bool
	var lag, retained *float64
	err := pool.QueryRow(cctx, `
		SELECT confirmed_flush_lsn::text, active,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::double precision,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::double precision
		FROM pg_replication_slots WHERE slot_name = $1`, slot).
		Scan(&confS, &active, &lag, &retained)
	if errors.Is(err, pgx.ErrNoRows) {
		metrics.SlotActive.Set(0)
		return
	}
	if err != nil {
		log.Debug("monitor: slot query", "err", err)
		return
	}
	metrics.SlotActive.Set(b2f(active))
	if confS != nil {
		if conf, err := pglogrepl.ParseLSN(*confS); err == nil {
			metrics.SlotConfirmedLSN.Set(float64(conf))
		}
	}
	if lag != nil {
		metrics.WALLagBytes.Set(*lag)
	}
	if retained != nil {
		metrics.WALRetainedBytes.Set(*retained)
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
