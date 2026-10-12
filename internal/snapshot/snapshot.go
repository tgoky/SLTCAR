// Package snapshot performs the initial load from an exported replication-slot
// snapshot. Correctness: every worker runs in a REPEATABLE READ transaction
// pinned to the slot's exported snapshot, so the copy is one consistent image
// taken exactly at ConsistentPoint. Streaming then starts from that point;
// snapshot rows carry _lsn=0 so any replayed streamed event supersedes them.
//
// The replication connection that created the slot stays open for the whole
// copy: the exported snapshot is only valid while that connection lives.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/YOURNAME/slotcar/internal/decode"
	"github.com/YOURNAME/slotcar/internal/inspect"
	"github.com/YOURNAME/slotcar/internal/metrics"
	"github.com/YOURNAME/slotcar/internal/pipeline"
	"github.com/YOURNAME/slotcar/internal/source"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	Workers   int // parallel copy connections
	ChunkRows int // keyset page size within a range
}

// Snapshot names look like "00000003-00000002-1": hex groups joined by dashes.
var snapshotNameRe = regexp.MustCompile(`^[0-9A-Fa-f]+(-[0-9A-Fa-f]+)*$`)

type unit struct {
	table inspect.Table
	cols  []inspect.Col
	pk    []string
	// Range fields are valid only when rng is set (single integer PK).
	rng    bool
	lo, hi int64
	incl   bool // hi inclusive (last span only)
}

func Run(ctx context.Context, pgDSN, slot string, match func(schema, table string) bool,
	sk source.Sink, log *slog.Logger, opts Options) (pglogrepl.LSN, error) {

	if opts.Workers < 1 {
		opts.Workers = 4
	}
	if opts.ChunkRows < 1 {
		opts.ChunkRows = 50_000
	}

	repl, err := pgconn.Connect(ctx, source.ReplDSN(pgDSN))
	if err != nil {
		return 0, fmt.Errorf("replication connect: %w", err)
	}
	defer repl.Close(context.Background())

	res, err := pglogrepl.CreateReplicationSlot(ctx, repl, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			return 0, fmt.Errorf("slot %q already exists: drop it (pg_drop_replication_slot) or run `stream` mode", slot)
		}
		return 0, fmt.Errorf("create slot with snapshot: %w", err)
	}
	if !snapshotNameRe.MatchString(res.SnapshotName) {
		return 0, fmt.Errorf("refusing to interpolate snapshot name %q", res.SnapshotName)
	}
	cp, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("consistent point %q: %w", res.ConsistentPoint, err)
	}
	log.Info("snapshot exported", "consistent_point", res.ConsistentPoint, "snapshot", res.SnapshotName)

	// Pool must hold the workers plus the planning query.
	cfg, err := pgxpool.ParseConfig(pgDSN)
	if err != nil {
		return 0, err
	}
	cfg.MaxConns = int32(opts.Workers + 2)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	tables, err := inspect.Tables(ctx, pool, match)
	if err != nil {
		return 0, err
	}

	var units []unit
	for _, t := range tables {
		cols, err := inspect.Columns(ctx, pool, t)
		if err != nil {
			return 0, err
		}
		pk, err := inspect.PrimaryKey(ctx, pool, t)
		if err != nil {
			return 0, err
		}
		if len(pk) == 1 && rangeableOID(cols, pk[0]) {
			lo, hi, ok, err := keyBounds(ctx, pool, t, pk[0], res.SnapshotName)
			if err != nil {
				return 0, err
			}
			if !ok { // empty table: nothing to copy
				continue
			}
			for _, s := range splitSpans(lo, hi, opts.Workers) {
				units = append(units, unit{table: t, cols: cols, pk: pk, rng: true, lo: s.lo, hi: s.hi, incl: s.incl})
			}
		} else {
			units = append(units, unit{table: t, cols: cols, pk: pk})
		}
	}
	log.Info("snapshot plan", "tables", len(tables), "units", len(units), "workers", opts.Workers)

	// Fail fast: the first worker error cancels everyone and is returned.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		once     sync.Once
		firstErr error
	)
	fail := func(err error) {
		once.Do(func() { firstErr = err; cancel() })
	}

	start := time.Now()
	jobs := make(chan unit)
	var wg sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				if err := copyUnit(wctx, pool, u, res.SnapshotName, sk, opts.ChunkRows, log); err != nil {
					fail(fmt.Errorf("copy %s.%s: %w", u.table.Schema, u.table.Name, err))
					return
				}
			}
		}()
	}
enqueue:
	for _, u := range units {
		select {
		case jobs <- u:
		case <-wctx.Done():
			break enqueue
		}
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return 0, firstErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	log.Info("snapshot complete", "tables", len(tables), "took", time.Since(start).Round(time.Millisecond))
	return cp, nil
}

func rangeableOID(cols []inspect.Col, name string) bool {
	for _, c := range cols {
		if c.Name == name {
			return c.OID == 20 || c.OID == 21 || c.OID == 23 // int8/int2/int4
		}
	}
	return false
}

// keyBounds reads min/max of the key inside the exported snapshot.
func keyBounds(ctx context.Context, pool *pgxpool.Pool, t inspect.Table, pk, snap string) (lo, hi int64, ok bool, err error) {
	tx, err := beginPinned(ctx, pool, snap)
	if err != nil {
		return 0, 0, false, err
	}
	defer tx.Rollback(ctx)
	var l, h *int64
	err = tx.QueryRow(ctx, fmt.Sprintf("SELECT min(%s)::bigint, max(%s)::bigint FROM %s",
		inspect.Q(pk), inspect.Q(pk), t.Qualified())).Scan(&l, &h)
	if err != nil {
		return 0, 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, false, err
	}
	if l == nil || h == nil {
		return 0, 0, false, nil
	}
	return *l, *h, true, nil
}

func beginPinned(ctx context.Context, pool *pgxpool.Pool, snap string) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	// snap was validated against snapshotNameRe, so interpolation is safe.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snap)); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

type span struct {
	lo, hi int64
	incl   bool
}

// splitSpans divides [lo, hi] into at most `workers` contiguous spans. Every
// span is half-open [lo, hi) except the last, which is closed so max(key) is
// included. Spans never overlap and together cover [lo, hi] exactly.
func splitSpans(lo, hi int64, workers int) []span {
	if workers < 1 {
		workers = 1
	}
	width := uint64(hi) - uint64(lo) // unsigned: no overflow for full int64 range
	step := width/uint64(workers) + 1
	var out []span
	for at := lo; ; {
		if uint64(hi)-uint64(at) < step { // remaining range fits in one span
			out = append(out, span{lo: at, hi: hi, incl: true})
			return out
		}
		next := int64(uint64(at) + step)
		out = append(out, span{lo: at, hi: next})
		at = next
	}
}

func copyUnit(ctx context.Context, pool *pgxpool.Pool, u unit, snap string, sk source.Sink, chunk int, log *slog.Logger) error {
	tx, err := beginPinned(ctx, pool, snap)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	pkSet := make(map[string]bool, len(u.pk))
	for _, k := range u.pk {
		pkSet[k] = true
	}
	sel := make([]string, len(u.cols))
	pkIdx := -1
	for i, c := range u.cols {
		sel[i] = inspect.Q(c.Name) + "::text"
		if len(u.pk) == 1 && c.Name == u.pk[0] {
			pkIdx = i
		}
	}
	selList := strings.Join(sel, ", ")
	label := u.table.Schema + "." + u.table.Name

	// stream drains one result set into the sink and returns the last PK seen.
	stream := func(rows pgx.Rows) (n int, lastKey int64, err error) {
		defer rows.Close()
		ptrs := make([]*string, len(u.cols))
		scans := make([]any, len(u.cols))
		for i := range ptrs {
			scans[i] = &ptrs[i]
		}
		for rows.Next() {
			if err := rows.Scan(scans...); err != nil {
				return n, lastKey, err
			}
			vals := make([]pipeline.Value, len(u.cols))
			for i, c := range u.cols {
				v := pipeline.Value{Name: c.Name, OID: c.OID, IsKey: pkSet[c.Name]}
				if ptrs[i] != nil {
					val, err := decode.ByOID(c.OID, []byte(*ptrs[i]))
					if err != nil {
						return n, lastKey, fmt.Errorf("col %s value %q: %w", c.Name, *ptrs[i], err)
					}
					v.Val = val
				}
				vals[i] = v
			}
			if pkIdx >= 0 {
				if iv, ok := vals[pkIdx].Val.(int64); ok {
					lastKey = iv
				}
			}
			// _lsn=0: any streamed event supersedes the snapshot image.
			if err := sk.Append(pipeline.Event{
				Schema: u.table.Schema, Table: u.table.Name, Op: pipeline.OpInsert, Columns: vals, Seen: time.Now(),
			}); err != nil {
				return n, lastKey, err
			}
			metrics.SnapshotRowsTotal.WithLabelValues(label).Inc()
			n++
		}
		return n, lastKey, rows.Err()
	}

	if !u.rng {
		// Whole table (no PK, composite PK, or non-integer PK): one stream.
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM %s", selList, u.table.Qualified()))
		if err != nil {
			return err
		}
		n, _, err := stream(rows)
		if err != nil {
			return err
		}
		log.Info("table copied", "table", label, "rows", n)
		return tx.Commit(ctx)
	}

	// Keyset pagination inside [lo, hi): ORDER BY is what makes LIMIT paging
	// correct — without it pages could skip or repeat rows.
	hiOp := "<"
	if u.incl {
		hiOp = "<="
	}
	pk := inspect.Q(u.pk[0])
	total := 0
	cursor, started := u.lo, false
	for {
		loOp := ">="
		if started {
			loOp = ">"
		}
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s %s $1 AND %s %s $2 ORDER BY %s LIMIT %d",
			selList, u.table.Qualified(), pk, loOp, pk, hiOp, pk, chunk)
		rows, err := tx.Query(ctx, q, cursor, u.hi)
		if err != nil {
			return err
		}
		n, last, err := stream(rows)
		if err != nil {
			return err
		}
		total += n
		if n < chunk {
			break
		}
		cursor, started = last, true
	}
	log.Info("range copied", "table", label, "lo", u.lo, "hi", u.hi, "rows", total)
	return tx.Commit(ctx)
}
