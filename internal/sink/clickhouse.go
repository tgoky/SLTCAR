// Package sink batches decoded events into ClickHouse. Delivery is
// at-least-once: replays and duplicates converge because every row carries
// _lsn and ReplacingMergeTree keeps the highest version per key. Confirmed()
// therefore only advances after a full flush cycle succeeds — that number is
// the ONLY thing we ever report back to Postgres.
//
// Concurrency model: Append is safe from any goroutine (it only touches the
// channel). Everything else lives on the single Run goroutine, so the buffers
// and schema caches need no locks.
package sink

import (
	"context"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/YOURNAME/slotcar/internal/metrics"
	"github.com/YOURNAME/slotcar/internal/pipeline"
)

type Options struct {
	Addr       string
	User       string
	Password   string
	BatchSize  int
	FlushEvery time.Duration
}

// OpenCH opens a ClickHouse native-protocol connection pool (lazy dial).
func OpenCH(addr, user, password string) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
}

type row struct {
	vals []any
	lsn  uint64
	seen time.Time
}

type Sink struct {
	conn       driver.Conn
	batchSize  int
	flushEvery time.Duration
	in         chan pipeline.Event
	stopCh     chan struct{}
	doneCh     chan struct{}

	// Run-goroutine only:
	buf     map[string][]row // key: schema\x00table
	cols    map[string][]pipeline.Value
	stmts   map[string]string
	pending int

	confirmed atomic.Uint64
}

func New(o Options) (*Sink, error) {
	conn, err := OpenCH(o.Addr, o.User, o.Password)
	if err != nil {
		return nil, err
	}
	return &Sink{
		conn:       conn,
		batchSize:  o.BatchSize,
		flushEvery: o.FlushEvery,
		in:         make(chan pipeline.Event, o.BatchSize*4),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		buf:        make(map[string][]row),
		cols:       make(map[string][]pipeline.Value),
		stmts:      make(map[string]string),
	}, nil
}

func (s *Sink) Append(e pipeline.Event) error {
	select {
	case s.in <- e: // full channel == backpressure all the way to the WAL
		return nil
	case <-s.stopCh:
		return context.Canceled
	}
}

func (s *Sink) Confirmed() uint64 { return s.confirmed.Load() }

func (s *Sink) Run() {
	t := time.NewTicker(s.flushEvery)
	defer t.Stop()
	for {
		select {
		case e := <-s.in:
			s.add(e)
			if s.pending >= s.batchSize {
				s.flush()
			}
		case <-t.C:
			s.flush()
		case <-s.stopCh:
			// Drain everything already handed to us, flush best-effort.
			// Anything unsent was never acked, so it replays on restart.
		drain:
			for {
				select {
				case e := <-s.in:
					s.add(e)
				default:
					break drain
				}
			}
			s.flush()
			close(s.doneCh)
			return
		}
	}
}

// Stop drains and flushes, then returns. Call it once.
func (s *Sink) Stop() {
	close(s.stopCh)
	<-s.doneCh
	_ = s.conn.Close()
}

func tableKey(schema, table string) string { return schema + "\x00" + table }

func sameSchema(a, b []pipeline.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].OID != b[i].OID {
			return false
		}
	}
	return true
}

func (s *Sink) add(e pipeline.Event) {
	key := tableKey(e.Schema, e.Table)
	if old, ok := s.cols[key]; ok && !sameSchema(old, e.Columns) {
		// The source table changed shape (e.g. ADD COLUMN). Flush rows built
		// against the old shape first, then rebuild the statement for the new
		// one; sendTable re-runs the idempotent ALTERs.
		s.flush()
		delete(s.stmts, key)
		s.cols[key] = e.Columns
	} else if !ok {
		s.cols[key] = e.Columns
	}
	s.buf[key] = append(s.buf[key], rowify(e))
	s.pending++
}

func chVal(v any) any {
	switch x := v.(type) {
	case bool:
		if x {
			return uint8(1)
		}
		return uint8(0)
	case float32:
		return float64(x)
	case int:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	default:
		return v // nil, int64, float64, string, time.Time pass through
	}
}

func rowify(e pipeline.Event) row {
	vals := make([]any, 0, len(e.Columns)+3)
	for _, c := range e.Columns {
		vals = append(vals, chVal(c.Val))
	}
	var deleted uint8
	if e.Op == pipeline.OpDelete {
		deleted = 1
	}
	vals = append(vals, deleted, e.LSN, e.Seen)
	return row{vals: vals, lsn: e.LSN, seen: e.Seen}
}

func (s *Sink) flush() {
	if s.pending == 0 {
		return
	}
	buf := s.buf
	s.buf = make(map[string][]row)
	s.pending = 0

	start := time.Now()
	n, maxLSN := 0, uint64(0)
	var maxSeen time.Time
	for _, rows := range buf {
		n += len(rows)
		for _, r := range rows {
			if r.lsn > maxLSN {
				maxLSN = r.lsn
			}
			if r.lsn > 0 && r.seen.After(maxSeen) {
				maxSeen = r.seen
			}
		}
	}

	for attempt := 1; ; attempt++ {
		err := s.sendAll(buf)
		if err == nil {
			break
		}
		metrics.FlushFailuresTotal.Inc()
		slog.Error("clickhouse flush failed; retrying", "attempt", attempt, "err", err)
		select {
		case <-s.stopCh:
			return // unacked rows replay after restart
		case <-time.After(backoff(attempt)):
		}
	}

	metrics.RowsFlushedTotal.Add(float64(n))
	metrics.FlushDuration.Observe(time.Since(start).Seconds())
	if !maxSeen.IsZero() {
		metrics.FlushLagSeconds.Set(time.Since(maxSeen).Seconds())
	}
	if maxLSN > 0 { // snapshot rows carry lsn=0 and must not move the checkpoint
		if maxLSN > s.confirmed.Load() { // single writer; no CAS needed
			s.confirmed.Store(maxLSN)
		}
	}
	metrics.ConfirmedLSNGauge.Set(float64(s.confirmed.Load()))
}

func (s *Sink) sendAll(buf map[string][]row) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for key, rows := range buf {
		if err := s.sendTable(ctx, key, rows); err != nil {
			return err // retry the whole cycle; already-sent tables are idempotent
		}
	}
	return nil
}

func (s *Sink) sendTable(ctx context.Context, key string, rows []row) error {
	stmt, ok := s.stmts[key]
	if !ok {
		schema, table := splitKey(key)
		cols := s.cols[key]
		if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+qi(schema)); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, BuildDDL(schema, table, cols)); err != nil {
			return err
		}
		for _, alter := range AlterStmts(schema, table, cols) {
			if err := s.conn.Exec(ctx, alter); err != nil {
				return err
			}
		}
		stmt = InsertStmt(schema, table, cols)
		s.stmts[key] = stmt
	}
	batch, err := s.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.vals...); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func splitKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

func backoff(attempt int) time.Duration {
	d := 250 * time.Millisecond << min(attempt, 7) // caps at 32s
	return d + time.Duration(rand.Int63n(int64(d/4)))
}
