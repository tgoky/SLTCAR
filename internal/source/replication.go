// Package source consumes the logical replication stream. The critical
// invariant: we only ever report to Postgres an LSN that the sink has
// durably persisted. Reporting ahead loses data on crash; reporting late
// only costs replay (which the idempotent sink absorbs).
package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YOURNAME/slotcar/internal/metrics"
	"github.com/YOURNAME/slotcar/internal/pipeline"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const statusInterval = 10 * time.Second

// Sink is what the source pushes rows into.
type Sink interface {
	Append(pipeline.Event) error
	Confirmed() uint64
}

type Source struct {
	replDSN     string
	slot        string
	publication string
	match       func(schema, table string) bool
	sink        Sink
	rels        *RelationCache
	toast       *ToastCache
	log         *slog.Logger
}

func New(replDSN, slot, publication string, match func(schema, table string) bool, sk Sink, log *slog.Logger) *Source {
	return &Source{
		replDSN: replDSN, slot: slot, publication: publication, match: match,
		sink: sk, rels: NewRelationCache(), toast: NewToastCache(50_000, 256<<20), log: log,
	}
}

// Run keeps a stream alive across failures until ctx is done.
func (s *Source) Run(ctx context.Context, startLSN pglogrepl.LSN) error {
	backoff := time.Second
	for {
		err := s.stream(ctx, startLSN)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		s.log.Error("stream ended; reconnecting", "err", err, "backoff", backoff)
		metrics.ReconnectsTotal.Inc()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Source) ackPos(floor pglogrepl.LSN) pglogrepl.LSN {
	if c := s.sink.Confirmed(); uint64(floor) < c {
		return pglogrepl.LSN(c)
	}
	return floor
}

func (s *Source) stream(ctx context.Context, fallback pglogrepl.LSN) error {
	conn, err := pgconn.Connect(ctx, s.replDSN)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.Background())

	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, s.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{}); err != nil {
		var pgErr *pgconn.PgError
		if !(errors.As(err, &pgErr) && pgErr.Code == "42710") { // duplicate_object: slot already exists
			return fmt.Errorf("create slot: %w", err)
		}
	} else {
		s.log.Info("created replication slot", "slot", s.slot)
	}

	startFrom := fallback
	if c := s.sink.Confirmed(); c > uint64(startFrom) {
		startFrom = pglogrepl.LSN(c)
	}
	if err := pglogrepl.StartReplication(ctx, conn, s.slot, startFrom, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + strings.ReplaceAll(s.publication, "'", "''") + "'"},
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	s.log.Info("streaming", "from", startFrom.String())

	// Schema state is only valid within one streaming session.
	s.rels.Reset()
	s.toast.Reset()

	nextReport := time.Now().Add(statusInterval)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if now := time.Now(); now.After(nextReport) {
			if err := s.report(ctx, conn, startFrom); err != nil {
				return err
			}
			nextReport = now.Add(statusInterval)
		}

		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // just lets the status timer tick
			}
			return err // force reconnect + resume from confirmed LSN
		}

		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					if err := s.report(ctx, conn, startFrom); err != nil {
						return err
					}
					nextReport = time.Now().Add(statusInterval)
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse xlogdata: %w", err)
				}
				if err := s.handleLogical(xld); err != nil {
					return err
				}
			}
		default:
			// CopyBothResponse, ParameterStatus, etc.: nothing to do.
		}
	}
}

// report tells Postgres how far we have durably persisted. Postgres advances a
// logical slot's confirmed_flush_lsn from the FLUSH position of the standby
// status update (not the write position), so all three are set.
func (s *Source) report(ctx context.Context, conn *pgconn.PgConn, floor pglogrepl.LSN) error {
	pos := s.ackPos(floor)
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pos,
		WALFlushPosition: pos,
		WALApplyPosition: pos,
		ClientTime:       time.Now(),
	})
}

func (s *Source) handleLogical(xld pglogrepl.XLogData) error {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("parse logical message: %w", err)
	}
	lsn := uint64(xld.WALStart)
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		s.rels.Store(m)
		return nil
	case *pglogrepl.InsertMessage:
		return s.emit(m.RelationID, m.Tuple, nil, pipeline.OpInsert, lsn)
	case *pglogrepl.UpdateMessage:
		return s.emit(m.RelationID, m.NewTuple, m.OldTuple, pipeline.OpUpdate, lsn)
	case *pglogrepl.DeleteMessage:
		return s.emitDelete(m.RelationID, m.OldTuple, lsn)
	case *pglogrepl.TruncateMessage:
		s.log.Warn("TRUNCATE not applied to sink (documented limitation)", "relations", fmt.Sprint(m.RelationIDs))
		return nil
	default:
		return nil // Begin/Commit/Origin/Type
	}
}

func (s *Source) relFor(oid uint32) *Rel {
	rel := s.rels.Get(oid)
	if rel == nil {
		metrics.DecodeErrorsTotal.Inc()
		s.log.Error("change for unknown relation OID (no Relation message seen)", "oid", oid)
	}
	return rel
}

func (s *Source) allowed(rel *Rel) bool {
	return s.match == nil || s.match(rel.Schema, rel.Table)
}

func (s *Source) emit(relOID uint32, newTuple, oldTuple *pglogrepl.TupleData, op pipeline.Op, lsn uint64) error {
	rel := s.relFor(relOID)
	if rel == nil || !s.allowed(rel) {
		return nil
	}
	row, complete, err := buildRow(rel, newTuple, oldTuple, s.toast)
	if err != nil {
		metrics.DecodeErrorsTotal.Inc()
		s.log.Error("skipping undecodable row", "table", rel.Schema+"."+rel.Table, "err", err) // never kill the stream over one row
		return nil
	}
	if complete {
		s.toast.Remember(rel.KeyString(newTuple), row)
	}
	metrics.EventsReceived.WithLabelValues(string(op)).Inc()
	return s.sink.Append(pipeline.Event{
		LSN: lsn, Schema: rel.Schema, Table: rel.Table, Op: op, Columns: row, Seen: time.Now(),
	})
}

func (s *Source) emitDelete(relOID uint32, oldTuple *pglogrepl.TupleData, lsn uint64) error {
	rel := s.relFor(relOID)
	if rel == nil || !s.allowed(rel) {
		return nil
	}
	if oldTuple == nil {
		metrics.MissingReplicaIdentity.Inc()
		return nil // table without PK: deletes cannot be represented
	}
	row, _, err := buildRow(rel, oldTuple, nil, s.toast) // old tuple: keys set, rest null
	if err != nil {
		metrics.DecodeErrorsTotal.Inc()
		s.log.Error("skipping undecodable delete", "table", rel.Schema+"."+rel.Table, "err", err)
		return nil
	}
	s.toast.Del(rel.KeyString(oldTuple))
	metrics.EventsReceived.WithLabelValues(string(pipeline.OpDelete)).Inc()
	return s.sink.Append(pipeline.Event{
		LSN: lsn, Schema: rel.Schema, Table: rel.Table, Op: pipeline.OpDelete, Columns: row, Seen: time.Now(),
	})
}

// ReplDSN appends the replication parameter to a normal Postgres DSN. It must
// never be used for regular (pgx pool) connections.
func ReplDSN(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}
