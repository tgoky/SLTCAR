package pipeline

import "time"

type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Value is one decoded column. IsKey marks replica-identity (PK) columns,
// which drive both ClickHouse ORDER BY and delete tombstones.
type Value struct {
	Name  string
	OID   uint32
	IsKey bool
	Val   any // nil | bool | int64 | float64 | string | time.Time
}

// Event is one row image. LSN is the WAL start position of the originating
// record (0 for snapshot rows); the sink uses max(LSN) per flush cycle as its
// durable checkpoint.
type Event struct {
	LSN     uint64
	Schema  string
	Table   string
	Op      Op
	Columns []Value
	Seen    time.Time // decode time; lag is measured against this
}
