# slotcar — Postgres → ClickHouse CDC replication engine in Go

![CI](https://github.com/YOURNAME/slotcar/actions/workflows/ci.yml/badge.svg)

Single binary. Streams Postgres WAL via logical replication (pgoutput) into
ClickHouse with effectively-once semantics: at-least-once delivery +
`ReplacingMergeTree(_lsn)` idempotent sink.

## Setup

    go mod tidy             # resolves dependencies and writes go.sum — commit it
    make vet && make test

## Quickstart

    make compose-up         # Postgres, ClickHouse, Prometheus (:9091), Grafana (:3000)
    make loadgen            # creates schema, seeds, generates traffic (LOADGEN_ARGS to tune)
    make run                # parallel snapshot + stream (Ctrl-C = graceful drain)
    make verify             # PASS/FAIL table per table
    curl localhost:9090/metrics | grep slotcar

Configuration is by environment variable: `PG_DSN`, `CH_ADDR`, `CH_USER`,
`CH_PASSWORD`, `SLOT`, `PUBLICATION`, `TABLES` (`schema.table,...` or `*`),
`BATCH_SIZE`, `FLUSH_EVERY`, `METRICS_ADDR`, `START_LSN`, `SNAPSHOT_WORKERS`,
`SNAPSHOT_CHUNK_ROWS`, `VERIFY_ATTEMPTS`.

## Correctness model
- **Ack discipline**: we report to Postgres only the max LSN durably flushed
  to ClickHouse (`slotcar_confirmed_lsn`), in the *flush* position of the
  standby status update (the field Postgres uses to advance
  `confirmed_flush_lsn`). Crash between flush and ack → replay; replay is
  harmless (ReplacingMergeTree keeps max `_lsn` per key).
- **Snapshot/stream handoff**: slot created with EXPORT_SNAPSHOT; tables are
  copied inside `SET TRANSACTION SNAPSHOT` at the consistent point; streaming
  starts exactly there. Snapshot rows carry `_lsn=0`, so any streamed event
  wins. No gap, no overlap.
- **Deletes** become `_deleted=1` tombstones (Postgres sends only PK columns
  on delete). Read paths use `FINAL` and filter `_deleted = 0`.
- **TOAST**: unchanged-TOAST columns on UPDATE are restored from a bounded
  LRU of last row images (`slotcar_toast_cache_{hits,misses}`), keyed by
  table + key and bounded by entries and bytes.
- **Schema change**: a column added in Postgres is added in ClickHouse on its
  next row (`ADD COLUMN IF NOT EXISTS`).
- **Parallel snapshot**: single **integer** PK tables are split into key
  ranges (`SNAPSHOT_WORKERS`) and paged by keyset; other tables are copied
  whole. All workers pin the exported snapshot; the first failure aborts the run.
- `slotcar verify` reconciles source vs sink (counts + XOR row checksums)
  after waiting for WAL drain.

## Known limitations (deliberate, documented)
- TRUNCATE is logged, not applied. PK-value changes leave the old key stale.
- Tables without a primary key become append-only `MergeTree` (no updates or
  deletes are possible on them in Postgres without REPLICA IDENTITY FULL).
- First update of a toast-heavy row after restart may write NULL for the
  unchanged column (cache miss) — set REPLICA IDENTITY FULL to avoid.
- numeric → Float64 (use Decimal + string decode for exactness).
  `infinity` dates become NULL; BC dates are rejected.
- Renames, drops and type changes of source columns require a resync.
- Partitioned tables are not snapshotted by their parent name.
- WAL produced by *unpublished* tables is not acked until published data
  moves the checkpoint, so `slotcar_wal_lag_bytes` can read high on a busy
  database with a quiet published set.
- One slot = one consumer. Multi-node = future work.

## Troubleshooting
- Never put `replication=database` in a DSN used by pgx/pgxpool — only in the
  pgconn replication connection (`source.ReplDSN` handles this).
- `wal_level is "replica"`: the compose file passes `-c wal_level=logical`;
  elsewhere set it and restart Postgres.
- `slot "slotcar" already exists` on `run`: use `make reset`, or `make stream`.
- pglogrepl API drift: this repo targets the unversioned `Parse` API with
  `proto_version '1'` (`RelationID`, `Namespace`, `DataType` field names).
