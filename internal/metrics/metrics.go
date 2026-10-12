package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slotcar_events_received_total", Help: "Row events decoded from WAL, by op",
	}, []string{"op"})

	RowsFlushedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_rows_flushed_total", Help: "Rows written to the sink",
	})
	FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "slotcar_flush_duration_seconds", Help: "Time per full flush cycle",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
	FlushFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_flush_failures_total", Help: "Failed sink flush attempts (retried)",
	})
	FlushLagSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_flush_lag_seconds", Help: "Now minus newest event decode time at last flush",
	})

	ConfirmedLSNGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_confirmed_lsn", Help: "Max LSN durably persisted in the sink (float64; exact below 2^53)",
	})

	DecodeErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_decode_errors_total", Help: "Rows skipped due to decode failures",
	})
	UnknownOIDTypes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_unknown_oid_types_total", Help: "Column values with unmapped type OIDs (decoded as string)",
	})
	MissingReplicaIdentity = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_missing_replica_identity_total", Help: "Deletes dropped because the source table lacks a PK",
	})
	ToastCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_toast_cache_hits_total", Help: "Unchanged-TOAST values restored from the row cache",
	})
	ToastCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_toast_cache_misses_total", Help: "Unchanged-TOAST values that could not be restored (written as NULL)",
	})
	ReconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slotcar_reconnects_total", Help: "Replication connection re-establishments",
	})

	SnapshotRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "slotcar_snapshot_rows_copied_total", Help: "Rows copied during initial snapshot, by table",
	}, []string{"table"})

	SlotActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_slot_active", Help: "1 if the replication slot has an active consumer",
	})
	PGCurrentLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_pg_current_lsn", Help: "Source pg_current_wal_lsn() as float (exact below 2^53)",
	})
	SlotConfirmedLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_slot_confirmed_lsn", Help: "Slot confirmed_flush_lsn as seen by Postgres",
	})
	WALLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_wal_lag_bytes", Help: "pg_current_wal_lsn() - confirmed_flush_lsn",
	})
	WALRetainedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "slotcar_wal_retained_bytes", Help: "pg_current_wal_lsn() - restart_lsn (disk retained by the slot)",
	})
)
