package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // /debug/pprof on the metrics mux
	"os"
	"os/signal"
	"syscall"

	"github.com/YOURNAME/slotcar/internal/config"
	"github.com/YOURNAME/slotcar/internal/monitor"
	"github.com/YOURNAME/slotcar/internal/sink"
	"github.com/YOURNAME/slotcar/internal/snapshot"
	"github.com/YOURNAME/slotcar/internal/source"
	"github.com/YOURNAME/slotcar/internal/verify"
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const usage = `usage: slotcar <mode>

modes:
  run       parallel snapshot (EXPORT_SNAPSHOT), then stream
  stream    stream only; resumes from the slot/sink checkpoint
  snapshot  snapshot only, then exit
  verify    reconcile source vs sink (counts + XOR checksums)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	mode := os.Args[1]
	switch mode {
	case "run", "stream", "snapshot", "verify":
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	log := slog.Default()
	cfg := config.Load()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		})
		if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
			log.Warn("metrics server stopped", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if mode == "verify" {
		if err := verify.Run(ctx, verify.Config{
			PGDsn: cfg.PGDsn, CHAddr: cfg.CHAddr, CHUser: cfg.CHUser, CHPassword: cfg.CHPassword,
			Slot: cfg.Slot, Attempts: cfg.VerifyAttempts,
		}, cfg.Match, log); err != nil {
			log.Error("verify", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := source.EnsurePrereqs(ctx, cfg.PGDsn, cfg.Publication); err != nil {
		log.Error("prerequisites", "err", err)
		os.Exit(1)
	}

	go monitor.Run(ctx, cfg.PGDsn, cfg.Slot, log)

	s, err := sink.New(sink.Options{
		Addr: cfg.CHAddr, User: cfg.CHUser, Password: cfg.CHPassword,
		BatchSize: cfg.BatchSize, FlushEvery: cfg.FlushEvery,
	})
	if err != nil {
		log.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	go s.Run()

	snapOpts := snapshot.Options{Workers: cfg.SnapshotWorkers, ChunkRows: cfg.SnapshotChunkRows}

	switch mode {
	case "run":
		cp, err := snapshot.Run(ctx, cfg.PGDsn, cfg.Slot, cfg.Match, s, log, snapOpts)
		if err != nil {
			log.Error("snapshot failed", "err", err)
			s.Stop()
			os.Exit(1)
		}
		runStream(ctx, cfg, s, cp, log)
	case "stream":
		var start pglogrepl.LSN
		if cfg.StartLSN != "" {
			start, err = pglogrepl.ParseLSN(cfg.StartLSN)
			if err != nil {
				log.Error("bad START_LSN", "err", err)
				s.Stop()
				os.Exit(2)
			}
		}
		runStream(ctx, cfg, s, start, log)
	case "snapshot":
		if _, err := snapshot.Run(ctx, cfg.PGDsn, cfg.Slot, cfg.Match, s, log, snapOpts); err != nil {
			log.Error("snapshot failed", "err", err)
			s.Stop()
			os.Exit(1)
		}
		s.Stop() // drains and flushes the snapshot rows
		return
	}

	log.Info("shutting down", "confirmed_lsn", pglogrepl.LSN(s.Confirmed()).String())
	s.Stop()
}

func runStream(ctx context.Context, cfg config.Config, s *sink.Sink, start pglogrepl.LSN, log *slog.Logger) {
	src := source.New(source.ReplDSN(cfg.PGDsn), cfg.Slot, cfg.Publication, cfg.Match, s, log)
	if err := src.Run(ctx, start); err != nil && ctx.Err() == nil {
		log.Error("source exited", "err", err)
	}
}
