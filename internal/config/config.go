package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	PGDsn       string
	CHAddr      string
	CHUser      string
	CHPassword  string
	Slot        string
	Publication string
	Tables      []string // "schema.table" list; empty or "*" = all non-system tables
	BatchSize   int
	FlushEvery  time.Duration
	MetricsAddr string
	StartLSN    string // stream-mode override, e.g. "0/16B37D0"

	SnapshotWorkers   int
	SnapshotChunkRows int
	VerifyAttempts    int
}

func Load() Config {
	return Config{
		PGDsn:       env("PG_DSN", "postgres://demo:demo@localhost:5432/demo?sslmode=disable"),
		CHAddr:      env("CH_ADDR", "localhost:9000"),
		CHUser:      env("CH_USER", "demo"),
		CHPassword:  env("CH_PASSWORD", "demo"),
		Slot:        env("SLOT", "slotcar"),
		Publication: env("PUBLICATION", "slotcar_pub"),
		Tables:      splitCSV(env("TABLES", "*")),
		BatchSize:   envInt("BATCH_SIZE", 10_000),
		FlushEvery:  envDur("FLUSH_EVERY", time.Second),
		MetricsAddr: env("METRICS_ADDR", ":9090"),
		StartLSN:    env("START_LSN", ""),

		SnapshotWorkers:   envInt("SNAPSHOT_WORKERS", 4),
		SnapshotChunkRows: envInt("SNAPSHOT_CHUNK_ROWS", 50_000),
		VerifyAttempts:    envInt("VERIFY_ATTEMPTS", 3),
	}
}

// Match implements the include filter.
func (c Config) Match(schema, table string) bool {
	if len(c.Tables) == 0 {
		return true
	}
	key := schema + "." + table
	for _, t := range c.Tables {
		if t == "*" || t == key {
			return true
		}
	}
	return false
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Sprintf("config: bad %s=%q", k, v))
		}
		return n
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			panic(fmt.Sprintf("config: bad %s=%q", k, v))
		}
		return d
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
