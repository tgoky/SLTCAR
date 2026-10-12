// loadgen writes a realistic ops mix (60% insert / 30% update / 10% delete)
// with occasional large `note` payloads so the TOAST path in the decoder is
// exercised under real traffic.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
  id         bigserial PRIMARY KEY,
  user_id    bigint NOT NULL,
  amount     numeric(12,2) NOT NULL,
  status     text NOT NULL,
  note       text,
  created_at timestamptz NOT NULL DEFAULT now()
)`

func main() {
	dsn := flag.String("dsn", "postgres://demo:demo@localhost:5432/demo?sslmode=disable", "postgres dsn")
	rate := flag.Int("rate", 200, "ops per second")
	dur := flag.Duration("duration", 60*time.Second, "run duration")
	seed := flag.Int("seed", 1000, "initial rows")
	toastPct := flag.Int("toast-pct", 20, "percent of inserts with a large (toastable) note")
	flag.Parse()
	if *rate < 1 || *seed < 1 {
		log.Fatal("rate and seed must be >= 1")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatal(err)
	}

	// Seed in one round trip; remember the highest id for update/delete picks.
	var maxID int64
	b := &pgx.Batch{}
	for i := 0; i < *seed; i++ {
		b.Queue(`INSERT INTO orders (user_id, amount, status, note) VALUES ($1,$2,$3,$4) RETURNING id`,
			int64(i), money(), "new", note(rand.Intn(100) < *toastPct))
	}
	br := pool.SendBatch(ctx, b)
	for i := 0; i < *seed; i++ {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			log.Fatal(err)
		}
		if id > maxID {
			maxID = id
		}
	}
	if err := br.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("seeded %d rows", *seed)

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.Now().Add(*dur)
	var ins, upd, del int64

	for time.Now().Before(deadline) {
		<-ticker.C
		id := rand.Int63n(maxID) + 1
		switch r := rand.Float64(); {
		case r < 0.6:
			var nid int64
			err := pool.QueryRow(ctx,
				`INSERT INTO orders (user_id, amount, status, note) VALUES ($1,$2,$3,$4) RETURNING id`,
				int64(rand.Intn(10_000)), money(), "new", note(rand.Intn(100) < *toastPct)).Scan(&nid)
			if err != nil {
				log.Fatal(err)
			}
			if nid > maxID {
				maxID = nid
			}
			ins++
		case r < 0.9:
			// Deliberately does NOT rewrite `note`: on big rows this produces
			// unchanged-TOAST placeholders, which is exactly what we want to test.
			_, err := pool.Exec(ctx, `UPDATE orders SET status=$2, amount=$3 WHERE id=$1`,
				id, []string{"paid", "shipped", "refunded"}[rand.Intn(3)], money())
			if err != nil {
				log.Fatal(err)
			}
			upd++
		default:
			if _, err := pool.Exec(ctx, `DELETE FROM orders WHERE id=$1`, id); err != nil {
				log.Fatal(err)
			}
			del++
		}
	}
	log.Printf("done: %d inserts, %d updates, %d deletes", ins, upd, del)
}

func money() string { return fmt.Sprintf("%.2f", rand.Float64()*1000) }

// note returns a payload. Big ones are random letters (poorly compressible)
// so Postgres really stores them out-of-line (TOAST) rather than inline.
func note(big bool) any {
	if !big {
		return "small note"
	}
	b := make([]byte, 20_000)
	for i := range b {
		b[i] = byte('a' + rand.Intn(26))
	}
	return string(b)
}
