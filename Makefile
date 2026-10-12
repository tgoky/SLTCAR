COMPOSE := docker compose -f deploy/docker-compose.yml
LOADGEN_ARGS ?= -rate=400 -duration=300s

compose-up:   ; $(COMPOSE) up -d
compose-down: ; $(COMPOSE) down -v
reset:
	docker exec slotcar-pg psql -U demo -d demo -c "SELECT pg_drop_replication_slot('slotcar') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slotcar');"
	docker exec slotcar-pg psql -U demo -d demo -c "DROP PUBLICATION IF EXISTS slotcar_pub;"
	docker exec slotcar-ch clickhouse-client --user demo --password demo --query "DROP DATABASE IF EXISTS public"

build:   ; go build ./...
vet:     ; go vet ./...
test:    ; go test ./... -count=1
race:    ; go test ./... -race -count=1
integration:
	$(COMPOSE) up -d --wait pg ch
	go test -tags integration ./test/integration -v -count=1
run:     ; go run ./cmd/slotcar run
stream:  ; go run ./cmd/slotcar stream
verify:  ; go run ./cmd/slotcar verify
loadgen: ; go run ./cmd/loadgen $(LOADGEN_ARGS)
