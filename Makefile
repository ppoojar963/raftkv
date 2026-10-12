.PHONY: test chaos bench build cluster stop
test:      ; go test -race ./...
chaos:     ; CHAOS_RUNS=20 go test ./kv -run Linearizable -v
bench:     ; go run ./cmd/bench -nodes 1,3,5 -clients 1,16,64 -duration 5s
build:     ; go build -o bin/raftkv ./cmd/raftkv
cluster:   ; ./scripts/cluster.sh start
stop:      ; ./scripts/cluster.sh stop
