#!/usr/bin/env bash
# Start a local 3-node cluster:   ./scripts/cluster.sh start
# Stop it:                         ./scripts/cluster.sh stop
# Kill one node (e.g. the leader): ./scripts/cluster.sh kill 2
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
mkdir -p bin logs

start_node() {
  ./bin/raftkv -id "$1" -cluster "$CLUSTER" -data "data/$1" >"logs/node$1.log" 2>&1 &
  echo $! >"logs/node$1.pid"
  echo "started node $1 (pid $!)"
}

case "${1:-}" in
  start)
    go build -o bin/raftkv ./cmd/raftkv
    for i in 1 2 3; do start_node "$i"; done ;;
  restart)
    start_node "$2" ;;
  kill)
    kill "$(cat "logs/node$2.pid")" && echo "killed node $2" ;;
  stop)
    for i in 1 2 3; do [ -f "logs/node$i.pid" ] && kill "$(cat "logs/node$i.pid")" 2>/dev/null || true; done
    echo "stopped" ;;
  *)
    echo "usage: $0 start|stop|kill <id>|restart <id>"; exit 1 ;;
esac
