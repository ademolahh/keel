#!/usr/bin/env bash
# Runs test.js against the Compose cluster, kills the leader with SIGKILL 60 seconds in
# and starts it again 30 seconds later. Writes the k6 summary, a timeline and every
# node's log to results/failover.
set -euo pipefail

cd "$(dirname "$0")/.."
out=results/failover
mkdir -p "$out"

leader() {
	curl -sf localhost:8081/leader | sed -E 's/.*"id":([0-9]+).*/\1/'
}

docker run --rm --network raft-network \
	-v "$PWD/scripts:/scripts:ro" -v "$PWD/$out:/out" \
	grafana/k6 run --dns ttl=1s -e FAILOVER=1 --summary-export /out/summary.json /scripts/test.js \
	>"$out/k6.txt" 2>/dev/null &
k6=$!

sleep 60
id=$(leader)
container=$(docker compose ps -q "raft-$id")
echo "killing raft-$id"
docker compose kill -s SIGKILL "raft-$id" >/dev/null 2>&1

sleep 30
echo "starting raft-$id"
docker compose start "raft-$id" >/dev/null 2>&1
until curl -sf -o /dev/null "localhost:$((8080 + id))/readyz"; do sleep 0.1; done

wait "$k6"
docker compose logs --no-color >"$out/nodes.log"

{
	echo "leader: raft-$id"
	echo "killed:  $(docker inspect -f '{{.State.FinishedAt}}' "$container")"
	echo "started: $(docker inspect -f '{{.State.StartedAt}}' "$container")"
	echo
	echo "elections:"
	grep -E '"(starting election|became leader)"' "$out/nodes.log"
	echo
	echo "raft-$id after it started:"
	grep -E "^raft-$id-.*\"(installed snapshot|became follower)\"" "$out/nodes.log" | tail -2
} >"$out/timeline.txt"

cat "$out/timeline.txt"
