# Keel

A key-value store replicated with Raft, written in Go. Nodes talk to each other over gRPC and serve clients over HTTP.

- Leader election, log replication and log compaction with snapshots, following the [Raft paper](https://raft.github.io/raft.pdf).
- Linearizable reads and writes: a write returns once it is applied, and a read sees every write that returned before it.
- Safe retries: a write tagged with a client ID and sequence number is never applied twice.
- Durable: the log is fsynced before a write is acknowledged, with one fsync shared by every write waiting on it.

## Run

```sh
docker compose up --build
```

This starts five nodes. Their HTTP APIs are on `localhost:8081` to `localhost:8085`, one per node.

Reads and writes must reach the leader. A follower redirects them there with `307`, but the redirect points at a Docker hostname such as `raft-2:8080`, which only resolves inside the Compose network. From your machine, ask any node who the leader is and send requests to that node's port:

```sh
curl localhost:8081/leader
# {"id":3,"address":"raft-3:8080"}   ->   use localhost:8083
```

## API

| Method | Path | Body or query | Success |
|---|---|---|---|
| `POST` | `/set` | `{"key": "a", "value": "1"}` | `201` |
| `POST` | `/delete` | `{"key": "a"}` | `204` |
| `GET` | `/get` | `?key=a` | `200` with `{"key": "a", "value": "1"}`, or `404` |

```sh
curl -X POST -d '{"key":"a","value":"1"}' localhost:8083/set
curl localhost:8083/get?key=a
```

A `503` means the request could not be completed right now: no leader is known, the write did not commit within two seconds, or the leader could not confirm with a majority that it is still leader. Responses that carry `Retry-After` are safe to retry after that many seconds.

### Retrying writes

A write that times out may still have been applied. To retry without applying it twice, give each write a `client_id` and a `seq` that starts at 1 and goes up by one with each new write from that client. A write whose `seq` is not above the last one applied for its `client_id` is skipped.

```sh
curl -X POST -d '{"key":"a","value":"1","client_id":"c1","seq":1}' localhost:8083/set
```

### Operations

| Path | Returns |
|---|---|
| `/leader` | The leader's ID and HTTP address, or `503` while there is none. |
| `/state` | This node's term, vote and log entries. |
| `/healthz` | `200` while the process is serving. |
| `/readyz` | `200` once the node knows a leader and has applied every committed entry. |
| `/metrics` | Prometheus metrics, including `raft_term`, `raft_state`, `raft_commit_index`, `raft_leader_changes_total`, `raft_persist_duration_seconds` and HTTP and gRPC request metrics. |

The image has no shell, so its container healthcheck runs `/keel healthcheck /readyz`, which calls the endpoint and exits 0 or 1.

## Configuration

Each node is configured through environment variables.

| Variable | Example | Meaning |
|---|---|---|
| `ID` | `1` | This node's ID. It must appear in `PEERS`. |
| `PEERS` | `1=raft-1:7000,2=raft-2:7000` | Every node's gRPC address, this one included. |
| `HTTP_PEERS` | `1=raft-1:8080,2=raft-2:8080` | Every node's HTTP address, used for redirects to the leader. |
| `PORT` | `7000` | The gRPC port to listen on. |
| `HTTP_PORT` | `:8080` | The HTTP address to listen on. |
| `DATA_DIR` | `/data` | Where state is stored. Defaults to the working directory. |
| `SNAPSHOT_THRESHOLD` | `4194304` | Bytes of applied log to keep before taking a snapshot. Defaults to 4 MB. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Defaults to `info`. |

## Storage

| File | Holds |
|---|---|
| `DATA_DIR/persist/raft.state` | The current term and vote, rewritten whole on each change. |
| `DATA_DIR/persist/raft.log` | Log entries after the last snapshot, appended to and cut back from an index. Each record carries a CRC-32C, and a partial record left by a crash is dropped on load. |
| `DATA_DIR/snapshot/raft.snapshot` | The latest snapshot of the store with the index and term it covers. |

## How it works

- **Replication.** The leader keeps one replicator per follower. Each sends one `AppendEntries` at a time carrying every entry the follower is missing, so writes that arrive together travel together. A follower that has fallen behind the leader's snapshot is sent the snapshot instead.
- **Durability.** Appending to the log only writes it. A sync worker fsyncs everything written so far with the lock released, so concurrent writes share one fsync. The leader counts itself towards a majority only up to its last synced entry, and a follower replies only once its new entries are synced.
- **Reads.** A new leader commits an empty entry for its term. A read records the commit index, confirms leadership through one heartbeat round shared with any concurrent reads, and waits until that index is applied. Reads do not write to the log.
- **Snapshots.** Once more than `SNAPSHOT_THRESHOLD` bytes of applied log build up, the node snapshots the store and drops the entries it covers.

## Develop

```sh
go test -race ./...    # all tests
make stress            # run the raft tests three times
make print-covr        # raft test coverage
golangci-lint run      # lint, as CI does
make gen               # regenerate the protobuf and gRPC code (needs protoc)
```

CI runs gofmt, vet, the tests and golangci-lint, then builds the image and scans it with Trivy.
