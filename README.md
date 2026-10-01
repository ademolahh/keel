# Keel

A key-value store replicated with Raft, written in Go. Nodes talk to each other over gRPC and serve clients over HTTP.

## Run

```sh
docker compose up --build
```

This starts five nodes, reachable on `localhost:8081` to `localhost:8085`.

## API

| Method | Path | Body or query |
|---|---|---|
| `POST` | `/set` | `{"key": "a", "value": "1"}` |
| `POST` | `/delete` | `{"key": "a"}` |
| `GET` | `/get` | `?key=a` |
| `GET` | `/leader` | — |

Writes must go to the leader; a follower redirects them there with `307`, so use `curl -L`.

```sh
curl -L -X POST -d '{"key":"a","value":"1"}' localhost:8081/set
curl localhost:8081/get?key=a
```

## Develop

```sh
go test -race ./...   # tests
make stress           # run the raft tests three times
make gen              # regenerate the gRPC code
```
