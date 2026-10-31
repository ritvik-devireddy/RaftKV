# RaftKV

[![CI](https://github.com/ritvik-devireddy/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/ritvik-devireddy/raftkv/actions/workflows/ci.yml)

A linearizable key-value store built on a from-scratch Raft implementation in Go, with a deterministic chaos-testing harness.

> **Status: work in progress.** A three-node cluster runs over gRPC with a CLI client. Leader election, log replication, and crash-safe durable storage are tested across hundreds of seeded failure scenarios (message loss, partitions, stale leaders, nodes crashing at arbitrary points). The linearizability proof (Porcupine) is next (see the [roadmap](#roadmap)).

## What it is

A small cluster of Go nodes that together act as one key-value store (`Get`, `Put`, `Delete`, `CompareAndSwap`). Data is replicated with Raft, so committed writes survive node crashes, restarts, and network partitions.

The goal is not just to implement Raft but to **demonstrate it is correct under failure**:

- The Raft core is independent of the network and the disk, so a whole cluster runs single-threaded on a simulated network with a seeded random number generator. Any failure can be replayed exactly from its seed.
- A chaos harness injects partitions, crashes, drops, delays, and reordering, records every client operation, and checks the history for linearizability with [Porcupine](https://github.com/anishathalye/porcupine).

## Architecture

```
            clients
               │  gRPC (KV service)
        ┌──────▼──────┐
        │ raftkv node │  ×3 or ×5
        │ ┌─────────┐ │
        │ │ kv store│ │  state machine + client dedup (internal/kv)
        │ ├─────────┤ │
        │ │  raft   │ │  consensus core (internal/raft)
        │ ├─────────┤ │
        │ │   WAL   │ │  durable log (internal/storage)
        │ └─────────┘ │
        └──────┬──────┘
               │  gRPC (Raft service)
            other nodes
```

An architecture diagram and design write-up will go in [`docs/design.md`](docs/design.md) as the project grows.

## Repository layout

```
api/                     .proto definitions (client API and Raft RPCs)
cmd/raftkv-server/       node binary
cmd/raftkv-cli/          command-line client
internal/raft/           consensus core (transport- and storage-agnostic)
internal/server/         joins a Raft node to the KV store: commit waiting, redirects
internal/rpc/            gRPC adapters (client API, node-to-node transport)
internal/client/         leader-finding client with safe retries
internal/simnet/         deterministic simulated network
internal/storage/        write-ahead log (snapshots planned)
internal/kv/             key-value state machine with request deduplication
test/linearizability/    chaos tests and Porcupine checks
bench/                   benchmarks
docs/design.md           design decisions, tradeoffs, bugs found
```

## Getting started

Requires Go 1.24 or newer.

```
go build ./...
go vet ./...
go test -race ./...
```

`-race` needs a C compiler on Windows (for example MinGW-w64 via MSYS2). CI runs it on Linux on every push.

### Run a cluster

Start three nodes, one per terminal:

```
go run ./cmd/raftkv-server -id 0 -listen :7001 -data data/node0 -peers localhost:7001,localhost:7002,localhost:7003
go run ./cmd/raftkv-server -id 1 -listen :7002 -data data/node1 -peers localhost:7001,localhost:7002,localhost:7003
go run ./cmd/raftkv-server -id 2 -listen :7003 -data data/node2 -peers localhost:7001,localhost:7002,localhost:7003
```

Then use the CLI (it finds the leader and follows redirects):

```
go run ./cmd/raftkv-cli put hello world
go run ./cmd/raftkv-cli get hello
go run ./cmd/raftkv-cli cas hello world there
go run ./cmd/raftkv-cli delete hello
```

Stop the leader with Ctrl+C: the other two elect a new one and reads and writes continue. Restart it and it recovers its term and log from its write-ahead log and catches up. Or run `docker compose up --build` for the same three nodes in containers.

Connections are unauthenticated and unencrypted (no TLS), which is fine for a local demo and not for a network you do not trust.

### Regenerating the protobuf code

Only needed after editing `api/raftkv.proto`. Install `protoc` (`winget install protobuf`) and the two plugins (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` and `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`), then run `scripts/gen-proto.ps1` (or `.sh`). The generated code in `api/raftkvpb` is committed.

## Roadmap

- [x] **Phase 0: setup.** Repo layout, CI, simulated network, KV state machine with dedup
- [x] **Phase 1: elections.** Terms, votes, randomized timeouts, heartbeats; at most one leader per term under message loss and partitions
- [x] **Phase 2: replication.** AppendEntries with log matching, commit index, follower catch-up, Apply; state machine safety and log matching checked on every simulated event
- [x] **Phase 3: durability.** Checksummed WAL with fsync, torn-write recovery, persisted term/vote/log; crash injection at every storage operation
- [x] **Phase 4: real API.** gRPC and Protobuf, leader-finding client with exactly-once retries, CLI, Docker Compose cluster
- [ ] **Phase 5: proof.** Operation histories, Porcupine checks, thousands of seeded chaos runs
- [ ] **Phase 6: polish.** Snapshots and log compaction, benchmarks, design write-up

## Design principles

- **Follow Figure 2 of the Raft paper exactly** before getting creative.
- **The Raft core never touches the network or the disk directly.** It talks to interfaces, so tests run it against `simnet` and production runs it over gRPC.
- **Every bug the chaos tester finds becomes a named regression test** and a line in the design doc.

## References

- Ongaro and Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [MIT 6.5840 Raft labs](https://pdos.csail.mit.edu/6.824/)

## License

MIT. See [LICENSE](LICENSE).
