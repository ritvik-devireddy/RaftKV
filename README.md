# RaftKV

[![CI](https://github.com/ritvik-devireddy/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/ritvik-devireddy/raftkv/actions/workflows/ci.yml)

A linearizable key-value store built on a from-scratch Raft implementation in Go, with a deterministic chaos-testing harness.

> **Status: work in progress.** Leader election works and is tested across hundreds of seeded failure scenarios. Log replication, durability, and the network API are next (see the [roadmap](#roadmap)).

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
internal/simnet/         deterministic simulated network
internal/storage/        write-ahead log and snapshots
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

## Roadmap

- [x] **Phase 0: setup.** Repo layout, CI, simulated network, KV state machine with dedup
- [x] **Phase 1: elections.** Terms, votes, randomized timeouts, heartbeats; at most one leader per term under message loss and partitions
- [ ] **Phase 2: replication.** AppendEntries, commit index, follower catch-up
- [ ] **Phase 3: durability.** Checksummed WAL, crash recovery, persisted term/vote/log
- [ ] **Phase 4: real API.** gRPC and Protobuf, CLI client, Docker Compose cluster
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
