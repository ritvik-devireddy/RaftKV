# RaftKV

[![CI](https://github.com/ritvik-devireddy/RaftKV/actions/workflows/ci.yml/badge.svg)](https://github.com/ritvik-devireddy/RaftKV/actions/workflows/ci.yml)
[![Nightly soak](https://github.com/ritvik-devireddy/RaftKV/actions/workflows/soak.yml/badge.svg)](https://github.com/ritvik-devireddy/RaftKV/actions/workflows/soak.yml)

A linearizable key-value store on a from-scratch Raft implementation in Go, built so that its correctness can be **tested under failure and checked, not just claimed**.

A small cluster of nodes acts as one database (`Get`, `Put`, `Delete`, `CompareAndSwap`). Writes are replicated with Raft, saved to a checksummed write-ahead log before they are acknowledged, and survive node crashes, restarts, and network partitions. Retried requests are applied exactly once.

## What has been verified

The point of the project is the evidence. Everything below is a test you can run.

| Property | How it is checked |
|---|---|
| At most one leader per term; Log Matching; State Machine Safety | Checked after every event in whole-cluster simulations with message loss, partitions and crashes, over hundreds of seeds |
| An acknowledged write is never lost to a crash | A node is crashed at **every** storage operation it performs, both before and after the operation reaches the disk, then must restart from disk alone and converge |
| The write-ahead log survives torn and corrupt writes | The log file is cut at every byte offset and bit-flipped; recovery must return exactly the records that were fully written, or report corruption |
| Clients see a linearizable history | 2,000 seeded chaos runs (about 1.1 million operations, with partitions, crashes and lost messages) are recorded and checked with [Porcupine](https://github.com/anishathalye/porcupine). A nightly job runs tens of thousands more on fresh seeds |
| Snapshots and log compaction are safe | The crash-at-every-storage-operation test also covers saving and installing snapshots; the WAL is cut at every byte offset after a snapshot record; chaos runs snapshot every 20 entries, so slow followers really are brought forward by InstallSnapshot (a 2,000-seed Porcupine run, 1.16 million operations, saw about 214,000 snapshots saved and 108,000 InstallSnapshot messages, all linearizable) |
| The linearizability checker can actually fail | The harness is deliberately broken (reads served from a node's local state; retries applied twice) and the checker must flag both |
| The tests catch real bugs | Mutation testing: bugs are injected into the real code and the suite must fail (table below) |

### Mutation testing

| Injected bug | Caught by |
|---|---|
| Commit an entry without a majority | Chaos test, within a few seeds |
| Grant votes without checking the candidate's log is up to date | Chaos test, within a few seeds |
| Complete a waiting client with a different entry's result | Chaos test, within a few seeds |
| Dedup cache returns the wrong saved result | Chaos test, within a few seeds |
| Remove the Figure 8 rule (commit an old-term entry by counting replicas) | **Not** caught by 3,000 chaos seeds; caught by a targeted unit test |
| 12 mutations of the PreVote logic | 11 caught; the twelfth is equivalent (a leader's timer never advances, so another rule already covers it) |

The Figure 8 row is the lesson: random chaos finds bugs nobody thought of, and targeted tests pin the ones people know about. You need both.

### Bugs the testing found

- **A client could wait forever after its leader was deposed.** The server skipped completing waiters at no-op log entries, so a command whose log slot was taken over by a new leader's no-op was never answered. Found by a targeted test; fixed and pinned by `TestDeposedLeadersPendingCommandFails`.
- **A restarted node forced an extra election.** Seen in a live three-node demo. Without PreVote, a node that is cut off keeps raising its term and deposes a healthy leader when it returns. Reproduced first as a failing test (leader 2 in term 1 became leader 0 in term 21), then fixed with PreVote.

More detail on each is in [`docs/design.md`](docs/design.md).

## Architecture

```mermaid
flowchart LR
    client["client / CLI<br/>finds the leader, retries safely"]
    subgraph leader["node (leader)"]
        srv["server<br/>commit waiting, redirects"] --> raftc["Raft core"]
        srv --> kvs["KV store + dedup"]
        raftc --> wal[("WAL<br/>CRC + fsync")]
    end
    f1["node (follower)"]
    f2["node (follower)"]
    client -->|"gRPC KV API"| srv
    raftc <-->|"gRPC Raft messages"| f1
    raftc <-->|"gRPC Raft messages"| f2
    f1 <--> f2
```

The Raft core never reads a clock, opens a socket, or touches a file. It is driven by `Tick`, `Step` and `Propose`, and talks to the world through a `Transport` and a `Storage` interface. In production those are gRPC and the WAL. In tests they are a seeded virtual network and in-memory storage that crashes on command, so a whole cluster runs in one thread and **any failure replays exactly from its seed**:

```mermaid
flowchart TB
    core["Raft core<br/>pure state machine: Tick, Step, Propose"]
    core --- tr{{"Transport interface"}}
    core --- st{{"Storage interface"}}
    tr -->|production| grpc["gRPC transport"]
    tr -->|tests| sim["simnet: seeded virtual network<br/>drops, delays, partitions"]
    st -->|production| walp["WAL on disk"]
    st -->|tests| fault["in-memory storage<br/>with crash injection"]
    sim --> chaos["chaos world: clients + faults<br/>replayable from a seed"]
    fault --> chaos
    chaos --> hist["recorded client history"]
    hist --> porc["Porcupine: is it linearizable?"]
```

## Features

- Leader election with randomized timeouts and **PreVote**, so a node that was cut off cannot depose a healthy leader when it returns
- Log replication with the Raft paper's commit rules, including the Figure 8 fix, and a no-op entry at the start of every term
- Crash-safe write-ahead log: CRC32C checksums, fsync before every reply, torn-write recovery, directory sync
- Persist-before-reply: terms, votes and log entries reach disk before any message that depends on them leaves the node
- Exactly-once writes: every request carries `(ClientID, Seq)`, and the replicated state machine deduplicates retries
- Reads go through the log, so they are linearizable
- **Snapshots and log compaction:** after every N applied entries (`-snapshot-every`, default 1000, 0 turns it off) a node snapshots the state machine, including the dedup table, and drops the covered log prefix. A follower that has fallen behind the compacted prefix is brought forward with InstallSnapshot
- gRPC API with a leader-finding client that follows redirects, a CLI, and a Docker Compose setup

## Run it

Requires Go 1.26 or newer.

```
go build ./...
go vet ./...
go test -race ./...
```

`-race` needs a C compiler on Windows (for example MinGW-w64 via MSYS2). CI runs it on Linux on every push.

### Start a three-node cluster

On Windows PowerShell, one command opens three node windows:

```
.\scripts\demo.ps1
```

Or start the nodes yourself, one per terminal:

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

Nodes snapshot every 1,000 applied entries by default; add `-snapshot-every 100` to see compaction sooner.

Stop the leader with Ctrl+C: the other two elect a new one and reads and writes continue. Restart it and it recovers its term and log from its write-ahead log and catches up. Or run `docker compose up --build` for the same three nodes in containers.

Connections are unauthenticated and unencrypted (no TLS), which is fine for a local demo and not for a network you do not trust.

### Check linearizability yourself

```
go test ./test/linearizability -v
```

That runs 200 seeded chaos runs. For a longer soak (PowerShell):

```
$env:RAFTKV_CHAOS_SEEDS=2000; go test ./test/linearizability -run TestLinearizableUnderChaos -timeout 1h
```

If a seed ever fails, the test prints the exact command that replays it and writes an HTML visualization of the failing history.

### Benchmarks

```
go run ./bench
```

This builds the server, starts real `raftkv-server` processes on your machine (1, 3 and 5 nodes, real gRPC, real fsynced WAL), runs concurrent clients against them, and prints Markdown tables: throughput and p50/p95/p99/max latency per cluster size and client count, and how long writes stay unavailable after the leader process is killed. Useful flags: `-sizes`, `-clients`, `-duration`, `-read-pct`, `-value-size`, `-trials`, `-dir` (put the data on the drive you want to measure), and `-no-sync` (fsync off, unsafe, only to see what fsync costs). Everything runs on one machine, so the numbers measure the software and the local disk, not a network; see [`docs/design.md`](docs/design.md) for what each number does and does not mean.

**Results** (one run each, 2026-11-02: Windows 11, 20 logical CPUs, data on the system drive, fsync on, 128-byte values, 1,000 keys, snapshots every 1,000 entries, 2 s warm-up then 10 s measured, all nodes and clients on the same machine):

| Nodes | Clients | ops/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|---|---|---|---|---|---|---|
| 1 | 1 | 1106 | 0.85 | 1.19 | 1.45 | 44.84 |
| 1 | 16 | 1126 | 13.58 | 14.66 | 48.54 | 54.00 |
| 1 | 64 | 1084 | 54.53 | 84.72 | 94.30 | 421.85 |
| 3 | 1 | 373 | 2.62 | 3.66 | 4.17 | 92.01 |
| 3 | 16 | 241 | 63.90 | 81.90 | 99.23 | 118.89 |
| 3 | 64 | 423 | 148.26 | 185.47 | 202.71 | 213.38 |
| 5 | 1 | 249 | 4.16 | 4.76 | 5.30 | 87.14 |
| 5 | 16 | 186 | 84.80 | 91.87 | 141.91 | 176.00 |
| 5 | 64 | 287 | 223.11 | 283.85 | 316.09 | 322.79 |

After the leader process is killed, writes succeed again after a median of 1.38 s (3 nodes, 5 kills, range 1.27 to 1.85 s) and 1.29 s (5 nodes, range 1.28 to 2.13 s). The election timeout was 1 to 2 s, so this is what it should be.

What the numbers say, and what they do not:

- **Write throughput does not grow with more clients.** One node does about 1,100 writes/s with 1, 16 or 64 clients; only latency rises, in proportion to the number of clients. A single client's 0.85 ms median is about one fsync. This is consistent with every write paying for its own fsync: there is no group commit, so concurrent writes queue instead of sharing a sync. Batching is the obvious next improvement.
- **Replication is much slower than one node.** Three nodes do 373 writes/s with one client and 5 nodes 249. With 16 clients the 3-node and 5-node runs got slower than with one client, which I have not explained (the run was not repeated, so run-to-run noise is unmeasured, and I have not profiled it).
- These are single runs on one machine, with the nodes sharing one drive and one CPU, so they are good for orders of magnitude and for comparing before and after a change, not as absolute figures.

### Regenerating the protobuf code

Only needed after editing `api/raftkv.proto`. Install `protoc` (`winget install protobuf`) and the two plugins (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` and `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`), then run `scripts/gen-proto.ps1` (or `.sh`). The generated code in `api/raftkvpb` is committed.

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
internal/storage/        write-ahead log
internal/kv/             key-value state machine with request deduplication
internal/chaos/          simulated cluster and clients with fault injection
internal/benchstat/      percentile and summary helpers for the benchmark
bench/                   benchmark tool (real processes, real gRPC)
test/linearizability/    Porcupine model and seeded chaos tests
scripts/                 protobuf generation, local demo cluster
docs/design.md           design decisions, tradeoffs, bugs found
```

## Limitations

Stated plainly, because they are the first things a reviewer should ask about:

- **Snapshots are sent whole.** There is no chunking, so a snapshot must fit in one gRPC message (80 MiB limit; the WAL refuses larger ones with `ErrTooLarge`). Taking a snapshot copies the whole state under the node lock, so a very large state stalls the node while it happens.
- Compaction keeps no trailing entries, so a follower that is even slightly behind the snapshot gets the whole snapshot instead of a few entries.
- The WAL compacts by rewriting the file and renaming it; on Windows the rename's durability is not verified.
- **No write batching.** Every write is fsynced separately, so write throughput is bounded by fsync latency and does not rise with more clients (see Benchmarks).
- **Reads go through the log.** Simple and linearizable, but slower than a leased or ReadIndex read.
- **Static membership.** Nodes cannot be added or removed at runtime.
- **No TLS or authentication.**
- The dedup table keeps one saved result per client (a client has one request in flight), and it is not yet expired, so it still grows with the number of distinct clients (and is carried in every snapshot).

## Roadmap

- [x] Leader election, log replication, Figure 8 commit rule
- [x] Durable storage: checksummed WAL, torn-write recovery, crash injection at every storage operation
- [x] gRPC API, CLI, Docker Compose cluster
- [x] Linearizability checking with Porcupine, with the checker validated by breaking the system on purpose
- [x] PreVote
- [x] Snapshots and log compaction
- [x] Benchmarks: throughput, p50/p99 latency, time to elect after a leader dies
- [ ] Group commit: share one fsync and one replication message between concurrent writes (benchmarks show throughput does not grow with clients)
- [ ] CheckQuorum: a leader that loses contact with a majority steps down
- [ ] Chaos replay visualizer

## References

- Ongaro and Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- Ongaro, [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation) (PreVote, section 9.6)
- [MIT 6.5840 Raft labs](https://pdos.csail.mit.edu/6.824/)
- [Porcupine](https://github.com/anishathalye/porcupine), the linearizability checker

## License

MIT. See [LICENSE](LICENSE).
