# RaftKV: project status and handoff

This file is the full context for continuing the project in a new session. It is written for an AI pair-programmer (Claude) picking the work up cold, and doubles as an engineering log. Read it end to end before changing anything.

Last updated: end of Phase 4 setup (2026-10-31).

## 1. What this is and why

RaftKV is a linearizable key-value store built on a from-scratch Raft implementation in Go, with a deterministic chaos-testing harness. It is a GitHub-first portfolio project for Summer 2027 SWE internship applications. The owner (Ritvik, ECE sophomore, Python/C background, prior SWE internships) may later swap it onto his resume, so the standard is: correct, well tested, honestly documented, explainable in an interview.

The project's selling point is not "implements Raft" but "demonstrably correct under failure": the Raft core is independent of the network and disk, so whole clusters run single-threaded on a seeded simulated network, any failure replays from its seed, and (Phase 5) recorded client histories are checked for linearizability with Porcupine.

## 2. How we work together (important conventions)

- **Ritvik commits and pushes; Claude writes code.** Claude never has git access to his machine. He runs commands in Windows 11 PowerShell. He prefers **one block of commands to paste, then one result to report**, not a drip of single steps. Explain what a failure means, not just what to run.
- **Repo:** `C:\SWEProjects\RaftKV` on his machine, GitHub `ritvik-devireddy/RaftKV` (display name has capitals; Go module path stays lowercase: `github.com/ritvik-devireddy/raftkv`). CI is GitHub Actions: gofmt, vet, build, `go test -race -count=1 ./...` on Linux.
- **Claude's workspace** is a cloud Linux container with Go 1.24.x. Source of truth while working: `/mnt/user-data/outputs/RaftKV`. Files are delivered to his machine with `device_commit_files` (target `C:\SWEProjects\RaftKV\...`), then **verified by comparing `sha256sum` of the container copy and the device copy through `device_bash`** (an early commit once reported success without landing).
- **The container and the device shell cannot download Go modules** (proxy allowlist). So Claude cannot compile code that imports gRPC, protobuf, or Porcupine. For such code: type-check against hand-written stub modules (`replace` directives in a temp copy), keep the logic testable in packages with no third-party imports, and have Ritvik run `go build`/`go test` and paste errors. The gRPC layer built cleanly against the real libraries on his machine on the first try.
- **Quality bar before delivering code:** `gofmt -l .` empty, `go vet ./...`, `go test -race -count=1 ./...`, and **mutation testing**: deliberately inject bugs (delete a persist call, drop a term check, skip a checksum) in a temp copy and confirm a test fails. Several real gaps were found this way. Treat a surviving mutant as a missing test unless it is semantically equivalent.
- **Edits:** large patches via small Python scripts with `assert s.count(old)==1` guards; Go files re-gofmt'd.
- **Windows quirks:** `go test -race` needs a C compiler (his machine has it). Tests that create thousands of files are slow on Windows (antivirus); avoid that pattern.
- Do not mention internal tool names to him; say "your folder", "your computer". He is fine with short, plain explanations; he asked for interview-explainable code.

## 3. Roadmap and state

| Phase | Scope | State |
|---|---|---|
| 0 | Repo layout, CI, simnet, KV state machine with dedup | done, pushed |
| 1 | Leader election | done, pushed |
| 2 | Log replication, commit, Figure 8 rule, no-op on election | done, pushed (55c1ae8) |
| 3 | Durability: WAL, crash recovery, crash injection | done, pushed (7843ff7), CI green |
| 4 | gRPC API, server binary, CLI, Docker Compose | **code written, builds and vets on his machine with real gRPC; tests and live demo not yet confirmed; not committed** |
| 5 | Porcupine linearizability, thousands of seeded chaos runs | not started |
| 6 | Snapshots and log compaction, benchmarks, README diagram, design-doc polish | not started |

### Phase 4 verification checklist (pick up here if not done)
1. `go test -race -count=1 ./...` all `ok` (new: `internal/server`, `internal/rpc` convert test, `internal/kv` codec).
2. `git status`; `go mod tidy` raised go.mod to `go 1.26.0` (grpc v1.85 requires it); the Dockerfile base image was updated to `golang:1.26` to match, and CI follows go.mod automatically.
3. Live demo: three terminals running `go run ./cmd/raftkv-server -id N -listen :700(N+1) -data data/nodeN -peers localhost:7001,localhost:7002,localhost:7003`, then `go run ./cmd/raftkv-cli put hello world` / `get hello`; kill the leader, confirm a new leader and the data survive, restart the node, confirm catch-up.
4. Optional: `docker compose up --build` (needs Docker Desktop; not confirmed installed).
5. Update README (status text, roadmap `[x]` for Phase 4, "Getting started" with the demo and proto regeneration commands) and `docs/design.md` (Server layer, proto decision), then commit generated code in `api/raftkvpb/` (it must be committed or CI cannot build) together with `go.mod` and `go.sum`.

## 4. Architecture

```
clients ── gRPC KV service ──► internal/rpc.KVService
                                    │
                              internal/server.Server  (Submit/Execute, waiters, redirects)
                               │               │
                     internal/kv.Store    internal/raft.Node ──► raft.Storage ──► internal/storage.WAL
                     (state machine)          │
                                       raft.Transport ──► internal/rpc.Transport (gRPC) / simnet (tests)
```

### Package map
- `internal/raft` (stdlib only): the consensus core. `Node` is a pure message-passing state machine: `Tick()` (logical time), `Step(Message)`, `Propose(cmd) (Index, Term, ok)`, `Status()`. No goroutines, no real clock, no network or disk access except through the `Transport` and `Storage` interfaces. `Apply` and `Transport.Send` run under the node lock and must not call back into the node.
- `internal/raft/storage.go`: `Storage` interface is incremental (`SaveHardState`, `Append`, `TruncateFrom`, `Load`); `MemoryStorage` for tests also has `Save(PersistentState)` as a seeding helper that is not part of the interface.
- `internal/storage/wal.go`: `WAL` implements `raft.Storage`. Record = `crc32c(4,LE) | bodyLen(4,LE) | type+payload`, CRC covers length+body; types hard-state, entry, truncate. fsync before return. A failed write/sync poisons the WAL. Recovery replays the file, cuts a torn tail, and returns `ErrCorrupt` for damage elsewhere (precise rules in `replay`'s comment and `docs/design.md`).
- `internal/simnet`: deterministic network (virtual clock, event heap, seeded drop/delay, `Partition`/`Heal`, `SetDropRate`).
- `internal/kv`: `Store` (Put/Get/Delete/CAS, dedup by `(ClientID, Seq)`, **caches only the last result per client**, so a client must have one operation in flight), `Command` binary codec (`Encode`/`DecodeCommand`, never empty because empty means no-op in Raft), `Snapshot()` copy for tests.
- `internal/server`: `Server` joins Node + Store. `Submit(cmd)` returns a `*Pending` or `*NotLeaderError{Leader}`; `Execute(ctx, cmd)` waits. Waiters are keyed by log index and carry the term of the proposal; `apply` completes a waiter with the result only if the applied entry has the same term, otherwise `ErrLostLeadership` (also for no-op entries, which replace a deposed leader's proposals: this was a real bug caught by `TestDeposedLeadersPendingCommandFails`). A `recent` map covers results that apply before `Submit` registers (single-node clusters commit inside `Propose`). Reads go through the log, so they are linearizable.
- `internal/rpc`: gRPC adapters only. `ToProto`/`FromProto`, `Transport` (per-peer bounded queue + sender goroutine, non-blocking, drops when full), `RaftService.Step`, `KVService` (maps `NotLeaderError` to `RedirectInfo` with a client-facing address hint; lost leadership to `Unavailable`; timeout to `DeadlineExceeded`).
- `internal/client`: leader-finding client; one `(ClientID, Seq)` per logical operation reused across retries; follows redirects; backs off.
- `cmd/raftkv-server`, `cmd/raftkv-cli`, `Dockerfile`, `docker-compose.yml`, `scripts/gen-proto.{ps1,sh}`.
- `api/raftkv.proto` and generated `api/raftkvpb/` (regenerate: `protoc --go_out=. --go_opt=module=github.com/ritvik-devireddy/raftkv --go-grpc_out=. --go-grpc_opt=module=github.com/ritvik-devireddy/raftkv api/raftkv.proto`; plugins `protoc-gen-go` and `protoc-gen-go-grpc` via `go install ...@latest`; protoc installed with `winget install protobuf`).
- `test/linearizability/`, `bench/`: stubs for Phases 5 and 6.

### Key design decisions (be able to explain each)
- Raft core is a deterministic state machine over logical ticks and messages, so a whole cluster is replayable from a seed.
- Persist before reply; storage first, memory second. Storage failure panics the node.
- Followers never truncate a matching entry (a delayed AppendEntries must not shorten a log). Follower commit index is capped at the last entry the message verified.
- Leader commits by counting replicas only for current-term entries (Figure 8) and appends a no-op on election.
- `AppendEntriesReply` has two extra fields (`MatchIndex`, `LastLogIndex`) so duplicate/reordered replies cannot corrupt `matchIndex` and a leader can back up in one step.
- Node-to-node wire protocol is one fire-and-forget `Raft.Step(RaftMessage)` RPC with a `oneof` payload, not request/response pairs, because the core treats a reply as an independent message.
- Exactly-once writes via `(ClientID, Seq)` dedup inside the replicated state machine.
- Crash tests inject crashes at every storage operation, with the operation reaching the disk or not.

## 5. Testing inventory (what exists and what it proves)
- `internal/raft/cluster_test.go`, `replication_test.go`: whole clusters on simnet; Election Safety, Log Matching, State Machine Safety, no phantom commands, checked after every event. Failure scenarios: loss, partitions, stale leader, follower catch-up.
- `internal/raft/crash_test.go`: `crashStorage` (panics at the Nth storage op, before or after it reaches the "disk"); cluster harness restarts nodes from storage only; `TestCrashAtEveryStorageOperation`, `TestSafetyUnderRandomCrashes`, `TestSafetyUnderCrashesAndPartitions`; `checkDurable` asserts memory equals disk.
- `internal/raft/durability_order_test.go`: no reply may outrun the disk.
- `internal/storage/wal_test.go`: model-based vs `MemoryStorage`; recovery from a cut at every byte offset (in memory via `replay`, plus real-file checks at a sample and at record boundaries); bit flips; torn batches; poison on I/O failure; end-to-end node restart on a real WAL.
- `internal/server/server_test.go`: put/get through the leader, redirects, retry dedup, deposed leader's pending command fails, acknowledged commands survive chaos (loss + partitions), store rebuilt from WAL after restart.
- `internal/kv`: store and codec tests (fuzz-ish decode of garbage).
- `internal/rpc/convert_test.go`: message round trip through real protobuf (needs real libraries; first run on his machine is pending).
- Test time: raft package ~35-45 s with `-race` on his machine; storage ~3 s after the Windows file-creation fix.

## 6. Known limitations and honest tradeoffs (documented or to document)
- A middle-record WAL length-field corruption that looks like a record running past EOF is indistinguishable from a torn tail and is treated as one (documented in `replay` and `docs/design.md`).
- KV dedup keeps only the last sequence number per client; sessions grow without bound (snapshot/expiry in Phase 6).
- Reads go through the log (simple and linearizable, but slow). ReadIndex / leases are a possible later optimization.
- Raft membership is static; no snapshots or `InstallSnapshot` yet, so the log grows without bound and a node restart replays the whole log.
- gRPC uses insecure credentials (no TLS/auth); fine for a demo, say so in the README.
- Server restarts rebuild the store by re-applying the log once the commit index is relearned (a restarted single node must win an election first).

## 7. Phase 5 plan (next major piece): prove linearizability

Goal: thousands of seeded chaos runs, each producing a client-visible history that Porcupine checks.
- Dependency: `go get github.com/anishathalye/porcupine` (Ritvik must run this; Claude cannot download modules). Build the model and history recorder in a package that imports Porcupine only in the test/linearizability code, so the rest stays buildable and testable in Claude's container; type-check against a stub if needed.
- Harness: extend the existing cluster pattern to `server.Server` nodes (with `crashStorage`, partitions, drops, delays, restarts) on simnet. Clients are step-driven state machines, not goroutines, so runs stay deterministic: each client has at most one op in flight, uses `Submit` with one `(ClientID, Seq)` per logical op, retries (same pair) on `ErrLostLeadership`, timeouts, and redirects, and picks a new node.
- History recording in virtual time: invoke time at first send, return time at the response. An op that never gets a definitive answer must be recorded as indeterminate (return time = infinity), because it may or may not have taken effect. Never record a failed attempt as "did not happen".
- Model: per-key partitioned register with Put/Get/Delete/CAS, matching `kv.Store` semantics (`Result.OK` meanings in `internal/kv/store.go`). Use Porcupine's `Partition` by key for speed.
- Output: on a violation, print the seed and the exact replay command, and write Porcupine's HTML visualization to `test/linearizability/*.html` (already gitignored). Make the seed count configurable (env var or flag), small by default for CI, large for local soak runs.
- Mutation-test the whole pipeline: break the Raft code on purpose (e.g. serve reads from a stale leader without going through the log, commit without a majority, skip the dedup) and confirm Porcupine flags a violation. A checker that cannot fail proves nothing.
- Add real bugs found to the table in `docs/design.md` with seed and regression test.

## 8. Phase 6 plan
Snapshots and log compaction (state machine snapshot incl. dedup sessions; `InstallSnapshot` message; WAL compaction/rotation), benchmarks (throughput, p50/p99 latency for 1/3/5 nodes, time to elect after killing the leader; state hardware and what bounded each number), README architecture diagram, design-doc polish, optional TLS.

## 9. Resume talking points (accurate claims only)
Implemented Raft (election, replication, commit rules incl. Figure 8) from scratch in Go; deterministic simulation testing with seeded failure injection; crash-safe checksummed WAL with torn-write recovery validated by cutting the file at every byte offset and crashing nodes at every storage operation; exactly-once client semantics via replicated dedup; gRPC API, CLI, Docker Compose cluster; (after Phase 5) linearizability verified with Porcupine across N seeded chaos runs. Do not claim Phase 5/6 items until they exist.

## 10. Starting prompt for a new session

> Read `docs/STATUS.md` in the RaftKV repo (C:\SWEProjects\RaftKV) and continue: first finish the Phase 4 verification checklist, then start Phase 5.
