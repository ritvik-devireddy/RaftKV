# RaftKV: working notes for Claude

A linearizable key-value store on a from-scratch Raft implementation in Go, with a deterministic chaos-testing harness. It is Ritvik's portfolio project for Summer 2027 SWE internship applications, so the standard is: correct, well tested, honestly documented, and explainable by him in an interview.

**Read `docs/HANDOFF.md` first.** It has the architecture, every package's API, test inventory, history of bugs, decisions not to reverse, and the roadmap. `docs/design.md` has the design reasoning. Keep both current.

## Commands

```
gofmt -l .                      # must print nothing
go vet ./...
go build ./...
go test -race -count=1 ./...    # the full bar; raft package takes about 45 to 90 s
go test ./test/linearizability -v          # 200 seeded chaos runs
RAFTKV_CHAOS_SEEDS=2000 go test ./test/linearizability -run TestLinearizableUnderChaos -timeout 1h
RAFTKV_CHAOS_FIRST_SEED=<n> RAFTKV_CHAOS_SEEDS=1 go test ./test/linearizability -run TestLinearizableUnderChaos -v   # replay one seed
scripts/gen-proto.sh            # only after editing api/raftkv.proto; needs protoc, protoc-gen-go, protoc-gen-go-grpc
```

CI (`.github/workflows/ci.yml`) runs gofmt, vet, build and `go test -race` on every push. `soak.yml` runs a long chaos soak nightly.

## Non-negotiable invariants

- **Persist before reply.** Storage first, memory second. A storage failure panics the node.
- The Raft core (`internal/raft`) is a deterministic state machine: no clocks, no goroutines, no global randomness, no direct network or disk access. Time is `Tick`, input is `Step`/`Propose`, output goes through `Transport` and `Storage`. Do not break this; it is what makes seeded replay work.
- A real command is never empty (an empty command means a no-op entry).
- A leader commits by counting replicas only for entries of its own term (Figure 8) and appends a no-op on election.
- Followers never truncate a matching entry.
- `Apply` and `Transport.Send` run under the node lock and must not call back into the node.
- One request in flight per client; dedup keeps the last result per `(ClientID, Seq)`.
- Any new `Storage` method must be implemented on `MemoryStorage`, the WAL, `crashStorage` (raft tests) and `faultStorage` (`internal/chaos`).
- Adding a Raft message type means updating `types.go`, `Node.Step`, `api/raftkv.proto`, `internal/rpc/convert.go` and `convert_test.go`, then regenerating `api/raftkvpb`.
- Unknown-outcome client operations are recorded as "may have happened at any time after the call", never as "did not happen".

## How to work

- **Write the failing test first** for any bug or new safety feature, and confirm it fails on the old code.
- **Mutation-test your own change**: inject a deliberate bug in a scratch copy and confirm a test fails. A surviving mutant is a missing test unless it is semantically equivalent (say why).
- Before calling anything done: `gofmt -l .` empty, `go vet ./...`, `go test -race -count=1 ./...`, and for anything touching Raft, storage or the server, the chaos and linearizability tests.
- Small commits with descriptive messages (for example `Add PreVote so a rejoining node cannot depose a healthy leader`). Never backdate commits and never make filler commits.
- Work on a branch and open a pull request rather than pushing to `main` directly, unless asked.
- Do not hand-edit generated files in `api/raftkvpb`; regenerate them.
- Update `README.md`, `docs/design.md` and `docs/HANDOFF.md` with each feature, and only claim what actually exists and was verified. Say plainly what was not run or could not be verified.
- Keep it simple and in the existing style. No scope creep, no rewrites of working code.

## How to talk to Ritvik

Concise and plain. Explain design choices briefly, because he needs to explain them in interviews. Lead with what you did and what the result was; give exact commands only when he has to run something himself (he uses Windows 11 PowerShell).

## Roadmap (see `docs/HANDOFF.md` section 11 for detail)

1. Snapshots and log compaction (design agreed: index-offset refactor first with no behavior change, then storage and WAL support, then `Compact` and `InstallSnapshot`, then server wiring and chaos coverage).
2. Benchmarks (throughput, p50/p99 latency, time to elect after a leader dies; state hardware and caveats).
3. CheckQuorum, a chaos replay visualizer, README demo GIF.
