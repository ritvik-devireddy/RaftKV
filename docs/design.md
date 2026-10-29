# RaftKV design notes

This document records decisions, tradeoffs, and the bugs the chaos tester found. Fill it in as you build, not at the end.

## Goals and non-goals

**Goals**
- Linearizable `Get`, `Put`, `Delete`, `CompareAndSwap`.
- Committed writes survive crashes, restarts, and partitions.
- Every claim of correctness backed by a reproducible test.

**Non-goals**
- Sharding, multi-region, dynamic membership (unless time allows), a web UI.

## Architecture

TODO: diagram and a one-paragraph description of each layer.

## Raft implementation notes

TODO: one short section per mechanism, each pointing at the code and the Figure 2 rule it implements.

- Leader election and randomized timeouts
- Log replication and the commit rule (only commit entries from the current term by counting replicas)
- Persistence: what is saved and when (term, vote, and log before replying to any RPC)
- Log compaction and snapshots

## Client semantics

- **Linearizable reads.** Reads go through the log, so they observe all earlier committed writes. Possible later optimization: read leases or ReadIndex, with the tradeoffs written down.
- **Exactly-once writes.** Each request carries `(client_id, seq)`. The state machine returns the cached result for a retry (see `internal/kv`).
- **Leader redirects.** A non-leader replies with a hint and the client retries.

## Testing strategy

- Unit tests per component.
- Deterministic simulation: whole cluster on `internal/simnet`, driven by a seed.
- Fault injection: partitions, crashes and restarts at arbitrary points, message drop, delay, duplication, reordering.
- Linearizability checking of recorded operation histories with Porcupine.
- Race detector (`go test -race`) in CI.

## Bugs found by the chaos tester

Add a row for every bug, with the seed that reproduces it and the regression test that now covers it.

| # | Symptom | Root cause | Seed | Regression test |
|---|---------|-----------|------|-----------------|
| | | | | |

## Benchmarks

TODO: throughput and p50/p99 latency for 1, 3, and 5 nodes; time to elect a new leader after the old one is killed. State the hardware, the workload, and what bounded each number.

## Open questions

- Is a single `Apply` callback enough, or should committed entries be delivered on a channel?
- When to take snapshots: by log size, by entry count, or on a timer?
