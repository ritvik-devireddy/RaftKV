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

Code is in `internal/raft`. Done so far, with the decisions worth being able to explain:

**Leader election** (`node.go`: `startElection`, `handleRequestVote`)
- Time is logical: the driver calls `Tick`, and timeouts are counted in ticks. That is what makes a whole cluster replayable from a seed.
- The vote and the term are persisted before the reply leaves the node, so a restart cannot cause a double vote in one term. `TestVoteSurvivesRestart` pins this.

**Log replication** (`node.go`: `sendAppendEntries`, `handleAppendEntries`, `advanceCommit`)
- *Followers never truncate a matching entry.* Only a conflicting entry (same index, different term) triggers deleting the suffix. A delayed or duplicated AppendEntries carrying old entries must not shorten a log that has moved on.
- *The follower's commit index is capped at the last entry the message verified* (`min(leaderCommit, prevLogIndex + len(entries))`). Entries after that point may still differ from the leader's log.
- *A leader commits by counting replicas only for entries from its own term* (Figure 8). Earlier-term entries commit indirectly, when a current-term entry above them does. `TestLeaderCommitsOnlyCurrentTermEntriesByCounting` reproduces the case.
- *A new leader appends a no-op entry.* Without one, a leader with no client traffic could not commit entries inherited from earlier terms. State machines ignore no-ops (`LogEntry.IsNoOp`).
- *Two fields beyond Figure 2 in `AppendEntriesReply`.* `MatchIndex` lets the leader record exactly what a follower holds, so duplicated or reordered replies cannot corrupt `matchIndex` (it only moves forward). `LastLogIndex` lets the leader skip back past a short follower log in one step instead of one index at a time.
- Catch-up is bounded to 64 entries per message and pipelined: each success reply triggers the next batch immediately.

**Still to do**
- Persistence: durable WAL behind `Storage` (phase 3). The log is already persisted through the interface before any acknowledgement.
- Log compaction and snapshots (phase 6).

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
