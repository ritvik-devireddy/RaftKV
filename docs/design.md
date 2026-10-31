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
- Log compaction and snapshots (phase 6).

## Durability

Code is in `internal/storage` (the WAL) and `internal/raft/storage.go` (the interface).

- **Persist before you reply.** Term, vote, and log entries reach disk before any RPC reply that depends on them leaves the node. The node writes to storage first and updates its memory second, so a crash in between loses nothing it had promised. `TestRepliesNeverOutrunTheDisk` crashes the storage at every operation of a message handler and checks that no reply is backed by state that is not on disk; reordering a reply before the vote is saved fails it.
- **Incremental storage interface.** `SaveHardState`, `Append`, `TruncateFrom`. Rewriting the whole log on every append would make each write O(log size). A storage failure panics the node: carrying on after failing to remember a vote could break safety.
- **Record format.** `crc32c | bodyLen | type + payload`, little endian. The checksum covers the length field and the body, so a damaged length cannot pass as a valid record. Record types are hard state, entry, and truncate; replaying them in order reproduces the state.
- **fsync before returning.** Each call syncs once, including a whole `Append` batch. `Options{NoSync}` exists for benchmarks only. The directory is synced when the file is created (skipped on Windows, which cannot open a directory for syncing).
- **A failed write or sync poisons the WAL.** After a failed fsync the kernel may have dropped dirty pages, so the file's contents are unknowable. The only safe move is to stop and recover from disk with a fresh `Open`.
- **Torn tail versus corruption.** A crash can leave the final record half-written, which recovery discards and cuts off. Damage anywhere else must not be skipped, because dropping committed history could break safety. The rule: a record that runs past the end of the file is torn; a checksum mismatch is torn only if nothing but zeros follows it (if later bytes are non-zero, a later write completed, so this record was damaged after being written); a zero length is torn only if the rest of the file is zeros. Everything else, including a valid record with impossible contents (an index gap, a term going backwards), is `ErrCorrupt`.
- **Known limitation.** Damage to a middle record's length field that makes it look like it runs past the end of the file is indistinguishable from a torn tail, so recovery treats it as one and discards the rest of the log. A real system would add a per-file header or footer checksum, or sync a commit marker, to tell them apart.

## Client semantics

- **Linearizable reads.** Reads go through the log, so they observe all earlier committed writes. Possible later optimization: read leases or ReadIndex, with the tradeoffs written down.
- **Exactly-once writes.** Each request carries `(client_id, seq)`. The state machine returns the cached result for a retry (see `internal/kv`).
- **Leader redirects.** A non-leader replies with a hint and the client retries.

## Testing strategy

- Unit tests per component.
- Deterministic simulation: whole cluster on `internal/simnet`, driven by a seed.
- Fault injection: partitions, crashes and restarts at arbitrary points, message drop, delay, duplication, reordering.
- Crash injection (`crash_test.go`): a node can crash at any storage operation, with the operation either reaching the disk or not, then restarts from nothing but its storage. `TestCrashAtEveryStorageOperation` crashes each node at each of its storage operations in turn; `TestSafetyUnderRandomCrashes` and `TestSafetyUnderCrashesAndPartitions` crash randomly. Every event checks Election Safety, Log Matching, State Machine Safety, no phantom commands, and that a running node's memory equals its disk.
- WAL tests (`internal/storage`): model-based against `MemoryStorage`, the file cut at every byte offset (and zero-filled after the cut), a bit flipped in every byte, plus an end-to-end node restart.
- Mutation testing: bugs deliberately injected (a vote, term, or entry not persisted, a reply sent before the vote is saved, a missing checksum check, a torn tail left in place, and others) must each fail a test.
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
