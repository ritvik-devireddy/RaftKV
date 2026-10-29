// Package storage will hold the durable storage behind Raft's persistent state.
//
// Planned for phase 3:
//   - an append-only write-ahead log of checksummed records (CRC32 over the
//     length and payload), fsync'd before any RPC reply that depends on it;
//   - crash recovery that replays the log and truncates a torn final record;
//   - a Storage implementation (see internal/raft) built on the log;
//   - snapshot files and log compaction (phase 6, if time allows).
package storage
