// Package storage holds the durable storage behind Raft's persistent state.
//
// WAL is an append-only write-ahead log of checksummed records that implements
// raft.Storage. Every operation is fsync'd before it returns, so the raft core
// can persist first and reply afterwards. Open recovers by replaying the file
// and cutting off a torn final record; see replay for exactly what is treated
// as torn and what as corruption.
//
// Planned for phase 6, if time allows: snapshot files and log compaction.
package storage
