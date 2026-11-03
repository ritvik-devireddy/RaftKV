package raft

import (
	"fmt"
	"sync"
)

// PersistentState is what Raft must write to stable storage before replying
// to any RPC (Figure 2, "Persistent state on all servers").
//
// The log does not have to start at index 1. Entries up to Snapshot.Index have
// been compacted into the snapshot, so Log holds the entries from
// Snapshot.Index+1 on: Log[i] has index Snapshot.Index+1+i.
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Snapshot    Snapshot
	Log         []LogEntry
}

// Storage is the durable home of PersistentState. The operations are
// incremental because a write-ahead log can only afford to write what changed:
// rewriting the whole log on every append would make each write O(log size).
//
// Every method must be durable before it returns: once it returns nil, the
// change survives a crash. The raft core relies on this to persist before it
// replies to any RPC.
//
// The real implementation is a checksummed write-ahead log in
// internal/storage; tests use MemoryStorage.
type Storage interface {
	// SaveHardState records the current term and vote.
	SaveHardState(term Term, votedFor NodeID) error
	// Append adds entries to the end of the log. Their indexes must continue
	// the existing log without a gap.
	Append(entries []LogEntry) error
	// TruncateFrom deletes the entry at index and everything after it.
	TruncateFrom(index Index) error
	// SaveSnapshot durably records a snapshot and discards the log entries it
	// covers, as PersistentState.ApplySnapshot defines: entries after
	// snap.Index survive only if the stored log has an entry at snap.Index
	// with term snap.Term. It must be atomic: after a crash the storage holds
	// either the old snapshot and log, or the new ones, never a mixture. The
	// snapshot's index must be greater than the current snapshot's.
	SaveSnapshot(snap Snapshot) error
	// Load returns everything that has been saved.
	Load() (PersistentState, error)
}

// ApplySnapshot makes snap the state's snapshot and removes the log entries it
// covers (those with index <= snap.Index).
//
// The entries after snap.Index are kept only if the log has an entry at
// snap.Index whose term is snap.Term: then the snapshot is just a prefix of this
// log. If it does not (the log is shorter, or disagrees with the snapshot's last
// entry), nothing in it can be trusted and the whole log is discarded.
//
// Node, MemoryStorage and the write-ahead log all use this one function, so
// they cannot disagree about what a snapshot does to a log. snap.Index must be
// greater than s.Snapshot.Index.
func (s *PersistentState) ApplySnapshot(snap Snapshot) {
	var tail []LogEntry
	if snap.Index > s.Snapshot.Index {
		if pos := snap.Index - s.Snapshot.Index; pos <= Index(len(s.Log)) && s.Log[pos-1].Term == snap.Term {
			tail = append([]LogEntry(nil), s.Log[pos:]...)
		}
	}
	s.Snapshot = Snapshot{Index: snap.Index, Term: snap.Term, Data: append([]byte(nil), snap.Data...)}
	s.Log = tail
}

// MemoryStorage is an in-memory Storage for tests. It survives a simulated
// node crash because the test keeps the value and hands it to the restarted node.
type MemoryStorage struct {
	mu    sync.Mutex
	state PersistentState
}

// NewMemoryStorage returns empty storage: term 0, no vote, empty log.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{state: PersistentState{VotedFor: NoVote}}
}

// Save replaces the whole state. It is a test helper for seeding a node with
// a particular history and is deliberately not part of Storage.
func (m *MemoryStorage) Save(s PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = clone(s)
	return nil
}

func (m *MemoryStorage) SaveHardState(term Term, votedFor NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.CurrentTerm = term
	m.state.VotedFor = votedFor
	return nil
}

func (m *MemoryStorage) Append(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.state.Snapshot.Index + Index(len(m.state.Log)) + 1
	for i, e := range entries {
		if e.Index != next+Index(i) {
			return fmt.Errorf("append: entry index %d, want %d", e.Index, next+Index(i))
		}
	}
	m.state.Log = append(m.state.Log, entries...)
	return nil
}

func (m *MemoryStorage) TruncateFrom(index Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	base := m.state.Snapshot.Index
	if index <= base || index > base+Index(len(m.state.Log)) {
		return fmt.Errorf("truncate: index %d outside the log (%d, %d]", index, base, base+Index(len(m.state.Log)))
	}
	m.state.Log = m.state.Log[:index-base-1]
	return nil
}

func (m *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.Index == 0 || snap.Index <= m.state.Snapshot.Index {
		return fmt.Errorf("snapshot: index %d does not advance past the current snapshot at %d", snap.Index, m.state.Snapshot.Index)
	}
	m.state.ApplySnapshot(snap)
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.state), nil
}

func clone(s PersistentState) PersistentState {
	s.Log = append([]LogEntry(nil), s.Log...)
	s.Snapshot.Data = append([]byte(nil), s.Snapshot.Data...)
	return s
}
