package raft

import (
	"fmt"
	"sync"
)

// PersistentState is what Raft must write to stable storage before replying
// to any RPC (Figure 2, "Persistent state on all servers").
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
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
	// Load returns everything that has been saved.
	Load() (PersistentState, error)
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
	next := Index(len(m.state.Log)) + 1
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
	if index < 1 || int(index) > len(m.state.Log) {
		return fmt.Errorf("truncate: index %d outside log of length %d", index, len(m.state.Log))
	}
	m.state.Log = m.state.Log[:index-1]
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.state), nil
}

func clone(s PersistentState) PersistentState {
	s.Log = append([]LogEntry(nil), s.Log...)
	return s
}
