package raft

import "sync"

// PersistentState is what Raft must write to stable storage before replying
// to any RPC (Figure 2, "Persistent state on all servers").
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Log         []LogEntry
}

// Storage persists and restores PersistentState. The real implementation is a
// checksummed write-ahead log in internal/storage; tests use MemoryStorage.
type Storage interface {
	Save(PersistentState) error
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

func (m *MemoryStorage) Save(s PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = clone(s)
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
