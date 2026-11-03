package chaos

import (
	"errors"

	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

// errCrash is what a faulty storage panics with to simulate a node dying in
// the middle of a storage operation.
var errCrash = errors.New("injected crash")

// faultStorage is in-memory storage that can be armed to "crash" at a chosen
// mutating operation, either before the operation reaches the disk or just
// after it does. The raft package has an equivalent in its own tests; this
// copy exists so other packages can use it.
type faultStorage struct {
	*raft.MemoryStorage
	ops     int
	crashAt int // crash when ops reaches this; 0 = disarmed
	applyOp bool

	snapshots int // snapshots saved, to show a run really compacted its log
}

func newFaultStorage() *faultStorage { return &faultStorage{MemoryStorage: raft.NewMemoryStorage()} }

func (s *faultStorage) armIn(n int, applyOp bool) { s.crashAt, s.applyOp = s.ops+n, applyOp }
func (s *faultStorage) disarm()                   { s.crashAt = 0 }

func (s *faultStorage) hit() bool {
	s.ops++
	if s.crashAt != 0 && s.ops == s.crashAt {
		s.crashAt = 0
		return true
	}
	return false
}

func (s *faultStorage) SaveHardState(term raft.Term, votedFor raft.NodeID) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.SaveHardState(term, votedFor)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.SaveHardState(term, votedFor)
}

func (s *faultStorage) Append(entries []raft.LogEntry) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.Append(entries)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.Append(entries)
}

func (s *faultStorage) TruncateFrom(index raft.Index) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.TruncateFrom(index)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.TruncateFrom(index)
}

func (s *faultStorage) SaveSnapshot(snap raft.Snapshot) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.SaveSnapshot(snap)
		}
		panic(errCrash)
	}
	err := s.MemoryStorage.SaveSnapshot(snap)
	if err == nil {
		s.snapshots++
	}
	return err
}
