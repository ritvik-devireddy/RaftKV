package raft

import "testing"

type nopTransport struct{}

func (nopTransport) Send(Message) {}

func newTestNode(t *testing.T, storage Storage) *Node {
	t.Helper()
	n, err := NewNode(
		Config{ID: 0, Peers: []NodeID{0, 1, 2}},
		storage,
		nopTransport{},
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	st := newTestNode(t, NewMemoryStorage()).Status()
	if st.Role != Follower {
		t.Errorf("role = %v, want follower", st.Role)
	}
	if st.Term != 0 {
		t.Errorf("term = %d, want 0", st.Term)
	}
}

func TestFollowerRejectsPropose(t *testing.T) {
	n := newTestNode(t, NewMemoryStorage())
	if _, _, isLeader := n.Propose([]byte("x")); isLeader {
		t.Error("a follower must not accept proposals")
	}
}

func TestMemoryStorageRoundTrip(t *testing.T) {
	s := NewMemoryStorage()

	empty, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if empty.CurrentTerm != 0 || empty.VotedFor != NoVote || len(empty.Log) != 0 {
		t.Fatalf("unexpected initial state: %+v", empty)
	}

	want := PersistentState{
		CurrentTerm: 3,
		VotedFor:    2,
		Log:         []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}},
	}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}

	// Mutating the caller's slice after Save must not change what was stored.
	want.Log[0].Term = 99

	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != 3 || got.VotedFor != 2 || len(got.Log) != 1 || got.Log[0].Term != 1 {
		t.Errorf("round trip mismatch: %+v", got)
	}
}
