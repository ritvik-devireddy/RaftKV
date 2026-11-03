package raft

import (
	"reflect"
	"strings"
	"testing"
)

// These tests cover the index arithmetic once the front of the log has been
// compacted into a snapshot: the entry with index i is log[i-snap.Index-1], and
// the snapshot's own last entry is known only by its index and term.

// stateWithSnapshot is a node state with a snapshot at index 10 (term 2) and
// three entries after it.
func stateWithSnapshot() PersistentState {
	return PersistentState{
		CurrentTerm: 3, VotedFor: NoVote,
		Snapshot: Snapshot{Index: 10, Term: 2, Data: []byte("state@10")},
		Log:      []LogEntry{entry(2, 11, "a"), entry(3, 12, "b"), entry(3, 13, "c")},
	}
}

func TestIndexLookupsAroundTheSnapshotBoundary(t *testing.T) {
	n, _, _, _ := newFollower(t, stateWithSnapshot())

	termAt := map[Index]Term{
		0:  0, // no entry
		9:  0, // compacted away: unknown
		10: 2, // the snapshot's last entry: its term is kept
		11: 2,
		12: 3,
		13: 3,
		14: 0, // past the end
	}
	for idx, want := range termAt {
		if got := n.termAt(idx); got != want {
			t.Errorf("termAt(%d) = %d, want %d", idx, got, want)
		}
	}
	if last, term := n.lastLog(); last != 13 || term != 3 {
		t.Errorf("lastLog = (%d, %d), want (13, 3)", last, term)
	}
	if e := n.entryAt(11); !reflect.DeepEqual(e, entry(2, 11, "a")) {
		t.Errorf("entryAt(11) = %+v", e)
	}
	if e := n.entryAt(13); !reflect.DeepEqual(e, entry(3, 13, "c")) {
		t.Errorf("entryAt(13) = %+v", e)
	}
}

func TestLastLogIsTheSnapshotWhenNothingFollowsIt(t *testing.T) {
	ps := stateWithSnapshot()
	ps.Log = nil
	n, _, _, _ := newFollower(t, ps)
	if last, term := n.lastLog(); last != 10 || term != 2 {
		t.Fatalf("lastLog = (%d, %d), want the snapshot's (10, 2)", last, term)
	}
	// The election restriction compares against that last entry.
	if !n.logUpToDate(10, 2) || n.logUpToDate(9, 2) || n.logUpToDate(100, 1) {
		t.Fatal("logUpToDate does not treat the snapshot's last entry as the log's last entry")
	}
}

func TestNodeStartsFromItsSnapshot(t *testing.T) {
	n, _, _, applied := newFollower(t, stateWithSnapshot())
	st := n.Status()
	if st.CommitIndex != 10 || st.LastApplied != 10 || st.LastLogIndex != 13 {
		t.Fatalf("status = %+v, want commit 10, applied 10, last 13", st)
	}
	if len(*applied) != 0 {
		t.Fatalf("applied %d entries at start, want none (the snapshot already covers 1..10)", len(*applied))
	}
}

func TestNewNodeRejectsALogThatDoesNotContinueFromTheSnapshot(t *testing.T) {
	for name, log := range map[string][]LogEntry{
		"starts at 1":  {entry(2, 1, "a")},
		"starts at 10": {entry(2, 10, "a")},
		"starts at 12": {entry(2, 12, "a")},
		"has a gap":    {entry(2, 11, "a"), entry(2, 13, "b")},
	} {
		t.Run(name, func(t *testing.T) {
			st := NewMemoryStorage()
			ps := stateWithSnapshot()
			ps.Log = log
			if err := st.Save(ps); err != nil {
				t.Fatal(err)
			}
			_, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}}, st, nopTransport{})
			if err == nil || !strings.Contains(err.Error(), "not contiguous") {
				t.Fatalf("err = %v, want a contiguity error", err)
			}
		})
	}
}

func TestFollowerWithSnapshotChecksPrevEntryAtTheBoundary(t *testing.T) {
	// prev == the snapshot's last entry: its term is known, so the check works.
	t.Run("match", func(t *testing.T) {
		n, st, tr, _ := newFollower(t, func() PersistentState { ps := stateWithSnapshot(); ps.Log = nil; return ps }())
		r := appendEntries(t, n, tr, AppendEntriesArgs{
			Term: 3, LeaderID: 1, PrevLogIndex: 10, PrevLogTerm: 2,
			Entries: []LogEntry{entry(3, 11, "x")}, LeaderCommit: 11,
		})
		if !r.Success || r.MatchIndex != 11 {
			t.Fatalf("reply = %+v, want success with MatchIndex 11", r)
		}
		if saved, _ := st.Load(); !reflect.DeepEqual(saved.Log, []LogEntry{entry(3, 11, "x")}) || saved.Snapshot.Index != 10 {
			t.Fatalf("persisted = %+v", saved)
		}
		if got := n.Status(); got.CommitIndex != 11 || got.LastApplied != 11 {
			t.Fatalf("status = %+v, want commit and applied 11", got)
		}
	})
	t.Run("wrong term", func(t *testing.T) {
		n, _, tr, _ := newFollower(t, stateWithSnapshot())
		r := appendEntries(t, n, tr, AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 10, PrevLogTerm: 1})
		if r.Success || r.LastLogIndex != 13 {
			t.Fatalf("reply = %+v, want failure reporting last index 13", r)
		}
	})
}

func TestFollowerWithSnapshotTruncatesAndAppendsAtTheRightPlace(t *testing.T) {
	n, st, tr, _ := newFollower(t, stateWithSnapshot())
	// A new leader's entry at index 12 conflicts with ours (term 3 vs 4).
	r := appendEntries(t, n, tr, AppendEntriesArgs{
		Term: 4, LeaderID: 1, PrevLogIndex: 11, PrevLogTerm: 2,
		Entries: []LogEntry{entry(4, 12, "y")},
	})
	if !r.Success || r.MatchIndex != 12 {
		t.Fatalf("reply = %+v", r)
	}
	want := []LogEntry{entry(2, 11, "a"), entry(4, 12, "y")}
	if got := n.logCopy(); !reflect.DeepEqual(got, want) {
		t.Fatalf("log = %+v, want %+v", got, want)
	}
	if saved, _ := st.Load(); !reflect.DeepEqual(saved.Log, want) {
		t.Fatalf("persisted log = %+v, want %+v", saved.Log, want)
	}
}

func TestLeaderWithSnapshotSendsEntriesFromTheRightPlace(t *testing.T) {
	tr := &recTransport{}
	n, _ := newNodeWith(t, stateWithSnapshot(), tr)
	// Win the election: node 1 grants the pre-vote and then the vote.
	for i := 0; i < 200 && len(tr.sent) == 0; i++ {
		n.Tick()
	}
	if len(tr.sent) == 0 {
		t.Fatal("the node never started an election")
	}
	n.Step(Message{From: 1, To: 0, Payload: PreVoteReply{Term: 3, PreVoteTerm: 4, VoteGranted: true}})
	if st := n.Status(); st.Role != Candidate || st.Term != 4 {
		t.Fatalf("status = %+v, want candidate in term 4", st)
	}
	n.Step(Message{From: 1, To: 0, Payload: RequestVoteReply{Term: 4, VoteGranted: true}})
	if n.Status().Role != Leader {
		t.Fatalf("role = %v, want leader", n.Status().Role)
	}

	// The leader's log is 11, 12, 13 and its new no-op at 14. A follower that
	// has everything up to 13 gets just the no-op, with prev = 13.
	tr.sent = nil
	n.mu.Lock()
	n.nextIndex[1] = 14
	n.sendAppendEntries(1)
	n.mu.Unlock()
	a := tr.sent[0].Payload.(AppendEntriesArgs)
	if a.PrevLogIndex != 13 || a.PrevLogTerm != 3 || len(a.Entries) != 1 || a.Entries[0].Index != 14 {
		t.Fatalf("append = %+v, want prev (13, 3) and entry 14", a)
	}

	// A follower that has exactly the snapshot gets 11..14 with prev = (10, 2).
	tr.sent = nil
	n.mu.Lock()
	n.nextIndex[2] = 11
	n.sendAppendEntries(2)
	n.mu.Unlock()
	a = tr.sent[0].Payload.(AppendEntriesArgs)
	if a.PrevLogIndex != 10 || a.PrevLogTerm != 2 || len(a.Entries) != 4 || a.Entries[0].Index != 11 || a.Entries[3].Index != 14 {
		t.Fatalf("append = %+v, want prev (10, 2) and entries 11..14", a)
	}
}

func TestMemoryStorageUsesTheSnapshotOffset(t *testing.T) {
	s := NewMemoryStorage()
	if err := s.Save(stateWithSnapshot()); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]LogEntry{entry(3, 14, "d")}); err != nil {
		t.Fatalf("append at 14: %v", err)
	}
	if err := s.Append([]LogEntry{entry(3, 14, "dup")}); err == nil {
		t.Fatal("append at 14 again was accepted")
	}
	if err := s.Append([]LogEntry{entry(3, 1, "old")}); err == nil {
		t.Fatal("append at index 1, inside the snapshot, was accepted")
	}
	for _, idx := range []Index{0, 10, 15} {
		if err := s.TruncateFrom(idx); err == nil {
			t.Fatalf("TruncateFrom(%d) was accepted; the log is (10, 14]", idx)
		}
	}
	if err := s.TruncateFrom(12); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Load()
	if want := []LogEntry{entry(2, 11, "a")}; !reflect.DeepEqual(got.Log, want) {
		t.Fatalf("log = %+v, want %+v", got.Log, want)
	}
}
