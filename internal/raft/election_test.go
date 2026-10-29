package raft

import "testing"

// recTransport records outgoing messages so a test can inspect the replies.
type recTransport struct{ sent []Message }

func (r *recTransport) Send(m Message) { r.sent = append(r.sent, m) }

// newNodeWith builds node 0 of a 3-node cluster whose storage already holds
// the given state, as if it had restarted.
func newNodeWith(t *testing.T, ps PersistentState, tr Transport) (*Node, *MemoryStorage) {
	t.Helper()
	st := NewMemoryStorage()
	if err := st.Save(ps); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}, Seed: 1}, st, tr)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n, st
}

// requestVote delivers a RequestVote to n and returns its reply.
func requestVote(t *testing.T, n *Node, tr *recTransport, from NodeID, args RequestVoteArgs) RequestVoteReply {
	t.Helper()
	tr.sent = nil
	n.Step(Message{From: from, To: n.cfg.ID, Payload: args})
	if len(tr.sent) != 1 {
		t.Fatalf("expected exactly one reply, got %d", len(tr.sent))
	}
	reply, ok := tr.sent[0].Payload.(RequestVoteReply)
	if !ok {
		t.Fatalf("reply payload is %T, want RequestVoteReply", tr.sent[0].Payload)
	}
	if tr.sent[0].To != from {
		t.Fatalf("reply sent to %d, want %d", tr.sent[0].To, from)
	}
	return reply
}

func TestNewNodeRejectsIDNotInPeers(t *testing.T) {
	_, err := NewNode(Config{ID: 5, Peers: []NodeID{0, 1, 2}}, NewMemoryStorage(), nopTransport{})
	if err == nil {
		t.Fatal("expected an error for an ID that is not in Peers")
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0}, Seed: 1}, NewMemoryStorage(), nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	st := n.Status()
	if st.Role != Leader || st.Term != 1 || st.Leader != 0 {
		t.Fatalf("status = %+v, want leader of term 1", st)
	}
}

func TestGrantsAtMostOneVotePerTerm(t *testing.T) {
	tr := &recTransport{}
	n, _ := newNodeWith(t, PersistentState{VotedFor: NoVote}, tr)

	if r := requestVote(t, n, tr, 1, RequestVoteArgs{Term: 1, CandidateID: 1}); !r.VoteGranted || r.Term != 1 {
		t.Fatalf("first candidate: %+v, want granted in term 1", r)
	}
	if r := requestVote(t, n, tr, 2, RequestVoteArgs{Term: 1, CandidateID: 2}); r.VoteGranted {
		t.Fatal("granted a second vote in the same term")
	}
	// A repeated request from the same candidate (a retransmission) is fine.
	if r := requestVote(t, n, tr, 1, RequestVoteArgs{Term: 1, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("refused a retransmitted request from the candidate we already voted for")
	}
	// A new term gives a fresh vote.
	if r := requestVote(t, n, tr, 2, RequestVoteArgs{Term: 2, CandidateID: 2}); !r.VoteGranted || r.Term != 2 {
		t.Fatalf("new term: %+v, want granted in term 2", r)
	}
}

func TestRejectsStaleTerm(t *testing.T) {
	tr := &recTransport{}
	n, _ := newNodeWith(t, PersistentState{CurrentTerm: 5, VotedFor: NoVote}, tr)

	r := requestVote(t, n, tr, 1, RequestVoteArgs{Term: 3, CandidateID: 1})
	if r.VoteGranted || r.Term != 5 {
		t.Fatalf("reply = %+v, want rejected with term 5", r)
	}
	if got := n.Status().Term; got != 5 {
		t.Fatalf("term moved backwards to %d", got)
	}
}

func TestRejectsCandidateWithOutOfDateLog(t *testing.T) {
	log := []LogEntry{{Term: 2, Index: 1}, {Term: 2, Index: 2}}
	cases := []struct {
		name string
		args RequestVoteArgs
		want bool
	}{
		{"older last term", RequestVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 1, LastLogIndex: 9}, false},
		{"same term, shorter log", RequestVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 1}, false},
		{"same term, same length", RequestVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 2}, true},
		{"same term, longer log", RequestVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 5}, true},
		{"newer last term, shorter log", RequestVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 3, LastLogIndex: 1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &recTransport{}
			n, _ := newNodeWith(t, PersistentState{CurrentTerm: 2, VotedFor: NoVote, Log: log}, tr)
			if r := requestVote(t, n, tr, 1, tc.args); r.VoteGranted != tc.want {
				t.Fatalf("VoteGranted = %v, want %v", r.VoteGranted, tc.want)
			}
		})
	}
}

// A node that granted a vote and then crashed must not vote for a different
// candidate in the same term after it restarts. This is why the vote has to
// reach stable storage before the reply leaves the node.
func TestVoteSurvivesRestart(t *testing.T) {
	tr := &recTransport{}
	n, st := newNodeWith(t, PersistentState{VotedFor: NoVote}, tr)

	if r := requestVote(t, n, tr, 1, RequestVoteArgs{Term: 1, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("expected the first vote to be granted")
	}

	// "Crash": throw the node away and build a new one from the same storage.
	tr2 := &recTransport{}
	restarted, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}, Seed: 1}, st, tr2)
	if err != nil {
		t.Fatal(err)
	}
	if r := requestVote(t, restarted, tr2, 2, RequestVoteArgs{Term: 1, CandidateID: 2}); r.VoteGranted {
		t.Fatal("restarted node voted twice in the same term")
	}
}

func TestHigherTermDeposesLeader(t *testing.T) {
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0}, Seed: 1}, NewMemoryStorage(), nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	term := n.Status().Term

	n.Step(Message{From: 1, To: 0, Payload: AppendEntriesArgs{Term: term + 1, LeaderID: 1}})

	st := n.Status()
	if st.Role != Follower || st.Term != term+1 || st.Leader != 1 {
		t.Fatalf("status = %+v, want follower of leader 1 in term %d", st, term+1)
	}
}

func TestStaleLeaderHeartbeatIsRejected(t *testing.T) {
	tr := &recTransport{}
	n, _ := newNodeWith(t, PersistentState{CurrentTerm: 5, VotedFor: NoVote}, tr)

	n.Step(Message{From: 1, To: 0, Payload: AppendEntriesArgs{Term: 3, LeaderID: 1}})

	if len(tr.sent) != 1 {
		t.Fatalf("expected one reply, got %d", len(tr.sent))
	}
	reply, ok := tr.sent[0].Payload.(AppendEntriesReply)
	if !ok || reply.Success || reply.Term != 5 {
		t.Fatalf("reply = %+v, want rejection carrying term 5", tr.sent[0].Payload)
	}
	if st := n.Status(); st.Leader != NoLeader {
		t.Fatalf("accepted a stale leader: %+v", st)
	}
}
