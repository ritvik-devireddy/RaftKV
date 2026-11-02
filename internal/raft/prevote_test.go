package raft

import (
	"reflect"
	"testing"
)

// PreVote (Raft thesis, section 9.6). Without it, a node that is cut off from
// the cluster keeps timing out, and every timeout raises its term. When the
// network heals, its high term deposes a perfectly healthy leader even though
// the cluster never needed a new election. These tests pin down the fix.

// A follower isolated for a long time must not disturb the leader when it
// comes back. The leader and its term must be exactly what they were.
func TestRejoiningNodeDoesNotDisruptStableLeader(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 40; seed++ {
			c := newCluster(t, size, seed, baseNet)
			c.run(200)
			leader := c.requireStable()

			// Cut one follower off from everyone else for a long while.
			victim := (int(leader.ID) + 1) % size
			var rest []int
			for id := range c.nodes {
				if id != victim {
					rest = append(rest, id)
				}
			}
			c.net.Partition([]int{victim}, rest)
			c.run(300)

			// The majority side must not have noticed anything.
			if st := c.nodes[leader.ID].Status(); st.Role != Leader || st.Term != leader.Term {
				t.Fatalf("size %d seed %d: leader changed while a follower was isolated: %+v", size, seed, st)
			}

			c.net.Heal()
			c.run(200)

			after := c.requireStable()
			if after.ID != leader.ID || after.Term != leader.Term {
				t.Fatalf("size %d seed %d: a rejoining node disrupted a healthy leader: "+
					"leader %d term %d before, leader %d term %d after",
					size, seed, leader.ID, leader.Term, after.ID, after.Term)
			}
		}
	}
}

// ---- unit tests for the PreVote rules ----

// preVote delivers a PreVote to n and returns its reply.
func preVote(t *testing.T, n *Node, tr *recTransport, from NodeID, args PreVoteArgs) PreVoteReply {
	t.Helper()
	tr.sent = nil
	n.Step(Message{From: from, To: n.cfg.ID, Payload: args})
	if len(tr.sent) != 1 {
		t.Fatalf("expected exactly one reply, got %d", len(tr.sent))
	}
	reply, ok := tr.sent[0].Payload.(PreVoteReply)
	if !ok {
		t.Fatalf("reply payload is %T, want PreVoteReply", tr.sent[0].Payload)
	}
	if tr.sent[0].To != from {
		t.Fatalf("reply sent to %d, want %d", tr.sent[0].To, from)
	}
	return reply
}

func TestPreVoteGrantRules(t *testing.T) {
	log := []LogEntry{{Term: 2, Index: 1}}
	cases := []struct {
		name       string
		state      PersistentState
		args       PreVoteArgs
		leaderSeen bool // a leader sent a heartbeat just now
		elapsed    int  // ticks since that heartbeat, if leaderSeen
		want       bool
	}{
		{"no leader known, candidate's term is ahead, log ok",
			PersistentState{CurrentTerm: 1, Log: log}, PreVoteArgs{Term: 2, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 1}, false, 0, true},
		{"candidate's term is not ahead of ours",
			PersistentState{CurrentTerm: 2, Log: log}, PreVoteArgs{Term: 2, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 1}, false, 0, false},
		{"candidate's term is behind ours",
			PersistentState{CurrentTerm: 5, Log: log}, PreVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 2, LastLogIndex: 1}, false, 0, false},
		{"candidate's log is out of date",
			PersistentState{CurrentTerm: 2, Log: log}, PreVoteArgs{Term: 3, CandidateID: 1, LastLogTerm: 1, LastLogIndex: 9}, false, 0, false},
		{"following a leader that just spoke",
			PersistentState{CurrentTerm: 2, Log: log}, PreVoteArgs{Term: 3, CandidateID: 2, LastLogTerm: 2, LastLogIndex: 1}, true, 0, false},
		{"following a leader that spoke almost a full timeout ago",
			PersistentState{CurrentTerm: 2, Log: log}, PreVoteArgs{Term: 3, CandidateID: 2, LastLogTerm: 2, LastLogIndex: 1}, true, 9, false},
		{"leader has been silent for the minimum election timeout",
			PersistentState{CurrentTerm: 2, Log: log}, PreVoteArgs{Term: 3, CandidateID: 2, LastLogTerm: 2, LastLogIndex: 1}, true, 10, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &recTransport{}
			n, _ := newNodeWith(t, tc.state, tr)
			if tc.leaderSeen {
				n.Step(Message{From: 1, To: 0, Payload: AppendEntriesArgs{
					Term: tc.state.CurrentTerm, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 2, LeaderCommit: 0,
				}})
				n.electionElapsed = tc.elapsed // white-box: age the heartbeat
			}
			r := preVote(t, n, tr, tc.args.CandidateID, tc.args)
			if r.VoteGranted != tc.want {
				t.Fatalf("VoteGranted = %v, want %v", r.VoteGranted, tc.want)
			}
			if r.Term != tc.state.CurrentTerm || r.PreVoteTerm != tc.args.Term {
				t.Fatalf("reply = %+v, want Term %d and PreVoteTerm %d", r, tc.state.CurrentTerm, tc.args.Term)
			}
		})
	}
}

func TestALeaderRefusesPreVotes(t *testing.T) {
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0}, Seed: 1}, NewMemoryStorage(), nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	tr := &recTransport{}
	n.transport = tr
	term := n.Status().Term
	if r := preVote(t, n, tr, 1, PreVoteArgs{Term: term + 1, CandidateID: 1}); r.VoteGranted {
		t.Fatal("a sitting leader granted a pre-vote")
	}
	if st := n.Status(); st.Role != Leader || st.Term != term {
		t.Fatalf("a pre-vote disturbed the leader: %+v", st)
	}
}

// Answering a pre-vote must not consume the node's vote, change its term, touch
// its timer, or write to disk. Otherwise PreVote itself becomes a way to
// disrupt the cluster, or to make a node vote twice.
func TestPreVoteChangesNothing(t *testing.T) {
	tr := &recTransport{}
	n, st := newNodeWith(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote}, tr)
	n.electionElapsed = 3
	before, _ := st.Load()

	if r := preVote(t, n, tr, 1, PreVoteArgs{Term: 2, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("expected the pre-vote to be granted")
	}

	if got := n.Status().Term; got != 1 {
		t.Fatalf("term changed to %d", got)
	}
	if n.electionElapsed != 3 {
		t.Fatalf("election timer was touched: %d", n.electionElapsed)
	}
	if after, _ := st.Load(); !reflect.DeepEqual(before, after) {
		t.Fatalf("storage changed: %+v -> %+v", before, after)
	}
	// The vote for term 2 is still free: a different candidate can have it.
	if r := requestVote(t, n, tr, 2, RequestVoteArgs{Term: 2, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("answering a pre-vote used up the node's real vote")
	}
}

// ---- cluster tests ----

// An isolated node keeps asking and keeps hearing nothing. Its term must stay
// where it was and it must never write its term to disk.
func TestIsolatedNodeNeverRaisesItsTerm(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := newCluster(t, 3, seed, baseNet)
		c.run(200)
		leader := c.requireStable()

		victim := (int(leader.ID) + 1) % 3
		var rest []int
		for id := range c.nodes {
			if id != victim {
				rest = append(rest, id)
			}
		}
		ops := c.storages[victim].ops
		c.net.Partition([]int{victim}, rest)
		c.run(500)

		if st := c.nodes[victim].Status(); st.Term != leader.Term {
			t.Fatalf("seed %d: isolated node's term went from %d to %d", seed, leader.Term, st.Term)
		}
		if got := c.storages[victim].ops; got != ops {
			t.Fatalf("seed %d: isolated node wrote to disk %d times while only asking for pre-votes", seed, got-ops)
		}
	}
}

// PreVote must not stop a cluster from replacing a leader that has really
// failed: the nodes that lost contact with it agree to a new election.
func TestPreVoteStillReplacesAnUnreachableLeader(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 40; seed++ {
			c := newCluster(t, size, seed, baseNet)
			c.run(200)
			old := c.requireStable()

			var rest []int
			for id := range c.nodes {
				if id != int(old.ID) {
					rest = append(rest, id)
				}
			}
			c.net.Partition([]int{int(old.ID)}, rest)
			c.run(300)

			var newLeaders int
			for _, id := range rest {
				if st := c.nodes[id].Status(); st.Role == Leader && st.Term > old.Term {
					newLeaders++
				}
			}
			if newLeaders != 1 {
				t.Fatalf("size %d seed %d: %d new leaders among the reachable nodes, want 1: %+v",
					size, seed, newLeaders, c.statuses())
			}

			c.net.Heal()
			c.run(200)
			after := c.requireStable()
			if after.Term <= old.Term || after.ID == old.ID {
				t.Fatalf("size %d seed %d: leader %d term %d before, leader %d term %d after",
					size, seed, old.ID, old.Term, after.ID, after.Term)
			}
		}
	}
}

// A reply that answers an old round, or that arrives after a leader has
// appeared, must not start an election.
func TestStalePreVoteRepliesAreIgnored(t *testing.T) {
	newPreCandidate := func(t *testing.T) (*Node, *recTransport) {
		tr := &recTransport{}
		n, _ := newNodeWith(t, PersistentState{CurrentTerm: 4, VotedFor: NoVote}, tr)
		for i := 0; i < 100 && n.preVotes == nil; i++ {
			n.Tick()
		}
		if n.preVotes == nil {
			t.Fatal("node never started a pre-vote")
		}
		if st := n.Status(); st.Term != 4 || st.Role != Follower {
			t.Fatalf("starting a pre-vote changed the node: %+v", st)
		}
		return n, tr
	}

	t.Run("reply to an older round", func(t *testing.T) {
		n, _ := newPreCandidate(t)
		n.Step(Message{From: 1, To: 0, Payload: PreVoteReply{Term: 4, PreVoteTerm: 4, VoteGranted: true}})
		if st := n.Status(); st.Term != 4 || st.Role != Follower {
			t.Fatalf("a stale reply started an election: %+v", st)
		}
	})

	t.Run("reply after a leader appeared", func(t *testing.T) {
		n, _ := newPreCandidate(t)
		n.Step(Message{From: 1, To: 0, Payload: AppendEntriesArgs{Term: 4, LeaderID: 1}})
		n.Step(Message{From: 2, To: 0, Payload: PreVoteReply{Term: 4, PreVoteTerm: 5, VoteGranted: true}})
		if st := n.Status(); st.Term != 4 || st.Role != Follower || st.Leader != 1 {
			t.Fatalf("a late reply disturbed a node that has a leader: %+v", st)
		}
	})

	t.Run("a granted reply for the current round starts the election", func(t *testing.T) {
		n, tr := newPreCandidate(t)
		tr.sent = nil
		n.Step(Message{From: 1, To: 0, Payload: PreVoteReply{Term: 4, PreVoteTerm: 5, VoteGranted: true}})
		if st := n.Status(); st.Term != 5 || st.Role != Candidate {
			t.Fatalf("status = %+v, want candidate in term 5", st)
		}
		var asked int
		for _, m := range tr.sent {
			if a, ok := m.Payload.(RequestVoteArgs); ok && a.Term == 5 {
				asked++
			}
		}
		if asked != 2 {
			t.Fatalf("sent %d RequestVotes for term 5, want 2", asked)
		}
	})

	t.Run("a reply showing a newer term is adopted", func(t *testing.T) {
		n, _ := newPreCandidate(t)
		n.Step(Message{From: 1, To: 0, Payload: PreVoteReply{Term: 9, PreVoteTerm: 5, VoteGranted: false}})
		if st := n.Status(); st.Term != 9 || st.Role != Follower {
			t.Fatalf("status = %+v, want follower in term 9", st)
		}
	})
}

// A node whose pre-vote got no answer must wait a fresh election timeout before
// asking again. Without that, an isolated node would flood the network with a
// new round on every tick.
func TestPreVoteRoundsAreSpacedByTheElectionTimeout(t *testing.T) {
	tr := &recTransport{}
	n, _ := newNodeWith(t, PersistentState{CurrentTerm: 4, VotedFor: NoVote}, tr)
	for i := 0; i < 100 && n.preVotes == nil; i++ {
		n.Tick()
	}
	if n.preVotes == nil {
		t.Fatal("node never started a pre-vote")
	}

	tr.sent = nil
	for i := 0; i < n.cfg.ElectionTickMin-1; i++ {
		n.Tick()
	}
	for _, m := range tr.sent {
		if _, ok := m.Payload.(PreVoteArgs); ok {
			t.Fatal("a second pre-vote round started before a new election timeout elapsed")
		}
	}
}
