package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// ---- helpers shared by the cluster tests ----

func (n *Node) logCopy() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.log...)
}

func commandsOf(entries []LogEntry) []string {
	var out []string
	for _, e := range entries {
		if !e.IsNoOp() {
			out = append(out, string(e.Command))
		}
	}
	return out
}

// onApply is each node's Apply callback. It checks, as entries are applied:
//   - entries arrive in order, once each;
//   - State Machine Safety (Raft paper, Figure 3): no two nodes ever apply
//     different entries at the same index;
//   - nothing is applied that no leader accepted.
func (c *cluster) onApply(id int, e LogEntry) {
	if want := Index(len(c.applied[id]) + 1); e.Index != want {
		c.t.Fatalf("seed %d: node %d applied index %d, want %d", c.seed, id, e.Index, want)
	}
	if prev, ok := c.global[e.Index]; ok {
		if prev.Term != e.Term || !bytes.Equal(prev.Command, e.Command) {
			c.t.Fatalf("seed %d: state machine safety violated at index %d: %+v vs %+v",
				c.seed, e.Index, prev, e)
		}
	} else {
		c.global[e.Index] = e
	}
	if !e.IsNoOp() && !c.proposed[string(e.Command)] {
		c.t.Fatalf("seed %d: node %d applied a command no leader accepted: %q", c.seed, id, e.Command)
	}
	c.applied[id] = append(c.applied[id], e)
}

// checkLogMatching checks the Log Matching property (Figure 3): if two logs
// hold an entry with the same index and term, the logs are identical up to
// and including that entry.
func (c *cluster) checkLogMatching() {
	logs := make([][]LogEntry, len(c.nodes))
	for i, n := range c.nodes {
		logs[i] = n.logCopy()
		for j, e := range logs[i] {
			if e.Index != Index(j+1) {
				c.t.Fatalf("seed %d: node %d has entry %+v at position %d", c.seed, i, e, j+1)
			}
		}
	}
	for a := range logs {
		for b := a + 1; b < len(logs); b++ {
			la, lb := logs[a], logs[b]
			last := -1
			for i := 0; i < min(len(la), len(lb)); i++ {
				if la[i].Term == lb[i].Term {
					last = i
				}
			}
			for i := 0; i <= last; i++ {
				if la[i].Term != lb[i].Term || !bytes.Equal(la[i].Command, lb[i].Command) {
					c.t.Fatalf("seed %d: log matching violated between nodes %d and %d at index %d: %+v vs %+v",
						c.seed, a, b, i+1, la[i], lb[i])
				}
			}
		}
	}
}

// proposeTo offers one command to a specific node and records it if accepted.
func (c *cluster) proposeTo(id int, cmd string) bool {
	if _, _, ok := c.nodes[id].Propose([]byte(cmd)); !ok {
		return false
	}
	c.proposed[cmd] = true
	return true
}

// propose gives a fresh command to every node that currently believes it is
// the leader (more than one during a partition) and returns how many accepted.
func (c *cluster) propose() int {
	accepted := 0
	for id := range c.nodes {
		if c.proposeTo(id, fmt.Sprintf("cmd-%d", c.nextCmd)) {
			c.nextCmd++
			accepted++
		}
	}
	return accepted
}

// requireConverged asserts that once the dust settles every node holds the
// same log and has applied everything any node ever applied: nothing that was
// committed was lost, and nothing uncommitted survived.
func (c *cluster) requireConverged() {
	c.t.Helper()
	if len(c.global) == 0 {
		c.t.Fatalf("seed %d: nothing was ever committed", c.seed)
	}
	ref := c.nodes[0].logCopy()
	for id, n := range c.nodes {
		if got := len(c.applied[id]); got != len(c.global) {
			c.t.Fatalf("seed %d: node %d applied %d entries but the cluster applied %d: %+v",
				c.seed, id, got, len(c.global), c.statuses())
		}
		if got := n.logCopy(); !reflect.DeepEqual(got, ref) {
			c.t.Fatalf("seed %d: node %d log differs from node 0:\n%+v\n%+v", c.seed, id, got, ref)
		}
	}
}

// ---- cluster tests ----

func TestReplicatesToAllNodes(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 60; seed++ {
			c := newCluster(t, size, seed, baseNet)
			c.run(200)

			var want []string
			for i := 0; i < 20; i++ {
				if c.propose() != 1 {
					t.Fatalf("seed %d: expected exactly one leader to accept command %d", seed, i)
				}
				want = append(want, fmt.Sprintf("cmd-%d", i))
				c.run(3)
			}
			c.run(200)

			c.requireConverged()
			if got := commandsOf(c.applied[0]); !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d: applied %v, want %v", seed, got, want)
			}
		}
	}
}

func TestReplicationWithMessageLoss(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		cfg := baseNet
		cfg.DropRate = 0.25
		c := newCluster(t, 5, seed, cfg)
		c.run(100)
		for i := 0; i < 60; i++ {
			c.propose()
			c.run(5)
		}

		// Stop the loss: everything committed must now reach everyone.
		c.net.SetDropRate(0)
		c.run(600)
		c.requireConverged()
	}
}

func TestReplicationUnderPartitions(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		c := newCluster(t, 5, seed, baseNet)
		rng := rand.New(rand.NewSource(seed + 2000))
		c.run(100)

		for step := 0; step < 30; step++ {
			if rng.Intn(3) == 0 {
				c.net.Heal()
			} else {
				var a, b []int
				for id := range c.nodes {
					if rng.Intn(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.net.Partition(a, b)
			}
			for i := 0; i < 3; i++ {
				c.propose()
				c.run(10)
			}
		}

		c.net.Heal()
		c.run(600)
		c.requireConverged()

		// And the healed cluster must still make progress.
		if c.propose() != 1 {
			t.Fatalf("seed %d: expected exactly one leader after healing", seed)
		}
		c.run(100)
		c.requireConverged()
		want := fmt.Sprintf("cmd-%d", c.nextCmd-1)
		for id := range c.nodes {
			cmds := commandsOf(c.applied[id])
			if cmds[len(cmds)-1] != want {
				t.Fatalf("seed %d: node %d never applied the final command %s", seed, id, want)
			}
		}
	}
}

// A follower that was cut off for a long time must catch up, in several
// bounded messages, once it reconnects.
func TestFollowerCatchesUp(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		c := newCluster(t, 3, seed, baseNet)
		c.run(200)
		leader := c.requireStable()

		lagging := int((leader.ID + 1) % 3)
		var rest []int
		for id := range c.nodes {
			if id != lagging {
				rest = append(rest, id)
			}
		}
		c.net.Partition([]int{lagging}, rest)

		const n = 3 * maxEntriesPerMessage // forces several catch-up batches
		for i := 0; i < n; i++ {
			if c.propose() != 1 {
				t.Fatalf("seed %d: leader stopped accepting commands", seed)
			}
			c.run(1)
		}
		c.run(50)
		if got := len(commandsOf(c.applied[lagging])); got != 0 {
			t.Fatalf("seed %d: isolated node applied %d commands", seed, got)
		}

		c.net.Heal()
		c.run(300)
		c.requireConverged()
		if got := len(commandsOf(c.applied[lagging])); got != n {
			t.Fatalf("seed %d: lagging node applied %d commands, want %d", seed, got, n)
		}
	}
}

// A leader cut off from the majority keeps accepting commands it can never
// commit. When the partition heals, those entries must be overwritten by the
// new leader's log and never applied anywhere.
func TestStaleLeaderEntriesAreOverwritten(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		c := newCluster(t, 3, seed, baseNet)
		c.run(200)
		old := c.requireStable()

		var rest []int
		for id := range c.nodes {
			if NodeID(id) != old.ID {
				rest = append(rest, id)
			}
		}
		c.net.Partition([]int{int(old.ID)}, rest)

		for i := 0; i < 5; i++ {
			if !c.proposeTo(int(old.ID), fmt.Sprintf("stale-%d", i)) {
				t.Fatalf("seed %d: the isolated leader should still accept commands", seed)
			}
		}
		c.run(200) // the other two elect a new leader in a later term

		fresh := 0
		for i := 0; i < 5; i++ {
			for _, id := range rest {
				if c.proposeTo(id, fmt.Sprintf("fresh-%d", fresh)) {
					fresh++
				}
			}
			c.run(5)
		}
		c.run(100)
		if fresh != 5 {
			t.Fatalf("seed %d: new leader accepted %d fresh commands, want 5", seed, fresh)
		}

		// The scenario is only meaningful if the stale leader really is
		// holding entries it has not committed.
		st := c.nodes[old.ID].Status()
		if st.LastLogIndex <= st.CommitIndex {
			t.Fatalf("seed %d: stale leader has no uncommitted entries: %+v", seed, st)
		}

		c.net.Heal()
		c.run(300)
		c.requireConverged()

		for id := range c.nodes {
			for _, cmd := range commandsOf(c.applied[id]) {
				if len(cmd) >= 5 && cmd[:5] == "stale" {
					t.Fatalf("seed %d: node %d applied uncommitted command %s", seed, id, cmd)
				}
			}
			if got := len(commandsOf(c.applied[id])); got != 5 {
				t.Fatalf("seed %d: node %d applied %d commands, want the 5 fresh ones", seed, id, got)
			}
		}
	}
}

// ---- unit tests: follower rules for AppendEntries (Figure 2) ----

func entry(term Term, index Index, cmd string) LogEntry {
	return LogEntry{Term: term, Index: index, Command: []byte(cmd)}
}

// newFollower builds node 0 of a 3-node cluster holding the given state and
// recording what it applies.
func newFollower(t *testing.T, ps PersistentState) (*Node, *MemoryStorage, *recTransport, *[]LogEntry) {
	t.Helper()
	st := NewMemoryStorage()
	if err := st.Save(ps); err != nil {
		t.Fatal(err)
	}
	tr := &recTransport{}
	applied := &[]LogEntry{}
	n, err := NewNode(Config{
		ID:    0,
		Peers: []NodeID{0, 1, 2},
		Seed:  1,
		Apply: func(e LogEntry) { *applied = append(*applied, e) },
	}, st, tr)
	if err != nil {
		t.Fatal(err)
	}
	return n, st, tr, applied
}

func appendEntries(t *testing.T, n *Node, tr *recTransport, args AppendEntriesArgs) AppendEntriesReply {
	t.Helper()
	tr.sent = nil
	n.Step(Message{From: 1, To: n.cfg.ID, Payload: args})
	if len(tr.sent) != 1 {
		t.Fatalf("expected exactly one reply, got %d", len(tr.sent))
	}
	reply, ok := tr.sent[0].Payload.(AppendEntriesReply)
	if !ok {
		t.Fatalf("reply payload is %T", tr.sent[0].Payload)
	}
	return reply
}

func TestAppendEntriesRejectsWhenPrevEntryMissing(t *testing.T) {
	n, _, tr, _ := newFollower(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote})

	r := appendEntries(t, n, tr, AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 1})
	if r.Success || r.LastLogIndex != 0 {
		t.Fatalf("reply = %+v, want failure reporting an empty log", r)
	}
}

func TestAppendEntriesRejectsPrevTermMismatch(t *testing.T) {
	log := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b")}
	n, _, tr, _ := newFollower(t, PersistentState{CurrentTerm: 2, VotedFor: NoVote, Log: log})

	r := appendEntries(t, n, tr, AppendEntriesArgs{Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2})
	if r.Success || r.LastLogIndex != 2 {
		t.Fatalf("reply = %+v, want failure reporting last index 2", r)
	}
}

func TestAppendEntriesAppendsAndApplies(t *testing.T) {
	n, st, tr, applied := newFollower(t, PersistentState{VotedFor: NoVote})

	r := appendEntries(t, n, tr, AppendEntriesArgs{
		Term: 1, LeaderID: 1,
		Entries:      []LogEntry{entry(1, 1, "a"), entry(1, 2, "b")},
		LeaderCommit: 2,
	})
	if !r.Success || r.MatchIndex != 2 {
		t.Fatalf("reply = %+v, want success with MatchIndex 2", r)
	}
	if got := commandsOf(*applied); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("applied %v, want [a b]", got)
	}
	if got := n.Status().CommitIndex; got != 2 {
		t.Fatalf("commitIndex = %d, want 2", got)
	}

	// The entries must be on stable storage before the reply was sent.
	saved, _ := st.Load()
	if len(saved.Log) != 2 {
		t.Fatalf("persisted log has %d entries, want 2", len(saved.Log))
	}
}

func TestAppendEntriesCommitIsCappedAtLastVerifiedEntry(t *testing.T) {
	log := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}
	n, _, tr, _ := newFollower(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log})

	// The leader only verified entries up to index 2 in this message. Entry 3
	// might not match the leader's log, so it must not be treated as committed
	// even though LeaderCommit is higher.
	appendEntries(t, n, tr, AppendEntriesArgs{
		Term: 1, LeaderID: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      []LogEntry{entry(1, 2, "b")},
		LeaderCommit: 99,
	})
	if got := n.Status().CommitIndex; got != 2 {
		t.Fatalf("commitIndex = %d, want 2", got)
	}
}

func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	log := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}
	n, st, tr, _ := newFollower(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log})

	r := appendEntries(t, n, tr, AppendEntriesArgs{
		Term: 2, LeaderID: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{entry(2, 2, "x")},
	})
	if !r.Success || r.MatchIndex != 2 {
		t.Fatalf("reply = %+v", r)
	}
	want := []LogEntry{entry(1, 1, "a"), entry(2, 2, "x")}
	if got := n.logCopy(); !reflect.DeepEqual(got, want) {
		t.Fatalf("log = %+v, want %+v", got, want)
	}
	if saved, _ := st.Load(); !reflect.DeepEqual(saved.Log, want) {
		t.Fatalf("persisted log = %+v, want %+v", saved.Log, want)
	}
}

// A duplicated or delayed AppendEntries carrying old entries must not
// shorten a log that has already moved on.
func TestAppendEntriesKeepsEntriesThatAlreadyMatch(t *testing.T) {
	log := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}
	n, _, tr, _ := newFollower(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log})

	r := appendEntries(t, n, tr, AppendEntriesArgs{
		Term: 1, LeaderID: 1,
		Entries: []LogEntry{entry(1, 1, "a"), entry(1, 2, "b")},
	})
	if !r.Success || r.MatchIndex != 2 {
		t.Fatalf("reply = %+v", r)
	}
	if got := len(n.logCopy()); got != 3 {
		t.Fatalf("log length = %d, want 3 (entry 3 must survive)", got)
	}
}

// ---- unit tests: leader rules ----

func TestSingleNodeCommitsAndApplies(t *testing.T) {
	var applied []LogEntry
	n, err := NewNode(Config{
		ID: 0, Peers: []NodeID{0}, Seed: 1,
		Apply: func(e LogEntry) { applied = append(applied, e) },
	}, NewMemoryStorage(), nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}

	index, term, ok := n.Propose([]byte("a"))
	if !ok || index != 2 || term != 1 {
		t.Fatalf("Propose = (%d, %d, %v), want (2, 1, true): the no-op takes index 1", index, term, ok)
	}
	if len(applied) != 2 || !applied[0].IsNoOp() || string(applied[1].Command) != "a" {
		t.Fatalf("applied = %+v, want [no-op, a]", applied)
	}
	if got := n.Status().CommitIndex; got != 2 {
		t.Fatalf("commitIndex = %d, want 2", got)
	}
}

func TestProposeRejectsEmptyCommand(t *testing.T) {
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0}, Seed: 1}, NewMemoryStorage(), nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	if _, _, ok := n.Propose(nil); ok {
		t.Fatal("an empty command must be rejected")
	}
}

// newLeader builds node 0 of a 3-node cluster as leader of term 3, with the
// given earlier log, and clears the messages sent while taking office.
func newLeader(t *testing.T, log []LogEntry) (*Node, *recTransport) {
	t.Helper()
	n, _, tr, _ := newFollower(t, PersistentState{CurrentTerm: 3, VotedFor: 0, Log: log})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	tr.sent = nil
	return n, tr
}

func ackFrom(n *Node, peer NodeID, term Term, match Index) {
	n.Step(Message{From: peer, To: n.cfg.ID, Payload: AppendEntriesReply{
		Term: term, Success: true, MatchIndex: match, LastLogIndex: match,
	}})
}

// Figure 8 of the Raft paper: an entry from an earlier term that is stored on
// a majority is NOT committed by counting replicas. It becomes committed only
// when an entry from the leader's own term, stored after it, is.
func TestLeaderCommitsOnlyCurrentTermEntriesByCounting(t *testing.T) {
	n, _ := newLeader(t, []LogEntry{entry(2, 1, "old")})
	// Log is now: [1: term 2 "old"], [2: term 3 no-op].

	ackFrom(n, 1, 3, 1) // a majority (nodes 0 and 1) now holds entry 1
	if got := n.Status().CommitIndex; got != 0 {
		t.Fatalf("commitIndex = %d: an earlier-term entry was committed by counting", got)
	}

	ackFrom(n, 1, 3, 2) // a majority now holds the current-term no-op
	if got := n.Status().CommitIndex; got != 2 {
		t.Fatalf("commitIndex = %d, want 2 (the no-op commits the old entry with it)", got)
	}
}

func TestLeaderBacksUpPastAShortFollowerLog(t *testing.T) {
	log := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}
	n, tr := newLeader(t, log)
	// Log is now 4 entries (the no-op is index 4); nextIndex for peers is 4.

	// Peer 1 has only entry 1.
	n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{
		Term: 3, Success: false, LastLogIndex: 1,
	}})

	if len(tr.sent) != 1 {
		t.Fatalf("expected an immediate retry, got %d messages", len(tr.sent))
	}
	retry := tr.sent[0].Payload.(AppendEntriesArgs)
	if tr.sent[0].To != 1 || retry.PrevLogIndex != 1 || len(retry.Entries) != 3 {
		t.Fatalf("retry = %+v to node %d, want prev=1 with entries 2..4 sent to node 1", retry, tr.sent[0].To)
	}
}

func TestLeaderIgnoresRepliesFromAnOlderTerm(t *testing.T) {
	n, _ := newLeader(t, nil)

	ackFrom(n, 1, 2, 5) // term 2 reply while we lead term 3

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex[1] != 0 || n.commitIndex != 0 {
		t.Fatalf("a stale reply changed state: matchIndex=%d commitIndex=%d", n.matchIndex[1], n.commitIndex)
	}
}

func TestLeaderStepsDownOnHigherTermReply(t *testing.T) {
	n, _ := newLeader(t, nil)

	n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{Term: 9, Success: false}})

	if st := n.Status(); st.Role != Follower || st.Term != 9 {
		t.Fatalf("status = %+v, want follower in term 9", st)
	}
}
