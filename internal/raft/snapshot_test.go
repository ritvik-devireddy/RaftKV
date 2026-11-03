package raft

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// ---- a node with a toy state machine, for tests of one node at a time ----

// snapRig is node 0 of a 3-node cluster whose "state machine" is the
// concatenation of the commands it has applied. Its snapshots are that string.
type snapRig struct {
	n          *Node
	st         *MemoryStorage
	tr         *recTransport
	state      string
	applied    []LogEntry
	restored   []string
	restoredAt []Index // the snapshot index of each restore
}

func newSnapRig(t *testing.T, ps PersistentState, every int) *snapRig {
	t.Helper()
	r := &snapRig{st: NewMemoryStorage(), tr: &recTransport{}}
	if err := r.st.Save(ps); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{
		ID: 0, Peers: []NodeID{0, 1, 2}, Seed: 1,
		ElectionTickMin: 10, ElectionTickMax: 10, // a fixed timeout, so tests can count ticks
		Apply: func(e LogEntry) {
			r.applied = append(r.applied, e)
			r.state += string(e.Command)
		},
		SnapshotEvery: every,
		Snapshot:      func() []byte { return []byte(r.state) },
		Restore: func(snap Snapshot) {
			r.restored = append(r.restored, string(snap.Data))
			r.restoredAt = append(r.restoredAt, snap.Index)
			r.state = string(snap.Data)
		},
	}, r.st, r.tr)
	if err != nil {
		t.Fatal(err)
	}
	r.n = n
	return r
}

// reply delivers a message from node 1 and returns the single reply sent.
func (r *snapRig) reply(t *testing.T, payload any) AppendEntriesReply {
	t.Helper()
	r.tr.sent = nil
	r.n.Step(Message{From: 1, To: 0, Payload: payload})
	if len(r.tr.sent) != 1 {
		t.Fatalf("expected one reply, got %d: %+v", len(r.tr.sent), r.tr.sent)
	}
	rep, ok := r.tr.sent[0].Payload.(AppendEntriesReply)
	if !ok {
		t.Fatalf("reply is %T, want AppendEntriesReply", r.tr.sent[0].Payload)
	}
	return rep
}

func (r *snapRig) disk(t *testing.T) PersistentState {
	t.Helper()
	ps, err := r.st.Load()
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

// elect makes the rig leader of the next term, with node 1 voting for it.
func (r *snapRig) elect(t *testing.T) {
	t.Helper()
	r.tr.sent = nil
	for i := 0; i < 200 && len(r.tr.sent) == 0; i++ {
		r.n.Tick()
	}
	term := r.n.Status().Term
	r.n.Step(Message{From: 1, To: 0, Payload: PreVoteReply{Term: term, PreVoteTerm: term + 1, VoteGranted: true}})
	r.n.Step(Message{From: 1, To: 0, Payload: RequestVoteReply{Term: term + 1, VoteGranted: true}})
	if r.n.Status().Role != Leader {
		t.Fatalf("status = %+v, want leader", r.n.Status())
	}
	r.tr.sent = nil
}

func cmdEntries(term Term, from Index, cmds ...string) []LogEntry {
	var out []LogEntry
	for i, c := range cmds {
		out = append(out, entry(term, from+Index(i), c))
	}
	return out
}

// ---- compaction ----

func TestCompactsAfterEveryNAppliedEntries(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 3)

	send := func(prev Index, prevTerm Term, commit Index, es []LogEntry) {
		t.Helper()
		if rep := r.reply(t, AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: es, LeaderCommit: commit}); !rep.Success {
			t.Fatalf("append rejected: %+v", rep)
		}
	}

	send(0, 0, 2, cmdEntries(1, 1, "a", "b"))
	if got := r.n.Status().SnapshotIndex; got != 0 {
		t.Fatalf("snapshot at %d after 2 applied entries, want none (every 3)", got)
	}

	send(2, 1, 3, cmdEntries(1, 3, "c"))
	disk := r.disk(t)
	if disk.Snapshot.Index != 3 || disk.Snapshot.Term != 1 || string(disk.Snapshot.Data) != "abc" || len(disk.Log) != 0 {
		t.Fatalf("after 3 applied: disk = %+v, want snapshot of \"abc\" at 3 and an empty log", disk)
	}
	if st := r.n.Status(); st.SnapshotIndex != 3 || st.LastLogIndex != 3 || st.CommitIndex != 3 {
		t.Fatalf("status = %+v", st)
	}
	if got := r.n.logCopy(); len(got) != 0 {
		t.Fatalf("memory still holds %d compacted entries", len(got))
	}

	// The log carries on from the snapshot, and the next snapshot covers it all.
	send(3, 1, 4, cmdEntries(2, 4, "d"))
	send(4, 2, 6, cmdEntries(2, 5, "e", "f", "g"))
	disk = r.disk(t)
	if disk.Snapshot.Index != 6 || disk.Snapshot.Term != 2 || string(disk.Snapshot.Data) != "abcdef" {
		t.Fatalf("second snapshot = %+v, want \"abcdef\" at 6, term 2", disk.Snapshot)
	}
	if want := cmdEntries(2, 7, "g"); !reflect.DeepEqual(disk.Log, want) || !reflect.DeepEqual(r.n.logCopy(), want) {
		t.Fatalf("log after compaction: disk %+v, memory %+v, want %+v", disk.Log, r.n.logCopy(), want)
	}
	if len(r.applied) != 6 {
		t.Fatalf("applied %d entries, want 6", len(r.applied))
	}
}

func TestNeverCompactsWhenDisabled(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
	r.reply(t, AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: cmdEntries(1, 1, "a", "b", "c", "d", "e"), LeaderCommit: 5})
	if disk := r.disk(t); disk.Snapshot.Index != 0 || len(disk.Log) != 5 {
		t.Fatalf("disk = %+v, want the whole log and no snapshot", disk)
	}
}

func TestSnapshotEveryNeedsASnapshotFunction(t *testing.T) {
	_, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}, SnapshotEvery: 10}, NewMemoryStorage(), nopTransport{})
	if err == nil || !strings.Contains(err.Error(), "Snapshot is nil") {
		t.Fatalf("err = %v, want a complaint about the missing Snapshot function", err)
	}
}

// A single-node cluster commits (and so compacts) inside Propose.
func TestSingleNodeCompacts(t *testing.T) {
	r := &snapRig{st: NewMemoryStorage(), tr: &recTransport{}}
	n, err := NewNode(Config{
		ID: 0, Peers: []NodeID{0}, Seed: 1,
		Apply:         func(e LogEntry) { r.state += string(e.Command) },
		SnapshotEvery: 4,
		Snapshot:      func() []byte { return []byte(r.state) },
	}, r.st, r.tr)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	for i := 0; i < 10; i++ {
		if _, _, ok := n.Propose([]byte{byte('a' + i)}); !ok {
			t.Fatal("propose refused")
		}
	}
	disk := r.disk(t)
	// The no-op took index 1, so the 10 commands are 2..11: snapshots at 4 and 8.
	if disk.Snapshot.Index != 8 || string(disk.Snapshot.Data) != "abcdefg" {
		t.Fatalf("snapshot = %d %q, want index 8 holding \"abcdefg\"", disk.Snapshot.Index, disk.Snapshot.Data)
	}
	if len(disk.Log) != 3 || disk.Log[0].Index != 9 {
		t.Fatalf("log = %+v, want entries 9..11", disk.Log)
	}
}

// ---- restart ----

func TestRestartRestoresTheStateMachineBeforeApplyingAnything(t *testing.T) {
	r := newSnapRig(t, PersistentState{
		CurrentTerm: 3, VotedFor: NoVote,
		Snapshot: Snapshot{Index: 10, Term: 2, Data: []byte("state@10")},
		Log:      cmdEntries(3, 11, "x", "y"),
	}, 0)
	if !reflect.DeepEqual(r.restored, []string{"state@10"}) || !reflect.DeepEqual(r.restoredAt, []Index{10}) || len(r.applied) != 0 {
		t.Fatalf("restored %q, applied %d entries; want one restore of state@10 and no applies", r.restored, len(r.applied))
	}
	// Entries after the snapshot are re-applied once the leader says they are committed.
	if rep := r.reply(t, AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 10, PrevLogTerm: 2, Entries: cmdEntries(3, 11, "x", "y"), LeaderCommit: 12}); !rep.Success {
		t.Fatalf("rejected: %+v", rep)
	}
	if r.state != "state@10xy" || len(r.applied) != 2 || r.applied[0].Index != 11 {
		t.Fatalf("state %q, applied %+v", r.state, r.applied)
	}
}

func TestNodeWithoutASnapshotDoesNotRestore(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote, Log: cmdEntries(1, 1, "a")}, 0)
	if len(r.restored) != 0 {
		t.Fatalf("restored %q with no snapshot saved", r.restored)
	}
}

// ---- InstallSnapshot: the follower ----

func installArgs(term Term, snap Snapshot) InstallSnapshotArgs {
	return InstallSnapshotArgs{Term: term, LeaderID: 1, Snapshot: snap}
}

func TestInstallSnapshotAheadOfTheLog(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
	snap := Snapshot{Index: 20, Term: 2, Data: []byte("state@20")}

	rep := r.reply(t, installArgs(2, snap))
	if !rep.Success || rep.MatchIndex != 20 || rep.Term != 2 || rep.LastLogIndex != 20 {
		t.Fatalf("reply = %+v, want success, match 20, term 2", rep)
	}
	if !reflect.DeepEqual(r.restored, []string{"state@20"}) || !reflect.DeepEqual(r.restoredAt, []Index{20}) || r.state != "state@20" || len(r.applied) != 0 {
		t.Fatalf("restored %q state %q applied %d", r.restored, r.state, len(r.applied))
	}
	st := r.n.Status()
	if st.CommitIndex != 20 || st.LastApplied != 20 || st.LastLogIndex != 20 || st.SnapshotIndex != 20 || st.Leader != 1 || st.Term != 2 {
		t.Fatalf("status = %+v", st)
	}
	disk := r.disk(t)
	if disk.Snapshot.Index != 20 || string(disk.Snapshot.Data) != "state@20" || len(disk.Log) != 0 || disk.CurrentTerm != 2 {
		t.Fatalf("disk = %+v", disk)
	}

	// The log continues right after the snapshot.
	if rep := r.reply(t, AppendEntriesArgs{Term: 2, LeaderID: 1, PrevLogIndex: 20, PrevLogTerm: 2, Entries: cmdEntries(2, 21, "z"), LeaderCommit: 21}); !rep.Success {
		t.Fatalf("append after snapshot rejected: %+v", rep)
	}
	if r.state != "state@20z" {
		t.Fatalf("state = %q", r.state)
	}
}

func TestInstallSnapshotKeepsTheLogAfterAMatchingEntry(t *testing.T) {
	// Our log is 1..8 in term 1. The snapshot ends at 5 in term 1: it is a prefix
	// of what we hold, so 6..8 are still good.
	log := cmdEntries(1, 1, "a", "b", "c", "d", "e", "f", "g", "h")
	r := newSnapRig(t, PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log}, 0)

	rep := r.reply(t, installArgs(1, Snapshot{Index: 5, Term: 1, Data: []byte("abcde")}))
	if !rep.Success || rep.MatchIndex != 5 || rep.LastLogIndex != 8 {
		t.Fatalf("reply = %+v, want match 5 and last 8", rep)
	}
	if want := log[5:]; !reflect.DeepEqual(r.n.logCopy(), want) || !reflect.DeepEqual(r.disk(t).Log, want) {
		t.Fatalf("log: memory %+v, disk %+v, want %+v", r.n.logCopy(), r.disk(t).Log, want)
	}
	if st := r.n.Status(); st.CommitIndex != 5 || st.LastApplied != 5 {
		t.Fatalf("status = %+v", st)
	}
}

func TestInstallSnapshotDropsALogThatDisagrees(t *testing.T) {
	// The snapshot says entry 5 has term 2; ours has term 1, so our entries 6..8
	// were built on something that was replaced.
	log := cmdEntries(1, 1, "a", "b", "c", "d", "e", "f", "g", "h")
	r := newSnapRig(t, PersistentState{CurrentTerm: 2, VotedFor: NoVote, Log: log}, 0)

	rep := r.reply(t, installArgs(2, Snapshot{Index: 5, Term: 2, Data: []byte("S")}))
	if !rep.Success || rep.LastLogIndex != 5 {
		t.Fatalf("reply = %+v, want last index 5 (the whole log dropped)", rep)
	}
	if len(r.n.logCopy()) != 0 || len(r.disk(t).Log) != 0 {
		t.Fatalf("log kept: memory %+v, disk %+v", r.n.logCopy(), r.disk(t).Log)
	}
}

func TestInstallSnapshotWeAlreadyPassedChangesNothing(t *testing.T) {
	for name, idx := range map[string]Index{"older than our commit": 3, "exactly our commit": 5} {
		t.Run(name, func(t *testing.T) {
			r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
			r.reply(t, AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: cmdEntries(1, 1, "a", "b", "c", "d", "e", "f"), LeaderCommit: 5})
			before := r.disk(t)

			rep := r.reply(t, installArgs(1, Snapshot{Index: idx, Term: 1, Data: []byte("old")}))
			if !rep.Success || rep.MatchIndex != idx {
				t.Fatalf("reply = %+v, want success with match %d", rep, idx)
			}
			if len(r.restored) != 0 || r.state != "abcde" {
				t.Fatalf("an old snapshot touched the state machine: restored %q, state %q", r.restored, r.state)
			}
			if !reflect.DeepEqual(r.disk(t), before) {
				t.Fatalf("an old snapshot changed storage:\n before %+v\n after  %+v", before, r.disk(t))
			}
			if st := r.n.Status(); st.CommitIndex != 5 || st.LastApplied != 5 || st.LastLogIndex != 6 {
				t.Fatalf("status = %+v", st)
			}
		})
	}
}

func TestInstallSnapshotFromAStaleTermIsRefused(t *testing.T) {
	r := newSnapRig(t, PersistentState{CurrentTerm: 5, VotedFor: NoVote}, 0)
	rep := r.reply(t, installArgs(4, Snapshot{Index: 9, Term: 4, Data: []byte("x")}))
	if rep.Success || rep.Term != 5 {
		t.Fatalf("reply = %+v, want refusal carrying term 5 so the sender steps down", rep)
	}
	if len(r.restored) != 0 || r.disk(t).Snapshot.Index != 0 || r.n.Status().Term != 5 {
		t.Fatal("a stale snapshot changed the node")
	}
}

func TestInstallSnapshotIsOnDiskBeforeTheReply(t *testing.T) {
	st := NewMemoryStorage()
	st.Save(PersistentState{VotedFor: NoVote})
	var seen *PersistentState
	tr := transportFunc(func(m Message) {
		if rep, ok := m.Payload.(AppendEntriesReply); ok && rep.Success {
			ps, _ := st.Load()
			seen = &ps
		}
	})
	n, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}, Seed: 1}, st, tr)
	if err != nil {
		t.Fatal(err)
	}
	n.Step(Message{From: 1, To: 0, Payload: installArgs(1, Snapshot{Index: 7, Term: 1, Data: []byte("d")})})
	if seen == nil || seen.Snapshot.Index != 7 || seen.CurrentTerm != 1 {
		t.Fatalf("at the moment the success reply was sent, the disk held %+v", seen)
	}
}

type transportFunc func(Message)

func (f transportFunc) Send(m Message) { f(m) }

func TestLeaderIgnoresAnInstallSnapshotInItsOwnTerm(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
	r.elect(t)
	term := r.n.Status().Term
	r.tr.sent = nil
	r.n.Step(Message{From: 1, To: 0, Payload: installArgs(term, Snapshot{Index: 50, Term: term, Data: []byte("evil")})})
	if len(r.tr.sent) != 0 || len(r.restored) != 0 || r.n.Status().Role != Leader || r.disk(t).Snapshot.Index != 0 {
		t.Fatal("a leader let another node install a snapshot into its own term")
	}
}

func TestAHigherTermInstallSnapshotDeposesALeader(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
	r.elect(t)
	term := r.n.Status().Term
	rep := r.reply(t, installArgs(term+1, Snapshot{Index: 50, Term: term + 1, Data: []byte("s")}))
	if !rep.Success || rep.Term != term+1 {
		t.Fatalf("reply = %+v", rep)
	}
	if st := r.n.Status(); st.Role != Follower || st.Term != term+1 || st.Leader != 1 {
		t.Fatalf("status = %+v, want follower of node 1 in term %d", st, term+1)
	}
}

// The leader may probe for a match inside what this node has compacted. It must
// be told to move on, not rejected (rejecting would send it back further).
func TestAppendEntriesPrevInsideTheSnapshotIsAnsweredWithTheCommitIndex(t *testing.T) {
	r := newSnapRig(t, PersistentState{
		CurrentTerm: 3, VotedFor: NoVote,
		Snapshot: Snapshot{Index: 10, Term: 2, Data: []byte("s")},
		Log:      cmdEntries(3, 11, "x", "y", "z"),
	}, 0)
	// Commit 11 and 12, so the commit index is ahead of the snapshot.
	r.reply(t, AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 10, PrevLogTerm: 2, Entries: cmdEntries(3, 11, "x", "y"), LeaderCommit: 12})

	rep := r.reply(t, AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 7, PrevLogTerm: 2, Entries: cmdEntries(2, 8, "p", "q")})
	if !rep.Success || rep.MatchIndex != 12 || rep.LastLogIndex != 13 {
		t.Fatalf("reply = %+v, want success with match 12 (the commit index) and last 13", rep)
	}
	if want := cmdEntries(3, 11, "x", "y", "z"); !reflect.DeepEqual(r.n.logCopy(), want) {
		t.Fatalf("log changed: %+v", r.n.logCopy())
	}
}

func TestInstallSnapshotCountsAsHearingFromTheLeader(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 0)
	for i := 0; i < 9; i++ {
		r.n.Tick() // one tick short of the election timeout
	}
	r.reply(t, installArgs(1, Snapshot{Index: 5, Term: 1, Data: []byte("s")}))
	r.tr.sent = nil
	for i := 0; i < 9; i++ {
		r.n.Tick()
	}
	if len(r.tr.sent) != 0 {
		t.Fatalf("the node started an election although a leader had just sent it a snapshot: %+v", r.tr.sent)
	}
}

// ---- InstallSnapshot: the leader ----

func leaderWithSnapshot(t *testing.T) *snapRig {
	t.Helper()
	r := newSnapRig(t, PersistentState{
		CurrentTerm: 3, VotedFor: NoVote,
		Snapshot: Snapshot{Index: 10, Term: 2, Data: []byte("state@10")},
		Log:      cmdEntries(3, 11, "a", "b", "c"),
	}, 0)
	r.elect(t)
	return r
}

func (r *snapRig) sendTo(peer NodeID, next Index) Message {
	r.tr.sent = nil
	r.n.mu.Lock()
	r.n.nextIndex[peer] = next
	r.n.sendAppendEntries(peer)
	r.n.mu.Unlock()
	if len(r.tr.sent) != 1 {
		panic(fmt.Sprintf("expected one message, got %d", len(r.tr.sent)))
	}
	return r.tr.sent[0]
}

func TestLeaderSendsTheSnapshotToAFollowerThatNeedsCompactedEntries(t *testing.T) {
	r := leaderWithSnapshot(t)
	term := r.n.Status().Term

	for _, next := range []Index{1, 5, 10} {
		r.n.mu.Lock()
		delete(r.n.snapSentAt, 1) // not under test here: see TestSnapshotIsNotResentOnEveryHeartbeat
		r.n.mu.Unlock()
		m := r.sendTo(1, next)
		is, ok := m.Payload.(InstallSnapshotArgs)
		if !ok {
			t.Fatalf("nextIndex %d: sent %T, want InstallSnapshotArgs", next, m.Payload)
		}
		if is.Term != term || is.LeaderID != 0 || is.Snapshot.Index != 10 || is.Snapshot.Term != 2 || string(is.Snapshot.Data) != "state@10" {
			t.Fatalf("nextIndex %d: sent %+v", next, is)
		}
	}
	// nextIndex 11 is the first entry after the snapshot: ordinary entries, prev = the snapshot's last.
	m := r.sendTo(1, 11)
	ae, ok := m.Payload.(AppendEntriesArgs)
	if !ok || ae.PrevLogIndex != 10 || ae.PrevLogTerm != 2 || len(ae.Entries) != 4 || ae.Entries[0].Index != 11 {
		t.Fatalf("nextIndex 11: sent %+v, want entries 11.. with prev (10, 2)", m.Payload)
	}
}

func TestLeaderMovesAFollowerOnAfterItAcknowledgesTheSnapshot(t *testing.T) {
	r := leaderWithSnapshot(t)
	term := r.n.Status().Term
	r.sendTo(1, 1)

	r.tr.sent = nil
	r.n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{Term: term, Success: true, MatchIndex: 10, LastLogIndex: 10}})
	r.n.mu.Lock()
	match, next := r.n.matchIndex[1], r.n.nextIndex[1]
	r.n.mu.Unlock()
	if match != 10 || next != 11 {
		t.Fatalf("matchIndex %d nextIndex %d, want 10 and 11", match, next)
	}
	if len(r.tr.sent) != 1 {
		t.Fatalf("sent %d messages, want the entries that follow the snapshot", len(r.tr.sent))
	}
	if ae, ok := r.tr.sent[0].Payload.(AppendEntriesArgs); !ok || ae.PrevLogIndex != 10 || len(ae.Entries) == 0 {
		t.Fatalf("sent %+v", r.tr.sent[0].Payload)
	}
}

// A snapshot can be big. Until the follower has had time to answer, the leader
// sends it empty heartbeats instead of the snapshot again.
func TestSnapshotIsNotResentOnEveryHeartbeat(t *testing.T) {
	r := leaderWithSnapshot(t)
	r.n.mu.Lock()
	r.n.nextIndex[1] = 3
	r.n.mu.Unlock()

	// What peer 1 is sent on each tick (the rig's election timeout is 10 ticks).
	var kinds []string
	for tick := 0; tick < 12; tick++ {
		r.tr.sent = nil
		r.n.Tick()
		for _, m := range r.tr.sent {
			if m.To != 1 {
				continue
			}
			switch p := m.Payload.(type) {
			case InstallSnapshotArgs:
				kinds = append(kinds, fmt.Sprintf("%d:snapshot", tick))
			case AppendEntriesArgs:
				if len(p.Entries) != 0 || p.PrevLogIndex != 10 || p.PrevLogTerm != 2 {
					t.Fatalf("tick %d: heartbeat %+v, want an empty AppendEntries with prev = the snapshot's last entry (10, 2)", tick, p)
				}
				kinds = append(kinds, fmt.Sprintf("%d:heartbeat", tick))
			}
		}
	}
	// Heartbeats go out every second tick. The snapshot goes out on the first and
	// is repeated only once an election timeout (10 ticks) has passed.
	want := []string{"1:snapshot", "3:heartbeat", "5:heartbeat", "7:heartbeat", "9:heartbeat", "11:snapshot"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("sent to peer 1: %v\nwant        %v", kinds, want)
	}
}

func TestAnAnswerEndsTheWaitForTheSnapshot(t *testing.T) {
	r := leaderWithSnapshot(t)
	term := r.n.Status().Term
	if _, ok := r.sendTo(1, 3).Payload.(InstallSnapshotArgs); !ok {
		t.Fatal("first send was not the snapshot")
	}
	// Peer 1 answers, but only up to 5: it still needs the snapshot, now.
	r.tr.sent = nil
	r.n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{Term: term, Success: true, MatchIndex: 5, LastLogIndex: 5}})
	if len(r.tr.sent) != 1 {
		t.Fatalf("sent %d messages after the answer", len(r.tr.sent))
	}
	if _, ok := r.tr.sent[0].Payload.(InstallSnapshotArgs); !ok {
		t.Fatalf("sent %T, want the snapshot straight away (nothing is pending any more)", r.tr.sent[0].Payload)
	}
}

func TestBackingUpPastTheSnapshotSwitchesToSendingIt(t *testing.T) {
	r := leaderWithSnapshot(t)
	term := r.n.Status().Term
	r.sendTo(1, 12)

	// The follower's log is empty, so it cannot match prev = 11.
	r.tr.sent = nil
	r.n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{Term: term, Success: false, LastLogIndex: 0}})
	if len(r.tr.sent) != 1 {
		t.Fatalf("sent %d messages", len(r.tr.sent))
	}
	if _, ok := r.tr.sent[0].Payload.(InstallSnapshotArgs); !ok {
		t.Fatalf("sent %T, want the snapshot", r.tr.sent[0].Payload)
	}
}

// A leader that compacts must still be able to commit: its own entries are
// counted from the log, not from the snapshot.
func TestLeaderKeepsCommittingAfterCompacting(t *testing.T) {
	r := newSnapRig(t, PersistentState{VotedFor: NoVote}, 2)
	r.elect(t)
	term := r.n.Status().Term
	for i := 0; i < 6; i++ {
		idx, _, ok := r.n.Propose([]byte{byte('a' + i)})
		if !ok {
			t.Fatal("propose refused")
		}
		r.n.Step(Message{From: 1, To: 0, Payload: AppendEntriesReply{Term: term, Success: true, MatchIndex: idx, LastLogIndex: idx}})
	}
	st := r.n.Status()
	if st.CommitIndex != 7 || st.SnapshotIndex < 6 {
		t.Fatalf("status = %+v, want commit 7 (no-op + 6 commands) and a snapshot near it", st)
	}
	if r.state != "abcdef" {
		t.Fatalf("state = %q", r.state)
	}
}

// ---- whole clusters ----

// Log compaction must change nothing a client or the safety checks can see.
func TestClusterWithSnapshotsStillReplicates(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 30; seed++ {
			cfg := baseNet
			cfg.DropRate = 0.15
			c := newClusterWithSnapshots(t, size, seed, cfg, 5)
			c.run(150)
			for i := 0; i < 60; i++ {
				c.propose()
				c.run(4)
			}
			c.net.SetDropRate(0)
			c.run(300)
			c.requireStable()
			c.requireConverged()

			compacted := 0
			for _, st := range c.statuses() {
				if st.SnapshotIndex > 0 {
					compacted++
				}
			}
			if compacted < size/2+1 {
				t.Fatalf("seed %d: only %d of %d nodes took a snapshot after 60 commands with a threshold of 5", seed, compacted, size)
			}
		}
	}
}

// A follower that is cut off while the rest compact their logs cannot be caught
// up with entries any more. It must be brought forward with a snapshot, end up
// in the same state as everyone else, and not have applied anything twice.
func TestLaggingFollowerCatchesUpFromASnapshot(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 30; seed++ {
			c := newClusterWithSnapshots(t, size, seed, baseNet, 5)
			c.run(200)
			leader := c.requireStable()

			lagging := int((leader.ID + 1) % NodeID(size))
			var rest []int
			for id := range c.nodes {
				if id != lagging {
					rest = append(rest, id)
				}
			}
			c.net.Partition([]int{lagging}, rest)
			for i := 0; i < 40; i++ {
				c.propose()
				c.run(4)
			}
			if got := c.nodes[lagging].Status().LastLogIndex; got >= c.nodes[leader.ID].Status().SnapshotIndex {
				t.Fatalf("seed %d: the follower (at %d) is not behind the leader's snapshot; the scenario proves nothing", seed, got)
			}

			sentBefore, restoresBefore := c.snapshotsSent, c.restores
			c.net.Heal()
			c.run(300)
			c.requireStable()
			c.requireConverged()

			sent := c.snapshotsSent - sentBefore
			if sent == 0 || c.restores == restoresBefore {
				t.Fatalf("seed %d: the follower caught up without a snapshot (sent %d, restores %d)", seed, sent, c.restores-restoresBefore)
			}
			if sent > 4 {
				t.Fatalf("seed %d: the snapshot was sent %d times to catch one follower up", seed, sent)
			}
		}
	}
}

func TestSafetyUnderRandomCrashesWithSnapshots(t *testing.T) {
	restores := 0
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 20; seed++ {
			cfg := baseNet
			cfg.DropRate = 0.1
			c := newClusterWithSnapshots(t, size, seed, cfg, 4)
			c.crashRate = 0.01

			c.run(100)
			for round := 0; round < 100; round++ {
				c.propose()
				c.run(5)
			}
			c.crashRate = 0
			for id := range c.storages {
				c.storages[id].disarm()
			}
			c.net.SetDropRate(0)
			c.run(600)
			c.requireAllUp()
			c.propose()
			c.run(200)
			c.requireStable()
			c.requireConverged()
			restores += c.restores
		}
	}
	if restores == 0 {
		t.Fatal("no node was ever restored from a snapshot; the test proves nothing")
	}
}

func TestSafetyUnderCrashesAndPartitionsWithSnapshots(t *testing.T) {
	installs := 0
	for seed := int64(1); seed <= 30; seed++ {
		c := newClusterWithSnapshots(t, 5, seed, baseNet, 4)
		c.crashRate = 0.01
		c.run(100)
		for round := 0; round < 40; round++ {
			if c.rng.Intn(3) == 0 {
				c.net.Heal()
			} else {
				var a, b []int
				for id := range c.nodes {
					if c.rng.Intn(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.net.Partition(a, b)
			}
			for i := 0; i < 5; i++ {
				c.propose()
				c.run(8)
			}
		}
		c.crashRate = 0
		for id := range c.storages {
			c.storages[id].disarm()
		}
		c.net.Heal()
		c.run(800)
		c.requireAllUp()
		c.propose()
		c.run(200)
		c.requireStable()
		c.requireConverged()
		installs += c.snapshotsSent
	}
	if installs == 0 {
		t.Fatal("no InstallSnapshot was ever sent; the test proves nothing")
	}
}
