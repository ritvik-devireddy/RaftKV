package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// The rule behind Raft's persistence requirement: a reply that depends on some
// state must not leave the node before that state is on disk. If it did, a
// crash right after sending would let the node forget something it had already
// promised, for example a vote.
//
// These tests hand a node a message, crash its storage at each operation in
// turn (before or after the operation reaches the disk), and check that every
// reply that did get sent is backed by what is on disk.

type orderScenario struct {
	name  string
	start PersistentState
	msg   Message
	// check inspects the replies that were sent and the storage afterwards.
	check func(t *testing.T, sent []Message, disk PersistentState)
}

func orderScenarios() []orderScenario {
	log3 := []LogEntry{entry(1, 1, "a"), entry(1, 2, "b")}
	return []orderScenario{
		{
			name:  "vote in a newer term",
			start: PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log3},
			msg:   Message{From: 1, To: 0, Payload: RequestVoteArgs{Term: 3, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 1}},
			check: func(t *testing.T, sent []Message, disk PersistentState) {
				for _, m := range sent {
					r := m.Payload.(RequestVoteReply)
					if r.VoteGranted && (disk.VotedFor != 1 || disk.CurrentTerm != r.Term) {
						t.Fatalf("granted a vote that is not on disk: reply %+v, disk term=%d vote=%d", r, disk.CurrentTerm, disk.VotedFor)
					}
					if r.Term > disk.CurrentTerm {
						t.Fatalf("replied with term %d but disk has term %d", r.Term, disk.CurrentTerm)
					}
				}
			},
		},
		{
			name:  "append after a conflict",
			start: PersistentState{CurrentTerm: 1, VotedFor: NoVote, Log: log3},
			msg: Message{From: 1, To: 0, Payload: AppendEntriesArgs{
				Term: 2, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1,
				Entries: []LogEntry{entry(2, 2, "B"), entry(2, 3, "C")},
			}},
			check: func(t *testing.T, sent []Message, disk PersistentState) {
				for _, m := range sent {
					r := m.Payload.(AppendEntriesReply)
					if r.Term > disk.CurrentTerm {
						t.Fatalf("replied with term %d but disk has term %d", r.Term, disk.CurrentTerm)
					}
					if r.Success {
						want := []LogEntry{entry(1, 1, "a"), entry(2, 2, "B"), entry(2, 3, "C")}
						if !reflect.DeepEqual(disk.Log, want) {
							t.Fatalf("acknowledged entries that are not on disk: %+v", disk.Log)
						}
					}
				}
			},
		},
		{
			name:  "plain append",
			start: PersistentState{CurrentTerm: 2, VotedFor: NoVote, Log: log3},
			msg: Message{From: 1, To: 0, Payload: AppendEntriesArgs{
				Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1,
				Entries: []LogEntry{entry(2, 3, "c")},
			}},
			check: func(t *testing.T, sent []Message, disk PersistentState) {
				for _, m := range sent {
					if r := m.Payload.(AppendEntriesReply); r.Success && len(disk.Log) < 3 {
						t.Fatalf("acknowledged an entry that is not on disk: %+v", disk.Log)
					}
				}
			},
		},
	}
}

func TestRepliesNeverOutrunTheDisk(t *testing.T) {
	for _, sc := range orderScenarios() {
		for crashOp := 0; crashOp <= 4; crashOp++ { // 0 = no crash
			for _, applyOp := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/crash-op-%d/applied=%v", sc.name, crashOp, applyOp), func(t *testing.T) {
					st := newCrashStorage()
					if err := st.Save(sc.start); err != nil {
						t.Fatal(err)
					}
					tr := &recTransport{}
					n, err := NewNode(Config{ID: 0, Peers: []NodeID{0, 1, 2}, Seed: 1}, st, tr)
					if err != nil {
						t.Fatal(err)
					}
					if crashOp > 0 {
						st.armIn(crashOp, applyOp)
					}

					func() {
						defer func() {
							if r := recover(); r != nil {
								if err, ok := r.(error); !ok || !errors.Is(err, errCrash) {
									panic(r)
								}
							}
						}()
						n.Step(sc.msg)
					}()

					disk, _ := st.Load()
					sc.check(t, tr.sent, disk)
				})
			}
		}
	}
}
