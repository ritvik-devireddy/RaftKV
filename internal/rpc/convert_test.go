package rpc

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

// Every message kind must survive conversion to the wire form, serialization,
// and back, unchanged.
func TestMessagesRoundTripThroughProtobuf(t *testing.T) {
	msgs := []raft.Message{
		{From: 1, To: 2, Payload: raft.RequestVoteArgs{Term: 7, CandidateID: 1, LastLogIndex: 9, LastLogTerm: 6}},
		{From: 2, To: 1, Payload: raft.RequestVoteReply{Term: 7, VoteGranted: true}},
		{From: 0, To: 1, Payload: raft.AppendEntriesArgs{
			Term: 3, LeaderID: 0, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4,
			Entries: []raft.LogEntry{{Term: 3, Index: 5, Command: []byte("x")}, {Term: 3, Index: 6}},
		}},
		{From: 0, To: 1, Payload: raft.AppendEntriesArgs{Term: 3, LeaderID: 0}}, // heartbeat
		{From: 1, To: 0, Payload: raft.AppendEntriesReply{Term: 3, Success: true, MatchIndex: 6, LastLogIndex: 6}},
		{From: 1, To: 0, Payload: raft.AppendEntriesReply{Term: 3, Success: false, LastLogIndex: 2}},
	}
	for _, m := range msgs {
		wire, err := ToProto(m)
		if err != nil {
			t.Fatal(err)
		}
		b, err := proto.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		back := &pb.RaftMessage{}
		if err := proto.Unmarshal(b, back); err != nil {
			t.Fatal(err)
		}
		got, err := FromProto(back)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(normalize(got), normalize(m)) {
			t.Fatalf("round trip changed the message:\n got  %+v\n want %+v", got, m)
		}
	}
}

// normalize makes nil and empty entry slices and commands compare equal.
func normalize(m raft.Message) raft.Message {
	if a, ok := m.Payload.(raft.AppendEntriesArgs); ok {
		if len(a.Entries) == 0 {
			a.Entries = nil
		}
		for i := range a.Entries {
			if len(a.Entries[i].Command) == 0 {
				a.Entries[i].Command = nil
			}
		}
		m.Payload = a
	}
	return m
}

func TestToProtoRejectsUnknownPayloads(t *testing.T) {
	if _, err := ToProto(raft.Message{Payload: "nope"}); err == nil {
		t.Fatal("an unknown payload type was accepted")
	}
}
