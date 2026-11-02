// Package rpc is the gRPC layer: it carries Raft messages between nodes and
// exposes the key-value API to clients. All behaviour lives in internal/raft
// and internal/server; this package only translates and moves bytes.
package rpc

import (
	"fmt"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

// ToProto converts a Raft message to its wire form.
func ToProto(m raft.Message) (*pb.RaftMessage, error) {
	out := &pb.RaftMessage{From: int32(m.From), To: int32(m.To)}
	switch p := m.Payload.(type) {
	case raft.RequestVoteArgs:
		out.Payload = &pb.RaftMessage_RequestVote{RequestVote: &pb.RequestVoteRequest{
			Term:         uint64(p.Term),
			CandidateId:  int32(p.CandidateID),
			LastLogIndex: uint64(p.LastLogIndex),
			LastLogTerm:  uint64(p.LastLogTerm),
		}}
	case raft.RequestVoteReply:
		out.Payload = &pb.RaftMessage_RequestVoteReply{RequestVoteReply: &pb.RequestVoteResponse{
			Term:        uint64(p.Term),
			VoteGranted: p.VoteGranted,
		}}
	case raft.PreVoteArgs:
		out.Payload = &pb.RaftMessage_PreVote{PreVote: &pb.PreVoteRequest{
			Term:         uint64(p.Term),
			CandidateId:  int32(p.CandidateID),
			LastLogIndex: uint64(p.LastLogIndex),
			LastLogTerm:  uint64(p.LastLogTerm),
		}}
	case raft.PreVoteReply:
		out.Payload = &pb.RaftMessage_PreVoteReply{PreVoteReply: &pb.PreVoteResponse{
			Term:        uint64(p.Term),
			PreVoteTerm: uint64(p.PreVoteTerm),
			VoteGranted: p.VoteGranted,
		}}
	case raft.AppendEntriesArgs:
		entries := make([]*pb.LogEntry, len(p.Entries))
		for i, e := range p.Entries {
			entries[i] = &pb.LogEntry{Term: uint64(e.Term), Index: uint64(e.Index), Command: e.Command}
		}
		out.Payload = &pb.RaftMessage_AppendEntries{AppendEntries: &pb.AppendEntriesRequest{
			Term:         uint64(p.Term),
			LeaderId:     int32(p.LeaderID),
			PrevLogIndex: uint64(p.PrevLogIndex),
			PrevLogTerm:  uint64(p.PrevLogTerm),
			Entries:      entries,
			LeaderCommit: uint64(p.LeaderCommit),
		}}
	case raft.AppendEntriesReply:
		out.Payload = &pb.RaftMessage_AppendEntriesReply{AppendEntriesReply: &pb.AppendEntriesResponse{
			Term:         uint64(p.Term),
			Success:      p.Success,
			MatchIndex:   uint64(p.MatchIndex),
			LastLogIndex: uint64(p.LastLogIndex),
		}}
	default:
		return nil, fmt.Errorf("rpc: cannot send payload of type %T", m.Payload)
	}
	return out, nil
}

// FromProto converts a wire message back to a Raft message.
func FromProto(m *pb.RaftMessage) (raft.Message, error) {
	out := raft.Message{From: raft.NodeID(m.GetFrom()), To: raft.NodeID(m.GetTo())}
	switch p := m.GetPayload().(type) {
	case *pb.RaftMessage_RequestVote:
		r := p.RequestVote
		out.Payload = raft.RequestVoteArgs{
			Term:         raft.Term(r.GetTerm()),
			CandidateID:  raft.NodeID(r.GetCandidateId()),
			LastLogIndex: raft.Index(r.GetLastLogIndex()),
			LastLogTerm:  raft.Term(r.GetLastLogTerm()),
		}
	case *pb.RaftMessage_RequestVoteReply:
		r := p.RequestVoteReply
		out.Payload = raft.RequestVoteReply{
			Term:        raft.Term(r.GetTerm()),
			VoteGranted: r.GetVoteGranted(),
		}
	case *pb.RaftMessage_PreVote:
		r := p.PreVote
		out.Payload = raft.PreVoteArgs{
			Term:         raft.Term(r.GetTerm()),
			CandidateID:  raft.NodeID(r.GetCandidateId()),
			LastLogIndex: raft.Index(r.GetLastLogIndex()),
			LastLogTerm:  raft.Term(r.GetLastLogTerm()),
		}
	case *pb.RaftMessage_PreVoteReply:
		r := p.PreVoteReply
		out.Payload = raft.PreVoteReply{
			Term:        raft.Term(r.GetTerm()),
			PreVoteTerm: raft.Term(r.GetPreVoteTerm()),
			VoteGranted: r.GetVoteGranted(),
		}
	case *pb.RaftMessage_AppendEntries:
		r := p.AppendEntries
		var entries []raft.LogEntry
		for _, e := range r.GetEntries() {
			entries = append(entries, raft.LogEntry{
				Term:    raft.Term(e.GetTerm()),
				Index:   raft.Index(e.GetIndex()),
				Command: e.GetCommand(),
			})
		}
		out.Payload = raft.AppendEntriesArgs{
			Term:         raft.Term(r.GetTerm()),
			LeaderID:     raft.NodeID(r.GetLeaderId()),
			PrevLogIndex: raft.Index(r.GetPrevLogIndex()),
			PrevLogTerm:  raft.Term(r.GetPrevLogTerm()),
			Entries:      entries,
			LeaderCommit: raft.Index(r.GetLeaderCommit()),
		}
	case *pb.RaftMessage_AppendEntriesReply:
		r := p.AppendEntriesReply
		out.Payload = raft.AppendEntriesReply{
			Term:         raft.Term(r.GetTerm()),
			Success:      r.GetSuccess(),
			MatchIndex:   raft.Index(r.GetMatchIndex()),
			LastLogIndex: raft.Index(r.GetLastLogIndex()),
		}
	default:
		return raft.Message{}, fmt.Errorf("rpc: message has no payload")
	}
	return out, nil
}
