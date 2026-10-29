// Package raft will hold the consensus core.
//
// It is deliberately independent of the network and the disk:
//   - time advances only when the driver calls Node.Tick,
//   - messages arrive only through Node.Step,
//   - messages leave only through the Transport interface,
//   - state is persisted only through the Storage interface.
//
// That makes a whole cluster runnable single-threaded under internal/simnet
// with a seeded random number generator, so any failure can be replayed
// exactly. Field names follow Figure 2 of the Raft paper.
package raft

// NodeID identifies a node in the cluster.
type NodeID int

// NoVote is the VotedFor value meaning "has not voted in this term".
const NoVote NodeID = -1

// NoLeader is the leader ID meaning "this node does not know who the leader is".
const NoLeader NodeID = -1

// Term is a Raft election term.
type Term uint64

// Index is a position in the replicated log. Index 0 means "no entry"; the
// first real entry has index 1.
type Index uint64

// Role is the state a node is currently in.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one replicated command.
type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte
}

// Config configures a Node. Timeouts are measured in ticks, not wall-clock
// time, so tests control time completely.
type Config struct {
	ID    NodeID
	Peers []NodeID // every node in the cluster, including this one

	ElectionTickMin int // inclusive lower bound for the randomized election timeout
	ElectionTickMax int // inclusive upper bound
	HeartbeatTick   int // how often a leader sends heartbeats

	Seed int64 // seeds the election-timeout randomness

	// Apply is called, in log order, once for every committed entry.
	Apply func(LogEntry)
}

// ---- RPC payloads (Raft paper, Figure 2) ----

type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
}

// Message is the envelope the Transport carries. Payload is one of the four
// RPC types above.
type Message struct {
	From    NodeID
	To      NodeID
	Payload any
}

// Transport sends a message to another node. Delivery is best-effort: the
// network may drop, delay, duplicate or reorder messages, and Raft must stay
// correct regardless. Send is called with the node's lock held, so it must
// not block and must not call back into the node; queue the message and return.
type Transport interface {
	Send(msg Message)
}
