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

// Snapshot is a compacted prefix of the log: the state machine's state after
// applying every entry up to and including Index, and the term of that entry.
// Index 0 means "no snapshot". Data is opaque to Raft; the state machine
// produces it and consumes it.
type Snapshot struct {
	Index Index
	Term  Term
	Data  []byte
}

// IsNoOp reports whether the entry carries no client command. A new leader
// appends one no-op entry at the start of its term: committing it commits
// every earlier entry too, which a leader may not do by counting replicas
// alone (Raft paper, section 5.4.2). State machines must ignore no-ops.
func (e LogEntry) IsNoOp() bool { return len(e.Command) == 0 }

// Config configures a Node. Timeouts are measured in ticks, not wall-clock
// time, so tests control time completely.
type Config struct {
	ID    NodeID
	Peers []NodeID // every node in the cluster, including this one

	ElectionTickMin int // inclusive lower bound for the randomized election timeout
	ElectionTickMax int // inclusive upper bound
	HeartbeatTick   int // how often a leader sends heartbeats

	Seed int64 // seeds the election-timeout randomness

	// Apply is called, in log order, exactly once for every committed entry,
	// including no-op entries (see LogEntry.IsNoOp). It runs with the node's
	// lock held, so it must be quick and must not call back into the node.
	Apply func(LogEntry)

	// Log compaction. When SnapshotEvery is positive, a node that has applied
	// that many entries since its last snapshot asks the state machine for a
	// snapshot, saves it, and drops the log entries it covers. Zero disables
	// compaction. Each node compacts on its own, without coordinating.
	SnapshotEvery int
	// Snapshot returns the state machine's state after every entry passed to
	// Apply so far (and nothing later). Required if SnapshotEvery is positive.
	// Like Apply, it runs with the node's lock held and must not call back into
	// the node.
	Snapshot func() []byte
	// Restore replaces the state machine's state with a snapshot's data (see
	// Snapshot), on this node or on the leader. It is called when the node
	// starts with a saved snapshot (before any Apply) and when a leader's
	// snapshot is installed. After it returns, the next entry passed to Apply
	// is snap.Index+1. Same restrictions as Apply. May be nil if the node has no
	// state machine to restore (some tests).
	Restore func(snap Snapshot)
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

// PreVoteArgs asks "would you vote for me if I started an election?" without
// anyone changing term or vote (Raft thesis, section 9.6). Term is the term
// the candidate WOULD use (its current term plus one); its own term is not
// changed by asking.
type PreVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

// PreVoteReply answers a PreVoteArgs. Term is the replier's current term (so a
// candidate that is behind can catch up) and PreVoteTerm echoes the asked-about
// term so a late reply to an old round is ignored.
type PreVoteReply struct {
	Term        Term
	PreVoteTerm Term
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

// AppendEntriesReply carries two fields beyond Figure 2:
//
//   - MatchIndex (on success) is the highest index the follower now knows it
//     shares with the leader. The leader uses it instead of guessing from the
//     request it sent, so duplicated or reordered replies cannot corrupt its
//     bookkeeping.
//   - LastLogIndex (always) is the follower's last log index. On a failure the
//     leader uses it to skip back past a follower whose log is much shorter,
//     instead of retrying one index at a time.
type AppendEntriesReply struct {
	Term         Term
	Success      bool
	MatchIndex   Index
	LastLogIndex Index
}

// InstallSnapshotArgs is how a leader brings a follower forward when the entries
// the follower needs have been compacted out of the leader's log. The snapshot
// travels whole, in one message (Raft paper section 7 describes chunking; this
// implementation does not do it). The follower answers with an
// AppendEntriesReply: success and MatchIndex = Snapshot.Index, the same way it
// acknowledges entries. Snapshot.Data must be treated as immutable.
type InstallSnapshotArgs struct {
	Term     Term
	LeaderID NodeID
	Snapshot Snapshot
}

// Message is the envelope the Transport carries. Payload is one of the RPC
// types above (by value).
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
