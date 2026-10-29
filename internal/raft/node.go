package raft

import (
	"fmt"
	"math/rand"
	"sync"
)

// Node is a single Raft participant.
//
// Implemented so far: leader election (RequestVote, terms, randomized
// election timeouts) and leader heartbeats. Heartbeats are AppendEntries
// messages with no entries; their log-consistency checks arrive in phase 2.
//
//	Phase 1: elections                         <- done
//	Phase 2: log replication (AppendEntries, commit index, Apply)
//	Phase 3: persist the log through Storage before replying
//
// All methods are safe for concurrent use. Transport.Send is called while the
// node's lock is held, so it must not block or call back into the node.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	storage   Storage
	transport Transport
	rng       *rand.Rand // randomizes election timeouts

	// Persistent state (restored from Storage on start).
	currentTerm Term
	votedFor    NodeID
	log         []LogEntry

	// Volatile state.
	role        Role
	leaderID    NodeID // who this node believes is leader, or NoLeader
	commitIndex Index
	lastApplied Index

	votes map[NodeID]bool // candidate only: who has voted for us this term

	// Logical timers, measured in ticks.
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
}

// Status is a point-in-time snapshot of a node, for tests and debugging.
type Status struct {
	ID          NodeID
	Role        Role
	Term        Term
	Leader      NodeID // NoLeader if unknown
	CommitIndex Index
}

// NewNode creates a node and restores its persistent state from storage.
// A new node always starts as a follower.
func NewNode(cfg Config, storage Storage, transport Transport) (*Node, error) {
	if !contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("raft: node %d is not in peers %v", cfg.ID, cfg.Peers)
	}
	if cfg.ElectionTickMin <= 0 {
		cfg.ElectionTickMin = 10
	}
	if cfg.ElectionTickMax < cfg.ElectionTickMin {
		cfg.ElectionTickMax = 2 * cfg.ElectionTickMin
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 2
	}

	ps, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
	}

	n := &Node{
		cfg:         cfg,
		storage:     storage,
		transport:   transport,
		rng:         rand.New(rand.NewSource(cfg.Seed*1_000_003 + int64(cfg.ID))),
		currentTerm: ps.CurrentTerm,
		votedFor:    ps.VotedFor,
		log:         ps.Log,
		role:        Follower,
		leaderID:    NoLeader,
	}
	n.resetElectionTimer()
	return n, nil
}

// Tick advances the node's logical clock by one tick. The driver (simnet in
// tests, a real timer in production) calls it at a fixed rate.
//
// A leader sends heartbeats on a fixed schedule. Everyone else counts ticks
// since it last heard from a leader (or granted a vote) and starts an
// election when that reaches its randomized timeout.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
		return
	}

	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.startElection()
	}
}

// Step delivers an incoming message to the node. Payloads are the RPC types
// by value (not pointers). Unknown payloads are ignored.
func (n *Node) Step(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch p := msg.Payload.(type) {
	case RequestVoteArgs:
		n.handleRequestVote(msg.From, p)
	case RequestVoteReply:
		n.handleRequestVoteReply(msg.From, p)
	case AppendEntriesArgs:
		n.handleAppendEntries(msg.From, p)
	case AppendEntriesReply:
		n.handleAppendEntriesReply(msg.From, p)
	}
}

// Propose asks the node to append a command to the replicated log. It returns
// the index and term the entry will have if committed, and whether this node
// believes it is the leader. A follower returns false and the caller should
// retry against the leader.
func (n *Node) Propose(command []byte) (Index, Term, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, n.currentTerm, false
	}
	// TODO(phase 2): append to the log, persist, replicate.
	return 0, n.currentTerm, false
}

// Status returns a snapshot of the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.cfg.ID,
		Role:        n.role,
		Term:        n.currentTerm,
		Leader:      n.leaderID,
		CommitIndex: n.commitIndex,
	}
}

// ---- elections ----

// startElection moves to a new term as a candidate, votes for itself, and
// asks everyone else for their vote. Figure 2, "Rules for Servers: Candidates".
func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.leaderID = NoLeader
	n.votedFor = n.cfg.ID
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.persist()
	n.resetElectionTimer()

	// A single-node cluster wins immediately.
	if len(n.votes) >= n.majority() {
		n.becomeLeader()
		return
	}

	lastIndex, lastTerm := n.lastLog()
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.send(peer, RequestVoteArgs{
			Term:         n.currentTerm,
			CandidateID:  n.cfg.ID,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
	}
}

// handleRequestVote implements the RequestVote receiver rules (Figure 2).
func (n *Node) handleRequestVote(from NodeID, args RequestVoteArgs) {
	// Rule for all servers: a higher term makes us a follower of that term.
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	// Grant only if: the candidate's term is current, we have not voted for
	// someone else in this term, and the candidate's log is at least as
	// up to date as ours. The vote is persisted before the reply is sent,
	// so a crash and restart cannot make us vote twice in one term.
	grant := args.Term == n.currentTerm &&
		(n.votedFor == NoVote || n.votedFor == args.CandidateID) &&
		n.candidateUpToDate(args)

	if grant {
		if n.votedFor != args.CandidateID {
			n.votedFor = args.CandidateID
			n.persist()
		}
		n.electionElapsed = 0 // granting a vote counts as hearing from a viable leader
	}

	n.send(from, RequestVoteReply{Term: n.currentTerm, VoteGranted: grant})
}

func (n *Node) handleRequestVoteReply(from NodeID, reply RequestVoteReply) {
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	// Ignore replies that are stale (older term) or that arrive after we have
	// already won or lost this election.
	if n.role != Candidate || reply.Term != n.currentTerm {
		return
	}
	if reply.VoteGranted {
		n.votes[from] = true
		if len(n.votes) >= n.majority() {
			n.becomeLeader()
		}
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.votes = nil
	n.heartbeatElapsed = 0
	// Announce leadership immediately so followers reset their timers.
	n.broadcastHeartbeat()
	// TODO(phase 2): initialize nextIndex and matchIndex for every peer.
}

// stepDown makes the node a follower in the given term (which must be at
// least the current term). If the term is newer, the old vote no longer
// applies.
func (n *Node) stepDown(term Term) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = NoVote
		n.persist()
	}
	n.role = Follower
	n.leaderID = NoLeader
	n.votes = nil
	n.resetElectionTimer()
}

// candidateUpToDate is the "election restriction" (Raft paper section 5.4.1):
// a candidate's log must be at least as up to date as the voter's, comparing
// the term of the last entry first and the log length second.
func (n *Node) candidateUpToDate(args RequestVoteArgs) bool {
	lastIndex, lastTerm := n.lastLog()
	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}
	return args.LastLogIndex >= lastIndex
}

// ---- heartbeats (AppendEntries with no entries) ----

func (n *Node) broadcastHeartbeat() {
	lastIndex, lastTerm := n.lastLog()
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		// TODO(phase 2): per-peer PrevLogIndex/PrevLogTerm from nextIndex, and
		// real entries.
		n.send(peer, AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.cfg.ID,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  lastTerm,
			LeaderCommit: n.commitIndex,
		})
	}
}

func (n *Node) handleAppendEntries(from NodeID, args AppendEntriesArgs) {
	// A stale leader: reject and tell it the current term so it steps down.
	if args.Term < n.currentTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: false})
		return
	}

	// The sender is the legitimate leader of this term.
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	} else if n.role == Candidate {
		// Someone else won this term's election: stop campaigning.
		n.role = Follower
		n.votes = nil
	}
	n.leaderID = args.LeaderID
	n.electionElapsed = 0

	// TODO(phase 2): log consistency check (PrevLogIndex/PrevLogTerm), append
	// entries, advance commitIndex. For now every current-term heartbeat succeeds.
	n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true})
}

func (n *Node) handleAppendEntriesReply(from NodeID, reply AppendEntriesReply) {
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
	}
	// TODO(phase 2): update nextIndex/matchIndex and advance commitIndex.
}

// ---- helpers ----

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) lastLog() (Index, Term) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.cfg.ElectionTickMax - n.cfg.ElectionTickMin + 1
	n.electionTimeout = n.cfg.ElectionTickMin + n.rng.Intn(span)
}

func (n *Node) send(to NodeID, payload any) {
	n.transport.Send(Message{From: n.cfg.ID, To: to, Payload: payload})
}

// persist writes term, vote and log to stable storage. Raft cannot safely
// continue if it fails to remember a vote or a term, so a storage failure is
// fatal.
func (n *Node) persist() {
	err := n.storage.Save(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
	if err != nil {
		panic(fmt.Sprintf("raft: node %d cannot persist state: %v", n.cfg.ID, err))
	}
}

func contains(ids []NodeID, id NodeID) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
