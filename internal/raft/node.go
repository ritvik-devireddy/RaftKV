package raft

import (
	"fmt"
	"sync"
)

// Node is a single Raft participant.
//
// The consensus logic is not written yet. Build it in this order, following
// Figure 2 of the Raft paper exactly (see docs/design.md):
//
//	Phase 1: Tick + Step handle elections (RequestVote, terms, timeouts)
//	Phase 2: log replication (AppendEntries, commit index, Apply)
//	Phase 3: persist term/vote/log through Storage before replying
type Node struct {
	mu        sync.Mutex
	cfg       Config
	storage   Storage
	transport Transport

	// Persistent state (restored from Storage on start).
	currentTerm Term
	votedFor    NodeID
	log         []LogEntry

	// Volatile state.
	role        Role
	commitIndex Index
	lastApplied Index
}

// Status is a point-in-time snapshot of a node, for tests and debugging.
type Status struct {
	ID          NodeID
	Role        Role
	Term        Term
	CommitIndex Index
}

// NewNode creates a node and restores its persistent state from storage.
// A new node always starts as a follower.
func NewNode(cfg Config, storage Storage, transport Transport) (*Node, error) {
	ps, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
	}
	return &Node{
		cfg:         cfg,
		storage:     storage,
		transport:   transport,
		currentTerm: ps.CurrentTerm,
		votedFor:    ps.VotedFor,
		log:         ps.Log,
		role:        Follower,
	}, nil
}

// Tick advances the node's logical clock by one tick. The driver (simnet in
// tests, a real timer in production) calls it at a fixed rate.
func (n *Node) Tick() {
	// TODO(phase 1): election timeout, leader heartbeats.
}

// Step delivers an incoming message to the node.
func (n *Node) Step(msg Message) {
	// TODO(phase 1): handle RequestVote and its reply.
	// TODO(phase 2): handle AppendEntries and its reply.
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
		CommitIndex: n.commitIndex,
	}
}
