package raft

import (
	"fmt"
	"math/rand"
	"sync"
)

// maxEntriesPerMessage bounds how many log entries one AppendEntries carries,
// so a far-behind follower catches up in several bounded messages.
const maxEntriesPerMessage = 64

// Node is a single Raft participant.
//
// Implemented so far:
//
//	Phase 1: leader election (RequestVote, terms, randomized timeouts)  <- done
//	Phase 2: log replication (AppendEntries, commit index, Apply)       <- done
//	Phase 3: durable storage behind the Storage interface (WAL)
//
// Log indexes are contiguous and start at 1, so the entry with index i is
// log[i-1]. (Snapshots, which would change that, come later.)
//
// All methods are safe for concurrent use. Transport.Send and Config.Apply
// are called while the node's lock is held, so neither may block or call back
// into the node.
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
	commitIndex Index  // highest log index known to be committed
	lastApplied Index  // highest log index passed to Apply

	votes map[NodeID]bool // candidate only: who has voted for us this term

	// Leader only, reinitialized after every election (Figure 2).
	nextIndex  map[NodeID]Index // next log index to send to each peer
	matchIndex map[NodeID]Index // highest index known to be replicated on each peer

	// Logical timers, measured in ticks.
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
}

// Status is a point-in-time snapshot of a node, for tests and debugging.
type Status struct {
	ID           NodeID
	Role         Role
	Term         Term
	Leader       NodeID // NoLeader if unknown
	CommitIndex  Index
	LastApplied  Index
	LastLogIndex Index
}

// NewNode creates a node and restores its persistent state from storage.
// A new node always starts as a follower. Entries that were committed before
// a restart are re-applied once the node learns the commit index again.
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
// A leader sends AppendEntries to every follower on a fixed schedule; with no
// new entries it acts as a heartbeat, and with missing entries it doubles as a
// retransmission. Everyone else counts ticks since it last heard from a leader
// (or granted a vote) and starts an election when that reaches its randomized
// timeout.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.replicateAll()
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

// Propose asks the node to append a command to the replicated log. If this
// node is the leader it appends the command and starts replicating it,
// returning the index and term the entry will have if it commits. The entry
// is NOT committed when Propose returns: watch for it in Apply. If the leader
// loses its leadership first, the entry may be overwritten and never commit.
//
// ok is false if this node is not the leader, or if the command is empty
// (an empty command is indistinguishable from a no-op). The caller should
// retry against the leader.
func (n *Node) Propose(command []byte) (index Index, term Term, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader || len(command) == 0 {
		return 0, n.currentTerm, false
	}
	e := n.appendLocal(append([]byte(nil), command...))
	n.advanceCommit() // a single-node cluster commits immediately
	n.replicateAll()
	return e.Index, e.Term, true
}

// Status returns a snapshot of the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	last, _ := n.lastLog()
	return Status{
		ID:           n.cfg.ID,
		Role:         n.role,
		Term:         n.currentTerm,
		Leader:       n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: last,
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

	// Figure 2: nextIndex starts just past our last entry, matchIndex at zero.
	last, _ := n.lastLog()
	n.nextIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	n.matchIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			n.nextIndex[peer] = last + 1
			n.matchIndex[peer] = 0
		}
	}

	// Append a no-op in the new term. Once it commits, every earlier entry in
	// the log is committed with it (section 5.4.2).
	n.appendLocal(nil)
	n.advanceCommit()
	n.replicateAll()
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
	n.nextIndex = nil
	n.matchIndex = nil
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

// ---- replication: leader side ----

// replicateAll sends every follower whatever log entries it is missing (or an
// empty heartbeat if it is up to date).
func (n *Node) replicateAll() {
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			n.sendAppendEntries(peer)
		}
	}
}

func (n *Node) sendAppendEntries(peer NodeID) {
	next := n.nextIndex[peer]
	prev := next - 1
	last, _ := n.lastLog()

	var entries []LogEntry
	if next <= last {
		end := min(last, next+maxEntriesPerMessage-1)
		// Copy: in tests, nodes share memory, and a follower truncating its log
		// must never be able to modify ours.
		entries = append([]LogEntry(nil), n.log[next-1:end]...)
	}

	n.send(peer, AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) handleAppendEntriesReply(from NodeID, reply AppendEntriesReply) {
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	// Ignore replies from an older term, or that arrive after we stopped being leader.
	if n.role != Leader || reply.Term != n.currentTerm {
		return
	}

	if reply.Success {
		// matchIndex only moves forward, so a duplicated or reordered reply
		// cannot undo progress.
		if reply.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = reply.MatchIndex
		}
		if n.nextIndex[from] < n.matchIndex[from]+1 {
			n.nextIndex[from] = n.matchIndex[from] + 1
		}
		n.advanceCommit()

		// More to send? Keep going now instead of waiting for the next heartbeat.
		if last, _ := n.lastLog(); n.nextIndex[from] <= last {
			n.sendAppendEntries(from)
		}
		return
	}

	// The follower's log does not contain our prevLogIndex entry. Back up:
	// at least one entry, and straight to the end of the follower's log if
	// that log is shorter. Never back up past what we know it already has.
	next := min(n.nextIndex[from]-1, reply.LastLogIndex+1)
	next = max(next, n.matchIndex[from]+1, 1)
	if next < n.nextIndex[from] {
		n.nextIndex[from] = next
		n.sendAppendEntries(from) // retry immediately with the earlier entries
	}
}

// advanceCommit moves commitIndex forward to the highest index that is stored
// on a majority and belongs to the current term (Figure 2, last leader rule).
//
// The current-term condition is what protects against the scenario in Figure 8
// of the paper: an entry from an earlier term can sit on a majority and still
// be overwritten by a later leader, so it is only treated as committed once an
// entry from the current term on top of it is.
func (n *Node) advanceCommit() {
	last, _ := n.lastLog()
	for idx := last; idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
			break // log terms never decrease, so everything below is older too
		}
		count := 1 // this node
		for _, peer := range n.cfg.Peers {
			if peer != n.cfg.ID && n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCommitted()
			return
		}
	}
}

// ---- replication: follower side ----

func (n *Node) handleAppendEntries(from NodeID, args AppendEntriesArgs) {
	// A stale leader: reject and tell it the current term so it steps down.
	if args.Term < n.currentTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: false})
		return
	}
	// Two leaders in one term would violate election safety, so this cannot
	// happen; if it ever did, do not let the other "leader" touch our log.
	if n.role == Leader && args.Term == n.currentTerm {
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

	// Consistency check (Figure 2, rule 2): our log must contain an entry at
	// PrevLogIndex whose term matches. If not, say so, and say how long our
	// log is so the leader can skip back quickly.
	lastIndex, _ := n.lastLog()
	if args.PrevLogIndex > lastIndex || n.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: false, LastLogIndex: lastIndex})
		return
	}

	// Rules 3 and 4: delete a conflicting suffix, then append what is new.
	// An entry that already matches is left alone, so a duplicated or delayed
	// message can never shorten a log that already holds newer entries.
	changed := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + Index(i)
		if idx <= Index(len(n.log)) {
			if n.log[idx-1].Term == e.Term {
				continue
			}
			n.log = n.log[:idx-1] // conflict: drop this entry and everything after it
		}
		n.log = append(n.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persist() // the log reaches stable storage before we acknowledge it
	}

	// Rule 5: learn the commit index, but only up to the last entry this
	// message verified. Entries after that point may still differ from the leader's.
	lastNew := args.PrevLogIndex + Index(len(args.Entries))
	if c := min(args.LeaderCommit, lastNew); c > n.commitIndex {
		n.commitIndex = c
		n.applyCommitted()
	}

	last, _ := n.lastLog()
	n.send(from, AppendEntriesReply{
		Term:         n.currentTerm,
		Success:      true,
		MatchIndex:   lastNew,
		LastLogIndex: last,
	})
}

// applyCommitted passes newly committed entries to the state machine, in order.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.cfg.Apply != nil {
			n.cfg.Apply(n.log[n.lastApplied-1])
		}
	}
}

// ---- helpers ----

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

// appendLocal appends an entry to this node's log in the current term and
// persists it. Only a leader calls it.
func (n *Node) appendLocal(command []byte) LogEntry {
	last, _ := n.lastLog()
	e := LogEntry{Term: n.currentTerm, Index: last + 1, Command: command}
	n.log = append(n.log, e)
	n.persist()
	return e
}

func (n *Node) lastLog() (Index, Term) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// termAt returns the term of the entry at index, or 0 for index 0 or an index
// past the end of the log.
func (n *Node) termAt(index Index) Term {
	if index == 0 || index > Index(len(n.log)) {
		return 0
	}
	return n.log[index-1].Term
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
// continue if it fails to remember a vote, a term or a log entry, so a storage
// failure is fatal.
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
