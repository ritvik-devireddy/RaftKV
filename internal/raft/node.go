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
//	Phase 3: durable storage behind the Storage interface (WAL) -- done
//
// Log indexes are contiguous and start at 1. Once a prefix of the log has been
// compacted into a snapshot, log holds only the entries after it: the entry
// with index i is log[i-snap.Index-1]. Use entryAt, termAt and lastLog rather
// than indexing log directly.
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
	log         []LogEntry // entries after the snapshot
	snap        Snapshot   // the compacted prefix of the log (Index 0 = none)

	// Volatile state.
	role        Role
	leaderID    NodeID // who this node believes is leader, or NoLeader
	commitIndex Index  // highest log index known to be committed
	lastApplied Index  // highest log index passed to Apply

	votes map[NodeID]bool // candidate only: who has voted for us this term

	// preVotes is non-nil while this node is asking whether it could win an
	// election (PreVote). It is a follower in every other respect: its term
	// and vote are untouched, and nothing about it is persisted.
	preVotes map[NodeID]bool

	// Leader only, reinitialized after every election (Figure 2).
	nextIndex  map[NodeID]Index // next log index to send to each peer
	matchIndex map[NodeID]Index // highest index known to be replicated on each peer
	// snapSentAt is the tick at which a snapshot was last sent to each peer that
	// has not answered since, so a big snapshot is not re-sent on every heartbeat.
	snapSentAt map[NodeID]int

	// Logical timers, measured in ticks.
	ticks            int // ticks since this node started
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
	// SnapshotIndex is the last log index covered by the node's snapshot
	// (0 if it has none).
	SnapshotIndex Index
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
	if cfg.SnapshotEvery > 0 && cfg.Snapshot == nil {
		return nil, fmt.Errorf("raft: SnapshotEvery is %d but Config.Snapshot is nil", cfg.SnapshotEvery)
	}

	ps, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
	}

	for i, e := range ps.Log {
		if want := ps.Snapshot.Index + Index(i) + 1; e.Index != want {
			return nil, fmt.Errorf("raft: stored log is not contiguous: position %d holds index %d, want %d", i+1, e.Index, want)
		}
	}

	n := &Node{
		cfg:         cfg,
		storage:     storage,
		transport:   transport,
		rng:         rand.New(rand.NewSource(cfg.Seed*1_000_003 + int64(cfg.ID))),
		currentTerm: ps.CurrentTerm,
		votedFor:    ps.VotedFor,
		log:         ps.Log,
		snap:        ps.Snapshot,
		commitIndex: ps.Snapshot.Index, // a snapshot only ever holds committed, applied state
		lastApplied: ps.Snapshot.Index,
		role:        Follower,
		leaderID:    NoLeader,
	}
	n.resetElectionTimer()

	// The snapshot is the state machine's state as of snap.Index; entries after
	// it are re-applied as the node learns they are committed.
	if ps.Snapshot.Index > 0 && cfg.Restore != nil {
		cfg.Restore(ps.Snapshot)
	}
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
	n.ticks++

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
		n.startPreVote()
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
	case PreVoteArgs:
		n.handlePreVote(msg.From, p)
	case PreVoteReply:
		n.handlePreVoteReply(msg.From, p)
	case AppendEntriesArgs:
		n.handleAppendEntries(msg.From, p)
	case AppendEntriesReply:
		n.handleAppendEntriesReply(msg.From, p)
	case InstallSnapshotArgs:
		n.handleInstallSnapshot(msg.From, p)
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

		SnapshotIndex: n.snap.Index,
	}
}

// ---- elections ----

// startPreVote runs when the election timer fires. Instead of raising its term
// at once, the node first asks the others whether they would vote for it. A
// node that is cut off from the cluster therefore never raises its term (no
// one answers), and when it comes back it cannot depose a healthy leader.
// Only if a majority says yes does it start a real election.
func (n *Node) startPreVote() {
	n.role = Follower // a candidate whose election failed goes back to being a follower
	n.leaderID = NoLeader
	n.votes = nil
	n.preVotes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()

	// A single-node cluster needs no one's permission.
	if len(n.preVotes) >= n.majority() {
		n.startElection()
		return
	}

	lastIndex, lastTerm := n.lastLog()
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.send(peer, PreVoteArgs{
			Term:         n.currentTerm + 1,
			CandidateID:  n.cfg.ID,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
	}
}

// handlePreVote answers "would you vote for me?". It changes nothing: no term,
// no vote, no timer, no disk write. It says yes only if a real RequestVote at
// that term would succeed AND this node has not heard from a leader recently
// (a node following a live leader refuses, which is the whole point).
func (n *Node) handlePreVote(from NodeID, args PreVoteArgs) {
	heardFromLeader := n.role == Leader ||
		(n.leaderID != NoLeader && n.electionElapsed < n.cfg.ElectionTickMin)

	grant := args.Term > n.currentTerm &&
		!heardFromLeader &&
		n.logUpToDate(args.LastLogIndex, args.LastLogTerm)

	n.send(from, PreVoteReply{Term: n.currentTerm, PreVoteTerm: args.Term, VoteGranted: grant})
}

func (n *Node) handlePreVoteReply(from NodeID, reply PreVoteReply) {
	// A reply that shows we are behind is how any node learns of a newer term.
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	// Ignore replies when we are not pre-voting, or that answer an older round.
	if n.preVotes == nil || reply.PreVoteTerm != n.currentTerm+1 {
		return
	}
	if reply.VoteGranted {
		n.preVotes[from] = true
		if len(n.preVotes) >= n.majority() {
			n.startElection()
		}
	}
}

// startElection moves to a new term as a candidate, votes for itself, and
// asks everyone else for their vote. Figure 2, "Rules for Servers: Candidates".
func (n *Node) startElection() {
	n.preVotes = nil
	n.currentTerm++
	n.role = Candidate
	n.leaderID = NoLeader
	n.votedFor = n.cfg.ID
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.saveHardState()
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
			n.saveHardState()
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
	n.preVotes = nil
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.votes = nil
	n.heartbeatElapsed = 0

	// Figure 2: nextIndex starts just past our last entry, matchIndex at zero.
	last, _ := n.lastLog()
	n.nextIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	n.matchIndex = make(map[NodeID]Index, len(n.cfg.Peers))
	n.snapSentAt = make(map[NodeID]int)
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
		n.saveHardState()
	}
	n.role = Follower
	n.leaderID = NoLeader
	n.votes = nil
	n.preVotes = nil
	n.nextIndex = nil
	n.matchIndex = nil
	n.snapSentAt = nil
	n.resetElectionTimer()
}

// candidateUpToDate is the "election restriction" (Raft paper section 5.4.1):
// a candidate's log must be at least as up to date as the voter's, comparing
// the term of the last entry first and the log length second.
func (n *Node) candidateUpToDate(args RequestVoteArgs) bool {
	return n.logUpToDate(args.LastLogIndex, args.LastLogTerm)
}

// logUpToDate reports whether a log ending at (lastIndex, lastTerm) is at
// least as up to date as this node's own.
func (n *Node) logUpToDate(lastIndex Index, lastTerm Term) bool {
	myIndex, myTerm := n.lastLog()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
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
	if next <= n.snap.Index {
		// The entries this follower needs are gone from our log; all we have for
		// it is the snapshot that replaced them. A snapshot can be large, so once
		// one is on its way we do not send another until the follower has had
		// time to answer. Meanwhile an empty AppendEntries keeps it from timing
		// out, and also lets it tell us if it turns out to be further along.
		if at, pending := n.snapSentAt[peer]; pending && n.ticks-at < n.cfg.ElectionTickMax {
			n.send(peer, AppendEntriesArgs{
				Term: n.currentTerm, LeaderID: n.cfg.ID,
				PrevLogIndex: n.snap.Index, PrevLogTerm: n.snap.Term,
				LeaderCommit: n.commitIndex,
			})
			return
		}
		n.snapSentAt[peer] = n.ticks
		n.send(peer, InstallSnapshotArgs{Term: n.currentTerm, LeaderID: n.cfg.ID, Snapshot: n.snap})
		return
	}
	prev := next - 1
	last, _ := n.lastLog()

	var entries []LogEntry
	if next <= last {
		end := min(last, next+maxEntriesPerMessage-1)
		// Copy: in tests, nodes share memory, and a follower truncating its log
		// must never be able to modify ours.
		entries = append([]LogEntry(nil), n.log[next-n.snap.Index-1:end-n.snap.Index]...)
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
		delete(n.snapSentAt, from) // it answered, so whatever snapshot we sent is no longer pending
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

	n.acceptLeader(args.Term, args.LeaderID)

	// The leader is probing inside the part of our log that is now a snapshot.
	// Everything up to our commit index is committed, so it is identical on
	// every node that could lead: say how far we are known to agree and let the
	// leader carry on from there.
	if args.PrevLogIndex < n.snap.Index {
		last, _ := n.lastLog()
		n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: n.commitIndex, LastLogIndex: last})
		return
	}

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
	var toAppend []LogEntry
	truncateAt := Index(0) // 0 means no truncation
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + Index(i)
		if idx <= lastIndex {
			if n.termAt(idx) == e.Term {
				continue
			}
			truncateAt = idx // conflict: drop this entry and everything after it
		}
		toAppend = args.Entries[i:]
		break
	}
	if truncateAt != 0 {
		// Storage first, then memory: the log reaches stable storage before we
		// acknowledge it.
		n.must(n.storage.TruncateFrom(truncateAt), "truncate log")
		n.log = n.log[:truncateAt-n.snap.Index-1]
	}
	if len(toAppend) > 0 {
		n.must(n.storage.Append(toAppend), "append entries")
		n.log = append(n.log, toAppend...)
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

// acceptLeader is what a node does on hearing from the legitimate leader of a
// term that is at least its own, whether the message is entries or a snapshot.
func (n *Node) acceptLeader(term Term, leader NodeID) {
	if term > n.currentTerm {
		n.stepDown(term)
	} else if n.role == Candidate {
		// Someone else won this term's election: stop campaigning.
		n.role = Follower
		n.votes = nil
	}
	n.leaderID = leader
	n.electionElapsed = 0
	n.preVotes = nil // there is a live leader; stop asking for votes
}

// handleInstallSnapshot replaces this node's state with a leader's snapshot.
func (n *Node) handleInstallSnapshot(from NodeID, args InstallSnapshotArgs) {
	if args.Term < n.currentTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: false})
		return
	}
	if n.role == Leader && args.Term == n.currentTerm {
		return // cannot happen (election safety); do not let another "leader" touch us
	}
	n.acceptLeader(args.Term, args.LeaderID)

	snap := args.Snapshot
	if snap.Index > n.commitIndex {
		// Storage first, memory second, as for every other change. The state
		// machine is replaced wholesale: it may be behind, or it may hold
		// entries the snapshot supersedes.
		n.must(n.storage.SaveSnapshot(snap), "save snapshot")
		n.adoptSnapshot(snap)
		if n.cfg.Restore != nil {
			n.cfg.Restore(snap)
		}
		n.commitIndex, n.lastApplied = snap.Index, snap.Index
	}
	// Otherwise we already have everything the snapshot covers (a duplicate, a
	// retransmission, or one that raced with entries): nothing to do but answer.

	last, _ := n.lastLog()
	n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: snap.Index, LastLogIndex: last})
}

// adoptSnapshot makes snap this node's snapshot in memory and drops the log
// entries it covers. The storage must already hold it. It uses the same rule as
// the storage, so memory and disk cannot disagree.
func (n *Node) adoptSnapshot(snap Snapshot) {
	ps := PersistentState{Snapshot: n.snap, Log: n.log}
	ps.ApplySnapshot(snap)
	n.snap, n.log = ps.Snapshot, ps.Log
}

// applyCommitted passes newly committed entries to the state machine, in order,
// and compacts the log if enough have piled up.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.cfg.Apply != nil {
			n.cfg.Apply(n.entryAt(n.lastApplied))
		}
	}
	n.maybeCompact()
}

// maybeCompact takes a snapshot of the state machine, which at this moment has
// applied exactly the entries up to lastApplied, once SnapshotEvery entries have
// been applied since the last one. Everything runs under the node's lock, so the
// state machine cannot move while it is serialized. (That also means a large
// state machine stalls the node while it is saved; a production system would
// snapshot copy-on-write in the background.)
func (n *Node) maybeCompact() {
	if n.cfg.SnapshotEvery <= 0 || n.lastApplied-n.snap.Index < Index(n.cfg.SnapshotEvery) {
		return
	}
	snap := Snapshot{Index: n.lastApplied, Term: n.termAt(n.lastApplied), Data: n.cfg.Snapshot()}
	n.must(n.storage.SaveSnapshot(snap), "save snapshot")
	n.adoptSnapshot(snap)
}

// ---- helpers ----

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

// appendLocal appends an entry to this node's log in the current term and
// persists it. Only a leader calls it.
func (n *Node) appendLocal(command []byte) LogEntry {
	last, _ := n.lastLog()
	e := LogEntry{Term: n.currentTerm, Index: last + 1, Command: command}
	n.must(n.storage.Append([]LogEntry{e}), "append entry")
	n.log = append(n.log, e)
	return e
}

// lastLog returns the index and term of the last entry, which is the snapshot's
// last entry when nothing follows it.
func (n *Node) lastLog() (Index, Term) {
	if len(n.log) == 0 {
		return n.snap.Index, n.snap.Term
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// entryAt returns the entry at index, which must be in the log (after the
// snapshot and no later than the last entry).
func (n *Node) entryAt(index Index) LogEntry {
	return n.log[index-n.snap.Index-1]
}

// termAt returns the term of the entry at index. It knows the term of the
// snapshot's last entry; for index 0, an index inside the compacted prefix, or
// an index past the end of the log it returns 0 (unknown).
func (n *Node) termAt(index Index) Term {
	last, _ := n.lastLog()
	switch {
	case index == 0 || index < n.snap.Index || index > last:
		return 0
	case index == n.snap.Index:
		return n.snap.Term
	default:
		return n.entryAt(index).Term
	}
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.cfg.ElectionTickMax - n.cfg.ElectionTickMin + 1
	n.electionTimeout = n.cfg.ElectionTickMin + n.rng.Intn(span)
}

func (n *Node) send(to NodeID, payload any) {
	n.transport.Send(Message{From: n.cfg.ID, To: to, Payload: payload})
}

// saveHardState makes the current term and vote durable.
func (n *Node) saveHardState() {
	n.must(n.storage.SaveHardState(n.currentTerm, n.votedFor), "save term and vote")
}

// must treats a storage failure as fatal. Raft cannot safely continue if it
// fails to remember a vote, a term or a log entry: carrying on could let it
// acknowledge something it will forget after a restart.
func (n *Node) must(err error, what string) {
	if err != nil {
		panic(fmt.Sprintf("raft: node %d cannot %s: %v", n.cfg.ID, what, err))
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
