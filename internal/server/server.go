// Package server joins a Raft node to the key-value state machine and gives
// clients a simple contract: submit a command, learn its result once it has
// been committed and applied.
//
// It knows nothing about gRPC. The network layer (internal/rpc) is a thin
// adapter over Server, so everything interesting here runs and is tested on
// the deterministic simulated network.
package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/kv"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

// NotLeaderError means this node cannot take the request. Leader is who this
// node currently believes leads (raft.NoLeader if it does not know), so the
// client can go there instead.
type NotLeaderError struct{ Leader raft.NodeID }

func (e *NotLeaderError) Error() string {
	if e.Leader == raft.NoLeader {
		return "not the leader, and no leader is known"
	}
	return fmt.Sprintf("not the leader; node %d is", e.Leader)
}

// ErrLostLeadership means the command was accepted by a leader that was then
// replaced before it committed, and a different entry took its place in the
// log. The command was NOT applied by this attempt. The client may retry with
// the same (ClientID, Seq): if the command did commit elsewhere, the store's
// dedup returns the original result instead of applying it twice.
var ErrLostLeadership = errors.New("leadership changed before the command committed")

// Pending is a submitted command waiting to commit.
type Pending struct {
	done chan struct{}
	res  kv.Result
	err  error
}

// Done is closed when the outcome is known.
func (p *Pending) Done() <-chan struct{} { return p.done }

// Result returns the outcome. Call it only after Done is closed.
func (p *Pending) Result() (kv.Result, error) { return p.res, p.err }

func (p *Pending) finish(res kv.Result, err error) {
	p.res, p.err = res, err
	close(p.done)
}

type waiter struct {
	p    *Pending
	term raft.Term // the term of the entry we proposed at this index
}

type applied struct {
	term raft.Term
	res  kv.Result
}

// How many recent apply results to remember for commands that finish before
// Submit has had a chance to register for them.
const recentResults = 1024

// Server is one node: Raft consensus plus the state machine it drives.
type Server struct {
	node  *raft.Node
	store *kv.Store // touched only by apply, which Raft calls one entry at a time

	mu      sync.Mutex
	waiters map[raft.Index]waiter
	recent  map[raft.Index]applied
	order   []raft.Index // FIFO of recent's keys, for eviction
}

// New creates the node. cfg.Apply is overwritten: the server owns the state
// machine. Entries committed before a restart are applied again as the node
// relearns the commit index, which rebuilds the store from the log.
func New(cfg raft.Config, storage raft.Storage, transport raft.Transport) (*Server, error) {
	s := &Server{
		store:   kv.NewStore(),
		waiters: make(map[raft.Index]waiter),
		recent:  make(map[raft.Index]applied),
	}
	cfg.Apply = s.apply
	n, err := raft.NewNode(cfg, storage, transport)
	if err != nil {
		return nil, err
	}
	s.node = n
	return s, nil
}

// apply runs under the Raft node's lock, once per committed entry, in order.
func (s *Server) apply(e raft.LogEntry) {
	var res kv.Result
	if !e.IsNoOp() {
		cmd, err := kv.DecodeCommand(e.Command)
		if err != nil {
			// Only this package writes commands, and each is checksummed on
			// disk, so a committed entry that does not decode means a bug.
			// Applying anything else would diverge the replicas.
			panic(fmt.Sprintf("server: committed entry %d does not decode: %v", e.Index, err))
		}
		res = s.store.Apply(cmd)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// A waiter must hear about its index even when the entry that landed there
	// is a no-op: that is exactly what replaces a deposed leader's proposal.
	if w, ok := s.waiters[e.Index]; ok {
		delete(s.waiters, e.Index)
		if w.term == e.Term && !e.IsNoOp() {
			w.p.finish(res, nil)
		} else {
			w.p.finish(kv.Result{}, ErrLostLeadership)
		}
		return
	}
	if e.IsNoOp() {
		return
	}
	s.recent[e.Index] = applied{term: e.Term, res: res}
	s.order = append(s.order, e.Index)
	if len(s.order) > recentResults {
		delete(s.recent, s.order[0])
		s.order = s.order[1:]
	}
}

// Submit offers a command to the Raft log. It fails at once with
// *NotLeaderError if this node is not the leader. Otherwise the returned
// Pending completes when the command has been committed and applied, or with
// ErrLostLeadership if the entry was replaced.
//
// A nil error from Submit does not mean the command will commit: a leader cut
// off from its followers keeps its entry forever without committing it. Callers
// must bound the wait (see Execute).
func (s *Server) Submit(cmd kv.Command) (*Pending, error) {
	index, term, ok := s.node.Propose(cmd.Encode())
	if !ok {
		return nil, &NotLeaderError{Leader: s.node.Status().Leader}
	}
	p := &Pending{done: make(chan struct{})}

	s.mu.Lock()
	defer s.mu.Unlock()
	// A single-node cluster can commit and apply inside Propose, before we got
	// here to register.
	if a, ok := s.recent[index]; ok && a.term == term {
		p.finish(a.res, nil)
		return p, nil
	}
	if old, ok := s.waiters[index]; ok {
		// An earlier proposal at this index from a previous stint as leader.
		// Its entry has been replaced by ours.
		old.p.finish(kv.Result{}, ErrLostLeadership)
	}
	s.waiters[index] = waiter{p: p, term: term}
	return p, nil
}

// Execute submits a command and waits for its result or for ctx to end. If
// ctx ends first, the command may still commit later; retry with the same
// (ClientID, Seq) to find out safely.
func (s *Server) Execute(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	p, err := s.Submit(cmd)
	if err != nil {
		return kv.Result{}, err
	}
	select {
	case <-p.Done():
		return p.Result()
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}

// Step delivers a Raft message from a peer.
func (s *Server) Step(m raft.Message) { s.node.Step(m) }

// Tick advances Raft's logical clock by one tick.
func (s *Server) Tick() { s.node.Tick() }

// Status reports Raft's view of this node.
func (s *Server) Status() raft.Status { return s.node.Status() }

// Run ticks the node every interval until ctx ends. Production uses it; tests
// call Tick themselves so time is deterministic.
func (s *Server) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.node.Tick()
		}
	}
}

// Data returns a copy of the key-value data. It is for tests: it must not run
// concurrently with applying entries.
func (s *Server) Data() map[string]string { return s.store.Snapshot() }
