// Package chaos runs a whole RaftKV cluster, with simulated clients, on the
// deterministic simulated network while injecting faults, and records every
// client operation so the history can be checked for linearizability.
//
// Everything is driven by one seed: the same Options always produce the same
// run and the same history, so a failing seed is a reproducible bug report.
//
// The package does not depend on any linearizability checker; it only produces
// the history. test/linearizability checks it.
package chaos

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/kv"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
	"github.com/ritvik-devireddy/raftkv/internal/server"
	"github.com/ritvik-devireddy/raftkv/internal/simnet"
)

// TickDuration is the virtual length of one tick.
const TickDuration = 10 * time.Millisecond

const (
	attemptTimeout = 100 // ticks before a client gives up on one attempt and tries elsewhere
	giveUp         = 400 // ticks before a client abandons an operation entirely
)

// Options configure a run.
type Options struct {
	Seed    int64
	Nodes   int // cluster size; default 3
	Clients int // concurrent clients, each with at most one operation in flight; default 4
	Keys    int // size of the key space; fewer keys means more contention; default 2

	DropRate      float64 // chance a message is lost; default 0.1
	CrashRate     float64 // per node per tick chance of a crash; default 0.003
	PartitionRate float64 // per tick chance of changing the partition; default 0.01
	AbandonRate   float64 // chance a client walks away right after sending a command; default 0.03

	// Deliberate bugs, used to prove the checker can fail.

	// StaleReads makes Gets read the contacted node's local state without
	// going through the Raft log. A lagging or partitioned node then serves
	// old data, which is not linearizable.
	StaleReads bool
	// NoDedup stops clients tagging commands with (ClientID, Seq), so a retried
	// command that already committed is applied a second time.
	NoDedup bool
}

func (o *Options) fill() {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.Clients == 0 {
		o.Clients = 4
	}
	if o.Keys == 0 {
		o.Keys = 2
	}
	if o.DropRate == 0 {
		o.DropRate = 0.1
	}
	if o.CrashRate == 0 {
		o.CrashRate = 0.003
	}
	if o.PartitionRate == 0 {
		o.PartitionRate = 0.01
	}
	if o.AbandonRate == 0 {
		o.AbandonRate = 0.03
	}
}

// Op is one client operation as the client experienced it.
//
// Call and Return are virtual nanoseconds. If Done is false the client never
// learned the outcome (it gave up, or the run ended): the command may or may
// not have taken effect, at any time after Call. Result is meaningful only
// when Done is true.
type Op struct {
	Client int
	Cmd    kv.Command
	Call   int64
	Return int64
	Result kv.Result
	Done   bool
}

type simTransport struct{ net *simnet.Network }

func (s simTransport) Send(m raft.Message) { s.net.Send(int(m.From), int(m.To), m) }

// World is a running cluster plus its clients.
type World struct {
	opts Options
	net  *simnet.Network
	rng  *rand.Rand // fault decisions only

	servers []*server.Server
	stores  []*faultStorage
	down    []int // ticks until a crashed node restarts; 0 = up
	gen     []int // restarts so far per node

	clients []*client
	history []*Op
	lastVal map[string]string // most recent value issued per key, so CAS can hit
	tick    int
	stopNew bool
	faults  bool
}

// NewWorld builds the cluster. Nothing happens until Run.
func NewWorld(opts Options) *World {
	opts.fill()
	w := &World{
		opts: opts,
		net: simnet.New(simnet.Config{
			Seed: opts.Seed, MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, DropRate: opts.DropRate,
		}),
		rng:     rand.New(rand.NewSource(opts.Seed*31 + 7)),
		down:    make([]int, opts.Nodes),
		gen:     make([]int, opts.Nodes),
		lastVal: make(map[string]string),
		faults:  true,
	}
	for id := 0; id < opts.Nodes; id++ {
		w.stores = append(w.stores, newFaultStorage())
		w.servers = append(w.servers, w.newServer(id))
		id := id
		w.net.Register(id, func(_, _ int, msg any) {
			if w.down[id] > 0 {
				return // a crashed node hears nothing
			}
			w.guard(id, func() { w.servers[id].Step(msg.(raft.Message)) })
		})
	}
	for c := 0; c < opts.Clients; c++ {
		w.clients = append(w.clients, &client{id: c, rng: rand.New(rand.NewSource(opts.Seed*131 + int64(c)))})
	}
	return w
}

func (w *World) newServer(id int) *server.Server {
	peers := make([]raft.NodeID, w.opts.Nodes)
	for i := range peers {
		peers[i] = raft.NodeID(i)
	}
	s, err := server.New(raft.Config{
		ID: raft.NodeID(id), Peers: peers,
		ElectionTickMin: 10, ElectionTickMax: 20, HeartbeatTick: 2,
		Seed: w.opts.Seed + int64(w.gen[id])*7919,
	}, w.stores[id], simTransport{w.net})
	if err != nil {
		panic(fmt.Sprintf("chaos: cannot start node %d: %v", id, err))
	}
	return s
}

// Run drives the cluster for chaosTicks with faults and clients active, then
// stops the faults, heals everything, lets clients finish what they started
// (up to settleTicks), and returns the history in invocation order.
func (w *World) Run(chaosTicks, settleTicks int) []Op {
	for i := 0; i < chaosTicks; i++ {
		w.step()
	}

	w.faults = false
	w.stopNew = true
	w.net.Heal()
	w.net.SetDropRate(0)
	for _, st := range w.stores {
		st.disarm()
	}
	for i := 0; i < settleTicks && !w.quiet(); i++ {
		w.step()
	}

	out := make([]Op, len(w.history))
	for i, op := range w.history {
		out[i] = *op
	}
	return out
}

// quiet reports whether every node is up and no client has an operation in flight.
func (w *World) quiet() bool {
	for _, d := range w.down {
		if d > 0 {
			return false
		}
	}
	for _, c := range w.clients {
		if c.cur != nil {
			return false
		}
	}
	return true
}

func (w *World) step() {
	w.tick++
	if w.faults {
		w.injectFaults()
	}
	for id := range w.servers {
		if w.down[id] > 0 {
			if w.down[id]--; w.down[id] == 0 {
				w.gen[id]++
				w.servers[id] = w.newServer(id) // restarts from nothing but its storage
			}
			continue
		}
		if w.faults && w.rng.Float64() < w.opts.CrashRate {
			if w.rng.Intn(3) == 0 {
				w.crash(id)
				continue
			}
			w.stores[id].armIn(1+w.rng.Intn(3), w.rng.Intn(2) == 0)
		}
		w.guard(id, w.servers[id].Tick)
	}
	for _, c := range w.clients {
		w.stepClient(c)
	}
	w.net.RunUntil(w.net.Now() + TickDuration)
}

func (w *World) injectFaults() {
	if w.rng.Float64() >= w.opts.PartitionRate {
		return
	}
	switch r := w.rng.Intn(10); {
	case r < 4:
		w.net.Heal()
	case r < 7:
		// Cut the current leader off: the nastiest partition for stale reads
		// and for commands a deposed leader has accepted.
		for id, s := range w.servers {
			if w.down[id] == 0 && s.Status().Role == raft.Leader {
				var rest []int
				for j := range w.servers {
					if j != id {
						rest = append(rest, j)
					}
				}
				w.net.Partition([]int{id}, rest)
				return
			}
		}
	default:
		var a, b []int
		for id := range w.servers {
			if w.rng.Intn(2) == 0 {
				a = append(a, id)
			} else {
				b = append(b, id)
			}
		}
		w.net.Partition(a, b)
	}
}

// guard runs fn against node id and turns an injected crash into the node
// going down. It reports whether a crash happened.
func (w *World) guard(id int, fn func()) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, errCrash) {
				w.crash(id)
				crashed = true
				return
			}
			panic(r)
		}
	}()
	fn()
	return false
}

func (w *World) crash(id int) {
	w.stores[id].disarm()
	w.down[id] = 5 + w.rng.Intn(26)
}

// ---- clients ----

type client struct {
	id        int
	rng       *rand.Rand
	seq       uint64
	cur       *attempt
	idleUntil int
}

// attempt is the operation a client is currently trying to complete.
type attempt struct {
	op        *Op
	startedAt int
	target    int
	pend      *server.Pending
	pendGen   int // the target's restart count when the command was submitted
	sentAt    int
	retryAt   int
}

func (w *World) startOp(c *client) {
	c.seq++
	key := fmt.Sprintf("k%d", c.rng.Intn(w.opts.Keys))
	cmd := kv.Command{Key: key, ClientID: fmt.Sprintf("c%d", c.id), Seq: c.seq}
	if w.opts.NoDedup {
		cmd.ClientID, cmd.Seq = "", 0
	}
	value := fmt.Sprintf("v%d.%d", c.id, c.seq)
	switch r := c.rng.Intn(100); {
	case r < 30:
		cmd.Op = kv.OpPut
		cmd.Value = value
		w.lastVal[key] = value
	case r < 60:
		cmd.Op = kv.OpGet
	case r < 75:
		cmd.Op = kv.OpDelete
	default:
		cmd.Op = kv.OpCAS
		cmd.Value = value
		switch c.rng.Intn(5) {
		case 0:
			cmd.Expected = "" // succeeds only if the key is absent
		default:
			cmd.Expected = w.lastVal[key]
		}
		w.lastVal[key] = value
	}

	op := &Op{Client: c.id, Cmd: cmd, Call: int64(w.net.Now())}
	w.history = append(w.history, op)
	c.cur = &attempt{op: op, startedAt: w.tick, target: c.rng.Intn(w.opts.Nodes)}
}

func (w *World) complete(c *client, res kv.Result) {
	a := c.cur
	a.op.Done = true
	a.op.Result = res
	a.op.Return = int64(w.net.Now())
	if a.op.Return <= a.op.Call {
		a.op.Return = a.op.Call + 1
	}
	c.cur = nil
	c.idleUntil = w.tick + c.rng.Intn(8)
}

func (w *World) otherNode(c *client, not int) int {
	for {
		if t := c.rng.Intn(w.opts.Nodes); t != not || w.opts.Nodes == 1 {
			return t
		}
	}
}

func (w *World) stepClient(c *client) {
	if c.cur == nil {
		if !w.stopNew && w.tick >= c.idleUntil {
			w.startOp(c)
		}
		return
	}
	a := c.cur

	if a.pend != nil {
		select {
		case <-a.pend.Done():
			res, err := a.pend.Result()
			a.pend = nil
			if err == nil {
				w.complete(c, res)
				return
			}
			// The leader that took the command was replaced and the command
			// did not commit there. Retry, same sequence number.
			a.retryAt = w.tick + 3
			a.target = w.otherNode(c, a.target)
		default:
			if w.gen[a.target] != a.pendGen || w.tick-a.sentAt > attemptTimeout {
				a.pend = nil // the node restarted or the attempt is stuck: try another
				a.retryAt = w.tick
				a.target = w.otherNode(c, a.target)
			}
			return
		}
	}

	if w.tick-a.startedAt > giveUp {
		c.cur = nil // outcome unknown: the Op stays Done=false
		c.idleUntil = w.tick + c.rng.Intn(8)
		return
	}
	if w.tick < a.retryAt {
		return
	}

	t := a.target
	if w.down[t] > 0 {
		a.target = w.otherNode(c, t)
		a.retryAt = w.tick + 2
		return
	}
	cmd := a.op.Cmd

	if w.opts.StaleReads && cmd.Op == kv.OpGet {
		v, ok := w.servers[t].Data()[cmd.Key]
		w.complete(c, kv.Result{Value: v, OK: ok})
		return
	}

	var p *server.Pending
	var err error
	crashed := w.guard(t, func() { p, err = w.servers[t].Submit(cmd) })
	switch {
	case crashed:
		// The command may or may not have reached the log before the crash.
		a.target = w.otherNode(c, t)
		a.retryAt = w.tick + 2
	case err != nil:
		var nl *server.NotLeaderError
		if errors.As(err, &nl) && nl.Leader != raft.NoLeader && int(nl.Leader) < w.opts.Nodes {
			a.target = int(nl.Leader)
			a.retryAt = w.tick + 1
		} else {
			a.target = w.otherNode(c, t)
			a.retryAt = w.tick + 3
		}
	default:
		a.pend, a.pendGen, a.sentAt = p, w.gen[t], w.tick
		if c.rng.Float64() < w.opts.AbandonRate {
			// The client gives up (timeout, process death) having sent a
			// command whose fate it will never learn. It very likely commits.
			c.cur = nil
			c.idleUntil = w.tick + c.rng.Intn(8)
		}
	}
}
