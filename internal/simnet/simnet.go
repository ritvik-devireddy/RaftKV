// Package simnet is a deterministic, single-threaded simulated network.
//
// Messages are not delivered by goroutines. Send puts a delivery event on a
// queue with a random delay drawn from a seeded generator, and RunUntil / Step
// pop events in virtual-time order and call the destination handler
// synchronously. With the same seed and the same sequence of calls, a run is
// reproducible bit for bit, so a failing chaos test can be replayed exactly.
//
// A Network is not safe for concurrent use. That is the point.
package simnet

import (
	"container/heap"
	"math/rand"
	"time"
)

// Handler receives a delivered message.
type Handler func(from, to int, msg any)

// Config controls the faults the network injects.
type Config struct {
	Seed     int64
	MinDelay time.Duration // lower bound on delivery delay
	MaxDelay time.Duration // upper bound; values below MinDelay are raised to it
	DropRate float64       // probability in [0,1] that a message is silently lost
}

// Stats counts what happened to messages so far.
type Stats struct {
	Sent      int
	Delivered int
	Dropped   int
}

type event struct {
	at       time.Duration
	seq      uint64 // tie-breaker so equal timestamps stay in send order
	from, to int
	msg      any
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Network is the simulated network.
type Network struct {
	cfg      Config
	rng      *rand.Rand
	now      time.Duration
	seq      uint64
	queue    eventHeap
	handlers map[int]Handler
	blocked  map[[2]int]bool
	stats    Stats
}

// New creates a network. Virtual time starts at zero.
func New(cfg Config) *Network {
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	return &Network{
		cfg:      cfg,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		handlers: make(map[int]Handler),
		blocked:  make(map[[2]int]bool),
	}
}

// Register attaches a handler for messages addressed to id. Registering again
// replaces the handler, which is how a test simulates a crash and restart.
func (n *Network) Register(id int, h Handler) { n.handlers[id] = h }

// Now returns the current virtual time.
func (n *Network) Now() time.Duration { return n.now }

// SetDropRate changes the probability that a message is lost. A test can run a
// cluster under heavy loss and then set it to zero to let the cluster settle.
func (n *Network) SetDropRate(p float64) { n.cfg.DropRate = p }

// Stats returns message counters.
func (n *Network) Stats() Stats { return n.stats }

// Send queues msg for delivery from one node to another. It may be dropped by
// the configured drop rate or by a partition, and is delayed by a random amount.
func (n *Network) Send(from, to int, msg any) {
	n.stats.Sent++

	// Always draw both random numbers so the generator advances identically
	// no matter what happens to the message.
	drop := n.rng.Float64() < n.cfg.DropRate
	delay := n.cfg.MinDelay
	if span := int64(n.cfg.MaxDelay - n.cfg.MinDelay); span > 0 {
		delay += time.Duration(n.rng.Int63n(span + 1))
	}

	if drop || n.isBlocked(from, to) {
		n.stats.Dropped++
		return
	}
	n.seq++
	heap.Push(&n.queue, &event{at: n.now + delay, seq: n.seq, from: from, to: to, msg: msg})
}

// Partition splits the registered nodes into the given groups. Messages
// between nodes in different groups are dropped, including messages already
// in flight. Registered nodes missing from every group form one extra group.
// A new Partition replaces the previous one; Heal removes it.
func (n *Network) Partition(groups ...[]int) {
	groupOf := make(map[int]int)
	for g, members := range groups {
		for _, id := range members {
			groupOf[id] = g
		}
	}
	lookup := func(id int) int {
		if g, ok := groupOf[id]; ok {
			return g
		}
		return -1
	}

	n.blocked = make(map[[2]int]bool)
	for a := range n.handlers {
		for b := range n.handlers {
			if a < b && lookup(a) != lookup(b) {
				n.blocked[[2]int{a, b}] = true
			}
		}
	}
}

// Heal removes any partition.
func (n *Network) Heal() { n.blocked = make(map[[2]int]bool) }

func (n *Network) isBlocked(a, b int) bool {
	if a > b {
		a, b = b, a
	}
	return n.blocked[[2]int{a, b}]
}

// Step delivers the next queued message, advancing virtual time to its
// delivery time. It reports false when nothing is queued.
func (n *Network) Step() bool {
	if len(n.queue) == 0 {
		return false
	}
	ev := heap.Pop(&n.queue).(*event)
	n.now = ev.at

	h, ok := n.handlers[ev.to]
	if !ok || n.isBlocked(ev.from, ev.to) {
		n.stats.Dropped++
		return true
	}
	n.stats.Delivered++
	h(ev.from, ev.to, ev.msg)
	return true
}

// RunUntil delivers every message due at or before t, then sets the virtual
// clock to t. Handlers may call Send while it runs.
func (n *Network) RunUntil(t time.Duration) {
	for len(n.queue) > 0 && n.queue[0].at <= t {
		n.Step()
	}
	if n.now < t {
		n.now = t
	}
}
