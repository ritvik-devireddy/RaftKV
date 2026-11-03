package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"testing"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/simnet"
)

// These tests run whole clusters on simnet. Everything is driven by a seed, so
// if a seed fails, rerunning that seed reproduces the failure exactly.

const tickDuration = 10 * time.Millisecond

// simTransport sends over the simulated network and counts the snapshots sent,
// so tests can check that a scenario really exercised InstallSnapshot.
type simTransport struct {
	net       *simnet.Network
	snapshots *int
}

func (s simTransport) Send(m Message) {
	if _, ok := m.Payload.(InstallSnapshotArgs); ok {
		*s.snapshots++
	}
	s.net.Send(int(m.From), int(m.To), m)
}

type cluster struct {
	t     *testing.T
	seed  int64
	net   *simnet.Network
	nodes []*Node

	// leaders records every node that has ever been leader, per term.
	leaders map[Term]map[NodeID]bool

	// What each node's state machine has been given, in order.
	applied [][]LogEntry
	// Every entry any node has applied, by index (for State Machine Safety).
	global map[Index]LogEntry
	// Every command a leader has accepted, so a command nobody proposed can
	// never show up as committed.
	proposed map[string]bool
	nextCmd  int

	// Crash and restart support (see crash_test.go).
	peers     []NodeID
	storages  []*crashStorage // each node's "disk": survives crashes
	down      []int           // ticks until a crashed node restarts; 0 = up
	gen       []int           // restarts so far, so each life gets its own seed
	rng       *rand.Rand      // drives crash decisions only
	crashRate float64         // per node per tick chance of a crash; 0 disables

	durableChecks int

	// Snapshots. With snapEvery > 0 every node compacts its log after that many
	// applied entries. A node's "state machine" is the list of entries it has
	// applied, and a snapshot is that list, which makes every property of the
	// applied history checkable after a restore.
	snapEvery     int
	snapshotsSent int // InstallSnapshot messages sent
	restores      int // times a state machine was restored from a snapshot
}

func newCluster(t *testing.T, size int, seed int64, netCfg simnet.Config) *cluster {
	t.Helper()
	return newClusterWithSnapshots(t, size, seed, netCfg, 0)
}

// newClusterWithSnapshots is newCluster with log compaction every snapEvery
// applied entries (0 = never).
func newClusterWithSnapshots(t *testing.T, size int, seed int64, netCfg simnet.Config, snapEvery int) *cluster {
	t.Helper()
	netCfg.Seed = seed
	c := &cluster{
		t:         t,
		seed:      seed,
		net:       simnet.New(netCfg),
		leaders:   make(map[Term]map[NodeID]bool),
		applied:   make([][]LogEntry, size),
		global:    make(map[Index]LogEntry),
		proposed:  make(map[string]bool),
		storages:  make([]*crashStorage, size),
		snapEvery: snapEvery,
		down:      make([]int, size),
		gen:       make([]int, size),
		rng:       rand.New(rand.NewSource(seed*31 + 7)),
	}

	peers := make([]NodeID, size)
	for i := range peers {
		peers[i] = NodeID(i)
	}
	c.peers = peers
	for i := 0; i < size; i++ {
		id := i
		c.storages[id] = newCrashStorage()
		n, err := NewNode(c.nodeConfig(id), c.storages[id], simTransport{c.net, &c.snapshotsSent})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)

		c.net.Register(id, func(_, _ int, msg any) {
			if c.down[id] > 0 {
				return // a crashed node hears nothing
			}
			c.guard(id, func() { c.nodes[id].Step(msg.(Message)) })
			c.observe()
		})
	}
	return c
}

// nodeConfig is the configuration for a node's current life. Each restart gets
// a new seed, so a restarted node does not replay its previous timeouts.
func (c *cluster) nodeConfig(id int) Config {
	return Config{
		ID:              NodeID(id),
		Peers:           c.peers,
		ElectionTickMin: 10,
		ElectionTickMax: 20,
		HeartbeatTick:   2,
		Seed:            c.seed + int64(c.gen[id])*7919,
		Apply:           func(e LogEntry) { c.onApply(id, e) },

		SnapshotEvery: c.snapEvery,
		Snapshot:      func() []byte { return encodeApplied(c.applied[id]) },
		Restore:       func(snap Snapshot) { c.onRestore(id, snap) },
	}
}

func encodeApplied(es []LogEntry) []byte {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(es); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func decodeApplied(data []byte) ([]LogEntry, error) {
	var es []LogEntry
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&es)
	return es, err
}

// onRestore replaces node id's state machine with a snapshot. Every entry in it
// must be one some node really applied, in order from index 1.
func (c *cluster) onRestore(id int, snap Snapshot) {
	es, err := decodeApplied(snap.Data)
	if err != nil {
		c.t.Fatalf("seed %d: node %d was given a snapshot that does not decode: %v", c.seed, id, err)
	}
	for i, e := range es {
		if e.Index != Index(i+1) {
			c.t.Fatalf("seed %d: node %d restored entry %+v at position %d", c.seed, id, e, i+1)
		}
		if g, ok := c.global[e.Index]; !ok || g.Term != e.Term || !bytes.Equal(g.Command, e.Command) {
			c.t.Fatalf("seed %d: node %d restored entry %+v that the cluster never applied (global: %+v)", c.seed, id, e, g)
		}
	}
	if Index(len(es)) != snap.Index {
		c.t.Fatalf("seed %d: node %d restored %d entries from a snapshot at index %d", c.seed, id, len(es), snap.Index)
	}
	c.applied[id] = es
	c.restores++
}

// observe checks Election Safety (Raft paper, Figure 3): at most one leader
// can be elected in any given term. It runs after every event.
func (c *cluster) observe() {
	for id, n := range c.nodes {
		if c.down[id] > 0 {
			continue
		}
		st := n.Status()
		if st.Role != Leader {
			continue
		}
		if c.leaders[st.Term] == nil {
			c.leaders[st.Term] = make(map[NodeID]bool)
		}
		c.leaders[st.Term][st.ID] = true
		if len(c.leaders[st.Term]) > 1 {
			c.t.Fatalf("seed %d: election safety violated: term %d has leaders %v",
				c.seed, st.Term, c.leaders[st.Term])
		}
	}
}

// run advances the cluster by the given number of ticks.
func (c *cluster) run(ticks int) {
	for i := 0; i < ticks; i++ {
		for id := range c.nodes {
			if c.down[id] > 0 {
				if c.down[id]--; c.down[id] == 0 {
					c.restart(id)
				}
				continue
			}
			c.maybeCrash(id)
			if c.down[id] == 0 {
				c.guard(id, c.nodes[id].Tick)
			}
			c.observe()
		}
		c.net.RunUntil(c.net.Now() + tickDuration)
		c.observe()
		c.checkLogMatching()
		c.checkDurable()
	}
}

func (c *cluster) statuses() []Status {
	out := make([]Status, len(c.nodes))
	for i, n := range c.nodes {
		out[i] = n.Status()
	}
	return out
}

// requireStable asserts the whole cluster has converged: exactly one leader,
// everyone in the leader's term, and everyone else following it.
func (c *cluster) requireStable() Status {
	c.t.Helper()
	var leader *Status
	sts := c.statuses()
	for i := range sts {
		if sts[i].Role == Leader {
			if leader != nil {
				c.t.Fatalf("seed %d: two leaders at the end: %+v", c.seed, sts)
			}
			leader = &sts[i]
		}
	}
	if leader == nil {
		c.t.Fatalf("seed %d: no leader after settling: %+v", c.seed, sts)
	}
	for _, st := range sts {
		if st.Term != leader.Term || st.Leader != leader.ID {
			c.t.Fatalf("seed %d: node %d has not converged on leader %d in term %d: %+v",
				c.seed, st.ID, leader.ID, leader.Term, sts)
		}
	}
	return *leader
}

var baseNet = simnet.Config{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}

func TestClusterElectsALeader(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 200; seed++ {
			c := newCluster(t, size, seed, baseNet)
			c.run(200)
			c.requireStable()
		}
	}
}

func TestElectionSafetyWithMessageLoss(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		cfg := baseNet
		cfg.DropRate = 0.3
		c := newCluster(t, 5, seed, cfg)
		c.run(400)
		if len(c.leaders) == 0 {
			t.Fatalf("seed %d: no leader was ever elected with 30%% message loss", seed)
		}
	}
}

func TestElectionSafetyUnderRandomPartitions(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		c := newCluster(t, 5, seed, baseNet)
		rng := rand.New(rand.NewSource(seed + 1000))

		for step := 0; step < 20; step++ {
			if rng.Intn(3) == 0 {
				c.net.Heal()
			} else {
				var a, b []int
				for id := range c.nodes {
					if rng.Intn(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.net.Partition(a, b)
			}
			c.run(40)
		}

		// Once the network heals, the cluster must settle on one leader.
		c.net.Heal()
		c.run(300)
		c.requireStable()
	}
}

func TestLeaderFailover(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		c := newCluster(t, 3, seed, baseNet)
		c.run(200)
		old := c.requireStable()

		// Cut the leader off from the other two.
		var others []int
		for id := range c.nodes {
			if NodeID(id) != old.ID {
				others = append(others, id)
			}
		}
		c.net.Partition([]int{int(old.ID)}, others)
		c.run(200)

		// The majority side must have elected a new leader in a later term.
		var newLeaders []Status
		for _, id := range others {
			if st := c.nodes[id].Status(); st.Role == Leader {
				newLeaders = append(newLeaders, st)
			}
		}
		if len(newLeaders) != 1 || newLeaders[0].Term <= old.Term {
			t.Fatalf("seed %d: after isolating leader %d (term %d), majority side has leaders %+v",
				seed, old.ID, old.Term, newLeaders)
		}

		// After healing, the old leader must step down and follow the new one.
		c.net.Heal()
		c.run(200)
		final := c.requireStable()
		if final.ID == old.ID || final.Term <= old.Term {
			t.Fatalf("seed %d: final leader %+v should differ from the old leader %+v", seed, final, old)
		}
	}
}
