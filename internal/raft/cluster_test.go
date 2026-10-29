package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/simnet"
)

// These tests run whole clusters on simnet. Everything is driven by a seed, so
// if a seed fails, rerunning that seed reproduces the failure exactly.

const tickDuration = 10 * time.Millisecond

type simTransport struct{ net *simnet.Network }

func (s simTransport) Send(m Message) { s.net.Send(int(m.From), int(m.To), m) }

type cluster struct {
	t     *testing.T
	seed  int64
	net   *simnet.Network
	nodes []*Node

	// leaders records every node that has ever been leader, per term.
	leaders map[Term]map[NodeID]bool
}

func newCluster(t *testing.T, size int, seed int64, netCfg simnet.Config) *cluster {
	t.Helper()
	netCfg.Seed = seed
	c := &cluster{
		t:       t,
		seed:    seed,
		net:     simnet.New(netCfg),
		leaders: make(map[Term]map[NodeID]bool),
	}

	peers := make([]NodeID, size)
	for i := range peers {
		peers[i] = NodeID(i)
	}
	for i := 0; i < size; i++ {
		n, err := NewNode(Config{
			ID:              NodeID(i),
			Peers:           peers,
			ElectionTickMin: 10,
			ElectionTickMax: 20,
			HeartbeatTick:   2,
			Seed:            seed,
		}, NewMemoryStorage(), simTransport{c.net})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)

		id := i
		c.net.Register(id, func(_, _ int, msg any) {
			c.nodes[id].Step(msg.(Message))
			c.observe()
		})
	}
	return c
}

// observe checks Election Safety (Raft paper, Figure 3): at most one leader
// can be elected in any given term. It runs after every event.
func (c *cluster) observe() {
	for _, n := range c.nodes {
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
		for _, n := range c.nodes {
			n.Tick()
			c.observe()
		}
		c.net.RunUntil(c.net.Now() + tickDuration)
		c.observe()
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
