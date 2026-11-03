package raft

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// Crash testing. A node "crashes" in one of two ways:
//
//   - a storage operation panics partway through (errCrash), either before the
//     operation reached the disk or just after it did but before the node
//     could do anything else, which is what a power cut at an unlucky moment
//     looks like;
//   - the node is simply killed between events.
//
// Either way the node's memory is discarded. After a few ticks it restarts
// from nothing but its storage, which is the only thing that survives.

var errCrash = errors.New("injected crash")

// crashStorage is MemoryStorage that can be told to crash at a chosen
// mutating operation.
type crashStorage struct {
	*MemoryStorage
	ops     int  // mutating operations so far
	crashAt int  // crash when ops reaches this; 0 = disarmed
	applyOp bool // whether the crashing operation reaches the disk first
}

func newCrashStorage() *crashStorage { return &crashStorage{MemoryStorage: NewMemoryStorage()} }

// armIn crashes the nth mutating operation from now.
func (s *crashStorage) armIn(n int, applyOp bool) { s.crashAt, s.applyOp = s.ops+n, applyOp }

// armAt crashes the nth mutating operation since the beginning.
func (s *crashStorage) armAt(op int, applyOp bool) { s.crashAt, s.applyOp = op, applyOp }

func (s *crashStorage) disarm() { s.crashAt = 0 }

// hit counts one operation and reports whether this is the one that crashes.
func (s *crashStorage) hit() bool {
	s.ops++
	if s.crashAt != 0 && s.ops == s.crashAt {
		s.crashAt = 0
		return true
	}
	return false
}

func (s *crashStorage) SaveHardState(term Term, votedFor NodeID) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.SaveHardState(term, votedFor)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.SaveHardState(term, votedFor)
}

func (s *crashStorage) Append(entries []LogEntry) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.Append(entries)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.Append(entries)
}

func (s *crashStorage) TruncateFrom(index Index) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.TruncateFrom(index)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.TruncateFrom(index)
}

func (s *crashStorage) SaveSnapshot(snap Snapshot) error {
	if s.hit() {
		if s.applyOp {
			s.MemoryStorage.SaveSnapshot(snap)
		}
		panic(errCrash)
	}
	return s.MemoryStorage.SaveSnapshot(snap)
}

// guard runs fn against node id and turns an injected crash into a node going
// down. It reports whether a crash happened.
func (c *cluster) guard(id int, fn func()) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, errCrash) {
				c.crash(id)
				crashed = true
				return
			}
			panic(r)
		}
	}()
	fn()
	return false
}

// crash takes a node down for a while. Its storage is untouched.
func (c *cluster) crash(id int) {
	c.storages[id].disarm()
	c.down[id] = 5 + c.rng.Intn(26)
}

// restart builds a fresh node from nothing but the node's storage.
func (c *cluster) restart(id int) {
	c.gen[id]++
	// The state machine restarts empty too and is fed the log again as the
	// node learns what is committed.
	c.applied[id] = nil
	n, err := NewNode(c.nodeConfig(id), c.storages[id], simTransport{c.net, &c.snapshotsSent})
	if err != nil {
		c.t.Fatalf("seed %d: node %d failed to restart: %v", c.seed, id, err)
	}
	c.nodes[id] = n
}

// maybeCrash randomly kills or arms a node.
func (c *cluster) maybeCrash(id int) {
	if c.crashRate == 0 || c.rng.Float64() >= c.crashRate {
		return
	}
	if c.rng.Intn(3) == 0 {
		c.crash(id)
		return
	}
	c.storages[id].armIn(1+c.rng.Intn(3), c.rng.Intn(2) == 0)
}

// logView is a node's log together with how much of its front is a snapshot.
type logView struct {
	snapIndex Index
	snapTerm  Term
	entries   []LogEntry // entries[i] has index snapIndex+1+i
}

func (v logView) last() Index { return v.snapIndex + Index(len(v.entries)) }

// at returns the entry with the given index, which must be in v.entries.
func (v logView) at(i Index) LogEntry { return v.entries[i-v.snapIndex-1] }

// logOf is node i's log as it would be after a restart: the live log for a
// running node, the disk for a crashed one.
func (c *cluster) logOf(i int) logView {
	if c.down[i] > 0 {
		ps, _ := c.storages[i].Load()
		return logView{ps.Snapshot.Index, ps.Snapshot.Term, ps.Log}
	}
	n := c.nodes[i]
	n.mu.Lock()
	defer n.mu.Unlock()
	return logView{n.snap.Index, n.snap.Term, append([]LogEntry(nil), n.log...)}
}

// summary reads a storage's term, vote, snapshot, log length and last entry
// without copying the log.
func (s *crashStorage) summary() (term Term, vote NodeID, snapIndex Index, n int, last LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.state
	if len(st.Log) > 0 {
		last = st.Log[len(st.Log)-1]
	}
	return st.CurrentTerm, st.VotedFor, st.Snapshot.Index, len(st.Log), last
}

// checkDurable asserts that nothing a running node believes is missing from
// its disk. If the node crashed right now, it would come back with exactly
// what it had. This is what "persist before you reply" means.
//
// Every call compares term, vote, snapshot index, log length and the last
// entry; every 25th call compares the whole log and snapshot, which is much
// more expensive.
func (c *cluster) checkDurable() {
	c.durableChecks++
	full := c.durableChecks%25 == 0
	for id, n := range c.nodes {
		if c.down[id] > 0 {
			continue
		}
		dTerm, dVote, dSnap, dLen, dLast := c.storages[id].summary()
		n.mu.Lock()
		term, vote, snap, length := n.currentTerm, n.votedFor, n.snap.Index, len(n.log)
		var last LogEntry
		if length > 0 {
			last = n.log[length-1]
		}
		n.mu.Unlock()
		if dTerm != term || dVote != vote || dSnap != snap || dLen != length || !reflect.DeepEqual(dLast, last) {
			c.t.Fatalf("seed %d: node %d memory and disk disagree:\n memory term=%d vote=%d snapshot=%d len=%d last=%+v\n disk   term=%d vote=%d snapshot=%d len=%d last=%+v",
				c.seed, id, term, vote, snap, length, last, dTerm, dVote, dSnap, dLen, dLast)
		}
		if full {
			ps, _ := c.storages[id].Load()
			n.mu.Lock()
			memSnap, memLog := n.snap, append([]LogEntry(nil), n.log...)
			n.mu.Unlock()
			if len(memLog)+len(ps.Log) > 0 && !reflect.DeepEqual(ps.Log, memLog) {
				c.t.Fatalf("seed %d: node %d log differs from its disk:\n memory %+v\n disk   %+v", c.seed, id, memLog, ps.Log)
			}
			if memSnap.Index != ps.Snapshot.Index || memSnap.Term != ps.Snapshot.Term || !bytes.Equal(memSnap.Data, ps.Snapshot.Data) {
				c.t.Fatalf("seed %d: node %d snapshot differs from its disk:\n memory %d/%d (%d bytes)\n disk   %d/%d (%d bytes)",
					c.seed, id, memSnap.Index, memSnap.Term, len(memSnap.Data), ps.Snapshot.Index, ps.Snapshot.Term, len(ps.Snapshot.Data))
			}
		}
	}
}

func (c *cluster) requireAllUp() {
	c.t.Helper()
	for id, d := range c.down {
		if d > 0 {
			c.t.Fatalf("seed %d: node %d is still down; settle for longer", c.seed, id)
		}
	}
}

// ---- tests ----

// Random crashes and restarts under message loss, with clients proposing
// throughout. Election Safety, Log Matching, State Machine Safety, durability
// and "no phantom commands" are checked on every event; afterwards the cluster
// must converge with nothing committed lost.
func TestSafetyUnderRandomCrashes(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 20; seed++ {
			cfg := baseNet
			cfg.DropRate = 0.1
			c := newCluster(t, size, seed, cfg)
			c.crashRate = 0.01

			c.run(100)
			for round := 0; round < 100; round++ {
				c.propose()
				c.run(5)
			}

			// Stop the chaos and let everything recover.
			c.crashRate = 0
			for id := range c.storages {
				c.storages[id].disarm()
			}
			c.net.SetDropRate(0)
			c.run(600)
			c.requireAllUp()
			c.propose()
			c.run(200)
			c.requireStable()
			c.requireConverged()
		}
	}
}

// The same, with partitions on top of crashes: the nastiest combination, since
// a crashed leader's uncommitted entries may or may not survive on disk while
// the other side elects a new leader.
func TestSafetyUnderCrashesAndPartitions(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := newCluster(t, 5, seed, baseNet)
		c.crashRate = 0.01
		c.run(100)
		for round := 0; round < 40; round++ {
			if c.rng.Intn(3) == 0 {
				c.net.Heal()
			} else {
				var a, b []int
				for id := range c.nodes {
					if c.rng.Intn(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.net.Partition(a, b)
			}
			for i := 0; i < 5; i++ {
				c.propose()
				c.run(8)
			}
		}

		c.crashRate = 0
		for id := range c.storages {
			c.storages[id].disarm()
		}
		c.net.Heal()
		c.run(800)
		c.requireAllUp()
		c.propose()
		c.run(200)
		c.requireStable()
		c.requireConverged()
	}
}

// Crash one node at every single storage operation it performs, before and
// after the operation reaches the disk. The first run counts the operations;
// then each one is crashed in turn.
func TestCrashAtEveryStorageOperation(t *testing.T) {
	crashAtEveryOperation(t, 0, func(c *cluster) {
		c.run(80)
		for i := 0; i < 12; i++ {
			c.propose()
			c.run(4)
		}
		c.run(60)
	})
}

// The same with log compaction on, so saving a snapshot is one of the
// operations that gets interrupted, before and after it reaches the disk.
func TestCrashAtEveryStorageOperationWithSnapshots(t *testing.T) {
	crashAtEveryOperation(t, 4, func(c *cluster) {
		c.run(80)
		for i := 0; i < 16; i++ {
			c.propose()
			c.run(4)
		}
		c.run(60)
	})
}

// And with a follower that falls behind the leader's compaction and has to be
// brought forward by InstallSnapshot, which is the other way a snapshot reaches
// disk.
func TestCrashAtEveryStorageOperationWhileInstallingASnapshot(t *testing.T) {
	crashAtEveryOperation(t, 4, func(c *cluster) {
		c.run(80)
		c.net.Partition([]int{2}, []int{0, 1})
		for i := 0; i < 16; i++ {
			c.propose()
			c.run(4)
		}
		c.net.Heal()
		c.run(150)
	})
}

func crashAtEveryOperation(t *testing.T, snapEvery int, scenario func(*cluster)) {
	finish := func(c *cluster) {
		c.run(400)
		c.requireAllUp()
		c.propose()
		c.run(200)
		c.requireStable()
		c.requireConverged()
	}

	for seed := int64(1); seed <= 3; seed++ {
		probe := newClusterWithSnapshots(t, 3, seed, baseNet, snapEvery)
		scenario(probe)
		if snapEvery > 0 && probe.restores+probe.snapshotsSent == 0 && probe.nodes[0].Status().SnapshotIndex == 0 {
			t.Fatalf("seed %d: the scenario never took a snapshot", seed)
		}
		for victim := 0; victim < 3; victim++ {
			total := probe.storages[victim].ops
			if total == 0 {
				t.Fatalf("seed %d: node %d never wrote to storage", seed, victim)
			}
			for op := 1; op <= total; op++ {
				for _, applyOp := range []bool{false, true} {
					t.Run(fmt.Sprintf("seed%d/node%d/op%d/applied=%v", seed, victim, op, applyOp), func(t *testing.T) {
						c := newClusterWithSnapshots(t, 3, seed, baseNet, snapEvery)
						c.storages[victim].armAt(op, applyOp)
						scenario(c)
						finish(c)
					})
				}
			}
		}
	}
}
