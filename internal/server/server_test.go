package server

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/kv"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
	"github.com/ritvik-devireddy/raftkv/internal/simnet"
	"github.com/ritvik-devireddy/raftkv/internal/storage"
)

const tick = 10 * time.Millisecond

type simTransport struct{ net *simnet.Network }

func (s simTransport) Send(m raft.Message) { s.net.Send(int(m.From), int(m.To), m) }

type tcluster struct {
	t         *testing.T
	net       *simnet.Network
	servers   []*Server
	storages  []*raft.MemoryStorage // each node's "disk", kept so a node can be restarted
	peers     []raft.NodeID
	seed      int64
	snapEvery int
}

func newTCluster(t *testing.T, size int, seed int64, drop float64) *tcluster {
	t.Helper()
	return newTClusterWithSnapshots(t, size, seed, drop, 0)
}

// newTClusterWithSnapshots is newTCluster with log compaction every snapEvery
// applied entries (0 = never).
func newTClusterWithSnapshots(t *testing.T, size int, seed int64, drop float64, snapEvery int) *tcluster {
	t.Helper()
	c := &tcluster{t: t, seed: seed, snapEvery: snapEvery, net: simnet.New(simnet.Config{
		Seed: seed, MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, DropRate: drop,
	})}
	c.peers = make([]raft.NodeID, size)
	for i := range c.peers {
		c.peers[i] = raft.NodeID(i)
	}
	for i := 0; i < size; i++ {
		id := i
		c.storages = append(c.storages, raft.NewMemoryStorage())
		c.servers = append(c.servers, c.newServer(id))
		c.net.Register(id, func(_, _ int, msg any) { c.servers[id].Step(msg.(raft.Message)) })
	}
	return c
}

func (c *tcluster) newServer(id int) *Server {
	s, err := New(raft.Config{
		ID: raft.NodeID(id), Peers: c.peers, Seed: c.seed,
		ElectionTickMin: 10, ElectionTickMax: 20, HeartbeatTick: 2,
		SnapshotEvery: c.snapEvery,
	}, c.storages[id], simTransport{c.net})
	if err != nil {
		c.t.Fatal(err)
	}
	return s
}

// restart replaces node id with a fresh server built from nothing but its storage.
func (c *tcluster) restart(id int) { c.servers[id] = c.newServer(id) }

func (c *tcluster) run(ticks int) {
	for i := 0; i < ticks; i++ {
		for _, s := range c.servers {
			s.Tick()
		}
		c.net.RunUntil(c.net.Now() + tick)
	}
}

// leader returns the node that currently leads in the highest term.
func (c *tcluster) leader() int {
	best, bestTerm := -1, raft.Term(0)
	for i, s := range c.servers {
		if st := s.Status(); st.Role == raft.Leader && st.Term >= bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	if best < 0 {
		c.t.Fatal("no leader")
	}
	return best
}

// await runs the cluster until p completes or the tick budget runs out.
func (c *tcluster) await(p *Pending, budget int) (kv.Result, error, bool) {
	for i := 0; i < budget; i++ {
		select {
		case <-p.Done():
			res, err := p.Result()
			return res, err, true
		default:
		}
		c.run(1)
	}
	select {
	case <-p.Done():
		res, err := p.Result()
		return res, err, true
	default:
		return kv.Result{}, nil, false
	}
}

func (c *tcluster) do(id int, cmd kv.Command) kv.Result {
	c.t.Helper()
	p, err := c.servers[id].Submit(cmd)
	if err != nil {
		c.t.Fatalf("submit to node %d: %v", id, err)
	}
	res, err, ok := c.await(p, 300)
	if !ok || err != nil {
		c.t.Fatalf("command %+v did not complete: ok=%v err=%v", cmd, ok, err)
	}
	return res
}

func TestPutThenGetThroughTheLeader(t *testing.T) {
	c := newTCluster(t, 3, 1, 0)
	c.run(200)
	l := c.leader()

	if res := c.do(l, kv.Command{Op: kv.OpPut, Key: "a", Value: "1"}); !res.OK {
		t.Fatalf("put: %+v", res)
	}
	if res := c.do(l, kv.Command{Op: kv.OpGet, Key: "a"}); !res.OK || res.Value != "1" {
		t.Fatalf("get: %+v", res)
	}
	if res := c.do(l, kv.Command{Op: kv.OpCAS, Key: "a", Expected: "1", Value: "2"}); !res.OK || res.Value != "2" {
		t.Fatalf("cas: %+v", res)
	}
	if res := c.do(l, kv.Command{Op: kv.OpCAS, Key: "a", Expected: "1", Value: "3"}); res.OK || res.Value != "2" {
		t.Fatalf("failing cas: %+v", res)
	}
	if res := c.do(l, kv.Command{Op: kv.OpDelete, Key: "a"}); !res.OK {
		t.Fatalf("delete: %+v", res)
	}
	if res := c.do(l, kv.Command{Op: kv.OpGet, Key: "a"}); res.OK {
		t.Fatalf("get after delete: %+v", res)
	}

	// Every replica reaches the same state.
	c.run(100)
	for i, s := range c.servers {
		if d := s.Data(); len(d) != 0 {
			t.Fatalf("node %d still holds %v", i, d)
		}
	}
}

func TestFollowerRedirectsToTheLeader(t *testing.T) {
	c := newTCluster(t, 3, 2, 0)
	c.run(200)
	l := c.leader()
	for id, s := range c.servers {
		if id == l {
			continue
		}
		_, err := s.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: "v"})
		var nl *NotLeaderError
		if !errors.As(err, &nl) || nl.Leader != raft.NodeID(l) {
			t.Fatalf("node %d: got %v, want a redirect to leader %d", id, err, l)
		}
	}
}

// A client whose request timed out retries with the same (ClientID, Seq). The
// command must run once.
func TestRetriedCommandRunsOnce(t *testing.T) {
	c := newTCluster(t, 3, 3, 0)
	c.run(200)
	l := c.leader()
	c.do(l, kv.Command{Op: kv.OpPut, Key: "k", Value: "v"})

	del := kv.Command{Op: kv.OpDelete, Key: "k", ClientID: "cli", Seq: 1}
	if res := c.do(l, del); !res.OK {
		t.Fatalf("first delete: %+v", res)
	}
	// Retried: must return the original answer ("it existed"), not re-run.
	if res := c.do(l, del); !res.OK {
		t.Fatalf("retried delete re-ran the command: %+v", res)
	}
	// A genuinely new request finds nothing to delete.
	del.Seq = 2
	if res := c.do(l, del); res.OK {
		t.Fatalf("new delete: %+v, want key already gone", res)
	}
}

// A leader that is cut off keeps accepting commands it can never commit. When
// the partition heals its entries are overwritten, and the waiting clients
// must be told the command did not happen, not left hanging or told it
// succeeded.
func TestDeposedLeadersPendingCommandFails(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := newTCluster(t, 3, seed, 0)
		c.run(200)
		old := c.leader()
		var others []int
		for i := range c.servers {
			if i != old {
				others = append(others, i)
			}
		}
		c.net.Partition([]int{old}, others)

		stale, err := c.servers[old].Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: "stale"})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		c.run(150) // the majority elects a new leader
		var nl int = -1
		for _, id := range others {
			if c.servers[id].Status().Role == raft.Leader {
				nl = id
			}
		}
		if nl < 0 {
			t.Fatalf("seed %d: majority side elected no leader", seed)
		}
		if res := c.do(nl, kv.Command{Op: kv.OpPut, Key: "k", Value: "fresh"}); !res.OK {
			t.Fatalf("seed %d: %+v", seed, res)
		}
		select {
		case <-stale.Done():
			t.Fatalf("seed %d: a command that cannot commit completed", seed)
		default:
		}

		c.net.Heal()
		_, err, ok := c.await(stale, 400)
		if !ok {
			t.Fatalf("seed %d: the deposed leader's client was never told anything", seed)
		}
		if !errors.Is(err, ErrLostLeadership) {
			t.Fatalf("seed %d: got %v, want ErrLostLeadership", seed, err)
		}
		c.run(100)
		for i, s := range c.servers {
			if v := s.Data()["k"]; v != "fresh" {
				t.Fatalf("seed %d: node %d has k=%q, want fresh", seed, i, v)
			}
		}
	}
}

// Many commands through whichever node is leader, under message loss and
// partitions. Every command a client was told succeeded must survive on every
// replica, every replica must end identical, and nothing nobody proposed may
// appear.
func TestAcknowledgedCommandsSurviveChaos(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		c := newTCluster(t, 5, seed, 0.15)
		rng := rand.New(rand.NewSource(seed + 99))

		proposed := map[string]string{}
		var pendings []*Pending
		var cmds []kv.Command
		for round := 0; round < 60; round++ {
			if rng.Intn(4) == 0 {
				if rng.Intn(2) == 0 {
					c.net.Heal()
				} else {
					var a, b []int
					for id := range c.servers {
						if rng.Intn(2) == 0 {
							a = append(a, id)
						} else {
							b = append(b, id)
						}
					}
					c.net.Partition(a, b)
				}
			}
			for id, s := range c.servers {
				if s.Status().Role != raft.Leader || rng.Intn(2) == 0 {
					continue
				}
				key := fmt.Sprintf("k%d", len(proposed))
				cmd := kv.Command{Op: kv.OpPut, Key: key, Value: "v" + key}
				p, err := s.Submit(cmd)
				if err != nil {
					t.Fatalf("seed %d: node %d reported leader but refused: %v", seed, id, err)
				}
				proposed[key] = cmd.Value
				pendings = append(pendings, p)
				cmds = append(cmds, cmd)
			}
			c.run(8)
		}

		c.net.Heal()
		c.net.SetDropRate(0)
		c.run(600)

		var acked int
		for i, p := range pendings {
			select {
			case <-p.Done():
			default:
				continue // never learned an outcome; allowed, but must not be lied about
			}
			if _, err := p.Result(); err == nil {
				acked++
				for n, s := range c.servers {
					if got := s.Data()[cmds[i].Key]; got != cmds[i].Value {
						t.Fatalf("seed %d: acknowledged %s=%s is %q on node %d", seed, cmds[i].Key, cmds[i].Value, got, n)
					}
				}
			}
		}
		ref := c.servers[0].Data()
		for n, s := range c.servers {
			d := s.Data()
			if len(d) != len(ref) {
				t.Fatalf("seed %d: node %d holds %d keys, node 0 holds %d", seed, n, len(d), len(ref))
			}
			for k, v := range d {
				if ref[k] != v {
					t.Fatalf("seed %d: node %d disagrees about %s", seed, n, k)
				}
				if proposed[k] != v {
					t.Fatalf("seed %d: key %s=%q was never proposed", seed, k, v)
				}
			}
		}
		_ = acked
	}
}

// A real node on a real WAL: restart it and the store is rebuilt from the log.
func TestStoreIsRebuiltFromTheWALAfterRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Server, *storage.WAL, *simnet.Network) {
		w, err := storage.Open(dir, storage.Options{NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		net := simnet.New(simnet.Config{Seed: 1})
		s, err := New(raft.Config{ID: 0, Peers: []raft.NodeID{0}, Seed: 1}, w, simTransport{net})
		if err != nil {
			t.Fatal(err)
		}
		return s, w, net
	}
	runUntil := func(s *Server, cond func() bool) {
		t.Helper()
		for i := 0; i < 500; i++ {
			if cond() {
				return
			}
			s.Tick()
		}
		t.Fatal("condition never became true")
	}

	s, w, _ := open()
	runUntil(s, func() bool { return s.Status().Role == raft.Leader })
	for _, kvp := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}} {
		p, err := s.Submit(kv.Command{Op: kv.OpPut, Key: kvp[0], Value: kvp[1]})
		if err != nil {
			t.Fatal(err)
		}
		runUntil(s, func() bool {
			select {
			case <-p.Done():
				return true
			default:
				return false
			}
		})
	}
	w.Close()

	s2, w2, _ := open()
	defer w2.Close()
	if len(s2.Data()) != 0 {
		t.Fatal("a freshly started node should not have applied anything yet")
	}
	runUntil(s2, func() bool { d := s2.Data(); return d["a"] == "3" && d["b"] == "2" })
}

// ---- snapshots ----

func put(key, value string) kv.Command { return kv.Command{Op: kv.OpPut, Key: key, Value: value} }

// With compaction on, every replica still ends up with the same data, and each
// has really compacted its log.
func TestReplicasAgreeAfterCompaction(t *testing.T) {
	c := newTClusterWithSnapshots(t, 3, 1, 0, 5)
	c.run(200)
	l := c.leader()
	want := map[string]string{}
	for i := 0; i < 40; i++ {
		k, v := fmt.Sprintf("k%d", i%7), fmt.Sprintf("v%d", i)
		c.do(l, put(k, v))
		want[k] = v
	}
	c.run(100)
	for i, s := range c.servers {
		if got := s.Data(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d: %v, want %v", i, got, want)
		}
		if st := s.Status(); st.SnapshotIndex < 30 {
			t.Fatalf("node %d: snapshot at %d after 40 commands with a threshold of 5", i, st.SnapshotIndex)
		}
	}
}

// A node restarted from its storage has the snapshotted data at once, without
// having to replay the log it no longer has.
func TestRestartedNodeStartsFromItsSnapshot(t *testing.T) {
	c := newTClusterWithSnapshots(t, 3, 2, 0, 5)
	c.run(200)
	l := c.leader()
	for i := 0; i < 30; i++ {
		c.do(l, put(fmt.Sprintf("k%d", i), "v"))
	}
	c.run(50)

	victim := (l + 1) % 3
	snapIndex := c.servers[victim].Status().SnapshotIndex
	if snapIndex == 0 {
		t.Fatal("the node never compacted")
	}
	before := c.servers[victim].Data()
	c.restart(victim)

	got := c.servers[victim].Data()
	if len(got) == 0 || len(got) > len(before) {
		t.Fatalf("restarted node holds %d keys (it had %d); it should hold what the snapshot covers", len(got), len(before))
	}
	for k, v := range got {
		if before[k] != v {
			t.Fatalf("restored %s=%q, was %q", k, v, before[k])
		}
	}
	if st := c.servers[victim].Status(); st.SnapshotIndex != snapIndex || st.LastApplied != snapIndex {
		t.Fatalf("status = %+v, want snapshot and applied index %d", st, snapIndex)
	}
	// Then it catches up with the rest from the log.
	c.run(100)
	if fmt.Sprint(c.servers[victim].Data()) != fmt.Sprint(before) {
		t.Fatalf("after catching up: %v, want %v", c.servers[victim].Data(), before)
	}
}

// The reason sessions travel in the snapshot. A client's command was applied
// long ago and every log entry for it has been compacted away. A replica that
// was restored from a snapshot must still recognise the retry.
func TestRetryIsStillRecognisedAfterTheLogHasBeenCompacted(t *testing.T) {
	c := newTClusterWithSnapshots(t, 3, 3, 0, 5)
	c.run(200)
	l := c.leader()

	cmd := kv.Command{Op: kv.OpPut, Key: "x", Value: "mine", ClientID: "cli", Seq: 1}
	c.do(l, cmd)
	for i := 0; i < 30; i++ { // someone else overwrites x, and the log is compacted past the command
		c.do(l, put("x", fmt.Sprintf("other%d", i)))
	}
	c.run(50)
	for i, s := range c.servers {
		if s.Status().SnapshotIndex < 10 {
			t.Fatalf("node %d has not compacted past the command", i)
		}
	}

	// Restart every node, so each one's state comes from its snapshot plus what
	// followed, then retry the command anywhere.
	for i := range c.servers {
		c.restart(i)
	}
	c.run(250)
	l = c.leader()
	res := c.do(l, cmd)
	if !res.OK || res.Value != "mine" {
		t.Fatalf("retry returned %+v, want the original result", res)
	}
	if v := c.servers[l].Data()["x"]; v == "mine" {
		t.Fatal("the retried command was applied a second time")
	}
}

// A follower that is cut off while the others compact is brought forward with a
// snapshot and ends up identical to everyone else.
func TestLaggingNodeCatchesUpFromASnapshot(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		c := newTClusterWithSnapshots(t, 3, seed, 0, 5)
		c.run(200)
		l := c.leader()
		lag := (l + 1) % 3
		c.net.Partition([]int{lag}, []int{l, (l + 2) % 3})

		for i := 0; i < 40; i++ {
			c.do(l, put(fmt.Sprintf("k%d", i%9), fmt.Sprintf("v%d", i)))
		}
		if got := c.servers[lag].Status().LastLogIndex; got >= c.servers[l].Status().SnapshotIndex {
			t.Fatalf("seed %d: the node is not behind the leader's snapshot (%d)", seed, got)
		}

		c.net.Heal()
		c.run(300)
		want := fmt.Sprint(c.servers[l].Data())
		for i, s := range c.servers {
			if got := fmt.Sprint(s.Data()); got != want {
				t.Fatalf("seed %d: node %d has %s, want %s", seed, i, got, want)
			}
		}
	}
}

// A deposed leader whose proposal is overtaken by a snapshot, not by log
// entries, must still tell its client the command did not complete.
func TestDeposedLeadersPendingCommandFailsWhenASnapshotArrives(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := newTClusterWithSnapshots(t, 3, seed, 0, 5)
		c.run(200)
		old := c.leader()
		var others []int
		for i := range c.servers {
			if i != old {
				others = append(others, i)
			}
		}
		c.net.Partition([]int{old}, others)

		stale, err := c.servers[old].Submit(put("k", "stale"))
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		c.run(150)
		nl := -1
		for _, id := range others {
			if c.servers[id].Status().Role == raft.Leader {
				nl = id
			}
		}
		if nl < 0 {
			t.Fatalf("seed %d: no new leader", seed)
		}
		for i := 0; i < 30; i++ { // enough to compact well past the stale entry
			c.do(nl, put("k", fmt.Sprintf("fresh%d", i)))
		}

		c.net.Heal()
		_, err, ok := c.await(stale, 600)
		if !ok {
			t.Fatalf("seed %d: the deposed leader's client was never told anything", seed)
		}
		if !errors.Is(err, ErrLostLeadership) {
			t.Fatalf("seed %d: got %v, want ErrLostLeadership", seed, err)
		}
		c.run(100)
		for i, s := range c.servers {
			if v := s.Data()["k"]; v != "fresh29" {
				t.Fatalf("seed %d: node %d has k=%q", seed, i, v)
			}
		}
	}
}

// A real node on a real WAL, compacting for real: the log file shrinks, and a
// restart rebuilds the store from the snapshot in it.
func TestSnapshotOnARealWALSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Server, *storage.WAL) {
		w, err := storage.Open(dir, storage.Options{NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		net := simnet.New(simnet.Config{Seed: 1})
		s, err := New(raft.Config{ID: 0, Peers: []raft.NodeID{0}, Seed: 1, SnapshotEvery: 10}, w, simTransport{net})
		if err != nil {
			t.Fatal(err)
		}
		return s, w
	}
	s, w := open()
	for i := 0; i < 100 && s.Status().Role != raft.Leader; i++ {
		s.Tick()
	}
	want := map[string]string{}
	for i := 0; i < 55; i++ {
		k, v := fmt.Sprintf("k%d", i%11), fmt.Sprintf("v%d", i)
		p, err := s.Submit(kv.Command{Op: kv.OpPut, Key: k, Value: v, ClientID: "c", Seq: uint64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		<-p.Done()
		want[k] = v
	}
	if st := s.Status(); st.SnapshotIndex < 50 {
		t.Fatalf("snapshot at %d after 55 commands with a threshold of 10", st.SnapshotIndex)
	}
	size := func() int64 { fi, _ := os.Stat(w.Path()); return fi.Size() }
	if size() > 2000 {
		t.Fatalf("the log file is %d bytes after compaction; it should hold a snapshot and a few entries", size())
	}
	w.Close()

	s2, w2 := open()
	defer w2.Close()
	if fmt.Sprint(s2.Data()) == "map[]" {
		t.Fatal("restarted node started empty although it has a snapshot")
	}
	for i := 0; i < 200 && fmt.Sprint(s2.Data()) != fmt.Sprint(want); i++ {
		s2.Tick()
	}
	if fmt.Sprint(s2.Data()) != fmt.Sprint(want) {
		t.Fatalf("restarted store = %v, want %v", s2.Data(), want)
	}
}

// Installing a snapshot at index 10 answers the commands this node proposed at
// indexes 10 and below (they will never be applied here) and leaves later ones
// waiting.
func TestRestoreFailsOnlyTheWaitersTheSnapshotCovers(t *testing.T) {
	s, err := New(raft.Config{ID: 0, Peers: []raft.NodeID{0}, Seed: 1}, raft.NewMemoryStorage(), simTransport{simnet.New(simnet.Config{Seed: 1})})
	if err != nil {
		t.Fatal(err)
	}
	pend := map[raft.Index]*Pending{}
	for _, idx := range []raft.Index{5, 10, 11} {
		pend[idx] = &Pending{done: make(chan struct{})}
		s.waiters[idx] = waiter{p: pend[idx], term: 1}
	}

	s.restore(raft.Snapshot{Index: 10, Term: 1, Data: kv.NewStore().MarshalState()})

	for _, idx := range []raft.Index{5, 10} {
		select {
		case <-pend[idx].Done():
			if _, err := pend[idx].Result(); !errors.Is(err, ErrLostLeadership) {
				t.Fatalf("waiter at %d: %v, want ErrLostLeadership", idx, err)
			}
		default:
			t.Fatalf("the waiter at %d was left hanging", idx)
		}
	}
	select {
	case <-pend[11].Done():
		t.Fatal("the waiter at 11, beyond the snapshot, was answered")
	default:
	}
}
