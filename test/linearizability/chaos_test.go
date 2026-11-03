package linearizability

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/ritvik-devireddy/raftkv/internal/chaos"
	"github.com/ritvik-devireddy/raftkv/internal/kv"
)

const (
	chaosTicks  = 1500
	settleTicks = 1500
	checkBudget = 60 * time.Second
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Sprintf("%s=%q is not a number", name, v))
		}
		return n
	}
	return def
}

// runSeed simulates one cluster, checks its history, and writes a Porcupine
// visualization if the history is not linearizable.
func runSeed(t *testing.T, opts chaos.Options, visualize bool) (porcupine.CheckResult, []chaos.Op, chaos.Stats) {
	t.Helper()
	world := chaos.NewWorld(opts)
	history := world.Run(chaosTicks, settleTicks)
	ops := Operations(history)
	res, info := porcupine.CheckOperationsVerbose(Model, ops, checkBudget)
	if res == porcupine.Illegal && visualize {
		path := fmt.Sprintf("failure-seed-%d.html", opts.Seed)
		if err := porcupine.VisualizePath(Model, info, path); err == nil {
			t.Logf("seed %d: wrote the failing history to %s (open it in a browser)", opts.Seed, path)
		}
	}
	return res, history, world.Stats()
}

func optionsFor(seed int64) chaos.Options {
	return chaos.Options{Seed: seed, Nodes: 3 + 2*int(seed%2)} // alternate 3 and 5 nodes
}

// The main event: many seeded runs of a cluster under partitions, crashes and
// message loss, each producing a client-visible history that must be
// linearizable.
//
// Replay one seed:
//
//	RAFTKV_CHAOS_FIRST_SEED=17 RAFTKV_CHAOS_SEEDS=1 go test ./test/linearizability -run TestLinearizableUnderChaos -v
//
// Soak run (PowerShell: $env:RAFTKV_CHAOS_SEEDS=2000):
//
//	RAFTKV_CHAOS_SEEDS=2000 go test ./test/linearizability -run TestLinearizableUnderChaos -timeout 1h
func TestLinearizableUnderChaos(t *testing.T) {
	first := int64(envInt("RAFTKV_CHAOS_FIRST_SEED", 1))
	count := envInt("RAFTKV_CHAOS_SEEDS", 200)

	var total, done int
	var stats chaos.Stats
	for seed := first; seed < first+int64(count); seed++ {
		res, history, st := runSeed(t, optionsFor(seed), true)
		stats.Snapshots += st.Snapshots
		stats.Installs += st.Installs
		switch res {
		case porcupine.Illegal:
			t.Fatalf("seed %d: history is NOT linearizable.\nreplay: RAFTKV_CHAOS_FIRST_SEED=%d RAFTKV_CHAOS_SEEDS=1 go test ./test/linearizability -run TestLinearizableUnderChaos -v",
				seed, seed)
		case porcupine.Unknown:
			t.Fatalf("seed %d: the checker ran out of time (%v) on %d operations", seed, checkBudget, len(history))
		}
		for _, op := range history {
			total++
			if op.Done {
				done++
			}
		}
	}
	// A checker passing on an empty history proves nothing.
	if total < 20*count || done*2 < total {
		t.Fatalf("runs were too quiet to mean anything: %d operations, %d completed", total, done)
	}
	// Nodes compact their logs in these runs (every 20 applied entries). If they
	// never did, or never had to catch a follower up from a snapshot, "all
	// linearizable" would not cover the snapshot code at all.
	if stats.Snapshots < count || (count >= 20 && stats.Installs == 0) {
		t.Fatalf("the runs barely exercised snapshots: %d saved, %d sent in %d seeds", stats.Snapshots, stats.Installs, count)
	}
	t.Logf("%d seeds, %d operations (%d with a known outcome), %d snapshots saved, %d InstallSnapshot messages: all linearizable",
		count, total, done, stats.Snapshots, stats.Installs)
}

// The checker is only worth something if it can fail. These tests run the
// system with a deliberate bug and require that some seed is flagged.

// Serving reads from a node's local state, skipping the log, lets a lagging or
// partitioned node return stale data.
func TestCheckerCatchesStaleReads(t *testing.T) {
	requireViolation(t, "stale reads", func(seed int64) chaos.Options {
		o := optionsFor(seed)
		o.StaleReads = true
		return o
	})
}

// Without (ClientID, Seq) a retry of a command that already committed is
// applied again, so a client can be told something that never held.
func TestCheckerCatchesMissingDedup(t *testing.T) {
	requireViolation(t, "no dedup", func(seed int64) chaos.Options {
		o := optionsFor(seed)
		o.NoDedup = true
		return o
	})
}

func requireViolation(t *testing.T, name string, opts func(seed int64) chaos.Options) {
	t.Helper()
	const maxSeeds = 150
	for seed := int64(1); seed <= maxSeeds; seed++ {
		if res, _, _ := runSeed(t, opts(seed), false); res == porcupine.Illegal {
			t.Logf("%s: caught at seed %d", name, seed)
			return
		}
	}
	t.Fatalf("%s: %d seeds ran and none was flagged; the checker or the history recording cannot see this bug", name, maxSeeds)
}

// ---- the model itself, on histories small enough to check by hand ----

func op(client int, in input, call int64, out output, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret}
}

func TestModelOnHandWrittenHistories(t *testing.T) {
	put := func(v string) input { return input{Op: kv.OpPut, Key: "k", Value: v} }
	get := input{Op: kv.OpGet, Key: "k"}
	del := input{Op: kv.OpDelete, Key: "k"}
	cas := func(exp, v string) input { return input{Op: kv.OpCAS, Key: "k", Expected: exp, Value: v} }
	ok := output{OK: true}
	val := func(v string) output { return output{OK: true, Value: v} }
	missing := output{}
	unknown := output{Unknown: true}

	cases := []struct {
		name string
		hist []porcupine.Operation
		want bool
	}{
		{"read sees an earlier completed write", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(1, get, 20, val("1"), 30)}, true},
		{"read misses an earlier completed write", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(1, get, 20, missing, 30)}, false},
		{"concurrent write may or may not be seen", []porcupine.Operation{
			op(0, put("1"), 0, ok, 30), op(1, get, 10, missing, 20)}, true},
		{"reads cannot go backwards", []porcupine.Operation{
			op(0, put("1"), 0, ok, 100),
			op(1, get, 10, val("1"), 20), op(1, get, 30, missing, 40)}, false},
		{"delete reports whether the key existed", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(0, del, 20, ok, 30), op(0, del, 40, missing, 50)}, true},
		{"deleting an absent key cannot report existed", []porcupine.Operation{
			op(0, del, 0, ok, 10)}, false},
		{"cas succeeds on a match", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(0, cas("1", "2"), 20, val("2"), 30), op(0, get, 40, val("2"), 50)}, true},
		{"cas on a missing key matches the empty string", []porcupine.Operation{
			op(0, cas("", "x"), 0, val("x"), 10), op(0, get, 20, val("x"), 30)}, true},
		{"failed cas reports the current value", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(0, cas("9", "2"), 20, output{Value: "1"}, 30)}, true},
		{"failed cas that lies about the value", []porcupine.Operation{
			op(0, put("1"), 0, ok, 10), op(0, cas("9", "2"), 20, output{Value: "7"}, 30)}, false},
		{"an operation of unknown outcome may explain a later read", []porcupine.Operation{
			op(0, put("1"), 0, unknown, 1<<62), op(1, get, 20, val("1"), 30)}, true},
		{"an operation of unknown outcome may also have done nothing", []porcupine.Operation{
			op(0, put("1"), 0, unknown, 1<<62), op(1, get, 20, missing, 30)}, true},
		{"an unknown outcome cannot excuse a value nobody wrote", []porcupine.Operation{
			op(0, put("1"), 0, unknown, 1<<62), op(1, get, 20, val("2"), 30)}, false},
		{"an unknown write cannot take effect before it was called", []porcupine.Operation{
			op(1, get, 0, val("1"), 10), op(0, put("1"), 20, unknown, 1<<62)}, false},
	}
	for _, c := range cases {
		if got := porcupine.CheckOperations(Model, c.hist); got != c.want {
			t.Errorf("%s: linearizable = %v, want %v", c.name, got, c.want)
		}
	}
}
