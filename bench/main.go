// Command raftkv-bench measures a real RaftKV cluster: it starts raftkv-server
// processes on this machine, talks to them over gRPC with the real client, and
// every node writes a real fsynced write-ahead log.
//
//	go run ./bench                       # 1, 3 and 5 nodes, default workload
//	go run ./bench -sizes 3 -clients 1,32 -duration 20s
//
// It reports, for each cluster size and client count, throughput and latency
// percentiles (every operation goes through Raft, reads included), and for
// clusters of 3 or more, how long writes are unavailable after the leader
// process is killed.
//
// Read the numbers with the caveats in mind: all nodes and all clients share
// one machine and one disk, so this measures the software and the local disk,
// not a network. fsync cost depends entirely on the drive that holds -dir.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/benchstat"
	"github.com/ritvik-devireddy/raftkv/internal/client"
)

type config struct {
	serverBin     string
	sizes         []int
	clients       []int
	duration      time.Duration
	warmup        time.Duration
	valueSize     int
	keys          int
	readPct       int
	trials        int
	settle        time.Duration
	tick          time.Duration
	electionMin   int
	electionMax   int
	heartbeat     int
	snapshotEvery int
	noSync        bool
	dir           string
	skipFailover  bool
}

func main() {
	var (
		cfg              config
		sizes, clientCnt string
	)
	flag.StringVar(&cfg.serverBin, "server-bin", "", "path to a built raftkv-server (default: build one from ./cmd/raftkv-server; run from the repository root)")
	flag.StringVar(&sizes, "sizes", "1,3,5", "cluster sizes to test")
	flag.StringVar(&clientCnt, "clients", "1,16,64", "numbers of concurrent clients to test (each has one request in flight)")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "measured time per run")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "unmeasured warm-up before each run")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "bytes per value written")
	flag.IntVar(&cfg.keys, "keys", 1000, "number of distinct keys")
	flag.IntVar(&cfg.readPct, "read-pct", 0, "percentage of operations that are reads (0-100); reads also go through the log")
	flag.IntVar(&cfg.trials, "trials", 5, "leader kills per cluster size for the failover measurement")
	flag.DurationVar(&cfg.settle, "settle", 3*time.Second, "pause after restarting a killed node, before the next kill")
	flag.DurationVar(&cfg.tick, "tick", 100*time.Millisecond, "server tick length")
	flag.IntVar(&cfg.electionMin, "election-min", 10, "server minimum election timeout, in ticks")
	flag.IntVar(&cfg.electionMax, "election-max", 20, "server maximum election timeout, in ticks")
	flag.IntVar(&cfg.heartbeat, "heartbeat", 2, "server heartbeat interval, in ticks")
	flag.IntVar(&cfg.snapshotEvery, "snapshot-every", 1000, "server snapshot interval in applied entries (0 = never)")
	flag.BoolVar(&cfg.noSync, "no-sync", false, "run the servers with fsync OFF (unsafe; only to see what fsync costs)")
	flag.StringVar(&cfg.dir, "dir", "", "directory for the nodes' data and logs (default: a temporary directory); its drive decides fsync cost")
	flag.BoolVar(&cfg.skipFailover, "skip-failover", false, "skip the leader-kill measurement")
	flag.Parse()

	var err error
	if cfg.sizes, err = benchstat.ParseIntList(sizes); err != nil {
		fatal("-sizes: %v", err)
	}
	if cfg.clients, err = benchstat.ParseIntList(clientCnt); err != nil {
		fatal("-clients: %v", err)
	}
	if cfg.readPct < 0 || cfg.readPct > 100 || cfg.valueSize < 1 || cfg.keys < 1 || cfg.trials < 1 {
		fatal("-read-pct must be 0-100; -value-size, -keys and -trials must be at least 1")
	}

	root, err := os.MkdirTemp(cfg.dir, "raftkv-bench-")
	if err != nil {
		fatal("cannot create a data directory: %v", err)
	}
	defer os.RemoveAll(root)

	if cfg.serverBin == "" {
		if cfg.serverBin, err = buildServer(root); err != nil {
			fatal("%v", err)
		}
	}

	var (
		mu     sync.Mutex
		active *cluster
	)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		mu.Lock()
		if active != nil {
			active.stop()
		}
		mu.Unlock()
		os.RemoveAll(root)
		os.Exit(130)
	}()

	printHeader(cfg, root)

	var load, failover []string
	for _, n := range cfg.sizes {
		c, err := startCluster(cfg, filepath.Join(root, fmt.Sprintf("n%d", n)), n)
		if err != nil {
			fatal("%d-node cluster: %v", n, err)
		}
		mu.Lock()
		active = c
		mu.Unlock()

		fmt.Fprintf(os.Stderr, "== %d node(s)\n", n)
		for _, k := range cfg.clients {
			fmt.Fprintf(os.Stderr, "   %d client(s) ... ", k)
			res, err := runLoad(c.addrs, k, cfg)
			if err != nil {
				c.stop()
				fatal("load run: %v", err)
			}
			fmt.Fprintf(os.Stderr, "%.0f ops/s\n", res.opsPerSec)
			s := res.latency
			load = append(load, fmt.Sprintf("| %d | %d | %.0f | %s | %s | %s | %s | %d |",
				n, k, res.opsPerSec, benchstat.MS(s.P50), benchstat.MS(s.P95), benchstat.MS(s.P99), benchstat.MS(s.Max), res.errors))
		}
		if n >= 3 && !cfg.skipFailover {
			fmt.Fprintf(os.Stderr, "   killing the leader %d time(s) ...\n", cfg.trials)
			ds, err := runFailover(c, cfg)
			if err != nil {
				c.stop()
				fatal("failover run: %v", err)
			}
			var all []string
			lo, hi := ds[0], ds[0]
			for _, d := range ds {
				all = append(all, benchstat.MS(d))
				lo, hi = min(lo, d), max(hi, d)
			}
			failover = append(failover, fmt.Sprintf("| %d | %d | %s | %s | %s | %s |",
				n, len(ds), benchstat.MS(lo), benchstat.MS(benchstat.Median(ds)), benchstat.MS(hi), strings.Join(all, ", ")))
		}
		c.stop()
		mu.Lock()
		active = nil
		mu.Unlock()
	}

	fmt.Println("\n### Throughput and latency")
	fmt.Println()
	fmt.Println("| Nodes | Clients | ops/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | errors |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, l := range load {
		fmt.Println(l)
	}
	if len(failover) > 0 {
		fmt.Println("\n### Time until writes succeed again after the leader process is killed")
		fmt.Println()
		fmt.Println("| Nodes | Kills | min (ms) | median (ms) | max (ms) | every trial (ms) |")
		fmt.Println("|---|---|---|---|---|---|")
		for _, l := range failover {
			fmt.Println(l)
		}
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "raftkv-bench: "+format+"\n", args...)
	os.Exit(1)
}

func printHeader(cfg config, root string) {
	timeout := func(ticks int) time.Duration { return time.Duration(ticks) * cfg.tick }
	fsync := "on"
	if cfg.noSync {
		fsync = "OFF (unsafe)"
	}
	fmt.Println("## RaftKV benchmark")
	fmt.Println()
	fmt.Printf("- Machine: %s/%s, %d logical CPUs, %s\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Printf("- All nodes and clients run on this machine; data under %s\n", root)
	fmt.Printf("- fsync: %s; snapshot-every: %d; value size: %d B; keys: %d; reads: %d%%\n", fsync, cfg.snapshotEvery, cfg.valueSize, cfg.keys, cfg.readPct)
	fmt.Printf("- Raft timing: tick %s, election timeout %s to %s, heartbeat %s\n", cfg.tick, timeout(cfg.electionMin), timeout(cfg.electionMax), timeout(cfg.heartbeat))
	fmt.Printf("- Each run: %s warm-up (not measured), then %s measured\n", cfg.warmup, cfg.duration)
}

// ---- cluster management ----

type cluster struct {
	cfg   config
	dir   string
	addrs []string
	cmds  []*exec.Cmd // nil when the node is stopped
	logs  []*os.File
}

func buildServer(dir string) (string, error) {
	if _, err := os.Stat("go.mod"); err != nil {
		return "", fmt.Errorf("run from the repository root (no go.mod here), or pass -server-bin")
	}
	out := filepath.Join(dir, "raftkv-server")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	fmt.Fprintln(os.Stderr, "building raftkv-server ...")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/raftkv-server")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go build ./cmd/raftkv-server: %w", err)
	}
	return out, nil
}

// freeAddrs picks n free localhost ports by briefly listening on each.
func freeAddrs(n int) ([]string, error) {
	var addrs []string
	for range n {
		l, err := netListen()
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, l.addr)
		l.close()
	}
	return addrs, nil
}

func startCluster(cfg config, dir string, n int) (*cluster, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	addrs, err := freeAddrs(n)
	if err != nil {
		return nil, err
	}
	c := &cluster{cfg: cfg, dir: dir, addrs: addrs, cmds: make([]*exec.Cmd, n), logs: make([]*os.File, n)}
	for i := range n {
		if err := c.start(i); err != nil {
			c.stop()
			return nil, err
		}
	}
	// Ready means a write succeeds, which needs a leader.
	cl, err := client.New(c.addrs)
	if err != nil {
		c.stop()
		return nil, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := cl.Put(ctx, "bench-ready", "1"); err != nil {
		c.stop()
		return nil, fmt.Errorf("the cluster never accepted a write (see the node logs in %s): %w", dir, err)
	}
	return c, nil
}

func (c *cluster) start(i int) error {
	logf, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("node%d.log", i)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	args := []string{
		"-id", strconv.Itoa(i),
		"-listen", c.addrs[i],
		"-data", filepath.Join(c.dir, fmt.Sprintf("node%d", i)),
		"-peers", strings.Join(c.addrs, ","),
		"-tick", c.cfg.tick.String(),
		"-election-min", strconv.Itoa(c.cfg.electionMin),
		"-election-max", strconv.Itoa(c.cfg.electionMax),
		"-heartbeat", strconv.Itoa(c.cfg.heartbeat),
		"-snapshot-every", strconv.Itoa(c.cfg.snapshotEvery),
	}
	if c.cfg.noSync {
		args = append(args, "-no-sync")
	}
	cmd := exec.Command(c.cfg.serverBin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("cannot start node %d: %w", i, err)
	}
	c.cmds[i], c.logs[i] = cmd, logf
	return nil
}

// kill stops node i abruptly (no graceful shutdown), like a crash.
func (c *cluster) kill(i int) {
	if cmd := c.cmds[i]; cmd != nil {
		cmd.Process.Kill()
		cmd.Wait()
		c.logs[i].Close()
		c.cmds[i] = nil
	}
}

func (c *cluster) stop() {
	for i := range c.cmds {
		c.kill(i)
	}
}

func (c *cluster) indexOf(addr string) int {
	for i, a := range c.addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// ---- workloads ----

type loadResult struct {
	latency   benchstat.Summary
	opsPerSec float64
	errors    int
}

// runLoad drives k clients, each with one request in flight, for the warm-up
// plus the measured duration, and summarizes the operations that started in the
// measured window.
func runLoad(addrs []string, k int, cfg config) (loadResult, error) {
	value := strings.Repeat("x", cfg.valueSize)
	start := time.Now()
	measureFrom := start.Add(cfg.warmup)
	end := measureFrom.Add(cfg.duration)

	type out struct {
		samples []time.Duration
		errs    int
	}
	outs := make([]out, k)
	var wg sync.WaitGroup
	for w := range k {
		cl, err := client.New(addrs)
		if err != nil {
			return loadResult{}, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cl.Close()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			o := &outs[w]
			for time.Now().Before(end) {
				key := "key-" + strconv.Itoa(rng.Intn(cfg.keys))
				read := rng.Intn(100) < cfg.readPct
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				t0 := time.Now()
				var err error
				if read {
					_, _, err = cl.Get(ctx, key)
				} else {
					err = cl.Put(ctx, key, value)
				}
				d := time.Since(t0)
				cancel()
				if t0.Before(measureFrom) {
					continue
				}
				if err != nil {
					o.errs++
					continue
				}
				o.samples = append(o.samples, d)
			}
		}()
	}
	wg.Wait()

	var all []time.Duration
	errs := 0
	for _, o := range outs {
		all = append(all, o.samples...)
		errs += o.errs
	}
	if len(all) == 0 {
		return loadResult{}, fmt.Errorf("no operation succeeded (%d errors); see the node logs", errs)
	}
	return loadResult{latency: benchstat.Summarize(all), opsPerSec: float64(len(all)) / cfg.duration.Seconds(), errors: errs}, nil
}

// runFailover kills the leader cfg.trials times and, each time, measures how
// long it takes until a write is accepted again. A fresh short-lived client is
// used for every probe so the client's own retry back-off does not inflate the
// result; the resolution is about 0.1 s. The killed node is restarted between
// trials so the cluster keeps its size.
func runFailover(c *cluster, cfg config) ([]time.Duration, error) {
	var results []time.Duration
	for t := range cfg.trials {
		leader, err := findLeader(c)
		if err != nil {
			return nil, err
		}
		t0 := time.Now()
		c.kill(leader)

		deadline := t0.Add(60 * time.Second)
		for {
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("trial %d: no write succeeded within 60s of killing node %d", t+1, leader)
			}
			if probe(c.addrs, 400*time.Millisecond) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		results = append(results, time.Since(t0))

		if err := c.start(leader); err != nil {
			return nil, err
		}
		time.Sleep(cfg.settle)
	}
	return results, nil
}

// findLeader returns the index of the node that currently serves writes.
func findLeader(c *cluster) (int, error) {
	cl, err := client.New(c.addrs)
	if err != nil {
		return -1, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Put(ctx, "bench-probe", "1"); err != nil {
		return -1, fmt.Errorf("cannot find the leader: %w", err)
	}
	i := c.indexOf(cl.LastServer())
	if i < 0 || c.cmds[i] == nil {
		return -1, fmt.Errorf("the serving node %q is not one of the running nodes", cl.LastServer())
	}
	return i, nil
}

// probe reports whether a write succeeds within limit.
func probe(addrs []string, limit time.Duration) bool {
	cl, err := client.New(addrs)
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	return cl.Put(ctx, "bench-probe", "1") == nil
}
