package chaos

import (
	"reflect"
	"testing"
)

// The same seed must give the same history, byte for byte: that is what makes
// a failing seed a reproducible bug report.
func TestRunsAreDeterministic(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		a := NewWorld(Options{Seed: seed}).Run(1500, 1000)
		b := NewWorld(Options{Seed: seed}).Run(1500, 1000)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("seed %d produced two different histories", seed)
		}
	}
	x := NewWorld(Options{Seed: 1}).Run(1500, 1000)
	y := NewWorld(Options{Seed: 2}).Run(1500, 1000)
	if reflect.DeepEqual(x, y) {
		t.Fatal("different seeds produced identical histories")
	}
}

// Without faults every operation must complete, so a checker is not being
// handed a history of nothing.
func TestFaultFreeRunCompletesEverything(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		w := NewWorld(Options{Seed: seed, DropRate: 1e-9, CrashRate: 1e-12, PartitionRate: 1e-12, AbandonRate: 1e-12})
		h := w.Run(1000, 1000)
		if len(h) < 40 {
			t.Fatalf("seed %d: only %d operations were issued", seed, len(h))
		}
		for i, op := range h {
			if !op.Done {
				t.Fatalf("seed %d: operation %d (%+v) never completed without faults", seed, i, op.Cmd)
			}
			if op.Return <= op.Call {
				t.Fatalf("seed %d: operation %d returns (%d) before it is called (%d)", seed, i, op.Return, op.Call)
			}
		}
	}
}

// Under chaos most operations should still finish; a few may be left
// indeterminate, which is the point.
func TestChaosRunMakesProgress(t *testing.T) {
	var total, done int
	for seed := int64(1); seed <= 10; seed++ {
		h := NewWorld(Options{Seed: seed, Nodes: 3 + 2*int(seed%2)}).Run(1500, 1500)
		for _, op := range h {
			total++
			if op.Done {
				done++
				if op.Return <= op.Call {
					t.Fatalf("seed %d: bad times %+v", seed, op)
				}
			}
		}
	}
	if done*2 < total {
		t.Fatalf("only %d of %d operations completed under chaos", done, total)
	}
	t.Logf("%d of %d operations completed", done, total)
}

// Snapshots are on by default, so a chaos run compacts logs, restarts nodes
// from snapshots and catches followers up with them. The runs must really get
// that far, or checking them for linearizability would say nothing about it.
func TestChaosRunsCompactAndInstallSnapshots(t *testing.T) {
	var snapshots, installs int
	for seed := int64(1); seed <= 20; seed++ {
		w := NewWorld(Options{Seed: seed, Nodes: 3 + 2*int(seed%2)})
		w.Run(1500, 1500)
		st := w.Stats()
		if st.Snapshots == 0 {
			t.Fatalf("seed %d: no node ever compacted its log", seed)
		}
		snapshots += st.Snapshots
		installs += st.Installs
	}
	if installs == 0 {
		t.Fatal("no InstallSnapshot was sent in 20 runs")
	}
	t.Logf("20 runs: %d snapshots saved, %d InstallSnapshot messages", snapshots, installs)
}

func TestSnapshotsCanBeSwitchedOff(t *testing.T) {
	w := NewWorld(Options{Seed: 3, SnapshotEvery: -1})
	w.Run(1500, 1500)
	if st := w.Stats(); st.Snapshots != 0 || st.Installs != 0 {
		t.Fatalf("snapshots were taken with SnapshotEvery < 0: %+v", st)
	}
}
