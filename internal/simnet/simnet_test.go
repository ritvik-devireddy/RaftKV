package simnet

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// schedule sends 20 messages around a 3-node ring and returns the delivery log.
func schedule(seed int64) []string {
	n := New(Config{Seed: seed, MinDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
	var log []string
	for id := 0; id < 3; id++ {
		n.Register(id, func(from, to int, msg any) {
			log = append(log, fmt.Sprintf("%d->%d:%v@%v", from, to, msg, n.Now()))
		})
	}
	for i := 0; i < 20; i++ {
		n.Send(i%3, (i+1)%3, i)
	}
	n.RunUntil(time.Second)
	return log
}

func TestSameSeedSameSchedule(t *testing.T) {
	a, b := schedule(42), schedule(42)
	if len(a) != 20 {
		t.Fatalf("delivered %d messages, want 20", len(a))
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different schedules")
	}
}

func TestDifferentSeedsDifferentSchedules(t *testing.T) {
	if reflect.DeepEqual(schedule(1), schedule(2)) {
		t.Fatal("different seeds produced identical schedules")
	}
}

func TestDropRateOneDropsEverything(t *testing.T) {
	n := New(Config{Seed: 1, DropRate: 1})
	delivered := 0
	n.Register(1, func(int, int, any) { delivered++ })
	for i := 0; i < 10; i++ {
		n.Send(0, 1, i)
	}
	n.RunUntil(time.Second)
	if delivered != 0 || n.Stats().Dropped != 10 {
		t.Fatalf("delivered=%d stats=%+v", delivered, n.Stats())
	}
}

func TestPartitionAndHeal(t *testing.T) {
	n := New(Config{Seed: 1, MinDelay: time.Millisecond, MaxDelay: time.Millisecond})
	got := map[string]int{}
	for id := 0; id < 3; id++ {
		n.Register(id, func(from, to int, _ any) { got[fmt.Sprintf("%d->%d", from, to)]++ })
	}

	n.Partition([]int{0}, []int{1, 2})
	n.Send(0, 1, "x") // crosses the partition: dropped
	n.Send(1, 2, "x") // same side: delivered
	n.RunUntil(time.Second)
	if got["0->1"] != 0 || got["1->2"] != 1 {
		t.Fatalf("during partition: %v", got)
	}

	n.Heal()
	n.Send(0, 1, "x")
	n.RunUntil(2 * time.Second)
	if got["0->1"] != 1 {
		t.Fatalf("after heal: %v", got)
	}
}

func TestPartitionDropsInFlightMessages(t *testing.T) {
	n := New(Config{Seed: 1, MinDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond})
	delivered := 0
	n.Register(0, func(int, int, any) {})
	n.Register(1, func(int, int, any) { delivered++ })

	n.Send(0, 1, "x")               // in flight for 10ms
	n.Partition([]int{0}, []int{1}) // cut the link before it arrives
	n.RunUntil(time.Second)
	if delivered != 0 {
		t.Fatal("message crossed a partition that was created while it was in flight")
	}
}
