package kv

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func populated() *Store {
	s := NewStore()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1", ClientID: "c1", Seq: 1})
	s.Apply(Command{Op: OpPut, Key: "b", Value: "", ClientID: "c2", Seq: 1}) // an empty value is a value
	s.Apply(Command{Op: OpDelete, Key: "a", ClientID: "c1", Seq: 2})
	s.Apply(Command{Op: OpCAS, Key: "b", Expected: "", Value: "2", ClientID: "c2", Seq: 2})
	s.Apply(Command{Op: OpCAS, Key: "b", Expected: "nope", Value: "3", ClientID: "c3", Seq: 7}) // a failed CAS result is cached too
	s.Apply(Command{Op: OpPut, Key: "k\x00é", Value: "v\xff"})                                  // no client: no session
	return s
}

func TestStateRoundTrip(t *testing.T) {
	s := populated()
	got := NewStore()
	if err := got.UnmarshalState(s.MarshalState()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.data, s.data) || !reflect.DeepEqual(got.sessions, s.sessions) {
		t.Fatalf("restored %+v / %+v, want %+v / %+v", got.data, got.sessions, s.data, s.sessions)
	}
}

func TestEmptyStateRoundTrip(t *testing.T) {
	got := populated()
	if err := got.UnmarshalState(NewStore().MarshalState()); err != nil {
		t.Fatal(err)
	}
	if len(got.data) != 0 || len(got.sessions) != 0 {
		t.Fatalf("restoring an empty snapshot left %+v / %+v", got.data, got.sessions)
	}
}

// The reason sessions are in the snapshot: a restored replica must still
// recognize a retry of a command it never saw applied.
func TestRestoredStoreStillDeduplicatesRetries(t *testing.T) {
	orig := NewStore()
	put := Command{Op: OpPut, Key: "x", Value: "1", ClientID: "c", Seq: 5}
	orig.Apply(put)

	restored := NewStore()
	if err := restored.UnmarshalState(orig.MarshalState()); err != nil {
		t.Fatal(err)
	}
	// The same command again, after something else changed x. If the retry
	// were applied it would overwrite "2" with "1".
	restored.Apply(Command{Op: OpPut, Key: "x", Value: "2", ClientID: "other", Seq: 1})
	if res := restored.Apply(put); res != (Result{Value: "1", OK: true}) {
		t.Fatalf("retry returned %+v, want the cached result", res)
	}
	if restored.data["x"] != "2" {
		t.Fatalf("x = %q: the retry was applied a second time", restored.data["x"])
	}
	// A newer command from the same client does run.
	if res := restored.Apply(Command{Op: OpPut, Key: "x", Value: "3", ClientID: "c", Seq: 6}); !res.OK || restored.data["x"] != "3" {
		t.Fatalf("a new command was not applied: %+v, x=%q", res, restored.data["x"])
	}
}

// Replicas must produce byte-identical snapshots for identical history, however
// Go happens to order its maps.
func TestStateEncodingIsDeterministic(t *testing.T) {
	want := populated().MarshalState()
	for i := 0; i < 50; i++ {
		if got := populated().MarshalState(); !bytes.Equal(got, want) {
			t.Fatalf("run %d produced different bytes", i)
		}
	}
	// Even a store built by restoring (different map history) agrees.
	r := NewStore()
	if err := r.UnmarshalState(want); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.MarshalState(), want) {
		t.Fatal("re-encoding a restored store changed the bytes")
	}
}

func TestRandomStatesRoundTrip(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		s := NewStore()
		for i := 0; i < rng.Intn(60); i++ {
			cmd := Command{
				Op:       Op(rng.Intn(4)),
				Key:      fmt.Sprintf("k%d", rng.Intn(8)),
				Value:    fmt.Sprintf("v%d", rng.Intn(1000)),
				Expected: fmt.Sprintf("v%d", rng.Intn(1000)),
			}
			if rng.Intn(4) != 0 {
				cmd.ClientID, cmd.Seq = fmt.Sprintf("c%d", rng.Intn(5)), uint64(i+1)
			}
			s.Apply(cmd)
		}
		got := NewStore()
		if err := got.UnmarshalState(s.MarshalState()); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !reflect.DeepEqual(got.data, s.data) || !reflect.DeepEqual(got.sessions, s.sessions) {
			t.Fatalf("seed %d: state changed in the round trip", seed)
		}
	}
}

func TestMalformedStateIsRejectedAndLeavesTheStoreAlone(t *testing.T) {
	good := populated().MarshalState()
	check := func(name string, b []byte) {
		t.Helper()
		s := populated()
		before := s.MarshalState()
		if err := s.UnmarshalState(b); err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if !bytes.Equal(s.MarshalState(), before) {
			t.Fatalf("%s: a failed restore changed the store", name)
		}
	}

	// Every proper prefix is truncated somewhere.
	for n := 0; n < len(good); n++ {
		check(fmt.Sprintf("prefix of %d bytes", n), good[:n])
	}
	check("trailing byte", append(append([]byte(nil), good...), 0))
	check("wrong version", append([]byte{99}, good[1:]...))
	check("absurd key count", []byte{stateVersion, 0xff, 0xff, 0xff, 0xff, 0x0f})

	// A session flag other than 0 or 1.
	bad := NewStore()
	bad.Apply(Command{Op: OpPut, Key: "a", Value: "b", ClientID: "c", Seq: 1})
	enc := bad.MarshalState()
	enc[len(enc)-1] = 2
	check("session flag 2", enc)
}
