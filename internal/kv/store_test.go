package kv

import "testing"

func TestPutGetDelete(t *testing.T) {
	s := NewStore()

	if r := s.Apply(Command{Op: OpGet, Key: "a"}); r.OK {
		t.Fatalf("get of missing key: %+v", r)
	}
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	if r := s.Apply(Command{Op: OpGet, Key: "a"}); !r.OK || r.Value != "1" {
		t.Fatalf("get after put: %+v", r)
	}
	if r := s.Apply(Command{Op: OpDelete, Key: "a"}); !r.OK {
		t.Fatalf("delete of existing key: %+v", r)
	}
	if r := s.Apply(Command{Op: OpDelete, Key: "a"}); r.OK {
		t.Fatalf("delete of missing key should report OK=false: %+v", r)
	}
}

func TestCompareAndSwap(t *testing.T) {
	s := NewStore()

	// A missing key compares equal to "", so this creates it.
	if r := s.Apply(Command{Op: OpCAS, Key: "k", Expected: "", Value: "v1"}); !r.OK {
		t.Fatalf("create-if-absent failed: %+v", r)
	}
	if r := s.Apply(Command{Op: OpCAS, Key: "k", Expected: "wrong", Value: "v2"}); r.OK || r.Value != "v1" {
		t.Fatalf("CAS with wrong expected value: %+v", r)
	}
	if r := s.Apply(Command{Op: OpCAS, Key: "k", Expected: "v1", Value: "v2"}); !r.OK || r.Value != "v2" {
		t.Fatalf("CAS with right expected value: %+v", r)
	}
}

func TestRetriedCommandIsAppliedOnce(t *testing.T) {
	s := NewStore()

	s.Apply(Command{Op: OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1})
	s.Apply(Command{Op: OpPut, Key: "k", Value: "second", ClientID: "c1", Seq: 2})

	// The client never saw the reply to seq 1 and retries it. Re-applying it
	// would overwrite "second" with "first".
	r := s.Apply(Command{Op: OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1})
	if !r.OK {
		t.Fatalf("retry should return a result: %+v", r)
	}
	if got := s.Apply(Command{Op: OpGet, Key: "k"}); got.Value != "second" {
		t.Fatalf("retry was applied again: k = %q, want %q", got.Value, "second")
	}
}

func TestDedupIsPerClient(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1", ClientID: "c1", Seq: 5})
	s.Apply(Command{Op: OpPut, Key: "b", Value: "2", ClientID: "c2", Seq: 1})

	if got := s.Apply(Command{Op: OpGet, Key: "b"}); got.Value != "2" {
		t.Fatalf("client c2 was wrongly deduplicated against c1: %+v", got)
	}
}
