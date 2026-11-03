package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

func fill(t *testing.T, w *WAL, n int) []raft.LogEntry {
	t.Helper()
	var log []raft.LogEntry
	for i := 1; i <= n; i++ {
		log = append(log, entry(raft.Term((i+1)/2), raft.Index(i), "cmd"+string(rune('a'+i%26))))
	}
	if err := w.SaveHardState(raft.Term((n+1)/2), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(log); err != nil {
		t.Fatal(err)
	}
	return log
}

func TestSnapshotCompactsAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	log := fill(t, w, 10)
	before, _ := os.Stat(w.Path())

	snap := raft.Snapshot{Index: 6, Term: log[5].Term, Data: []byte("state@6")}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}

	want := raft.PersistentState{CurrentTerm: 5, VotedFor: 1, Snapshot: snap, Log: log[6:]}
	if got := mustLoad(t, w); !same(got, want) {
		t.Fatalf("after snapshot: %+v, want %+v", got, want)
	}
	after, _ := os.Stat(w.Path())
	if after.Size() >= before.Size() {
		t.Logf("file did not shrink (%d -> %d): the tail and snapshot outweigh the dropped prefix here", before.Size(), after.Size())
	}

	// The WAL keeps working on the new file: append at 11, truncate, and reject
	// anything that reaches into the snapshot.
	if err := w.Append([]raft.LogEntry{entry(6, 11, "k")}); err != nil {
		t.Fatalf("append at 11: %v", err)
	}
	if err := w.Append([]raft.LogEntry{entry(6, 7, "old")}); err == nil {
		t.Fatal("append inside the snapshot accepted")
	}
	for _, idx := range []raft.Index{0, 5, 6, 12} {
		if err := w.TruncateFrom(idx); err == nil {
			t.Fatalf("TruncateFrom(%d) accepted; the log is (6, 11]", idx)
		}
	}
	if err := w.TruncateFrom(9); err != nil {
		t.Fatal(err)
	}
	if err := w.SaveSnapshot(raft.Snapshot{Index: 6, Term: snap.Term}); err == nil {
		t.Fatal("a snapshot that does not advance was accepted")
	}

	w.Close()
	w = openWAL(t, dir)
	defer w.Close()
	want.Log = append([]raft.LogEntry(nil), log[6:8]...)
	if got := mustLoad(t, w); !same(got, want) {
		t.Fatalf("after reopen: %+v, want %+v", got, want)
	}
	if err := w.Append([]raft.LogEntry{entry(7, 9, "next")}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
}

func TestSnapshotThatDisagreesWithTheLogDiscardsIt(t *testing.T) {
	cases := map[string]raft.Snapshot{
		"wrong term at that index": {Index: 5, Term: 99, Data: []byte("x")},
		"beyond the end":           {Index: 20, Term: 4, Data: []byte("x")},
	}
	for name, snap := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w := openWAL(t, dir)
			fill(t, w, 10)
			if err := w.SaveSnapshot(snap); err != nil {
				t.Fatal(err)
			}
			want := raft.PersistentState{CurrentTerm: 5, VotedFor: 1, Snapshot: snap}
			if got := mustLoad(t, w); !same(got, want) {
				t.Fatalf("got %+v, want %+v", got, want)
			}
			// New entries continue right after the snapshot.
			if err := w.Append([]raft.LogEntry{entry(6, snap.Index+1, "n")}); err != nil {
				t.Fatalf("append after snapshot: %v", err)
			}
			w.Close()
			w = openWAL(t, dir)
			defer w.Close()
			if got := mustLoad(t, w); len(got.Log) != 1 || got.Log[0].Index != snap.Index+1 || got.Snapshot.Index != snap.Index {
				t.Fatalf("after reopen: %+v", got)
			}
		})
	}
}

func TestLeftoverCompactionFileIsIgnoredAndRemoved(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	log := fill(t, w, 4)
	w.Close()

	// A crash after writing the temporary file but before renaming it.
	tmp := filepath.Join(dir, FileName+tmpSuffix)
	if err := os.WriteFile(tmp, []byte("half a compaction"), 0o644); err != nil {
		t.Fatal(err)
	}
	w = openWAL(t, dir)
	defer w.Close()
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leftover compaction file still exists: %v", err)
	}
	if got := mustLoad(t, w); !same(got, raft.PersistentState{CurrentTerm: 2, VotedFor: 1, Log: log}) {
		t.Fatalf("log changed by a leftover compaction file: %+v", got)
	}
}

func TestFailedCompactionLeavesTheLogUntouched(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	defer w.Close()
	log := fill(t, w, 6)
	snap := raft.Snapshot{Index: 3, Term: log[2].Term, Data: []byte("s")}

	// A directory where the temporary file should go makes creating it fail.
	if err := os.Mkdir(filepath.Join(dir, FileName+tmpSuffix), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.SaveSnapshot(snap); err == nil {
		t.Fatal("SaveSnapshot succeeded although it could not write its file")
	}
	if got := mustLoad(t, w); !same(got, raft.PersistentState{CurrentTerm: 3, VotedFor: 1, Log: log}) {
		t.Fatalf("a failed compaction changed the log: %+v", got)
	}
	// Not poisoned, and it cleaned up after itself: the WAL works and compacts.
	if err := w.Append([]raft.LogEntry{entry(4, 7, "more")}); err != nil {
		t.Fatalf("append after failed compaction: %v", err)
	}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := mustLoad(t, w); got.Snapshot.Index != 3 || len(got.Log) != 4 {
		t.Fatalf("after retry: %+v", got)
	}
}

func TestOversizedSnapshotIsRejected(t *testing.T) {
	w := openWAL(t, t.TempDir())
	defer w.Close()
	fill(t, w, 3)
	if err := w.SaveSnapshot(raft.Snapshot{Index: 2, Term: 1, Data: make([]byte, maxBody)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	if got := mustLoad(t, w); got.Snapshot.Index != 0 || len(got.Log) != 3 {
		t.Fatalf("a rejected snapshot changed the log: %+v", got)
	}
}

func TestSnapshotRecordThatMovesBackwardsIsCorrupt(t *testing.T) {
	rec := func(index, term uint64) []byte {
		body := make([]byte, 17)
		body[0] = recSnapshot
		for i := 0; i < 8; i++ {
			body[1+i] = byte(index >> (8 * i))
			body[9+i] = byte(term >> (8 * i))
		}
		return frame(body)
	}
	for name, data := range map[string][]byte{
		"index 0":         rec(0, 1),
		"same index":      append(rec(5, 1), rec(5, 1)...),
		"goes backwards":  append(rec(5, 1), rec(4, 1)...),
		"too short":       frame([]byte{recSnapshot, 1, 2, 3}),
		"entry before it": append(rec(5, 1), frame(append([]byte{recEntry}, make([]byte, 16)...))...), // index 0, want 6
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := replay(data); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v, want ErrCorrupt", err)
			}
		})
	}
	// And a snapshot record in the middle of a file applies the same rule as SaveSnapshot.
	log := []raft.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "c")}
	var data []byte
	for _, e := range log {
		body := append([]byte{recEntry}, make([]byte, 16)...)
		body[1], body[9] = byte(e.Term), byte(e.Index)
		data = append(data, frame(append(body, e.Command...))...)
	}
	data = append(data, rec(2, 1)...)
	st, _, err := replay(data)
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshot.Index != 2 || !reflect.DeepEqual(st.Log, log[2:]) {
		t.Fatalf("got %+v, want snapshot at 2 and entry 3", st)
	}
}

// A crash can cut the file anywhere after the compaction. The compacted file
// itself appears whole (the rename only happens after its fsync), so the cuts
// that matter start at its end.
func TestRecoveryAfterASnapshotFromTornWriteAtEveryOffset(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	defer w.Close()
	log := fill(t, w, 8)
	if err := w.SaveSnapshot(raft.Snapshot{Index: 5, Term: log[4].Term, Data: bytes.Repeat([]byte("s"), 40)}); err != nil {
		t.Fatal(err)
	}

	var s script
	record := func() {
		data, err := os.ReadFile(w.Path())
		if err != nil {
			t.Fatal(err)
		}
		s.sizes = append(s.sizes, len(data))
		s.states = append(s.states, mustLoad(t, w))
		s.data = data
	}
	record() // state 0 of this script is the freshly compacted file
	for i, step := range []func() error{
		func() error { return w.Append([]raft.LogEntry{entry(5, 9, "after")}) },
		func() error { return w.SaveHardState(6, raft.NoVote) },
		func() error { return w.TruncateFrom(8) },
		func() error { return w.Append([]raft.LogEntry{entry(6, 8, "again")}) },
		func() error { return w.Append([]raft.LogEntry{entry(6, 9, "and more")}) },
	} {
		if err := step(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		record()
	}

	for _, zeroFill := range []bool{false, true} {
		for cut := s.sizes[0]; cut <= len(s.data); cut++ {
			content := append([]byte(nil), s.data[:cut]...)
			if zeroFill {
				content = append(content, make([]byte, len(s.data)-cut)...)
			}
			want, wantSize := s.expectedFor(content)
			got, valid, err := replay(content)
			if err != nil {
				t.Fatalf("zeroFill=%v cut=%d: %v", zeroFill, cut, err)
			}
			if !same(got, want) || valid != wantSize {
				t.Fatalf("zeroFill=%v cut=%d: got %+v (valid %d), want %+v (valid %d)", zeroFill, cut, got, valid, want, wantSize)
			}
		}
	}
}
