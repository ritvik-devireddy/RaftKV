package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

func openWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := Open(dir, Options{NoSync: true}) // fsync is irrelevant to logic tests
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustLoad(t *testing.T, s raft.Storage) raft.PersistentState {
	t.Helper()
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// same compares states, treating a nil and an empty log as equal.
func same(a, b raft.PersistentState) bool {
	if len(a.Log) == 0 && len(b.Log) == 0 {
		a.Log, b.Log = nil, nil
	}
	return reflect.DeepEqual(a, b)
}

func entry(term raft.Term, index raft.Index, cmd string) raft.LogEntry {
	e := raft.LogEntry{Term: term, Index: index}
	if cmd != "" {
		e.Command = []byte(cmd)
	}
	return e
}

func TestEmptyWALLoadsInitialState(t *testing.T) {
	w := openWAL(t, t.TempDir())
	defer w.Close()
	want := raft.PersistentState{VotedFor: raft.NoVote}
	if got := mustLoad(t, w); !same(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// The WAL must behave exactly like MemoryStorage, including across reopens.
func TestWALMatchesMemoryStorageModel(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		w := openWAL(t, dir)
		model := raft.NewMemoryStorage()
		term := raft.Term(0)

		for step := 0; step < 200; step++ {
			// Reading the file back is the expensive part (slow on Windows,
			// where every open is scanned), so compare every 10th step and
			// right after every reopen rather than after each operation.
			check := step%10 == 0
			switch rng.Intn(12) {
			case 10, 11:
				// Compact up to a random point in the log, or install a snapshot that
				// reaches past it or disagrees with it.
				cur := mustLoad(t, model)
				idx := cur.Snapshot.Index + raft.Index(rng.Intn(len(cur.Log)+3)+1)
				snapTerm := term
				if pos := int(idx - cur.Snapshot.Index); pos <= len(cur.Log) && rng.Intn(4) != 0 {
					snapTerm = cur.Log[pos-1].Term // a prefix of our own log
				}
				snap := raft.Snapshot{Index: idx, Term: snapTerm, Data: bytes.Repeat([]byte{byte(step)}, rng.Intn(50))}
				if err := w.SaveSnapshot(snap); err != nil {
					t.Fatal(err)
				}
				if err := model.SaveSnapshot(snap); err != nil {
					t.Fatal(err)
				}
				check = true
			case 0, 1, 2:
				if rng.Intn(2) == 0 {
					term += raft.Term(rng.Intn(3))
				}
				vote := raft.NodeID(rng.Intn(4) - 1)
				if err := w.SaveHardState(term, vote); err != nil {
					t.Fatal(err)
				}
				model.SaveHardState(term, vote)
			case 3:
				cur := mustLoad(t, model)
				if len(cur.Log) == 0 {
					continue
				}
				idx := cur.Snapshot.Index + raft.Index(rng.Intn(len(cur.Log))+1)
				if err := w.TruncateFrom(idx); err != nil {
					t.Fatal(err)
				}
				if err := model.TruncateFrom(idx); err != nil {
					t.Fatal(err)
				}
			case 4:
				// reopen: recovery must reproduce the state exactly
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				w = openWAL(t, dir)
				check = true
			default:
				cur := mustLoad(t, model)
				var batch []raft.LogEntry
				for i := 0; i <= rng.Intn(3); i++ {
					cmd := ""
					if rng.Intn(5) != 0 {
						cmd = string(bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, rng.Intn(40)+1))
					}
					batch = append(batch, entry(term, cur.Snapshot.Index+raft.Index(len(cur.Log)+i+1), cmd))
				}
				if err := w.Append(batch); err != nil {
					t.Fatal(err)
				}
				if err := model.Append(batch); err != nil {
					t.Fatal(err)
				}
			}
			if !check {
				continue
			}
			if got, want := mustLoad(t, w), mustLoad(t, model); !same(got, want) {
				t.Fatalf("seed %d step %d: wal %+v, model %+v", seed, step, got, want)
			}
		}
		w.Close()
	}
}

// script builds a WAL one record at a time and remembers the file size and the
// expected state after each record.
type script struct {
	sizes  []int
	states []raft.PersistentState
	data   []byte
}

func buildScript(t *testing.T) script {
	t.Helper()
	dir := t.TempDir()
	w := openWAL(t, dir)
	defer w.Close()

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
	// state 0: the empty file
	s.sizes = append(s.sizes, 0)
	s.states = append(s.states, raft.PersistentState{VotedFor: raft.NoVote})

	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(w.SaveHardState(1, 2))
	record()
	check(w.Append([]raft.LogEntry{entry(1, 1, "")}))
	record()
	check(w.Append([]raft.LogEntry{entry(1, 2, "put x=1")}))
	record()
	check(w.Append([]raft.LogEntry{entry(1, 3, string(bytes.Repeat([]byte("z"), 300)))})) // length > 255
	record()
	check(w.SaveHardState(2, raft.NoVote))
	record()
	check(w.TruncateFrom(2))
	record()
	check(w.Append([]raft.LogEntry{entry(2, 2, "put y=2")}))
	record()
	check(w.SaveHardState(3, 1))
	record()
	return s
}

// expectedFor returns the state after the last record that content holds
// intact, i.e. whose bytes are identical to the original file's.
func (s script) expectedFor(content []byte) (raft.PersistentState, int) {
	k := 0
	for i, size := range s.sizes {
		if size <= len(content) && bytes.Equal(content[:size], s.data[:size]) {
			k = i
		}
	}
	return s.states[k], s.sizes[k]
}

// A crash can cut the file at any byte. Whatever the cut, recovery must give
// back exactly the records that were completely written, and the WAL must then
// be usable.
//
// The pure recovery logic (replay) is checked at every offset in memory. The
// real file path (Open cutting the tail, then appending) is checked at a
// sample of offsets plus every record boundary and its neighbours: creating
// thousands of files is slow on some filesystems.
func TestRecoveryFromTornWriteAtEveryOffset(t *testing.T) {
	s := buildScript(t)
	nearBoundary := func(cut int) bool {
		for _, size := range s.sizes {
			if cut >= size-1 && cut <= size+1 {
				return true
			}
		}
		return false
	}
	for _, zeroFill := range []bool{false, true} {
		for cut := 0; cut <= len(s.data); cut++ {
			content := append([]byte(nil), s.data[:cut]...)
			if zeroFill {
				// Some filesystems extend the file with zeros after a crash.
				content = append(content, make([]byte, len(s.data)-cut)...)
			}
			want, wantSize := s.expectedFor(content)

			got, valid, err := replay(content)
			if err != nil {
				t.Fatalf("zeroFill=%v cut=%d: replay: %v", zeroFill, cut, err)
			}
			if !same(got, want) || valid != wantSize {
				t.Fatalf("zeroFill=%v cut=%d: replay gave %+v (valid %d), want %+v (valid %d)",
					zeroFill, cut, got, valid, want, wantSize)
			}

			if cut%17 != 0 && !nearBoundary(cut) {
				continue
			}
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, FileName), content, 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := Open(dir, Options{NoSync: true})
			if err != nil {
				t.Fatalf("zeroFill=%v cut=%d: open: %v", zeroFill, cut, err)
			}
			if info, _ := os.Stat(w.Path()); info.Size() != int64(wantSize) {
				t.Fatalf("zeroFill=%v cut=%d: file is %d bytes after recovery, want %d",
					zeroFill, cut, info.Size(), wantSize)
			}

			// Writing after recovery must land on a clean boundary and survive a reopen.
			next := raft.Index(len(want.Log)) + 1
			if err := w.Append([]raft.LogEntry{entry(9, next, "after")}); err != nil {
				t.Fatalf("zeroFill=%v cut=%d: append after recovery: %v", zeroFill, cut, err)
			}
			w.Close()
			w2 := openWAL(t, dir)
			after := mustLoad(t, w2)
			if n := len(after.Log); n != len(want.Log)+1 || string(after.Log[n-1].Command) != "after" {
				t.Fatalf("zeroFill=%v cut=%d: lost the post-recovery append: %+v", zeroFill, cut, after.Log)
			}
			w2.Close()
		}
	}
}

// A batch is several records written together. A crash in the middle leaves a
// prefix of the batch, never a gap.
func TestTornBatchRecoversAPrefix(t *testing.T) {
	w := openWAL(t, t.TempDir())
	var batch []raft.LogEntry
	for i := 1; i <= 6; i++ {
		batch = append(batch, entry(1, raft.Index(i), "cmd"))
	}
	if err := w.Append(batch); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(w.Path())
	w.Close()

	for cut := 0; cut <= len(data); cut++ {
		st, _, err := replay(data[:cut])
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if got := st.Log; len(got) > 0 && !reflect.DeepEqual(got, batch[:len(got)]) {
			t.Fatalf("cut=%d: recovered log is not a prefix of the batch: %+v", cut, got)
		}
	}
}

// Damage inside the log, with valid records after it, must be reported and
// never silently skipped.
func TestCorruptionInTheMiddleIsAnError(t *testing.T) {
	s := buildScript(t)
	// Flip one bit in every byte of every record except the last one.
	lastStart := s.sizes[len(s.sizes)-2]
	for pos := 0; pos < lastStart; pos++ {
		data := append([]byte(nil), s.data...)
		data[pos] ^= 0x10
		_, _, err := replay(data)
		if err == nil {
			// The only acceptable silent outcome is the documented limitation:
			// a length field made to look like a record running off the end.
			if !lengthFieldOfSomeRecord(s, pos) {
				t.Fatalf("flip at byte %d was not detected", pos)
			}
			continue
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flip at byte %d: got %v, want ErrCorrupt", pos, err)
		}
	}
}

func lengthFieldOfSomeRecord(s script, pos int) bool {
	for i := 0; i+1 < len(s.sizes); i++ {
		if pos >= s.sizes[i]+4 && pos < s.sizes[i]+8 {
			return true
		}
	}
	return false
}

// A damaged final record is what a half-finished write looks like, so it is
// dropped rather than treated as fatal.
func TestDamagedLastRecordIsTreatedAsTorn(t *testing.T) {
	s := buildScript(t)
	lastStart := s.sizes[len(s.sizes)-2]
	for pos := lastStart; pos < len(s.data); pos++ {
		data := append([]byte(nil), s.data...)
		data[pos] ^= 0x01
		got, valid, err := replay(data)
		if err != nil {
			t.Fatalf("flip at byte %d: %v", pos, err)
		}
		if want := s.states[len(s.states)-2]; !same(got, want) || valid != lastStart {
			t.Fatalf("flip at byte %d: got %+v (valid %d), want %+v (valid %d)", pos, got, valid, want, lastStart)
		}
	}
}

func TestAppendRejectsNonContiguousIndexes(t *testing.T) {
	w := openWAL(t, t.TempDir())
	defer w.Close()
	if err := w.Append([]raft.LogEntry{entry(1, 2, "gap")}); err == nil {
		t.Fatal("append that skips index 1 was accepted")
	}
	if err := w.Append([]raft.LogEntry{entry(1, 1, "a"), entry(1, 3, "b")}); err == nil {
		t.Fatal("batch with a gap was accepted")
	}
	if got := mustLoad(t, w); len(got.Log) != 0 {
		t.Fatalf("rejected appends left entries behind: %+v", got.Log)
	}
	if err := w.TruncateFrom(1); err == nil {
		t.Fatal("truncate of an empty log was accepted")
	}
}

func TestUnchangedHardStateWritesNothing(t *testing.T) {
	w := openWAL(t, t.TempDir())
	defer w.Close()
	w.SaveHardState(4, 1)
	before := w.size
	w.SaveHardState(4, 1)
	if w.size != before {
		t.Fatal("saving an unchanged term and vote grew the file")
	}
	if err := w.SaveHardState(3, 1); err == nil {
		t.Fatal("term going backwards was accepted")
	}
}

func TestFailedWritePoisonsTheWAL(t *testing.T) {
	w := openWAL(t, t.TempDir())
	w.f.Close() // every write now fails
	if err := w.SaveHardState(1, 0); err == nil {
		t.Fatal("write to a closed file succeeded")
	}
	if err := w.Append([]raft.LogEntry{entry(1, 1, "x")}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("got %v, want ErrPoisoned", err)
	}
}

func TestRecordWithImpossibleSemanticsIsCorrupt(t *testing.T) {
	// A perfectly checksummed record that skips an index cannot come from a
	// crash; it means a bug or tampering.
	data := frame(append([]byte{recEntry}, make([]byte, 16)...)) // term 0, index 0
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, FileName), data, 0o644)
	if _, err := Open(dir, Options{NoSync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestOversizedEntryIsRejected(t *testing.T) {
	w := openWAL(t, t.TempDir())
	defer w.Close()
	big := raft.LogEntry{Term: 1, Index: 1, Command: make([]byte, maxBody)}
	if err := w.Append([]raft.LogEntry{big}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

// ---- end to end: a real raft node on a real WAL ----

type nopTransport struct{}

func (nopTransport) Send(raft.Message) {}

func TestNodeRecoversFromWALAfterRestart(t *testing.T) {
	dir := t.TempDir()
	var applied []string
	cfg := func() raft.Config {
		return raft.Config{
			ID:    0,
			Peers: []raft.NodeID{0},
			Seed:  1,
			Apply: func(e raft.LogEntry) {
				if !e.IsNoOp() {
					applied = append(applied, string(e.Command))
				}
			},
		}
	}
	elect := func(n *raft.Node) {
		for i := 0; i < 100 && n.Status().Role != raft.Leader; i++ {
			n.Tick()
		}
		if n.Status().Role != raft.Leader {
			t.Fatal("single node never became leader")
		}
	}

	w := openWAL(t, dir)
	n, err := raft.NewNode(cfg(), w, nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	elect(n)
	for _, c := range []string{"a", "b", "c"} {
		if _, _, ok := n.Propose([]byte(c)); !ok {
			t.Fatal("propose rejected")
		}
	}
	before := n.Status()
	w.Close()

	// "Crash": drop the node, reopen the file.
	applied = nil
	w2 := openWAL(t, dir)
	defer w2.Close()
	n2, err := raft.NewNode(cfg(), w2, nopTransport{})
	if err != nil {
		t.Fatal(err)
	}
	after := n2.Status()
	if after.Term != before.Term || after.LastLogIndex != before.LastLogIndex {
		t.Fatalf("restart lost state: before %+v, after %+v", before, after)
	}
	if after.Role != raft.Follower {
		t.Fatalf("a restarted node must start as a follower, got %v", after.Role)
	}

	elect(n2)
	if got := n2.Status().Term; got <= before.Term {
		t.Fatalf("new leader term %d should exceed %d", got, before.Term)
	}
	if !reflect.DeepEqual(applied, []string{"a", "b", "c"}) {
		t.Fatalf("recovered log was not re-applied: %v", applied)
	}
}

// An empty record followed by real data cannot be a torn tail (a crash leaves
// nothing after the break), so it is reported rather than silently skipped.
func TestZeroLengthRecordFollowedByDataIsCorrupt(t *testing.T) {
	good := frame(append([]byte{recHardState}, make([]byte, 16)...))
	data := append(append([]byte(nil), good...), make([]byte, headerSize)...)
	data = append(data, good...)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, FileName), data, 0o644)
	if _, err := Open(dir, Options{NoSync: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}
