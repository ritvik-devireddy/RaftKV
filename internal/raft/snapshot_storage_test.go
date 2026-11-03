package raft

import (
	"reflect"
	"testing"
)

func logOfTerms(terms ...Term) []LogEntry {
	var log []LogEntry
	for i, tm := range terms {
		log = append(log, LogEntry{Term: tm, Index: Index(i + 1), Command: []byte{byte('a' + i)}})
	}
	return log
}

// ApplySnapshot is the one place that decides what a snapshot does to a log.
func TestApplySnapshot(t *testing.T) {
	// log: indexes 1..5 with terms 1 1 2 2 3
	for name, tc := range map[string]struct {
		snap     Snapshot
		wantLog  []LogEntry
		wantKept bool
	}{
		"compacts a prefix of the log":      {Snapshot{Index: 3, Term: 2}, logOfTerms(1, 1, 2, 2, 3)[3:], true},
		"compacts up to the last entry":     {Snapshot{Index: 5, Term: 3}, nil, true},
		"compacts only the first entry":     {Snapshot{Index: 1, Term: 1}, logOfTerms(1, 1, 2, 2, 3)[1:], true},
		"wrong term at that index: dropped": {Snapshot{Index: 3, Term: 3}, nil, false},
		"beyond the log: dropped":           {Snapshot{Index: 9, Term: 3}, nil, false},
		"just past the log: dropped":        {Snapshot{Index: 6, Term: 3}, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			s := PersistentState{CurrentTerm: 3, VotedFor: 1, Log: logOfTerms(1, 1, 2, 2, 3)}
			snap := tc.snap
			snap.Data = []byte("data")
			s.ApplySnapshot(snap)

			if s.Snapshot.Index != snap.Index || s.Snapshot.Term != snap.Term || string(s.Snapshot.Data) != "data" {
				t.Fatalf("snapshot = %+v", s.Snapshot)
			}
			if !reflect.DeepEqual(s.Log, tc.wantLog) {
				t.Fatalf("log = %+v, want %+v", s.Log, tc.wantLog)
			}
			if s.CurrentTerm != 3 || s.VotedFor != 1 {
				t.Fatalf("term and vote changed: %+v", s)
			}
			snap.Data[0] = 'X' // the state must own its copy
			if string(s.Snapshot.Data) != "data" {
				t.Fatal("ApplySnapshot kept a reference to the caller's data")
			}
		})
	}
}

func TestApplySnapshotOnTopOfAnEarlierSnapshot(t *testing.T) {
	s := PersistentState{
		Snapshot: Snapshot{Index: 10, Term: 2},
		Log:      []LogEntry{{Term: 2, Index: 11}, {Term: 3, Index: 12}, {Term: 3, Index: 13}},
	}
	s.ApplySnapshot(Snapshot{Index: 12, Term: 3})
	if s.Snapshot.Index != 12 || len(s.Log) != 1 || s.Log[0].Index != 13 {
		t.Fatalf("got %+v, want snapshot at 12 and entry 13", s)
	}
}

func TestMemoryStorageSnapshots(t *testing.T) {
	st := NewMemoryStorage()
	st.Save(PersistentState{CurrentTerm: 2, VotedFor: 1, Log: logOfTerms(1, 1, 2)})

	if err := st.SaveSnapshot(Snapshot{Index: 2, Term: 1, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Load()
	if got.Snapshot.Index != 2 || len(got.Log) != 1 || got.Log[0].Index != 3 || got.CurrentTerm != 2 || got.VotedFor != 1 {
		t.Fatalf("got %+v", got)
	}
	// Appends continue after the log's last index, not after the snapshot's.
	if err := st.Append([]LogEntry{{Term: 2, Index: 4}}); err != nil {
		t.Fatal(err)
	}

	for _, snap := range []Snapshot{{Index: 0}, {Index: 1, Term: 1}, {Index: 2, Term: 1}} {
		if err := st.SaveSnapshot(snap); err == nil {
			t.Fatalf("SaveSnapshot(%+v) was accepted although it does not advance", snap)
		}
	}

	// Load hands out copies: changing them must not change what is stored.
	got, _ = st.Load()
	got.Snapshot.Data[0] = 'X'
	got.Log[0].Term = 99
	again, _ := st.Load()
	if string(again.Snapshot.Data) != "s" || again.Log[0].Term == 99 {
		t.Fatal("Load returned a view into the stored state")
	}
}
