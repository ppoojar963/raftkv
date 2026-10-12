package raft

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func logOf(terms ...int) []LogEntry {
	l := []LogEntry{{}}
	for i, t := range terms {
		l = append(l, LogEntry{Term: t, Index: i + 1, Command: []byte{byte(i), byte(t)}})
	}
	return l
}

func reopen(t *testing.T, dir string) PersistentState {
	t.Helper()
	w, err := NewWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	st, _, _ := w.LoadState()
	return st
}

func TestWALRoundTripWithTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	must := func(st PersistentState) {
		if err := w.SaveState(st); err != nil {
			t.Fatal(err)
		}
	}
	must(PersistentState{CurrentTerm: 1, VotedFor: 2, Log: logOf(1, 1, 1)})
	must(PersistentState{CurrentTerm: 2, VotedFor: -1, Log: logOf(1, 1, 1, 2)})
	// Conflict at index 3: follower overwrites with a term-3 suffix.
	want := PersistentState{CurrentTerm: 3, VotedFor: 4, Log: logOf(1, 1, 3, 3)}
	must(want)
	w.Close()
	if got := reopen(t, dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered %+v\nwant %+v", got, want)
	}
}

// A crash in the middle of a write leaves a torn record; recovery must keep
// every record before it and drop the partial one.
func TestWALTornTail(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWALStorage(dir)
	want := PersistentState{CurrentTerm: 1, VotedFor: 1, Log: logOf(1, 1)}
	w.SaveState(want)
	w.SaveState(PersistentState{CurrentTerm: 1, VotedFor: 1, Log: logOf(1, 1, 1)})
	w.Close()
	path := filepath.Join(dir, "raft.wal")
	fi, _ := os.Stat(path)
	os.Truncate(path, fi.Size()-3)
	if got := reopen(t, dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered %+v\nwant %+v", got, want)
	}
	// And the WAL stays writable after recovery.
	w2, _ := NewWALStorage(dir)
	next := PersistentState{CurrentTerm: 2, VotedFor: -1, Log: logOf(1, 1, 2)}
	w2.SaveState(next)
	w2.Close()
	if got := reopen(t, dir); !reflect.DeepEqual(got, next) {
		t.Fatalf("after torn-tail recovery got %+v", got)
	}
}

// Randomized model check: apply random appends/truncations/term bumps, and
// compare the recovered state against what we last saved.
func TestWALRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 50; trial++ {
		dir := t.TempDir()
		w, _ := NewWALStorage(dir)
		st := PersistentState{VotedFor: -1, Log: []LogEntry{{}}}
		for step := 0; step < 40; step++ {
			switch rng.Intn(3) {
			case 0:
				st.CurrentTerm++
				st.VotedFor = rng.Intn(5) - 1
			case 1:
				for k := rng.Intn(5); k > 0; k-- {
					st.Log = append(st.Log, LogEntry{Term: st.CurrentTerm, Index: len(st.Log), Command: []byte{byte(step)}})
				}
			case 2:
				// Raft's Log Matching property: an (index, term) pair identifies a
				// unique entry, so replacement entries always carry a newer term.
				if len(st.Log) > 1 {
					st.Log = append([]LogEntry(nil), st.Log[:1+rng.Intn(len(st.Log)-1)]...)
					st.CurrentTerm++
				}
			}
			if err := w.SaveState(cloneState(st)); err != nil {
				t.Fatal(err)
			}
		}
		w.Close()
		if got := reopen(t, dir); !reflect.DeepEqual(got, st) {
			t.Fatalf("trial %d: recovered state differs", trial)
		}
	}
}
