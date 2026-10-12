// Package linearizability checks whether a concurrent history of key-value
// operations is linearizable: whether there is a single total order of the
// operations that (1) respects real time (if A returned before B was called,
// A comes first) and (2) matches a sequential key-value store.
//
// The search is Wing & Gong's algorithm with Lowe's memoization, the same
// approach used by Jepsen's Knossos and Porcupine. Linearizability is a local
// property (Herlihy & Wing), so the history is checked one key at a time.
package linearizability

import (
	"fmt"
	"sort"
	"time"
)

type Kind int

const (
	Get Kind = iota
	Put
	Delete
)

// Operation is one completed client call. Call and Return are timestamps from
// a single monotonic clock.
type Operation struct {
	ClientID string
	Key      string
	Kind     Kind
	Value    string // Put input
	OutValue string // Get output
	OutFound bool   // Get output
	Call     int64
	Return   int64
}

type Result struct {
	OK       bool
	TimedOut bool   // search budget exhausted; neither proven nor refuted
	Key      string // first key that failed, if any
	Ops      int
}

func (r Result) String() string {
	switch {
	case r.TimedOut:
		return fmt.Sprintf("UNKNOWN (search timed out, %d ops)", r.Ops)
	case r.OK:
		return fmt.Sprintf("linearizable (%d ops)", r.Ops)
	}
	return fmt.Sprintf("NOT linearizable: violation on key %q (%d ops)", r.Key, r.Ops)
}

// Check partitions the history by key and checks each key independently.
func Check(history []Operation, timeout time.Duration) Result {
	byKey := map[string][]Operation{}
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	deadline := time.Now().Add(timeout)
	res := Result{OK: true, Ops: len(history)}
	for _, k := range keys {
		ok, timedOut := checkKey(byKey[k], deadline)
		if timedOut {
			res.OK, res.TimedOut, res.Key = false, true, k
			return res
		}
		if !ok {
			res.OK, res.Key = false, k
			return res
		}
	}
	return res
}

type regState struct {
	value  string
	exists bool
}

// step applies op to the single-key register model; ok=false if op's observed
// output is impossible in state s.
func step(s regState, op *Operation) (regState, bool) {
	switch op.Kind {
	case Put:
		return regState{op.Value, true}, true
	case Delete:
		return regState{}, true
	default:
		if op.OutFound != s.exists || (s.exists && op.OutValue != s.value) {
			return s, false
		}
		return s, true
	}
}

func checkKey(ops []Operation, deadline time.Time) (ok, timedOut bool) {
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	n := len(ops)
	done := make([]uint64, (n+63)/64)
	visited := map[string]struct{}{}
	steps := 0

	var search func(count int, s regState) bool
	search = func(count int, s regState) bool {
		if count == n {
			return true
		}
		if steps++; steps%4096 == 0 && time.Now().After(deadline) {
			timedOut = true
			return false
		}
		// An op may be linearized next only if it was called before every other
		// pending op returned; otherwise real-time order would be violated.
		minRet := int64(1<<63 - 1)
		for i := 0; i < n; i++ {
			if done[i/64]&(1<<(i%64)) == 0 && ops[i].Return < minRet {
				minRet = ops[i].Return
			}
		}
		for i := 0; i < n && ops[i].Call <= minRet; i++ {
			if done[i/64]&(1<<(i%64)) != 0 {
				continue
			}
			ns, legal := step(s, &ops[i])
			if !legal {
				continue
			}
			done[i/64] |= 1 << (i % 64)
			key := memoKey(done, ns)
			if _, seen := visited[key]; !seen {
				visited[key] = struct{}{}
				if search(count+1, ns) {
					return true
				}
				if timedOut {
					return false
				}
			}
			done[i/64] &^= 1 << (i % 64)
		}
		return false
	}
	ok = search(0, regState{})
	return ok, timedOut
}

func memoKey(done []uint64, s regState) string {
	b := make([]byte, 0, len(done)*8+len(s.value)+2)
	for _, w := range done {
		for k := 0; k < 8; k++ {
			b = append(b, byte(w>>(8*k)))
		}
	}
	if s.exists {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return string(append(b, s.value...))
}
