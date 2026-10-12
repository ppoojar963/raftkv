package linearizability

import (
	"testing"
	"time"
)

func put(c, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Key: k, Kind: Put, Value: v, Call: call, Return: ret}
}
func get(c, k, out string, found bool, call, ret int64) Operation {
	return Operation{ClientID: c, Key: k, Kind: Get, OutValue: out, OutFound: found, Call: call, Return: ret}
}

func TestSequentialOK(t *testing.T) {
	h := []Operation{
		get("a", "x", "", false, 0, 1),
		put("a", "x", "1", 2, 3),
		get("b", "x", "1", true, 4, 5),
	}
	if r := Check(h, time.Second); !r.OK {
		t.Fatal(r)
	}
}

// A read that overlaps a write may see either the old or the new value.
func TestConcurrentEitherValueOK(t *testing.T) {
	for _, seen := range []string{"old", "new"} {
		h := []Operation{
			put("a", "x", "old", 0, 1),
			put("a", "x", "new", 2, 10),
			get("b", "x", seen, true, 3, 9),
		}
		if r := Check(h, time.Second); !r.OK {
			t.Fatalf("read of %q during overlapping write rejected: %v", seen, r)
		}
	}
}

// The classic stale read: a write completes, then a later read returns the old value.
func TestStaleReadRejected(t *testing.T) {
	h := []Operation{
		put("a", "x", "old", 0, 1),
		put("a", "x", "new", 2, 3),
		get("b", "x", "old", true, 4, 5),
	}
	if r := Check(h, time.Second); r.OK {
		t.Fatal("stale read was accepted")
	}
}

// Two readers observe two concurrent writes in opposite orders; no single
// total order can explain both (each read strictly follows the other's).
func TestInconsistentOrderRejected(t *testing.T) {
	h := []Operation{
		put("w1", "x", "A", 0, 10),
		put("w2", "x", "B", 0, 10),
		get("r1", "x", "A", true, 11, 12),
		get("r2", "x", "B", true, 13, 14),
		get("r1", "x", "A", true, 15, 16),
	}
	if r := Check(h, time.Second); r.OK {
		t.Fatal("flip-flopping reads were accepted")
	}
}

func TestLostWriteRejected(t *testing.T) {
	h := []Operation{
		put("a", "x", "1", 0, 1),
		get("b", "x", "", false, 2, 3),
	}
	if r := Check(h, time.Second); r.OK {
		t.Fatal("lost write was accepted")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Operation{
		put("a", "x", "1", 0, 1),
		put("a", "y", "2", 0, 1),
		get("b", "y", "2", true, 2, 3),
		get("b", "x", "1", true, 2, 3),
	}
	if r := Check(h, time.Second); !r.OK {
		t.Fatal(r)
	}
}
