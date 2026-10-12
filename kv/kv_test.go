package kv_test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ppoojar963/raftkv/kv"
	"github.com/ppoojar963/raftkv/linearizability"
	"github.com/ppoojar963/raftkv/netsim"
	"github.com/ppoojar963/raftkv/raft"
)

type kvCluster struct {
	t        *testing.T
	n        int
	net      *netsim.Network
	storages []*raft.MemoryStorage

	mu      sync.Mutex
	stores  []*kv.Store
	running []bool
}

func newKVCluster(t *testing.T, n int, seed int64) *kvCluster {
	c := &kvCluster{
		t: t, n: n, net: netsim.New(seed),
		storages: make([]*raft.MemoryStorage, n),
		stores:   make([]*kv.Store, n),
		running:  make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) start(i int) {
	peers := make([]int, c.n)
	for p := range peers {
		peers[p] = p
	}
	cfg := raft.DefaultConfig(i, peers)
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 150*time.Millisecond, 300*time.Millisecond
	cfg.HeartbeatInterval = 40 * time.Millisecond
	s, err := kv.NewStore(cfg, c.net.Endpoint(i), c.storages[i])
	if err != nil {
		c.t.Fatal(err)
	}
	s.Timeout = 500 * time.Millisecond
	c.mu.Lock()
	c.stores[i], c.running[i] = s, true
	c.mu.Unlock()
	c.net.Register(i, s.Node())
	s.Start()
}

func (c *kvCluster) crash(i int) {
	c.mu.Lock()
	s, running := c.stores[i], c.running[i]
	c.running[i] = false
	c.mu.Unlock()
	if running {
		c.net.SetUp(i, false)
		s.Stop()
	}
}

func (c *kvCluster) server(i int) kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[i]
}

func (c *kvCluster) client() *kv.Client { return kv.NewClient(c.n, c.server) }

func TestBasicKV(t *testing.T) {
	c := newKVCluster(t, 3, 1)
	cl := c.client()
	if _, found, err := cl.Get("missing"); err != nil || found {
		t.Fatalf("get missing: found=%v err=%v", found, err)
	}
	must(t, cl.Put("lang", "go"))
	must(t, cl.Put("lang", "rust"))
	if v, _, err := cl.Get("lang"); err != nil || v != "rust" {
		t.Fatalf("got %q, %v", v, err)
	}
	must(t, cl.Delete("lang"))
	if _, found, _ := cl.Get("lang"); found {
		t.Fatal("key survived delete")
	}
}

// A retried write must apply exactly once even if the first attempt committed.
func TestDuplicateWriteAppliedOnce(t *testing.T) {
	c := newKVCluster(t, 3, 2)
	cl := c.client()
	must(t, cl.Put("k", "v1")) // establishes the leader
	s := c.server(0).(*kv.Store)
	op := kv.Op{Type: kv.OpPut, Key: "k", Value: "v2", ClientID: "dup-client", Seq: 1}
	later := kv.Op{Type: kv.OpPut, Key: "k", Value: "v3", ClientID: "dup-client", Seq: 2}
	submit := func(o kv.Op) {
		for i := 0; i < 50; i++ {
			for j := 0; j < 3; j++ {
				s = c.server(j).(*kv.Store)
				if _, err := s.Submit(o); err == nil {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("submit never succeeded")
	}
	submit(op)
	submit(later)
	submit(op) // stale retry of Seq 1 arrives late; must not overwrite v3
	if v, _, _ := cl.Get("k"); v != "v3" {
		t.Fatalf("stale duplicate re-applied: got %q, want v3", v)
	}
}

// TestLinearizableUnderFaults is a Jepsen-style test. Concurrent clients run
// random operations while a nemesis crashes nodes, partitions the network, and
// drops/delays messages. Every client call is recorded with invoke and return
// times, and the full history is then checked for linearizability.
//
// Run more iterations with: CHAOS_RUNS=20 go test ./kv -run Linearizable -v
func TestLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in -short mode")
	}
	runs := 1
	if v, err := strconv.Atoi(os.Getenv("CHAOS_RUNS")); err == nil && v > 0 {
		runs = v
	}
	for r := 0; r < runs; r++ {
		seed := time.Now().UnixNano()
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runChaos(t, seed) })
	}
}

func runChaos(t *testing.T, seed int64) {
	const (
		nServers = 5
		nClients = 5
		duration = 6 * time.Second
	)
	keys := []string{"a", "b", "c"}
	c := newKVCluster(t, nServers, seed)
	rng := rand.New(rand.NewSource(seed))
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	var (
		histMu  sync.Mutex
		history []linearizability.Operation
		stop    = make(chan struct{})
		wg      sync.WaitGroup
	)
	for ci := 0; ci < nClients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := c.client()
			r := rand.New(rand.NewSource(seed + int64(ci)))
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				op := linearizability.Operation{ClientID: cl.ID(), Key: keys[r.Intn(len(keys))], Call: now()}
				var err error
				x := r.Intn(10)
				if ci < 2 {
					x = 0 // clients 0-1 are read-only, like a cache or dashboard
				}
				switch {
				case x < 5:
					op.Kind = linearizability.Get
					op.OutValue, op.OutFound, err = cl.Get(op.Key)
				case x < 9:
					op.Kind, op.Value = linearizability.Put, fmt.Sprintf("c%d-%d", ci, k)
					err = cl.Put(op.Key, op.Value)
				default:
					op.Kind = linearizability.Delete
					err = cl.Delete(op.Key)
				}
				op.Return = now()
				if err != nil {
					// Clients retry for 30s and the network is healed before the
					// end, so a failure here means the cluster lost liveness.
					t.Errorf("client %d: %v", ci, err)
					return
				}
				histMu.Lock()
				history = append(history, op)
				histMu.Unlock()
				time.Sleep(time.Millisecond) // client think time
			}
		}(ci)
	}

	// Nemesis.
	c.net.SetUnreliable(0.05, 10*time.Millisecond)
	faults := map[string]int{}
	for time.Since(start) < duration {
		switch rng.Intn(4) {
		case 0:
			victims := rng.Perm(nServers)[:rng.Intn(nServers/2)+1]
			for _, v := range victims {
				c.crash(v)
			}
			time.Sleep(time.Duration(150+rng.Intn(350)) * time.Millisecond)
			for _, v := range victims {
				c.start(v)
			}
			faults["crash-restart"]++
		case 1:
			perm := rng.Perm(nServers)
			cut := rng.Intn(nServers-1) + 1
			c.net.Partition(perm[:cut], perm[cut:])
			time.Sleep(time.Duration(150+rng.Intn(1000)) * time.Millisecond)
			c.net.Partition()
			faults["partition"]++
		case 2:
			c.net.SetUnreliable(0.2, 25*time.Millisecond)
			time.Sleep(300 * time.Millisecond)
			c.net.SetUnreliable(0.05, 10*time.Millisecond)
			faults["lossy-network"]++
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
	c.net.Heal()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	res := linearizability.Check(history, 60*time.Second)
	t.Logf("%d ops, faults=%v, rpcs=%d dropped=%d -> %v",
		len(history), faults, c.net.RPCs.Load(), c.net.Dropped.Load(), res)
	if len(history) < 100 {
		t.Errorf("only %d operations completed; cluster was barely available", len(history))
	}
	if !res.OK {
		t.Fatalf("history check failed: %v", res)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
