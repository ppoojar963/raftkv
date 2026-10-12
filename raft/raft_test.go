package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ppoojar963/raftkv/netsim"
	"github.com/ppoojar963/raftkv/raft"
)

// cluster wires N raft nodes to a simulated network and checks, on every
// single apply, that no two nodes ever commit different commands at one index.
type cluster struct {
	t        *testing.T
	n        int
	net      *netsim.Network
	nodes    []*raft.Node
	storages []*raft.MemoryStorage
	running  []bool
	gen      []int
	lk       sync.Mutex // guards nodes, running, gen

	mu        sync.Mutex
	committed map[int][]byte // index -> command (first node to apply it)
	applied   []map[int]bool // per node: indices applied
	maxIdx    []int          // per node: highest index applied
	errs      []string
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t:         t,
		n:         n,
		net:       netsim.New(time.Now().UnixNano()),
		nodes:     make([]*raft.Node, n),
		storages:  make([]*raft.MemoryStorage, n),
		running:   make([]bool, n),
		gen:       make([]int, n),
		committed: map[int][]byte{},
		applied:   make([]map[int]bool, n),
		maxIdx:    make([]int, n),
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) peers() []int {
	p := make([]int, c.n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (c *cluster) start(i int) {
	cfg := raft.DefaultConfig(i, c.peers())
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 150*time.Millisecond, 300*time.Millisecond
	cfg.HeartbeatInterval = 40 * time.Millisecond
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNode(cfg, c.net.Endpoint(i), c.storages[i], ch)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.applied[i] = map[int]bool{} // state machine is rebuilt from the log after restart
	c.maxIdx[i] = 0
	c.mu.Unlock()
	c.lk.Lock()
	c.nodes[i] = node
	c.running[i] = true
	c.gen[i]++
	g := c.gen[i]
	c.lk.Unlock()
	c.net.Register(i, node)
	node.Start()
	go func() {
		for m := range ch {
			c.onApply(i, g, m)
		}
	}()
}

func (c *cluster) node(i int) (*raft.Node, bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	return c.nodes[i], c.running[i]
}

func (c *cluster) onApply(i, g int, m raft.ApplyMsg) {
	c.lk.Lock()
	stale := c.gen[i] != g
	c.lk.Unlock()
	if stale {
		return // buffered output of a crashed incarnation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if m.Index != c.maxIdx[i]+1 {
		c.errs = append(c.errs, fmt.Sprintf("node %d applied index %d out of order (last %d)", i, m.Index, c.maxIdx[i]))
	}
	c.maxIdx[i] = m.Index
	c.applied[i][m.Index] = true
	if prev, ok := c.committed[m.Index]; ok {
		if string(prev) != string(m.Command) {
			c.errs = append(c.errs, fmt.Sprintf("SAFETY VIOLATION: index %d committed as %q and %q", m.Index, prev, m.Command))
		}
	} else {
		c.committed[m.Index] = m.Command
	}
}

func (c *cluster) crash(i int) {
	c.net.SetUp(i, false)
	nd, _ := c.node(i)
	nd.Stop()
	c.lk.Lock()
	c.running[i] = false
	c.lk.Unlock()
}

func (c *cluster) restart(i int) {
	if _, running := c.node(i); running {
		c.crash(i)
	}
	c.start(i)
}

func (c *cluster) shutdown() {
	for i := 0; i < c.n; i++ {
		if nd, running := c.node(i); running {
			nd.Stop()
		}
	}
	c.checkNoErrors()
}

func (c *cluster) checkNoErrors() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.errs {
		c.t.Error(e)
	}
	c.errs = nil
}

// leader waits for exactly one leader among the given nodes, or fails.
func (c *cluster) leader(among []int) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		leaders := map[int][]int{} // term -> leader IDs
		for _, i := range among {
			nd, running := c.node(i)
			if !running {
				continue
			}
			term, isLeader, _ := nd.Status()
			if isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		lastTerm := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm >= 0 {
			return leaders[lastTerm][0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected among %v", among)
	return -1
}

func (c *cluster) all() []int { return c.peers() }

// nApplied counts how many nodes have applied index idx.
func (c *cluster) nApplied(idx int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cnt := 0
	for i := 0; i < c.n; i++ {
		if c.applied[i][idx] {
			cnt++
		}
	}
	return cnt
}

// one proposes cmd to whichever node is leader, retrying across leader changes,
// until it is applied by at least `expected` nodes. Returns its index.
func (c *cluster) one(cmd string, expected int) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			nd, running := c.node(i)
			if !running {
				continue
			}
			idx, _, ok := nd.Propose([]byte(cmd))
			if !ok {
				continue
			}
			wait := time.Now().Add(2 * time.Second)
			for time.Now().Before(wait) {
				if c.nApplied(idx) >= expected {
					c.mu.Lock()
					got := string(c.committed[idx])
					c.mu.Unlock()
					if got == cmd {
						return idx
					}
					break // a different command won this index; retry
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("one(%q) failed to reach agreement", cmd)
	return -1
}

// ---------------------------------------------------------------- tests

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader(c.all())
	nd, _ := c.node(l)
	term1, _, _ := nd.Status()
	time.Sleep(time.Second) // with no failures, the term should stay stable
	term2, _, _ := nd.Status()
	if term1 != term2 {
		t.Errorf("term changed with no failures: %d -> %d", term1, term2)
	}
	c.leader(c.all())
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.leader(c.all())

	c.net.SetUp(l1, false) // leader loses connectivity
	others := without(c.all(), l1)
	l2 := c.leader(others)
	if l2 == l1 {
		t.Fatal("disconnected node still leading the majority")
	}

	c.net.SetUp(l1, true) // old leader returns and must step down
	time.Sleep(500 * time.Millisecond)
	// The rejoin may have triggered a new election, so look up the current
	// leader rather than assuming l2 still leads.
	l3 := c.leader(c.all())

	// No quorum: a follower cut off on its own must never win an election.
	// (An isolated *leader* may keep believing it leads; it just cannot commit.)
	c.net.SetUp(l3, false)
	c.net.SetUp((l3+1)%3, false)
	isolated := (l3 + 2) % 3
	time.Sleep(time.Second)
	nd, _ := c.node(isolated)
	if _, isLeader, _ := nd.Status(); isLeader {
		t.Errorf("node %d became leader without a quorum", isolated)
	}
	c.net.Heal()
	c.leader(c.all())
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 5)
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprintf("cmd-%d", i), 5)
	}
}

func TestFollowerFailureAndRejoin(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)
	l := c.leader(c.all())
	f := (l + 1) % 3
	c.net.SetUp(f, false)
	c.one("b", 2) // still a majority
	c.one("c", 2)
	c.net.SetUp(f, true)
	idx := c.one("d", 3) // rejoined follower must catch up on b, c, d
	if c.nApplied(idx) != 3 {
		t.Fatal("rejoined follower did not catch up")
	}
}

// A leader cut off in a minority accepts writes that must never commit, and
// they must be overwritten once the partition heals.
func TestPartitionedLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	c.one("before", 5)
	old := c.leader(c.all())
	minority := []int{old, (old + 1) % 5}
	majority := without(c.all(), minority...)
	c.net.Partition(minority, majority)

	var stale []int
	for i := 0; i < 5; i++ {
		if idx, _, ok := c.nodes[old].Propose([]byte(fmt.Sprintf("stale-%d", i))); ok {
			stale = append(stale, idx)
		}
	}
	c.leader(majority)
	for i := 0; i < 5; i++ {
		c.one(fmt.Sprintf("fresh-%d", i), 3)
	}
	c.net.Heal()
	c.one("after", 5)

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, idx := range stale {
		if s := string(c.committed[idx]); len(s) >= 5 && s[:5] == "stale" {
			t.Errorf("minority leader's entry %q committed at %d", s, idx)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	c := newCluster(t, 3)
	c.one("x", 3)
	for i := 0; i < 3; i++ {
		c.restart(i) // every node crashes and recovers from storage
	}
	c.one("y", 3)
	// Crash everyone at once; the log must survive a full-cluster restart.
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.one("z", 3)
}

// TestChaos runs a randomized nemesis: crashes, restarts, partitions, and an
// unreliable network, while commands keep flowing. The harness checks the Raft
// safety property (no two different commands committed at one index) throughout.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in -short mode")
	}
	const n = 5
	c := newCluster(t, n)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	c.net.SetUnreliable(0.1, 15*time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // client load
		defer wg.Done()
		for k := 0; ; k++ {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < n; i++ {
				if nd, running := c.node(i); running {
					nd.Propose([]byte(fmt.Sprintf("op-%d", k)))
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	for round := 0; round < 15; round++ {
		switch rng.Intn(3) {
		case 0: // crash a random minority, then restart them
			victims := rng.Perm(n)[:rng.Intn(n/2)+1]
			for _, v := range victims {
				c.crash(v)
			}
			time.Sleep(time.Duration(200+rng.Intn(400)) * time.Millisecond)
			for _, v := range victims {
				c.restart(v)
			}
		case 1: // random partition
			perm := rng.Perm(n)
			cut := rng.Intn(n-1) + 1
			c.net.Partition(perm[:cut], perm[cut:])
			time.Sleep(time.Duration(200+rng.Intn(400)) * time.Millisecond)
			c.net.Partition()
		case 2:
			time.Sleep(300 * time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()
	c.net.Heal()
	c.one("final", n) // liveness: once healed, the cluster makes progress
	t.Logf("chaos: %d RPCs, %d dropped", c.net.RPCs.Load(), c.net.Dropped.Load())
}

func without(ids []int, remove ...int) []int {
	var out []int
outer:
	for _, id := range ids {
		for _, r := range remove {
			if id == r {
				continue outer
			}
		}
		out = append(out, id)
	}
	return out
}

// TestFigure8 targets the scenario in Figure 8 of the Raft paper: leaders that
// crash right after replicating to a partial set of followers, so logs diverge
// across many short terms. A leader that commits an earlier-term entry just by
// counting replicas (instead of waiting for a current-term entry) will lose a
// committed entry here, and the harness reports it as a safety violation.
func TestFigure8(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	const n = 5
	c := newCluster(t, n)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	c.one("init", 1)
	// Real-world latency: without it, entries replicate almost instantly and a
	// leader rarely dies while holding entries only it has.
	c.net.SetUnreliable(0, 20*time.Millisecond)
	up := n
	for iter := 0; iter < 400; iter++ {
		leader := -1
		for i := 0; i < n; i++ {
			if nd, running := c.node(i); running {
				if _, _, ok := nd.Propose([]byte(fmt.Sprintf("f8-%d", iter))); ok {
					leader = i
				}
			}
		}
		if rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(rng.Intn(150)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rng.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rng.Intn(2) == 0 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			for {
				s := rng.Intn(n)
				if _, running := c.node(s); !running {
					c.restart(s)
					up++
					break
				}
			}
		}
	}
	for i := 0; i < n; i++ {
		if _, running := c.node(i); !running {
			c.restart(i)
		}
	}
	c.net.Heal()
	c.one("done", n)
}

// Regression: found by the benchmark. A single-node cluster never committed,
// because commitIndex only advanced when a follower acknowledged an entry.
func TestSingleNodeCommits(t *testing.T) {
	c := newCluster(t, 1)
	for i := 0; i < 5; i++ {
		c.one(fmt.Sprintf("solo-%d", i), 1)
	}
}
