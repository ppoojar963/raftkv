// Package netsim is an in-process network for fault-injection testing. It can
// partition nodes, take them down, drop RPCs (requests or replies), and add
// random latency that reorders messages, all deterministically seedable.
package netsim

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ppoojar963/raftkv/raft"
)

var ErrUnreachable = errors.New("netsim: unreachable")

// Handler is implemented by *raft.Node.
type Handler interface {
	HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply
}

type Network struct {
	mu       sync.Mutex
	handlers map[int]Handler
	up       map[int]bool
	group    map[int]int // nodes talk only within the same partition group
	dropRate float64
	maxDelay time.Duration
	rng      *rand.Rand

	RPCs    atomic.Int64
	Dropped atomic.Int64
}

func New(seed int64) *Network {
	return &Network{
		handlers: map[int]Handler{},
		up:       map[int]bool{},
		group:    map[int]int{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (n *Network) Register(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.up[id] = true
}

// SetUp marks a node as reachable or not (a crashed/disconnected node is down).
func (n *Network) SetUp(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up[id] = up
}

// Partition splits the cluster; nodes in different groups cannot communicate.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	for gi, g := range groups {
		for _, id := range g {
			n.group[id] = gi
		}
	}
}

// Heal removes partitions and message loss, and brings every node back up.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	n.dropRate, n.maxDelay = 0, 0
	for id := range n.up {
		n.up[id] = true
	}
}

// SetUnreliable sets the probability of dropping each request and each reply,
// and the maximum random one-way delay (which also causes reordering).
func (n *Network) SetUnreliable(dropRate float64, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.maxDelay = dropRate, maxDelay
}

func (n *Network) reachable(from, to int) bool {
	return n.up[from] && n.up[to] && n.group[from] == n.group[to]
}

// hop models one direction of a message: returns false if the message is lost.
func (n *Network) hop(from, to int) bool {
	n.mu.Lock()
	ok := n.reachable(from, to)
	drop := ok && n.dropRate > 0 && n.rng.Float64() < n.dropRate
	var d time.Duration
	if n.maxDelay > 0 {
		d = time.Duration(n.rng.Int63n(int64(n.maxDelay)))
	}
	n.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if !ok {
		time.Sleep(5 * time.Millisecond) // a failed connection is not instant
		return false
	}
	if drop {
		n.Dropped.Add(1)
		return false
	}
	return true
}

func (n *Network) handler(id int) Handler {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.handlers[id]
}

// Endpoint returns the raft.Transport a given node uses to send RPCs.
func (n *Network) Endpoint(from int) raft.Transport { return &endpoint{net: n, from: from} }

type endpoint struct {
	net  *Network
	from int
}

func (e *endpoint) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	e.net.RPCs.Add(1)
	if !e.net.hop(e.from, to) {
		return nil, ErrUnreachable
	}
	h := e.net.handler(to)
	if h == nil {
		return nil, ErrUnreachable
	}
	reply := h.HandleRequestVote(args)
	if !e.net.hop(to, e.from) {
		return nil, ErrUnreachable
	}
	return reply, nil
}

func (e *endpoint) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	e.net.RPCs.Add(1)
	if !e.net.hop(e.from, to) {
		return nil, ErrUnreachable
	}
	h := e.net.handler(to)
	if h == nil {
		return nil, ErrUnreachable
	}
	reply := h.HandleAppendEntries(args)
	if !e.net.hop(to, e.from) {
		return nil, ErrUnreachable
	}
	return reply, nil
}
