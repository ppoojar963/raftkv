package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Node is a single Raft peer. All mutable fields are guarded by mu.
// Lock discipline: mu is never held across an RPC or a send on applyCh.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	id        int
	transport Transport
	storage   Storage
	applyCh   chan<- ApplyMsg

	// Persistent state (written to storage before responding to RPCs).
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a sentinel; log[i].Index == i

	// Volatile state.
	state       State
	leaderID    int
	commitIndex int
	lastApplied int

	// Leader-only volatile state.
	nextIndex  map[int]int
	matchIndex map[int]int
	kick       map[int]chan struct{} // wakes each peer's replication loop
	leaderDone chan struct{}         // closed when this leadership term ends

	electionDeadline time.Time
	lastBroadcast    time.Time
	rng              *rand.Rand

	applyCond *sync.Cond
	stopCh    chan struct{}
	stopped   bool
	wg        sync.WaitGroup
}

// NewNode restores persisted state (if any) and returns a stopped node; call Start.
func NewNode(cfg Config, t Transport, s Storage, applyCh chan<- ApplyMsg) (*Node, error) {
	n := &Node{
		cfg:       cfg,
		id:        cfg.ID,
		transport: t,
		storage:   s,
		applyCh:   applyCh,
		votedFor:  -1,
		leaderID:  -1,
		log:       []LogEntry{{Term: 0, Index: 0}},
		rng:       rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)*7919)),
		stopCh:    make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	st, ok, err := s.LoadState()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	if ok {
		n.currentTerm, n.votedFor, n.log = st.CurrentTerm, st.VotedFor, st.Log
	}
	n.resetElectionTimerLocked()
	return n, nil
}

func (n *Node) Start() {
	n.wg.Add(2)
	go n.ticker()
	go n.applier()
}

// Stop halts all background work. Safe to call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends a command to the leader's log. It returns immediately; the
// command is committed only once it later appears on applyCh at the returned index.
func (n *Node) Propose(cmd []byte) (index, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.state != Leader {
		return -1, n.currentTerm, false
	}
	index = n.appendLocked(cmd)
	n.broadcastLocked()
	return index, n.currentTerm, true
}

// Status reports the current term, whether this node believes it is leader,
// and the last known leader ID (-1 if unknown).
func (n *Node) Status() (term int, isLeader bool, leaderID int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state == Leader, n.leaderID
}

func (n *Node) ID() int { return n.id }

// ---------------------------------------------------------------- helpers

func (n *Node) lastIndex() int { return len(n.log) - 1 }
func (n *Node) lastTerm() int  { return n.log[len(n.log)-1].Term }
func (n *Node) majority() int  { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) persistLocked() {
	err := n.storage.SaveState(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
	if err != nil {
		// Raft's safety depends on durable state. Continuing after a failed write
		// could let this node vote twice in one term, so fail-stop instead.
		panic(fmt.Sprintf("raft %d: persist failed: %v", n.id, err))
	}
}

func (n *Node) appendLocked(cmd []byte) int {
	idx := n.lastIndex() + 1
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Index: idx, Command: cmd})
	n.persistLocked()
	n.matchIndex[n.id] = idx
	n.nextIndex[n.id] = idx + 1
	// In a single-node cluster the leader alone is a majority; without this,
	// commit would only ever advance on a follower's acknowledgement.
	n.advanceCommitLocked()
	return idx
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionDeadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)+1)))
}

func (n *Node) becomeFollowerLocked(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
		n.persistLocked()
	}
	if n.state == Leader {
		n.leaderID = -1
		close(n.leaderDone) // stop this term's replication loops
	}
	n.state = Follower
}

// ---------------------------------------------------------------- background loops

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		now := time.Now()
		switch {
		case n.state == Leader && now.Sub(n.lastBroadcast) >= n.cfg.HeartbeatInterval:
			n.broadcastLocked()
		case n.state != Leader && now.After(n.electionDeadline):
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

// applier delivers committed entries in order. It never holds mu while sending,
// so a slow state machine cannot block RPC handling.
func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		batch := make([]ApplyMsg, 0, n.commitIndex-n.lastApplied)
		for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
			e := n.log[i]
			batch = append(batch, ApplyMsg{Index: e.Index, Term: e.Term, Command: e.Command})
		}
		n.lastApplied = n.commitIndex
		n.mu.Unlock()

		for _, m := range batch {
			select {
			case n.applyCh <- m:
			case <-n.stopCh:
				return
			}
		}
	}
}

// ---------------------------------------------------------------- elections

func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = -1
	n.persistLocked()
	n.resetElectionTimerLocked()

	args := &RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	votes := 1
	if votes >= n.majority() { // single-node cluster
		n.becomeLeaderLocked()
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.id {
			continue
		}
		go func(peer int) {
			reply, err := n.transport.RequestVote(peer, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes == n.majority() {
				n.becomeLeaderLocked()
			}
		}(p)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[int]int, len(n.cfg.Peers))
	n.matchIndex = make(map[int]int, len(n.cfg.Peers))
	n.kick = make(map[int]chan struct{}, len(n.cfg.Peers))
	n.leaderDone = make(chan struct{})
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
		if p != n.id {
			n.kick[p] = make(chan struct{}, 1)
			n.wg.Add(1)
			go n.peerLoop(p, n.currentTerm, n.kick[p], n.leaderDone)
		}
	}
	// A no-op in the new term lets the leader commit entries left over from
	// earlier terms, which it is not allowed to count replicas for directly.
	n.appendLocked(nil)
	n.broadcastLocked()
}

// HandleRequestVote is the RequestVote RPC handler (Raft paper Figure 2).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	// Election restriction (§5.4.1): only vote for candidates whose log is at
	// least as up to date as ours, so every leader holds all committed entries.
	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persistLocked()
		n.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return reply
}

// ---------------------------------------------------------------- replication

// broadcastLocked wakes every peer's replication loop. A wake-up that arrives
// while an RPC is in flight is coalesced, so entries proposed meanwhile are
// batched into the next AppendEntries instead of each getting its own RPC.
func (n *Node) broadcastLocked() {
	n.lastBroadcast = time.Now()
	for _, ch := range n.kick {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// peerLoop is the single replication pipeline to one follower for one term:
// at most one AppendEntries in flight, keeping the follower's log ordered and
// reusing one connection.
func (n *Node) peerLoop(peer, term int, kick <-chan struct{}, done <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-kick:
		case <-done:
			return
		case <-n.stopCh:
			return
		}
		for n.replicateTo(peer, term) {
		}
	}
}

// replicateTo sends one AppendEntries and reports whether there is more to
// send right away (more entries pending, or nextIndex was backed up).
func (n *Node) replicateTo(peer, term int) bool {
	n.mu.Lock()
	if n.stopped || n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}
	prev := n.nextIndex[peer] - 1
	if prev > n.lastIndex() {
		prev = n.lastIndex()
	}
	end := n.lastIndex() + 1
	if max := n.cfg.MaxEntriesPerRPC; max > 0 && end-(prev+1) > max {
		end = prev + 1 + max
	}
	entries := make([]LogEntry, end-(prev+1))
	copy(entries, n.log[prev+1:end])
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log[prev].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.transport.AppendEntries(peer, args)
	if err != nil {
		return false // next heartbeat retries
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return false
	}
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return false
	}
	if n.state != Leader || n.currentTerm != term {
		return false // stale reply
	}
	if reply.Success {
		match := prev + len(entries)
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		n.advanceCommitLocked()
		return n.nextIndex[peer] <= n.lastIndex() // more to send; don't wait for heartbeat
	}
	// Rejected: back nextIndex up using the follower's conflict hint.
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		for i := n.lastIndex(); i > 0; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
			if n.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	if next < n.nextIndex[peer] {
		n.nextIndex[peer] = next
		return true
	}
	return false
}

// advanceCommitLocked commits the highest index replicated on a majority,
// but only if that entry is from the current term (§5.4.2, the "Figure 8" rule).
func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break // earlier-term entries commit indirectly
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			return
		}
	}
}

// HandleAppendEntries is the AppendEntries RPC handler (Raft paper Figure 2).
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &AppendEntriesReply{Term: n.currentTerm, ConflictTerm: -1}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(args.Term)
	}
	reply.Term = n.currentTerm
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	// Consistency check on the entry preceding the new ones.
	if args.PrevLogIndex > n.lastIndex() {
		reply.ConflictIndex = n.lastIndex() + 1
		return reply
	}
	if t := n.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && n.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Append new entries, truncating only on a real conflict. A delayed,
	// duplicate RPC must never erase entries we already accepted.
	changed := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.lastIndex() {
			if n.log[idx].Term == e.Term {
				continue
			}
			n.log = n.log[:idx]
		}
		n.log = append(n.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persistLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply
}
