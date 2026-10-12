// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout, 2014):
// leader election, log replication, and crash-recovery persistence.
package raft

import (
	"errors"
	"time"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// LogEntry is one replicated command. Index 0 is a sentinel that is never applied.
// A nil Command is a leader no-op, appended at the start of each term so that
// entries from earlier terms can be committed (Raft paper §5.4.2, §8).
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// ApplyMsg is delivered to the state machine once an entry is committed.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte // nil for leader no-ops
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply carries ConflictTerm/ConflictIndex so the leader can skip
// back a whole term at a time instead of one entry per round trip.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // -1 if the follower's log is simply too short
	ConflictIndex int
}

// Transport sends RPCs to peers. Implementations: in-memory simulated network
// (netsim) for tests, HTTP/JSON (transport) for real deployments.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Storage persists the state Raft requires to survive crashes:
// currentTerm, votedFor, and the log. SaveState must be durable before returning.
type Storage interface {
	SaveState(st PersistentState) error
	LoadState() (PersistentState, bool, error) // bool=false if nothing stored yet
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// Config tunes timing. Election timeouts must be comfortably larger than
// the heartbeat interval plus typical RPC latency.
type Config struct {
	ID                 int
	Peers              []int // all node IDs in the cluster, including ID
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	MaxEntriesPerRPC   int
}

func DefaultConfig(id int, peers []int) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		MaxEntriesPerRPC:   256,
	}
}

var ErrStopped = errors.New("raft: node stopped")
