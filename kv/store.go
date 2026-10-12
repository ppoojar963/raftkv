// Package kv is a linearizable key-value store replicated with Raft.
//
// Every operation, reads included, goes through the Raft log, so all replicas
// apply the same operations in the same order. Clients tag each request with
// (ClientID, Seq); the state machine remembers the last Seq applied per client,
// so a retried write (after a timeout or leader change) takes effect exactly once.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ppoojar963/raftkv/raft"
)

type OpType string

const (
	OpGet    OpType = "get"
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

type Op struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID string `json:"client_id"`
	Seq      int64  `json:"seq"`
}

type Result struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

var (
	ErrTimeout        = errors.New("kv: timed out waiting for commit")
	ErrLostLeadership = errors.New("kv: lost leadership before commit")
	ErrStopped        = errors.New("kv: server stopped")
)

// NotLeaderError tells the client which node to try next (-1 if unknown).
type NotLeaderError struct{ LeaderID int }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("kv: not leader (leader hint %d)", e.LeaderID)
}

type applied struct {
	term     int
	clientID string
	seq      int64
	res      Result
}

// Store is one replica: a Raft node plus the state machine it drives.
type Store struct {
	mu      sync.Mutex
	node    *raft.Node
	applyCh chan raft.ApplyMsg

	data    map[string]string
	lastSeq map[string]int64 // client -> highest Seq applied (dedup table)
	waiters map[int]chan applied

	Timeout time.Duration
	stopCh  chan struct{}
	done    chan struct{}
}

func NewStore(cfg raft.Config, t raft.Transport, s raft.Storage) (*Store, error) {
	st := &Store{
		applyCh: make(chan raft.ApplyMsg, 1024),
		data:    map[string]string{},
		lastSeq: map[string]int64{},
		waiters: map[int]chan applied{},
		Timeout: time.Second,
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	node, err := raft.NewNode(cfg, t, s, st.applyCh)
	if err != nil {
		return nil, err
	}
	st.node = node
	return st, nil
}

func (s *Store) Node() *raft.Node { return s.node }

func (s *Store) Start() {
	s.node.Start()
	go s.applyLoop()
}

func (s *Store) Stop() {
	s.node.Stop()
	close(s.stopCh)
	<-s.done
}

// Submit proposes op and blocks until it is committed and applied, or fails.
func (s *Store) Submit(op Op) (Result, error) {
	cmd, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	// Holding s.mu across Propose guarantees the waiter is registered before
	// applyLoop can possibly see this index.
	s.mu.Lock()
	idx, term, ok := s.node.Propose(cmd)
	if !ok {
		s.mu.Unlock()
		_, _, leader := s.node.Status()
		return Result{}, &NotLeaderError{LeaderID: leader}
	}
	ch := make(chan applied, 1)
	s.waiters[idx] = ch
	s.mu.Unlock()

	timer := time.NewTimer(s.Timeout)
	defer timer.Stop()
	select {
	case a := <-ch:
		// A different entry at our index means a new leader overwrote ours.
		if a.term != term || a.clientID != op.ClientID || a.seq != op.Seq {
			return Result{}, ErrLostLeadership
		}
		return a.res, nil
	case <-timer.C:
		s.mu.Lock()
		delete(s.waiters, idx)
		s.mu.Unlock()
		return Result{}, ErrTimeout
	case <-s.stopCh:
		return Result{}, ErrStopped
	}
}

func (s *Store) applyLoop() {
	defer close(s.done)
	for {
		var m raft.ApplyMsg
		select {
		case m = <-s.applyCh:
		case <-s.stopCh:
			return
		}
		s.mu.Lock()
		a := applied{term: m.Term}
		if m.Command != nil { // nil = leader no-op
			var op Op
			if err := json.Unmarshal(m.Command, &op); err != nil {
				panic(fmt.Sprintf("kv: corrupt log entry %d: %v", m.Index, err))
			}
			a.clientID, a.seq = op.ClientID, op.Seq
			a.res = s.applyLocked(op)
		}
		if ch, ok := s.waiters[m.Index]; ok {
			ch <- a
			delete(s.waiters, m.Index)
		}
		s.mu.Unlock()
	}
}

func (s *Store) applyLocked(op Op) Result {
	if op.Type == OpGet {
		v, ok := s.data[op.Key]
		return Result{Value: v, Found: ok}
	}
	if op.ClientID != "" && op.Seq <= s.lastSeq[op.ClientID] {
		return Result{} // duplicate of an already-applied write: do nothing
	}
	if op.ClientID != "" {
		s.lastSeq[op.ClientID] = op.Seq
	}
	switch op.Type {
	case OpPut:
		s.data[op.Key] = op.Value
	case OpDelete:
		delete(s.data, op.Key)
	}
	return Result{}
}
