package raft

import "testing"

// nodeWithLog builds a stopped node whose persisted state is exactly `terms`
// (terms[i] is the term of the entry at index i+1).
func nodeWithLog(t *testing.T, id, currentTerm int, terms ...int) *Node {
	t.Helper()
	log := []LogEntry{{Term: 0, Index: 0}}
	for i, term := range terms {
		log = append(log, LogEntry{Term: term, Index: i + 1, Command: []byte{byte(term)}})
	}
	st := NewMemoryStorage()
	st.SaveState(PersistentState{CurrentTerm: currentTerm, VotedFor: -1, Log: log})
	n, err := NewNode(DefaultConfig(id, []int{1, 2, 3, 4, 5}), nil, st, make(chan ApplyMsg, 16))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestFigure8Deterministic reconstructs Figure 8 of the Raft paper step by step.
//
// State (c): S1 is leader in term 4. It has re-replicated the term-2 entry at
// index 2 to S2 and S3, so that entry is on a majority. Its own term-4 entry at
// index 3 is on S1 only.
//
//	S1: [1 2 4]   S2: [1 2]   S3: [1 2]   S4: [1]   S5: [1 3]
//
// A naive leader would now mark index 2 committed ("it's on 3 of 5 nodes").
// State (d) shows why that is wrong: if S1 crashes, S5 can win term 5 with votes
// from S2, S3 and S4 (its last log term, 3, beats their 2), and then overwrite
// index 2 with its term-3 entry. A "committed" entry would be lost.
func TestFigure8Deterministic(t *testing.T) {
	s1 := nodeWithLog(t, 1, 4, 1, 2, 4)
	s1.state, s1.leaderID = Leader, 1
	s1.nextIndex = map[int]int{1: 4, 2: 3, 3: 3, 4: 2, 5: 2}
	s1.matchIndex = map[int]int{1: 3, 2: 2, 3: 2, 4: 0, 5: 0}

	s1.mu.Lock()
	s1.advanceCommitLocked()
	got := s1.commitIndex
	s1.mu.Unlock()
	if got != 0 {
		t.Fatalf("leader in term 4 committed index %d by counting replicas of a term-2 entry; "+
			"this is the Figure 8 bug (Raft §5.4.2)", got)
	}

	// Prove the danger is real: S5 can win an election without S1.
	s2 := nodeWithLog(t, 2, 4, 1, 2)
	s3 := nodeWithLog(t, 3, 4, 1, 2)
	s4 := nodeWithLog(t, 4, 4, 1)
	vote := &RequestVoteArgs{Term: 5, CandidateID: 5, LastLogIndex: 2, LastLogTerm: 3}
	votes := 1 // S5 votes for itself
	for _, peer := range []*Node{s2, s3, s4} {
		if peer.HandleRequestVote(vote).VoteGranted {
			votes++
		}
	}
	if votes < 3 {
		t.Fatalf("expected S5 to be electable (got %d votes); scenario is not reproducing Figure 8", votes)
	}

	// Once S1's current-term entry reaches a majority, index 3 commits, and
	// index 2 commits with it indirectly. That is the correct, safe path.
	s1.mu.Lock()
	s1.matchIndex[2] = 3
	s1.matchIndex[3] = 3
	s1.advanceCommitLocked()
	got = s1.commitIndex
	s1.mu.Unlock()
	if got != 3 {
		t.Fatalf("commitIndex = %d, want 3 once the term-4 entry is on a majority", got)
	}
	// And now S5 can no longer win: S2 (fresh copy, before the vote above) has
	// received the term-4 entry, and a last log term of 4 beats 3.
	s2 = nodeWithLog(t, 2, 4, 1, 2)
	if r := s2.HandleAppendEntries(&AppendEntriesArgs{Term: 4, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 4, Index: 3, Command: []byte{4}}}, LeaderCommit: 3}); !r.Success {
		t.Fatal("S2 rejected S1's term-4 entry")
	}
	if s2.HandleRequestVote(&RequestVoteArgs{Term: 6, CandidateID: 5, LastLogIndex: 2, LastLogTerm: 3}).VoteGranted {
		t.Fatal("S2 voted for a candidate with a stale log")
	}
}

// TestAppendEntriesIgnoresStaleDuplicate checks that a delayed, duplicate RPC
// carrying a prefix of entries the follower already has does not truncate its log.
// Truncating here would silently drop acknowledged entries.
func TestAppendEntriesIgnoresStaleDuplicate(t *testing.T) {
	f := nodeWithLog(t, 2, 1, 1, 1, 1)
	r := f.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte{1}}}})
	if !r.Success || f.lastIndex() != 3 {
		t.Fatalf("stale duplicate truncated the log to %d entries", f.lastIndex())
	}
}
