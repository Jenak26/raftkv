package raft_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
)

// deadTransport is an RPCTransport whose every send fails, so a node under test
// never receives votes or heartbeats. Combined with an un-advanced MockClock
// (whose timers never fire), it keeps the node perfectly quiescent — letting us
// test the RPC handlers and persistence in isolation from the cluster.
type deadTransport struct{}

func (deadTransport) SendRequestVote(context.Context, int, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, errors.New("dead")
}
func (deadTransport) SendAppendEntries(context.Context, int, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, errors.New("dead")
}
func (deadTransport) SendInstallSnapshot(context.Context, int, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, errors.New("dead")
}

func quiescentConfig(p storage.Persister) raft.Config {
	return raft.Config{
		ID:        0,
		Peers:     []int{0, 1, 2},
		Transport: deadTransport{},
		Persister: p,
		Clock:     clock.NewMockClock(time.Unix(0, 0)), // never advanced -> no timers fire
		ApplyCh:   make(chan raft.ApplyMsg, 8),
		Rand:      rand.New(rand.NewSource(1)),
	}
}

func TestPersistsTermAndVoteAcrossRestart(t *testing.T) {
	p := storage.NewInMemoryPersister()

	rf := raft.Make(quiescentConfig(p))
	// A candidate at term 5 requests our vote; we should grant it and durably
	// record currentTerm=5, votedFor=1.
	if r := rf.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("expected to grant vote to candidate 1 at term 5")
	}
	rf.Kill()

	// "Restart" from the same Persister with a fresh clock and transport.
	rf2 := raft.Make(quiescentConfig(p))
	defer rf2.Kill()

	if term, _ := rf2.State(); term != 5 {
		t.Errorf("recovered currentTerm = %d, want 5 (not restored from Persister)", term)
	}
	// votedFor must also have survived: a *different* candidate in the same term
	// must be refused.
	if r := rf2.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 2}); r.VoteGranted {
		t.Error("granted a second vote in term 5 — votedFor was not restored")
	}
}

// TestSingleNodeElectsItselfLeader guards the single-node edge case: a one-node
// cluster's self-vote is already a majority, so it must become leader on its first
// election timeout without sending any RPC. (Bug museum 01: the votes>=majority
// promotion check originally lived only inside a per-peer reply goroutine, so with
// no peers it never fired and a lone node stayed a perpetual candidate.)
func TestSingleNodeElectsItselfLeader(t *testing.T) {
	clk := clock.NewMockClock(time.Unix(0, 0))
	rf := raft.Make(raft.Config{
		ID:        0,
		Peers:     []int{0},
		Transport: deadTransport{},
		Persister: storage.NewInMemoryPersister(),
		Clock:     clk,
		ApplyCh:   make(chan raft.ApplyMsg, 8),
		Rand:      rand.New(rand.NewSource(1)),
	})
	defer rf.Kill()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		clk.Advance(50 * time.Millisecond) // push past the election timeout
		if _, isLeader := rf.State(); isLeader {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("single-node cluster never elected itself leader")
}

func TestStepsDownOnHigherTermInRequestVote(t *testing.T) {
	rf := raft.Make(quiescentConfig(storage.NewInMemoryPersister()))
	defer rf.Kill()

	reply := rf.HandleRequestVote(&raft.RequestVoteArgs{Term: 9, CandidateID: 1})
	if !reply.VoteGranted || reply.Term != 9 {
		t.Fatalf("expected vote granted at term 9, got granted=%v term=%d", reply.VoteGranted, reply.Term)
	}
	if term, isLeader := rf.State(); term != 9 || isLeader {
		t.Errorf("after higher term: term=%d isLeader=%v, want term=9 follower", term, isLeader)
	}
}

func TestRejectsVoteFromStaleTerm(t *testing.T) {
	rf := raft.Make(quiescentConfig(storage.NewInMemoryPersister()))
	defer rf.Kill()

	// Advance our term to 5 by granting a vote.
	rf.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 1})

	// A candidate stuck at term 3 must be rejected.
	reply := rf.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 2})
	if reply.VoteGranted {
		t.Error("granted vote to a candidate from a stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5 so the stale candidate updates itself", reply.Term)
	}
}

func TestProposeRejectedByFollower(t *testing.T) {
	rf := raft.Make(quiescentConfig(storage.NewInMemoryPersister()))
	defer rf.Kill()
	if _, _, isLeader := rf.Propose([]byte("x")); isLeader {
		t.Fatal("a follower must reject Propose")
	}
}

func TestAppendEntriesAppendsAndReportsConflicts(t *testing.T) {
	rf := raft.Make(quiescentConfig(storage.NewInMemoryPersister()))
	defer rf.Kill()

	// 1) Append two entries onto the empty log.
	r := rf.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 1, Index: 2, Command: []byte("b")}},
	})
	if !r.Success {
		t.Fatal("expected success appending to empty log")
	}
	if li, _, _ := rf.LogState(); li != 2 {
		t.Fatalf("lastIndex = %d, want 2", li)
	}

	// 2) prevLogIndex past the end -> failure with ConflictTerm -1 and the
	//    follower's next free slot as ConflictIndex.
	r = rf.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, PrevLogIndex: 5, PrevLogTerm: 1})
	if r.Success {
		t.Fatal("expected failure when prevLogIndex is beyond the log")
	}
	if r.ConflictTerm != -1 || r.ConflictIndex != 3 {
		t.Fatalf("got ConflictTerm=%d ConflictIndex=%d, want -1, 3", r.ConflictTerm, r.ConflictIndex)
	}

	// 3) prev term mismatch -> failure carrying the conflicting term and the
	//    first index of that term.
	r = rf.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, PrevLogIndex: 2, PrevLogTerm: 9})
	if r.Success {
		t.Fatal("expected failure on prev-term mismatch")
	}
	if r.ConflictTerm != 1 || r.ConflictIndex != 1 {
		t.Fatalf("got ConflictTerm=%d ConflictIndex=%d, want 1, 1", r.ConflictTerm, r.ConflictIndex)
	}
}

func TestAppendEntriesTruncatesOnConflictOnly(t *testing.T) {
	rf := raft.Make(quiescentConfig(storage.NewInMemoryPersister()))
	defer rf.Kill()

	// Build [sentinel, {1,1}, {2,2}, {2,3}].
	rf.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 2, Index: 2, Command: []byte("b")}, {Term: 2, Index: 3, Command: []byte("c")}},
	})

	// Re-send an entry that already matches: must NOT truncate the tail.
	r := rf.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.LogEntry{{Term: 2, Index: 2, Command: []byte("b")}},
	})
	if !r.Success {
		t.Fatal("expected success re-sending a matching entry")
	}
	if li, _, _ := rf.LogState(); li != 3 {
		t.Fatalf("lastIndex = %d, want 3 (matching entries must not be truncated)", li)
	}

	// Now a genuine conflict at index 2 (term 3 != 2): truncate from 2 and append.
	r = rf.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 3, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.LogEntry{{Term: 3, Index: 2, Command: []byte("x")}},
	})
	if !r.Success {
		t.Fatal("expected success applying a conflicting entry")
	}
	if li, lt, _ := rf.LogState(); li != 2 || lt != 3 {
		t.Fatalf("lastIndex=%d lastTerm=%d, want 2, 3", li, lt)
	}
}
