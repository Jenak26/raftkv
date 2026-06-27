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
