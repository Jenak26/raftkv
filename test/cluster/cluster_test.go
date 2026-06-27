package cluster

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport"
)

// bootCounter is a transport.Server test double that records, in its durable
// Persister, how many times it has been constructed ("booted"). It lets the
// harness tests prove that crash/restart preserves persistent storage: a fresh
// boot reading a non-zero prior count means the Persister survived the crash.
type bootCounter struct {
	id    int
	boots int
}

func newBootCounter(id int, _ []int, _ transport.Transport, p storage.Persister, _ clock.Clock) transport.Server {
	prev := 0
	if b := p.ReadRaftState(); len(b) == 8 {
		prev = int(binary.BigEndian.Uint64(b))
	}
	boots := prev + 1
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(boots))
	p.SaveRaftState(buf)
	return &bootCounter{id: id, boots: boots}
}

func (b *bootCounter) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply {
	return &raft.RequestVoteReply{Term: b.id}
}
func (b *bootCounter) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return &raft.AppendEntriesReply{Term: b.id, Success: true}
}
func (b *bootCounter) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{Term: b.id}
}

func TestNewClusterIsFullyConnected(t *testing.T) {
	c := New(t, 3, 1, newBootCounter)

	for from := 0; from < 3; from++ {
		for to := 0; to < 3; to++ {
			if from == to {
				continue
			}
			if _, err := c.Transport(from).SendRequestVote(context.Background(), to, &raft.RequestVoteArgs{}); err != nil {
				t.Errorf("%d -> %d should be reachable in a fresh cluster, got %v", from, to, err)
			}
		}
	}
}

func TestCrashRestartPreservesPersistentStorage(t *testing.T) {
	c := New(t, 3, 1, newBootCounter)

	// Every node booted exactly once when the cluster started.
	if got := c.Server(1).(*bootCounter).boots; got != 1 {
		t.Fatalf("node 1 initial boots = %d, want 1", got)
	}

	c.Crash(1)
	c.Restart(1)

	// The restarted node must observe the previous boot count from its
	// Persister — proving durable state survived the crash.
	if got := c.Server(1).(*bootCounter).boots; got != 2 {
		t.Fatalf("after restart node 1 boots = %d, want 2 (persister was not preserved)", got)
	}
}

func TestCrashedNodeIsUnreachableThenRecovers(t *testing.T) {
	c := New(t, 3, 1, newBootCounter)

	c.Crash(2)
	if _, err := c.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err == nil {
		t.Error("crashed node 2 should be unreachable")
	}

	c.Restart(2)
	if _, err := c.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Errorf("restarted node 2 should be reachable, got %v", err)
	}
}

// TestDeterministicPartitionThenTimedHeal exercises the headline Phase 1
// success criterion: "partition node 2 away, then heal after 500ms" — driven by
// the harness's deterministic MockClock.
func TestDeterministicPartitionThenTimedHeal(t *testing.T) {
	c := New(t, 3, 1, newBootCounter)

	c.Partition([]int{0, 1}, []int{2})
	if _, err := c.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err == nil {
		t.Error("node 2 should be partitioned away from 0")
	}

	c.Advance(500 * 1e6) // 500ms of simulated time
	c.Heal()

	if _, err := c.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Errorf("after heal, node 2 should be reachable, got %v", err)
	}
}
