package raft_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
)

// drainApplied reads command ApplyMsgs until it sees one at index want, returning
// any snapshot message encountered along the way. It fails if nothing arrives in
// time.
func waitForCommandIndex(t *testing.T, ch <-chan raft.ApplyMsg, want int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m := <-ch:
			if m.CommandValid && m.CommandIndex >= want {
				return
			}
		case <-deadline:
			t.Fatalf("did not apply through index %d in time", want)
		}
	}
}

// TestSnapshotCompactsAndReloads verifies the core Phase 6 mechanics on a single
// node: Snapshot trims the log prefix while keeping the suffix and the last index,
// the snapshot is persisted, and on restart the node delivers the snapshot up the
// applyCh (as SnapshotValid) before serving any further commands.
func TestSnapshotCompactsAndReloads(t *testing.T) {
	clk := clock.NewMockClock(time.Unix(0, 0))
	p := storage.NewInMemoryPersister()
	ch := make(chan raft.ApplyMsg, 128)
	cfg := raft.Config{
		ID: 0, Peers: []int{0}, Transport: deadTransport{},
		Persister: p, Clock: clk, ApplyCh: ch, Rand: rand.New(rand.NewSource(1)),
	}

	rf := raft.Make(cfg)
	electSingleNode(t, rf, clk)

	// A leader appends a no-op at index 1 on election, so the 10 commands occupy
	// indices 2..11.
	for i := 1; i <= 10; i++ {
		if _, _, ok := rf.Propose([]byte(fmt.Sprintf("cmd-%d", i))); !ok {
			t.Fatalf("propose %d rejected", i)
		}
	}
	waitForCommandIndex(t, ch, 11)

	// Compact through index 5; the tail (6..11) and the last index stay.
	rf.Snapshot(5, []byte("snapshot-through-5"))
	if li, _, commit := rf.LogState(); li != 11 || commit != 11 {
		t.Fatalf("after snapshot LogState lastIndex=%d commit=%d, want 11/11", li, commit)
	}
	sizeAfterSnap := p.RaftStateSize()
	rf.Kill()

	// Restart from the same persister: the snapshot + log tail must reload.
	ch2 := make(chan raft.ApplyMsg, 128)
	cfg2 := cfg
	cfg2.ApplyCh = ch2
	rf2 := raft.Make(cfg2)
	defer rf2.Kill()

	// The first thing delivered must be the snapshot, covering index 5.
	select {
	case m := <-ch2:
		if !m.SnapshotValid || m.SnapshotIndex != 5 {
			t.Fatalf("first ApplyMsg = %+v, want SnapshotValid at index 5", m)
		}
		if string(m.Snapshot) != "snapshot-through-5" {
			t.Fatalf("snapshot bytes = %q, want snapshot-through-5", m.Snapshot)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no snapshot delivered after restart")
	}

	// The log tail survived: last index is still 11.
	if li, _, _ := rf2.LogState(); li != 11 {
		t.Fatalf("after reload lastIndex=%d, want 11", li)
	}

	// It can resume: re-elect (which appends another no-op at index 12) and commit
	// a new entry above the snapshot.
	electSingleNode(t, rf2, clk)
	if _, _, ok := rf2.Propose([]byte("cmd-after")); !ok {
		t.Fatal("propose after reload rejected")
	}
	waitForCommandIndex(t, ch2, 13)

	if sizeAfterSnap == 0 {
		t.Fatal("expected a non-empty persisted state after snapshot")
	}
}

// electSingleNode drives the mock clock until rf (a one-node cluster) becomes
// leader.
func electSingleNode(t *testing.T, rf *raft.Raft, clk *clock.MockClock) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		clk.Advance(50 * time.Millisecond)
		if _, isLeader := rf.State(); isLeader {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("single node did not become leader")
}
