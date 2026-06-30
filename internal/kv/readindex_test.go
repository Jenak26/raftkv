package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/kv"
)

func leaderLastLogIndex(t *testing.T, h *kvHarness, id int) int {
	t.Helper()
	srv := h.c.Server(id)
	ls, ok := srv.(interface {
		LogState() (lastIndex, lastTerm, commit int)
	})
	if !ok {
		t.Fatalf("node %d does not expose LogState", id)
	}
	li, _, _ := ls.LogState()
	return li
}

// TestLinearizableReadDoesNotGrowLog proves a linearizable Get is a ReadIndex read,
// not a logged operation: issuing many reads leaves the leader's log length
// unchanged (a Phase-5 "read through the log" would have appended an entry each).
func TestLinearizableReadDoesNotGrowLog(t *testing.T) {
	h := newKVHarness(t, 3, 21, nil, 0)
	leader := h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := bg(t)
	defer cancel()

	if err := ck.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	before := leaderLastLogIndex(t, h, leader)
	for i := 0; i < 5; i++ {
		if v, ok, err := ck.Get(ctx, "k"); err != nil || !ok || v != "v" {
			t.Fatalf("get = (%q,%v,%v), want (v,true,nil)", v, ok, err)
		}
	}
	if after := leaderLastLogIndex(t, h, leader); after != before {
		t.Fatalf("linearizable reads appended to the log: lastLogIndex %d -> %d", before, after)
	}
}

// TestLinearizableReadAfterLeaderChange exercises the election no-op: a freshly
// elected leader can only serve a ReadIndex read once it has committed a
// current-term entry. After the old leader is crashed, a linearizable Get must
// still return the last committed write.
func TestLinearizableReadAfterLeaderChange(t *testing.T) {
	h := newKVHarness(t, 5, 22, nil, 0)
	h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := ck.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	leader := h.waitLeader(3 * time.Second)
	h.crash(leader)

	// The Clerk retries until a new leader is elected, commits its no-op, and serves
	// the ReadIndex read - which must reflect the committed write.
	if v, ok, err := ck.Get(ctx, "k"); err != nil || !ok || v != "v1" {
		t.Fatalf("linearizable get after leader change = (%q,%v,%v), want (v1,true,nil)", v, ok, err)
	}
}

// TestStaleReadServedByFollowerLocally demonstrates the read-consistency knob: a
// linearizable read sent straight to a follower is refused (only the leader can
// serve it), while a stale read is answered from the follower's own state machine,
// no leadership round required.
func TestStaleReadServedByFollowerLocally(t *testing.T) {
	h := newKVHarness(t, 3, 23, nil, 0)
	leader := h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := bg(t)
	defer cancel()

	if err := ck.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	follower := (leader + 1) % 3

	// A linearizable read aimed at a follower cannot be served by it.
	if _, err := h.server(follower).Submit(ctx, kv.Command{Kind: kv.OpGet, Key: "k", ClientID: 1, SeqNum: 1}); err != kv.ErrNotLeader {
		t.Fatalf("linearizable read on follower err = %v, want ErrNotLeader", err)
	}

	// A stale read is served locally by the follower. Once the write has replicated
	// and applied there, it returns the value - without ever contacting the leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res, err := h.server(follower).Submit(ctx, kv.Command{Kind: kv.OpGet, Key: "k", ReadStale: true})
		if err != nil {
			t.Fatalf("stale read on follower errored: %v", err)
		}
		if res.Ok && res.Value == "v" {
			return // follower served the value from its own state machine
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("follower never served the stale read locally")
}
