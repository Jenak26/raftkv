package kv_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/kv"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport"
	"github.com/janak/raftkv/test/cluster"
)

// kvHarness spins up an n-node KV cluster: each node is a Raft peer on the
// deterministic simulated network wrapped by a kv.Server. It reuses the cluster
// harness for partitions, crash/restart, and the background clock pump, and tracks
// the *current* kv.Server per node so a Clerk keeps working across restarts.
type kvHarness struct {
	t            *testing.T
	c            *cluster.Cluster
	n            int
	stop         func()
	newSM        func() kv.StateMachine
	maxRaftState int

	mu      sync.Mutex
	servers map[int]*kv.Server
	sms     map[int]kv.StateMachine
}

func newKVHarness(t *testing.T, n int, seed int64, newSM func() kv.StateMachine, maxRaftState int) *kvHarness {
	t.Helper()
	if newSM == nil {
		newSM = func() kv.StateMachine { return kv.NewMapStateMachine() }
	}
	h := &kvHarness{
		t:            t,
		n:            n,
		newSM:        newSM,
		maxRaftState: maxRaftState,
		servers:      map[int]*kv.Server{},
		sms:          map[int]kv.StateMachine{},
	}

	factory := func(id int, peers []int, tr transport.Transport, p storage.Persister, clk clock.Clock) transport.Server {
		ch := make(chan raft.ApplyMsg, 1024)
		rf := raft.Make(raft.Config{
			ID: id, Peers: peers, Transport: tr, Persister: p, Clock: clk,
			ApplyCh: ch, Rand: rand.New(rand.NewSource(seed*1_000_003 + int64(id) + 1)),
		})
		sm := h.newSM()
		srv := kv.NewServer(rf, ch, sm, h.maxRaftState)
		h.mu.Lock()
		h.servers[id] = srv
		h.sms[id] = sm
		h.mu.Unlock()
		return rf
	}

	h.c = cluster.New(t, n, seed, factory)
	h.stop = h.c.StartClockPump(time.Millisecond, 100*time.Microsecond)
	t.Cleanup(func() {
		h.mu.Lock()
		for _, s := range h.servers {
			s.Stop()
		}
		h.mu.Unlock()
		for id := 0; id < n; id++ {
			h.c.Crash(id)
		}
		h.stop()
	})
	return h
}

// server returns the current kv.Server for node id, or nil if it is crashed.
func (h *kvHarness) server(id int) *kv.Server {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.servers[id]
}

func (h *kvHarness) sm(id int) kv.StateMachine {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sms[id]
}

// crash stops node id's kv.Server and kills its Raft peer.
func (h *kvHarness) crash(id int) {
	if s := h.server(id); s != nil {
		s.Stop()
	}
	h.c.Crash(id)
}

// restart rebuilds node id; the factory registers the fresh kv.Server.
func (h *kvHarness) restart(id int) { h.c.Restart(id) }

// clerk builds a client whose endpoints forward to whichever kv.Server currently
// runs on each node id (so it survives restarts).
func (h *kvHarness) clerk(opts ...kv.ClerkOption) *kv.Clerk {
	servers := make([]kv.KV, h.n)
	for id := 0; id < h.n; id++ {
		servers[id] = nodeKV{h: h, id: id}
	}
	return kv.NewClerk(servers, opts...)
}

// nodeKV is a stable client endpoint for one node: it forwards Submit to the
// node's current kv.Server, or reports ErrNotLeader if the node is crashed.
type nodeKV struct {
	h  *kvHarness
	id int
}

func (n nodeKV) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	s := n.h.server(n.id)
	if s == nil {
		return kv.Result{}, kv.ErrNotLeader
	}
	return s.Submit(ctx, cmd)
}

// waitLeader blocks until some node reports itself leader, returning its id.
func (h *kvHarness) waitLeader(timeout time.Duration) int {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id := range h.c.Leaders() {
			return id
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("no leader elected within %s", timeout)
	return -1
}

func bg(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func TestKVBasicPutGetDeleteCAS(t *testing.T) {
	h := newKVHarness(t, 3, 1, nil, 0)
	h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := bg(t)
	defer cancel()

	if err := ck.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, ok, err := ck.Get(ctx, "k"); err != nil || !ok || v != "v1" {
		t.Fatalf("get after put = (%q,%v,%v), want (v1,true,nil)", v, ok, err)
	}

	// CAS with the wrong expected must fail and not change the value.
	if swapped, err := ck.CAS(ctx, "k", "WRONG", "v2"); err != nil || swapped {
		t.Fatalf("CAS wrong-expected = (%v,%v), want (false,nil)", swapped, err)
	}
	// CAS with the right expected swaps.
	if swapped, err := ck.CAS(ctx, "k", "v1", "v2"); err != nil || !swapped {
		t.Fatalf("CAS right-expected = (%v,%v), want (true,nil)", swapped, err)
	}
	if v, _, _ := ck.Get(ctx, "k"); v != "v2" {
		t.Fatalf("get after CAS = %q, want v2", v)
	}

	if existed, err := ck.Delete(ctx, "k"); err != nil || !existed {
		t.Fatalf("delete = (%v,%v), want (true,nil)", existed, err)
	}
	if _, ok, _ := ck.Get(ctx, "k"); ok {
		t.Fatal("key still present after delete")
	}
}

func TestKVLeaderRedirectAndRetry(t *testing.T) {
	h := newKVHarness(t, 3, 2, nil, 0)
	leader := h.waitLeader(3 * time.Second)

	// Submitting straight to a follower must be refused (the client would then
	// move on to another server).
	follower := (leader + 1) % 3
	ctx, cancel := bg(t)
	defer cancel()
	if _, err := h.server(follower).Submit(ctx, kv.Command{Kind: kv.OpPut, Key: "a", Value: "b", ClientID: 1, SeqNum: 1}); err != kv.ErrNotLeader {
		t.Fatalf("follower Submit err = %v, want ErrNotLeader", err)
	}

	// The Clerk, which discovers the leader by trying, still succeeds regardless of
	// which server it starts from.
	ck := h.clerk()
	if err := ck.Put(ctx, "a", "b"); err != nil {
		t.Fatalf("clerk put: %v", err)
	}
	if v, ok, _ := ck.Get(ctx, "a"); !ok || v != "b" {
		t.Fatalf("clerk get = (%q,%v), want (b,true)", v, ok)
	}
}

// countingSM wraps a MapStateMachine and counts how many times a *mutating* command
// actually reached Apply. The dedup layer must keep that count at one per logical
// write no matter how many times the client re-submits.
type countingSM struct {
	inner     *kv.MapStateMachine
	mu        sync.Mutex
	mutations int
}

func newCountingSM() *countingSM { return &countingSM{inner: kv.NewMapStateMachine()} }

func (c *countingSM) Apply(cmd kv.Command) kv.Result {
	if cmd.Kind == kv.OpPut || cmd.Kind == kv.OpDelete || cmd.Kind == kv.OpCAS {
		c.mu.Lock()
		c.mutations++
		c.mu.Unlock()
	}
	return c.inner.Apply(cmd)
}

func (c *countingSM) Snapshot() ([]byte, error)     { return c.inner.Snapshot() }
func (c *countingSM) Restore(snapshot []byte) error { return c.inner.Restore(snapshot) }

func (c *countingSM) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mutations
}

// TestKVExactlyOnceUnderDuplicateSubmit drives the dedup directly: submitting the
// identical (ClientID, SeqNum) command twice — as an at-least-once transport would
// on a lost reply — must mutate the state machine exactly once, while a fresh
// SeqNum applies again.
func TestKVExactlyOnceUnderDuplicateSubmit(t *testing.T) {
	sm := newCountingSM()
	h := newKVHarness(t, 1, 3, func() kv.StateMachine { return sm }, 0)
	leader := h.waitLeader(3 * time.Second)
	srv := h.server(leader)
	ctx, cancel := bg(t)
	defer cancel()

	dup := kv.Command{Kind: kv.OpPut, Key: "x", Value: "1", ClientID: 42, SeqNum: 7}
	if _, err := srv.Submit(ctx, dup); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if _, err := srv.Submit(ctx, dup); err != nil { // the retry of a "lost" reply
		t.Fatalf("duplicate submit: %v", err)
	}
	if got := sm.count(); got != 1 {
		t.Fatalf("state machine mutated %d times for a duplicate, want 1", got)
	}

	// A new SeqNum is a genuinely new operation and must apply.
	next := kv.Command{Kind: kv.OpPut, Key: "x", Value: "2", ClientID: 42, SeqNum: 8}
	if _, err := srv.Submit(ctx, next); err != nil {
		t.Fatalf("next submit: %v", err)
	}
	if got := sm.count(); got != 2 {
		t.Fatalf("state machine mutated %d times after a new seq, want 2", got)
	}
}

// TestKVProgressAcrossLeaderCrash shows the Clerk transparently following
// leadership: it keeps reading and writing while the leader is repeatedly crashed
// and restarted.
func TestKVProgressAcrossLeaderCrash(t *testing.T) {
	h := newKVHarness(t, 5, 4, nil, 0)
	h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for round := 0; round < 4; round++ {
		val := fmt.Sprintf("r%d", round)
		if err := ck.Put(ctx, "key", val); err != nil {
			t.Fatalf("round %d put: %v", round, err)
		}
		if v, ok, err := ck.Get(ctx, "key"); err != nil || !ok || v != val {
			t.Fatalf("round %d get = (%q,%v,%v), want (%s,true,nil)", round, v, ok, err, val)
		}

		// Crash the current leader and bring it back; the Clerk must re-find a leader.
		leader := h.waitLeader(5 * time.Second)
		h.crash(leader)
		h.restart(leader)
	}
}

// firstIndexOf reads node id's Raft snapshot anchor through the running server.
func firstIndexOf(t *testing.T, h *kvHarness, id int) int {
	t.Helper()
	srv := h.c.Server(id)
	fi, ok := srv.(interface{ FirstIndex() int })
	if !ok {
		t.Fatalf("node %d does not expose FirstIndex", id)
	}
	return fi.FirstIndex()
}

// TestSnapshotLaggingFollowerCatchesUp is the Phase 6 headline: with a small
// snapshot threshold, isolate a follower, write enough that the leader compacts
// away the entries the follower is missing, then reconnect it. The follower cannot
// be caught up with AppendEntries (those entries are gone) — it must receive an
// InstallSnapshot and rebuild its state machine from it.
func TestSnapshotLaggingFollowerCatchesUp(t *testing.T) {
	const threshold = 1000 // bytes; small so a few hundred writes force compaction
	h := newKVHarness(t, 3, 9, nil, threshold)
	leader := h.waitLeader(3 * time.Second)
	ck := h.clerk()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Isolate a follower; the majority keeps serving.
	follower := (leader + 1) % 3
	h.c.Disconnect(follower)

	const N = 200
	for i := 0; i < N; i++ {
		if err := ck.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// The leader must have compacted (log bounded, not 200+ entries retained).
	if fi := firstIndexOf(t, h, leader); fi == 0 {
		t.Fatalf("leader %d never snapshotted under a %d-byte threshold and %d writes", leader, threshold, N)
	}

	// Reconnect the lagging follower; it must catch up via InstallSnapshot and end
	// up with the full state in its own state machine.
	h.c.Connect(follower)

	key, want := fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// Read the follower's local state machine directly (not through Raft).
		got := h.sm(follower).Apply(kv.Command{Kind: kv.OpGet, Key: key})
		if got.Ok && got.Value == want && firstIndexOf(t, h, follower) > 0 {
			return // caught up, and it installed a snapshot to do so
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("follower %d did not catch up via snapshot (last %s=%q, firstIndex=%d)",
		follower, key, h.sm(follower).Apply(kv.Command{Kind: kv.OpGet, Key: key}).Value, firstIndexOf(t, h, follower))
}
