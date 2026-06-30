package netrpc_test

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/kv"
	"github.com/janak/raftkv/internal/netrpc"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
)

// TestNetRPCClusterEndToEnd brings up a real 3-node cluster over loopback TCP —
// net/rpc transport, real clock, file-free in-memory persisters — and drives it
// with a Clerk over the real KV client. This is the M4 demo as an automated test:
// it exercises the production transport that the simulated network never touches.
func TestNetRPCClusterEndToEnd(t *testing.T) {
	const n = 3
	ids := []int{0, 1, 2}

	// Phase 1: open all listeners first so every node has an address to dial before
	// any node starts sending.
	lns := make([]net.Listener, n)
	addrs := map[int]string{}
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}

	// Phase 2: build each node's transport, Raft peer, KV server, and serve it.
	transports := make([]*netrpc.RaftTransport, n)
	servers := make([]*netrpc.Server, n)
	rafts := make([]*raft.Raft, n)
	kvs := make([]*kv.Server, n)
	for i := 0; i < n; i++ {
		tr := netrpc.NewRaftTransport(i, addrs, 200*time.Millisecond)
		ch := make(chan raft.ApplyMsg, 256)
		rf := raft.Make(raft.Config{
			ID:        i,
			Peers:     ids,
			Transport: tr,
			Persister: storage.NewInMemoryPersister(),
			Clock:     clock.NewRealClock(),
			ApplyCh:   ch,
			Rand:      rand.New(rand.NewSource(int64(i) + 1)),
		})
		kvsrv := kv.NewServer(rf, ch, kv.NewMapStateMachine(), 0)
		srv, err := netrpc.Serve(lns[i], rf, kvsrv, 2*time.Second)
		if err != nil {
			t.Fatalf("serve node %d: %v", i, err)
		}
		transports[i], rafts[i], kvs[i], servers[i] = tr, rf, kvsrv, srv
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			servers[i].Close()
			rafts[i].Kill()
			kvs[i].Stop()
			transports[i].Close()
		}
	})

	// Client over the real KV RPC, talking to all three servers.
	endpoints := make([]kv.KV, n)
	for i := 0; i < n; i++ {
		kc := netrpc.DialKV(addrs[i], 1*time.Second)
		defer kc.Close()
		endpoints[i] = kc
	}
	ck := kv.NewClerk(endpoints)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// The first op blocks until an election completes and the Clerk finds the leader.
	if err := ck.Put(ctx, "color", "blue"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, ok, err := ck.Get(ctx, "color"); err != nil || !ok || v != "blue" {
		t.Fatalf("get = (%q,%v,%v), want (blue,true,nil)", v, ok, err)
	}
	if swapped, err := ck.CAS(ctx, "color", "blue", "green"); err != nil || !swapped {
		t.Fatalf("CAS = (%v,%v), want (true,nil)", swapped, err)
	}
	if v, _, _ := ck.Get(ctx, "color"); v != "green" {
		t.Fatalf("get after CAS = %q, want green", v)
	}
	if existed, err := ck.Delete(ctx, "color"); err != nil || !existed {
		t.Fatalf("delete = (%v,%v), want (true,nil)", existed, err)
	}
	if _, ok, _ := ck.Get(ctx, "color"); ok {
		t.Fatal("key still present after delete")
	}
}
