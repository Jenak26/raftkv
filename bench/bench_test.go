// Package bench measures the cost of consensus: write/read throughput and latency
// for the KV store over a real 3-node cluster (net/rpc on loopback, real clock).
// Run with: make bench   (go test -bench=. -benchmem -run='^$' ./bench/...)
//
// These numbers quantify the trade-offs the system was built to expose — most
// notably linearizable reads (a ReadIndex heartbeat round) versus stale reads
// (a local read). See docs/BENCHMARKS.md for methodology and results.
package bench

import (
	"context"
	"fmt"
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

// newCluster starts an n-node KV cluster over loopback net/rpc and returns a Clerk
// plus a teardown func. It warms up by committing one write, which also forces an
// election and commits the leader's no-op (so the first measured op isn't paying
// election cost).
func newCluster(b *testing.B, n int) (*kv.Clerk, func()) {
	b.Helper()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}

	lns := make([]net.Listener, n)
	addrs := map[int]string{}
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatalf("listen: %v", err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}

	transports := make([]*netrpc.RaftTransport, n)
	rafts := make([]*raft.Raft, n)
	kvs := make([]*kv.Server, n)
	servers := make([]*netrpc.Server, n)
	for i := 0; i < n; i++ {
		tr := netrpc.NewRaftTransport(i, addrs, 200*time.Millisecond)
		ch := make(chan raft.ApplyMsg, 4096)
		rf := raft.Make(raft.Config{
			ID: i, Peers: ids, Transport: tr, Persister: storage.NewInMemoryPersister(),
			Clock: clock.NewRealClock(), ApplyCh: ch, Rand: rand.New(rand.NewSource(int64(i) + 1)),
		})
		ksrv := kv.NewServer(rf, ch, kv.NewMapStateMachine(), 0)
		srv, err := netrpc.Serve(lns[i], rf, ksrv, 2*time.Second)
		if err != nil {
			b.Fatalf("serve: %v", err)
		}
		transports[i], rafts[i], kvs[i], servers[i] = tr, rf, ksrv, srv
	}

	endpoints := make([]kv.KV, n)
	for i := 0; i < n; i++ {
		endpoints[i] = netrpc.DialKV(addrs[i], time.Second)
	}
	ck := kv.NewClerk(endpoints)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := ck.Put(ctx, "__warmup__", "x"); err != nil {
		b.Fatalf("warmup put (no leader?): %v", err)
	}

	cleanup := func() {
		for i := 0; i < n; i++ {
			servers[i].Close()
			rafts[i].Kill()
			kvs[i].Stop()
			transports[i].Close()
		}
	}
	return ck, cleanup
}

func BenchmarkWrite(b *testing.B) {
	ck, cleanup := newCluster(b, 3)
	defer cleanup()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ck.Put(ctx, "k", fmt.Sprintf("v%d", i)); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkReadLinearizable(b *testing.B) {
	ck, cleanup := newCluster(b, 3)
	defer cleanup()
	ctx := context.Background()
	if err := ck.Put(ctx, "k", "v"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ck.Get(ctx, "k"); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkReadStale(b *testing.B) {
	ck, cleanup := newCluster(b, 3)
	defer cleanup()
	ctx := context.Background()
	if err := ck.Put(ctx, "k", "v"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ck.GetStale(ctx, "k"); err != nil {
			b.Fatalf("stale get: %v", err)
		}
	}
}
