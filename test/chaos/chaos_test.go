// Package chaos is the credibility suite: it runs a live KV cluster under a
// seeded nemesis (partitions, crashes, restarts) while concurrent clients issue
// operations, records the real-time history of those operations, and checks it for
// linearizability. This is the Jepsen-style methodology — generate, perturb,
// record, verify — made fully reproducible by the deterministic simulated network.
//
// Every run prints its seed; a violation is replayable with:
//
//	make chaos SEED=<n>      (or: go test ./test/chaos -run TestChaos -args -seed=<n>)
package chaos

import (
	"context"
	"flag"
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
	lin "github.com/janak/raftkv/test/linearizability"
)

var seedFlag = flag.Int64("seed", -1, "run only this seed (default: sweep several seeds)")

const (
	numNodes      = 5
	numClients    = 3
	numKeys       = 8
	opsPerClient  = 40
	opDeadline    = 15 * time.Second
	faultDuration = 250 * time.Millisecond
	healDuration  = 300 * time.Millisecond
)

// chaosCluster wraps the generic cluster harness with a KV server per node and
// tracks the current server for each id so clients keep working across restarts.
type chaosCluster struct {
	t    *testing.T
	c    *cluster.Cluster
	n    int
	stop func()

	mu      sync.Mutex
	servers map[int]*kv.Server
}

func newChaosCluster(t *testing.T, n int, seed int64) *chaosCluster {
	cc := &chaosCluster{t: t, n: n, servers: map[int]*kv.Server{}}
	factory := func(id int, peers []int, tr transport.Transport, p storage.Persister, clk clock.Clock) transport.Server {
		ch := make(chan raft.ApplyMsg, 2048)
		rf := raft.Make(raft.Config{
			ID: id, Peers: peers, Transport: tr, Persister: p, Clock: clk,
			ApplyCh: ch, Rand: rand.New(rand.NewSource(seed*1_000_003 + int64(id) + 1)),
		})
		srv := kv.NewServer(rf, ch, kv.NewMapStateMachine(), 0)
		cc.mu.Lock()
		cc.servers[id] = srv
		cc.mu.Unlock()
		return rf
	}
	cc.c = cluster.New(t, n, seed, factory)
	cc.stop = cc.c.StartClockPump(time.Millisecond, 50*time.Microsecond)
	t.Cleanup(func() {
		cc.mu.Lock()
		for _, s := range cc.servers {
			s.Stop()
		}
		cc.mu.Unlock()
		for id := 0; id < n; id++ {
			cc.c.Crash(id)
		}
		cc.stop()
	})
	return cc
}

func (cc *chaosCluster) server(id int) *kv.Server {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.servers[id]
}

func (cc *chaosCluster) crash(id int) {
	if s := cc.server(id); s != nil {
		s.Stop()
	}
	cc.c.Crash(id)
}

func (cc *chaosCluster) restart(id int) { cc.c.Restart(id) }

func (cc *chaosCluster) waitLeader(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(cc.c.Leaders()) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cc.t.Fatal("no leader elected")
}

// endpoint forwards a client's Submit to the current server for one node id.
type endpoint struct {
	cc *chaosCluster
	id int
}

func (e endpoint) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	s := e.cc.server(e.id)
	if s == nil {
		return kv.Result{}, kv.ErrNotLeader
	}
	return s.Submit(ctx, cmd)
}

func (cc *chaosCluster) clerk() *kv.Clerk {
	eps := make([]kv.KV, cc.n)
	for id := 0; id < cc.n; id++ {
		eps[id] = endpoint{cc: cc, id: id}
	}
	return kv.NewClerk(eps, kv.WithTimeout(800*time.Millisecond))
}

// nemesis injects faults from a seeded RNG until stopped, healing between them so
// the workload can make progress.
func (cc *chaosCluster) nemesis(rng *rand.Rand, stop <-chan struct{}) {
	sleep := func(d time.Duration) bool {
		select {
		case <-stop:
			return false
		case <-time.After(d):
			return true
		}
	}
	for {
		select {
		case <-stop:
			cc.c.Heal()
			return
		default:
		}
		switch rng.Intn(3) {
		case 0, 1: // network partition into two random groups
			perm := rng.Perm(cc.n)
			cut := 1 + rng.Intn(cc.n-1)
			cc.c.Partition(perm[:cut], perm[cut:])
			if !sleep(faultDuration) {
				cc.c.Heal()
				return
			}
			cc.c.Heal()
		case 2: // crash and restart a random node
			victim := rng.Intn(cc.n)
			cc.crash(victim)
			if !sleep(faultDuration) {
				cc.restart(victim)
				return
			}
			cc.restart(victim)
		}
		if !sleep(healDuration) {
			return
		}
	}
}

// client issues a fixed number of random operations, recording each into rec. It
// relies on the Clerk's internal retry to complete every operation exactly once
// (Phase 5 dedup), so every recorded op corresponds to one committed effect.
func (cc *chaosCluster) client(ctx context.Context, id int, rec *lin.Recorder, keys []string, rng *rand.Rand) error {
	for i := 0; i < opsPerClient; i++ {
		key := keys[rng.Intn(len(keys))]
		ck := cc.clerk()
		switch rng.Intn(3) {
		case 0: // Put
			val := fmt.Sprintf("c%d-%d", id, i)
			h := rec.Begin(id, lin.OpPut, key, val, "")
			if err := ck.Put(ctx, key, val); err != nil {
				return fmt.Errorf("put %s=%s: %w", key, val, err)
			}
			h.End("", true)
		case 1: // Get (linearizable)
			h := rec.Begin(id, lin.OpGet, key, "", "")
			v, ok, err := ck.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("get %s: %w", key, err)
			}
			h.End(v, ok)
		case 2: // CAS against a recently written value (often misses, sometimes hits)
			expected := fmt.Sprintf("c%d-%d", id, i-1)
			val := fmt.Sprintf("c%d-%d!", id, i)
			h := rec.Begin(id, lin.OpCAS, key, val, expected)
			swapped, err := ck.CAS(ctx, key, expected, val)
			if err != nil {
				return fmt.Errorf("cas %s: %w", key, err)
			}
			h.End("", swapped)
		}
	}
	return nil
}

func runChaos(t *testing.T, seed int64) {
	cc := newChaosCluster(t, numNodes, seed)
	cc.waitLeader(3 * time.Second)

	rec := lin.NewRecorder()
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opDeadline)
	defer cancel()

	nemStop := make(chan struct{})
	go cc.nemesis(rand.New(rand.NewSource(seed*7919+1)), nemStop)

	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for cid := 0; cid < numClients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			if err := cc.client(ctx, cid, rec, keys, rand.New(rand.NewSource(seed*100+int64(cid)))); err != nil {
				errs <- err
			}
		}(cid)
	}
	wg.Wait()
	close(nemStop)
	cc.c.Heal()
	close(errs)
	for err := range errs {
		t.Fatalf("client could not make progress (seed=%d): %v", seed, err)
	}

	hist := rec.History()
	if len(hist) == 0 {
		t.Fatalf("no operations recorded (seed=%d)", seed)
	}
	if ok, badKey := lin.Check(hist); !ok {
		t.Fatalf("LINEARIZABILITY VIOLATION (seed=%d): key %q over %d ops — reproduce with: make chaos SEED=%d",
			seed, badKey, len(hist), seed)
	}
	t.Logf("seed=%d: %d operations linearizable under chaos", seed, len(hist))
}

// TestChaosLinearizable runs the nemesis+linearizability suite. By default it
// sweeps several seeds; pass -seed=<n> to replay a single one.
func TestChaosLinearizable(t *testing.T) {
	if *seedFlag >= 0 {
		runChaos(t, *seedFlag)
		return
	}
	for s := int64(0); s < 6; s++ {
		s := s
		t.Run(fmt.Sprintf("seed=%d", s), func(t *testing.T) {
			runChaos(t, s)
		})
	}
}
