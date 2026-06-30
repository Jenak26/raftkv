package cluster

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport"
)

// newSeededRand derives a per-node deterministic RNG from the cluster seed, so a
// node's randomized election timeouts replay identically for a given seed.
func newSeededRand(seed int64, id int) *rand.Rand {
	return rand.New(rand.NewSource(seed*1_000_003 + int64(id) + 1))
}

// proposer is the leader-side write API a Raft node exposes.
type proposer interface {
	Propose(command []byte) (index, term int, isLeader bool)
}

// applyCollector drains every node's applyCh and records what each node applied
// at each index. It is the test oracle for the two replication safety properties:
//   - State Machine Safety: no two nodes ever apply *different* commands at the
//     same index (checked on every record).
//   - Agreement: after convergence, all nodes share the same committed prefix.
type applyCollector struct {
	t       *testing.T
	mu      sync.Mutex
	agreed  map[int][]byte         // index -> the one command committed there
	perNode map[int]map[int][]byte // node id -> index -> command
}

func newApplyCollector(t *testing.T) *applyCollector {
	return &applyCollector{t: t, agreed: map[int][]byte{}, perNode: map[int]map[int][]byte{}}
}

func (a *applyCollector) record(id int, m raft.ApplyMsg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if prev, ok := a.agreed[m.CommandIndex]; ok {
		if !bytes.Equal(prev, m.Command) {
			a.t.Errorf("STATE MACHINE SAFETY VIOLATED at index %d: %q already committed, node %d applied %q",
				m.CommandIndex, prev, id, m.Command)
		}
	} else {
		a.agreed[m.CommandIndex] = append([]byte(nil), m.Command...)
	}
	if a.perNode[id] == nil {
		a.perNode[id] = map[int][]byte{}
	}
	a.perNode[id][m.CommandIndex] = append([]byte(nil), m.Command...)
}

// appliedIndex returns the highest index k such that node id has applied a
// contiguous prefix 1..k.
func (a *applyCollector) appliedIndex(id int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	m := a.perNode[id]
	k := 0
	for {
		if _, ok := m[k+1]; ok {
			k++
		} else {
			return k
		}
	}
}

func (a *applyCollector) maxAgreedIndex() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	max := 0
	for i := range a.agreed {
		if i > max {
			max = i
		}
	}
	return max
}

// newReplCluster builds an n-node Raft cluster whose apply channels are drained
// into a fresh applyCollector, with the clock pump running. Teardown crashes the
// nodes, stops the drains, and stops the pump.
func newReplCluster(t *testing.T, n int, seed int64) (*Cluster, *applyCollector) {
	t.Helper()
	col := newApplyCollector(t)
	done := make(chan struct{})
	var wg sync.WaitGroup

	factory := func(id int, peers []int, tr transport.Transport, p storage.Persister, clk clock.Clock) transport.Server {
		ch := make(chan raft.ApplyMsg, 1024)
		rf := raft.Make(raft.Config{
			ID: id, Peers: peers, Transport: tr, Persister: p, Clock: clk,
			ApplyCh: ch, Rand: newSeededRand(seed, id),
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case m := <-ch:
					if m.CommandValid {
						col.record(id, m)
					}
				}
			}
		}()
		return rf
	}

	c := New(t, n, seed, factory)
	stop := c.StartClockPump(time.Millisecond, 100*time.Microsecond)
	t.Cleanup(func() {
		for id := 0; id < n; id++ {
			c.Crash(id)
		}
		close(done)
		wg.Wait()
		stop()
	})
	return c, col
}

// --- proposal helpers ---

func tryPropose(c *Cluster, id int, cmd []byte) (index int, ok bool) {
	srv := c.Server(id)
	if srv == nil {
		return 0, false
	}
	p, isP := srv.(proposer)
	if !isP {
		return 0, false
	}
	idx, _, isLeader := p.Propose(cmd)
	return idx, isLeader
}

func mustProposeToAnyLeader(t *testing.T, c *Cluster, cmd []byte) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for id := range c.Leaders() {
			if idx, ok := tryPropose(c, id, cmd); ok {
				return idx
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader accepted proposal %q", cmd)
	return -1
}

func waitAppliedAll(t *testing.T, col *applyCollector, upto int, ids []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, id := range ids {
			if col.appliedIndex(id) < upto {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("nodes %v did not all apply through index %d in time", ids, upto)
}

func assertConverged(t *testing.T, col *applyCollector, ids []int, upto int, expected map[int][]byte) {
	t.Helper()
	col.mu.Lock()
	defer col.mu.Unlock()
	for i := 1; i <= upto; i++ {
		want := expected[i]
		for _, id := range ids {
			got := col.perNode[id][i]
			if want != nil && !bytes.Equal(got, want) {
				t.Errorf("node %d index %d = %q, want %q", id, i, got, want)
			}
		}
	}
}

func allIDs(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func TestReplicatesAndAppliesInOrder(t *testing.T) {
	c, col := newReplCluster(t, 3, 1)
	checkOneLeader(t, c, 3*time.Second)

	expected := map[int][]byte{}
	for i := 1; i <= 5; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		idx := mustProposeToAnyLeader(t, c, cmd)
		expected[idx] = cmd
	}

	waitAppliedAll(t, col, 5, allIDs(3), 5*time.Second)
	assertConverged(t, col, allIDs(3), 5, expected)
}

func TestLogConvergesAfterPartition(t *testing.T) {
	c, col := newReplCluster(t, 5, 4)
	l1, term1 := checkOneLeader(t, c, 3*time.Second)

	// Indices are not hard-coded: a leader appends a no-op on election, so the
	// first client entry is not at index 1. We track the actual committed indices.
	expected := map[int][]byte{}
	lastA := 0
	for i := 1; i <= 3; i++ {
		cmd := []byte(fmt.Sprintf("A%d", i))
		idx := mustProposeToAnyLeader(t, c, cmd)
		expected[idx] = cmd
		lastA = idx
	}
	waitAppliedAll(t, col, lastA, allIDs(5), 5*time.Second)
	committedBeforePartition := lastA

	// Trap the old leader in a 2-node minority.
	others := []int{}
	for id := 0; id < 5; id++ {
		if id != l1 {
			others = append(others, id)
		}
	}
	minority := []int{l1, others[0]}
	majority := others[1:] // 3 nodes
	c.Partition(minority, majority)

	// The isolated old leader accepts divergent commands it can never commit.
	tryPropose(c, l1, []byte("OLD-4"))
	tryPropose(c, l1, []byte("OLD-5"))

	// The majority elects a new leader and commits real entries 4..6.
	var l2 int
	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("majority %v elected no leader above term %d", majority, term1)
		}
		found := false
		for id, tm := range c.Leaders() {
			for _, m := range majority {
				if id == m && tm > term1 {
					l2, found = id, true
				}
			}
		}
		if found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	lastB := 0
	for i := 1; i <= 3; i++ {
		cmd := []byte(fmt.Sprintf("B%d", i))
		if idx, ok := tryPropose(c, l2, cmd); ok {
			expected[idx] = cmd
			lastB = idx
		} else {
			t.Fatalf("new leader %d rejected proposal", l2)
		}
	}
	waitAppliedAll(t, col, lastB, majority, 5*time.Second)

	// The old leader must not have applied anything past the prefix that was
	// committed before the partition — its divergent OLD-* entries never commit.
	if col.appliedIndex(l1) > committedBeforePartition {
		t.Fatalf("old leader %d applied an uncommitted divergent entry (applied %d > %d)",
			l1, col.appliedIndex(l1), committedBeforePartition)
	}

	// Heal: every node converges on the same committed log; the OLD-* entries are gone.
	c.Heal()
	waitAppliedAll(t, col, lastB, allIDs(5), 6*time.Second)
	assertConverged(t, col, allIDs(5), lastB, expected)
}

// TestNoConflictingCommitsUnderChurn is the headline durability property: across
// seeds, while leaders are repeatedly crashed and restarted, no committed entry
// is ever lost or overwritten by a different command (enforced continuously by
// the collector), and the cluster keeps making progress.
func TestNoConflictingCommitsUnderChurn(t *testing.T) {
	for s := int64(0); s < 4; s++ {
		t.Run(fmt.Sprintf("seed=%d", s), func(t *testing.T) {
			c, col := newReplCluster(t, 5, 200+s)
			n := 0
			for round := 0; round < 4; round++ {
				for i := 0; i < 4; i++ {
					for id := range c.Leaders() {
						tryPropose(c, id, []byte(fmt.Sprintf("v-%d", n)))
						break
					}
					n++
					time.Sleep(8 * time.Millisecond)
				}
				for id := range c.Leaders() {
					c.Crash(id)
					c.Restart(id)
					break
				}
				time.Sleep(40 * time.Millisecond)
			}
			// After the chaos stops, the cluster must still make progress: commit a
			// fresh entry and confirm every node applies it. (Asserting only that
			// *some* in-loop entry committed is flaky — aggressive crash timing can
			// legitimately prevent any in-loop commit; proving post-churn liveness is
			// the property we actually care about, and it is deterministic.)
			checkOneLeader(t, c, 3*time.Second)
			idx := mustProposeToAnyLeader(t, c, []byte(fmt.Sprintf("final-%d", s)))
			waitAppliedAll(t, col, idx, allIDs(5), 8*time.Second)
		})
	}
}
