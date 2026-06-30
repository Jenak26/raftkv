package cluster

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport"
)

// raftFactory builds a NodeFactory whose nodes are real Raft instances. Each
// node's randomized election timeout is seeded deterministically from the
// cluster seed and the node id, so a run is reproducible.
func raftFactory(seed int64) NodeFactory {
	return func(id int, peers []int, tr transport.Transport, p storage.Persister, clk clock.Clock) transport.Server {
		return raft.Make(raft.Config{
			ID:        id,
			Peers:     peers,
			Transport: tr,
			Persister: p,
			Clock:     clk,
			ApplyCh:   make(chan raft.ApplyMsg, 256),
			Rand:      rand.New(rand.NewSource(seed*1_000_003 + int64(id) + 1)),
		})
	}
}

// newRaftCluster starts an n-node Raft cluster on the simulated network with a
// background clock pump driving its timers, and registers teardown.
func newRaftCluster(t *testing.T, n int, seed int64) *Cluster {
	t.Helper()
	c := New(t, n, seed, raftFactory(seed))
	stop := c.StartClockPump(time.Millisecond, 100*time.Microsecond)
	t.Cleanup(func() {
		for id := 0; id < n; id++ {
			c.Crash(id)
		}
		stop()
	})
	return c
}

// checkOneLeader samples leadership repeatedly until deadline, fails if any term
// ever has more than one leader (Election Safety), and returns the leader of the
// highest term seen.
func checkOneLeader(t *testing.T, c *Cluster, timeout time.Duration) (leader, term int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leadersByTerm := map[int]map[int]bool{}
		for r := 0; r < 12; r++ {
			for id, tm := range c.Leaders() {
				if leadersByTerm[tm] == nil {
					leadersByTerm[tm] = map[int]bool{}
				}
				leadersByTerm[tm][id] = true
			}
			time.Sleep(3 * time.Millisecond)
		}
		for tm, ids := range leadersByTerm {
			if len(ids) > 1 {
				t.Fatalf("ELECTION SAFETY VIOLATED: term %d has %d leaders: %v", tm, len(ids), keys(ids))
			}
		}
		lastTerm := -1
		for tm := range leadersByTerm {
			if tm > lastTerm {
				lastTerm = tm
			}
		}
		if lastTerm >= 0 {
			for id := range leadersByTerm[lastTerm] {
				return id, lastTerm
			}
		}
	}
	t.Fatalf("no leader elected within %s", timeout)
	return -1, -1
}

func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSingleLeaderElectedNoFaults(t *testing.T) {
	for _, n := range []int{3, 5} {
		c := newRaftCluster(t, n, 1)
		leader, term := checkOneLeader(t, c, 3*time.Second)
		if term < 1 {
			t.Errorf("n=%d: leader %d elected in term %d, want term >= 1", n, leader, term)
		}
	}
}

func TestLeaderReElectedAfterCrash(t *testing.T) {
	c := newRaftCluster(t, 5, 2)

	leader1, term1 := checkOneLeader(t, c, 3*time.Second)

	// Kill the leader; the remaining majority must elect a new one.
	c.Crash(leader1)

	leader2, term2 := checkOneLeader(t, c, 3*time.Second)
	if leader2 == leader1 {
		t.Errorf("crashed leader %d still reported as leader", leader1)
	}
	if term2 <= term1 {
		t.Errorf("re-election term %d should exceed original term %d", term2, term1)
	}
}

func TestLeaderReElectedAfterPartition(t *testing.T) {
	c := newRaftCluster(t, 5, 3)
	leader1, term1 := checkOneLeader(t, c, 3*time.Second)

	// Trap the leader in a 2-node minority; the 3-node majority must elect anew.
	others := make([]int, 0, 4)
	for id := 0; id < 5; id++ {
		if id != leader1 {
			others = append(others, id)
		}
	}
	minority := []int{leader1, others[0]}
	majority := others[1:]
	c.Partition(minority, majority)

	// The majority elects a new leader at a higher term.
	deadline := time.Now().Add(3 * time.Second)
	var term2 int
	found := false
	for time.Now().Before(deadline) && !found {
		for id, tm := range c.Leaders() {
			for _, m := range majority {
				if id == m && tm > term1 {
					term2, found = tm, true
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !found {
		t.Fatalf("majority %v failed to elect a new leader above term %d (leaders=%v)", majority, term1, c.Leaders())
	}

	// Heal: the whole cluster must converge to a single leader.
	c.Heal()
	_, term3 := checkOneLeader(t, c, 3*time.Second)
	if term3 < term2 {
		t.Errorf("after heal, term %d regressed below %d", term3, term2)
	}
}

// TestMinorityCannotElect partitions every node into its own singleton group
// before any time passes, so no node ever had a chance to become leader. None of
// the isolated nodes can reach a majority, so no leader may emerge.
func TestMinorityCannotElect(t *testing.T) {
	c := New(t, 3, 7, raftFactory(7))
	c.Partition([]int{0}, []int{1}, []int{2}) // isolate all, before the pump runs

	stop := c.StartClockPump(time.Millisecond, 100*time.Microsecond)
	t.Cleanup(func() {
		for id := 0; id < 3; id++ {
			c.Crash(id)
		}
		stop()
	})

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if ls := c.Leaders(); len(ls) > 0 {
			t.Fatalf("an isolated node (no quorum) became leader: %v", ls)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Healing restores a quorum, so a leader must now be electable.
	c.Heal()
	checkOneLeader(t, c, 3*time.Second)
}

// sampleSafety repeatedly checks, over dur, that no term is ever claimed by more
// than one leader - the Election Safety property.
func sampleSafety(t *testing.T, c *Cluster, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		byTerm := map[int]int{}
		for id, term := range c.Leaders() {
			if other, ok := byTerm[term]; ok && other != id {
				t.Fatalf("ELECTION SAFETY VIOLATED: term %d claimed by leaders %d and %d", term, other, id)
			}
			byTerm[term] = id
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// TestElectionSafetyUnderRandomPartitions is the headline correctness property:
// across many seeds, under churning random partitions and heals, two nodes must
// never both be leader in the same term. The seed makes every run reproducible.
func TestElectionSafetyUnderRandomPartitions(t *testing.T) {
	const (
		seeds  = 10
		cycles = 4
		window = 40 * time.Millisecond
	)
	for s := int64(0); s < seeds; s++ {
		t.Run(fmt.Sprintf("seed=%d", s), func(t *testing.T) {
			c := newRaftCluster(t, 5, s)
			rng := rand.New(rand.NewSource(s))
			for cycle := 0; cycle < cycles; cycle++ {
				var g0, g1 []int
				for id := 0; id < 5; id++ {
					if rng.Intn(2) == 0 {
						g0 = append(g0, id)
					} else {
						g1 = append(g1, id)
					}
				}
				c.Partition(g0, g1)
				sampleSafety(t, c, window)
				c.Heal()
				sampleSafety(t, c, window)
			}
		})
	}
}
