package cluster

import (
	"fmt"
	"testing"
	"time"
)

// logStater lets a test read a node's durable log extent directly, independent of
// what the applyCollector observed - the strongest proof that state survived a
// crash is reading it back out of the freshly-restarted node.
type logStater interface {
	LogState() (lastIndex, lastTerm, commit int)
}

func lastLogIndexOf(t *testing.T, c *Cluster, id int) int {
	t.Helper()
	srv := c.Server(id)
	if srv == nil {
		t.Fatalf("node %d is not running", id)
	}
	ls, ok := srv.(logStater)
	if !ok {
		t.Fatalf("node %d does not expose LogState", id)
	}
	idx, _, _ := ls.LogState()
	return idx
}

// TestPersistedDataSurvivesFullClusterRestart is the M3 demo as a test: commit a
// few entries, crash EVERY node (losing all volatile state), restart them all from
// their persisters, and show that (a) every node's durable log still holds the
// committed entries the instant it comes back, and (b) once a new leader commits a
// fresh entry, the whole pre-crash prefix re-applies identically on every node.
//
// The fresh proposal after restart is not incidental: a newly elected leader
// starts with commitIndex 0 and, by the Figure 8 rule, cannot mark the old
// prior-term entries committed by replica count alone. They commit transitively
// the moment a current-term entry above them commits.
func TestPersistedDataSurvivesFullClusterRestart(t *testing.T) {
	c, col := newReplCluster(t, 3, 7)
	checkOneLeader(t, c, 3*time.Second)

	expected := map[int][]byte{}
	for i := 1; i <= 3; i++ {
		cmd := []byte(fmt.Sprintf("pre-%d", i))
		expected[mustProposeToAnyLeader(t, c, cmd)] = cmd
	}
	waitAppliedAll(t, col, 3, allIDs(3), 5*time.Second)

	// Crash the entire cluster - every node loses its in-RAM term/role/commit.
	for id := 0; id < 3; id++ {
		c.Crash(id)
	}
	// Restart them all from durable storage.
	for id := 0; id < 3; id++ {
		c.Restart(id)
	}

	// Durability check that does not depend on the collector: each node's log must
	// still contain the three committed entries right out of the persister.
	for id := 0; id < 3; id++ {
		if got := lastLogIndexOf(t, c, id); got < 3 {
			t.Fatalf("node %d came back with lastLogIndex %d, want >= 3 - committed log was not persisted", id, got)
		}
	}

	// A new leader emerges from the durable logs and commits a fresh entry, which
	// transitively re-commits and re-applies the persisted prefix everywhere.
	checkOneLeader(t, c, 5*time.Second)
	cmd := []byte("post-4")
	expected[mustProposeToAnyLeader(t, c, cmd)] = cmd

	waitAppliedAll(t, col, 4, allIDs(3), 8*time.Second)
	assertConverged(t, col, allIDs(3), 4, expected)
}

// TestPersistUnderUnreliableChurn stresses persistence the hard way: messages are
// dropped and delayed by the network while leaders are repeatedly crashed and
// restarted. Throughout, the applyCollector enforces continuously that no two
// nodes ever apply different commands at the same index. After the chaos stops and
// the network heals, the cluster must still have a committed prefix and elect a
// stable leader - proving recovery from durable state works even when recovery
// itself races message loss.
func TestPersistUnderUnreliableChurn(t *testing.T) {
	for s := int64(0); s < 3; s++ {
		t.Run(fmt.Sprintf("seed=%d", s), func(t *testing.T) {
			c, col := newReplCluster(t, 5, 300+s)
			checkOneLeader(t, c, 3*time.Second)
			c.SetReliable(false)

			n := 0
			for round := 0; round < 5; round++ {
				for i := 0; i < 3; i++ {
					for id := range c.Leaders() {
						tryPropose(c, id, []byte(fmt.Sprintf("u-%d", n)))
						break
					}
					n++
					time.Sleep(10 * time.Millisecond)
				}
				for id := range c.Leaders() {
					c.Crash(id)
					c.Restart(id)
					break
				}
				time.Sleep(60 * time.Millisecond)
			}

			// Heal and let the cluster settle from durable state.
			c.SetReliable(true)
			checkOneLeader(t, c, 5*time.Second)
			time.Sleep(400 * time.Millisecond)
			if col.maxAgreedIndex() == 0 {
				t.Fatal("no entries committed under unreliable churn")
			}
		})
	}
}
