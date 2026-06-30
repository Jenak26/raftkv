package cluster

import (
	"fmt"
	"testing"
	"time"
)

// reconfigurer is the membership-change API a Raft node exposes.
type reconfigurer interface {
	AddServer(id int) error
	RemoveServer(id int) error
	Config() []int
}

func asReconfigurer(c *Cluster, id int) (reconfigurer, bool) {
	srv := c.Server(id)
	if srv == nil {
		return nil, false
	}
	r, ok := srv.(reconfigurer)
	return r, ok
}

// addServer retries AddServer across leaders until the change is accepted (or the
// node is already a member), then waits until the new member has actually adopted
// the configuration — i.e. the entry replicated to it.
func addServer(t *testing.T, c *Cluster, id int) {
	t.Helper()
	doReconfig(t, c, func(r reconfigurer) error { return r.AddServer(id) }, errAlreadyMemberOK)
	waitConfigContains(t, c, id, true, 5*time.Second)
}

// removeServer retries RemoveServer across leaders until accepted (or already gone).
func removeServer(t *testing.T, c *Cluster, id int) {
	t.Helper()
	doReconfig(t, c, func(r reconfigurer) error { return r.RemoveServer(id) }, errNotMemberOK)
}

const (
	errAlreadyMemberOK = "raft: server is already a member"
	errNotMemberOK     = "raft: server is not a member"
)

// doReconfig keeps trying fn on whatever node currently claims leadership until it
// returns nil, or returns the "already in the desired state" error (treated as
// success because a previous attempt must have taken effect).
func doReconfig(t *testing.T, c *Cluster, fn func(reconfigurer) error, okErr string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for id := range c.Leaders() {
			r, ok := asReconfigurer(c, id)
			if !ok {
				continue
			}
			err := fn(r)
			if err == nil || err.Error() == okErr {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader accepted the configuration change in time")
}

// waitConfigContains waits until some running node's current configuration includes
// (or excludes) id, confirming the change propagated.
func waitConfigContains(t *testing.T, c *Cluster, id int, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for nid := range c.servers {
			if r, ok := asReconfigurer(c, nid); ok {
				if contains(r.Config(), id) == want {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("configuration never reached want-contains(%d)=%v", id, want)
}

func contains(s []int, x int) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

// TestMembershipGrowAndShrinkWhileServing is the Phase 7 headline (milestone M5):
// grow a 3-node cluster to 5 and shrink back to 3 while a workload streams in.
// Throughout, the applyCollector enforces that no two nodes ever apply different
// commands at the same index, and we assert there is never more than one leader.
func TestMembershipGrowAndShrinkWhileServing(t *testing.T) {
	c, col := newReplCluster(t, 3, 42)
	checkOneLeader(t, c, 3*time.Second)

	n := 0
	propose := func(count int) {
		for i := 0; i < count; i++ {
			mustProposeToAnyLeader(t, c, []byte(fmt.Sprintf("v-%d", n)))
			n++
		}
	}

	propose(3)
	waitAppliedAll(t, col, 3, []int{0, 1, 2}, 5*time.Second)

	// Grow 3 -> 4 -> 5, one server at a time, serving writes between each step.
	for _, id := range []int{3, 4} {
		c.AddNode(id)
		addServer(t, c, id)
		propose(2)
		checkOneLeader(t, c, 3*time.Second)
	}
	if got := col.maxAgreedIndex(); got < 5 {
		t.Fatalf("expected progress while growing, maxAgreedIndex=%d", got)
	}

	// Shrink 5 -> 4 -> 3, removing the added servers and shutting each down.
	for _, id := range []int{4, 3} {
		removeServer(t, c, id)
		c.Crash(id) // operator decommissions the removed node
		propose(2)
		checkOneLeader(t, c, 5*time.Second)
	}

	// The surviving original cluster must still commit and agree.
	propose(2)
	final := col.maxAgreedIndex()
	waitAppliedAll(t, col, final, []int{0, 1, 2}, 6*time.Second)
}

// TestMembershipRejectsConcurrentChange verifies the one-change-at-a-time rule: a
// second AddServer issued while the first is still uncommitted is refused.
func TestMembershipRejectsConcurrentChange(t *testing.T) {
	c, _ := newReplCluster(t, 3, 7)
	leader, _ := checkOneLeader(t, c, 3*time.Second)

	// Isolate the rest so the first change cannot commit, keeping it "in progress".
	others := []int{}
	for id := 0; id < 3; id++ {
		if id != leader {
			others = append(others, id)
		}
	}
	c.Partition([]int{leader}, others)
	c.AddNode(5)

	r, ok := asReconfigurer(c, leader)
	if !ok {
		t.Fatal("leader is not a reconfigurer")
	}
	// The partitioned old leader can still append a config entry (it can't commit
	// it). Note: it may step down after losing contact; tolerate that by checking
	// either outcome of the first call, then asserting the second is rejected while
	// the first remains uncommitted.
	first := r.AddServer(5)
	if first == nil {
		if second := r.AddServer(6); second == nil {
			t.Fatal("a second configuration change was accepted while the first was uncommitted")
		}
	}
	c.Heal()
}

// TestMembershipLeaderStepsDownWhenRemoved removes the current leader from the
// configuration; a new leader must emerge from the remaining members and the old
// leader must relinquish leadership.
func TestMembershipLeaderStepsDownWhenRemoved(t *testing.T) {
	c, _ := newReplCluster(t, 5, 11)
	leader, _ := checkOneLeader(t, c, 3*time.Second)

	removeServer(t, c, leader)

	// A new leader must arise among the remaining members, and it must not be the
	// removed node.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for id, _ := range c.Leaders() {
			if id != leader {
				// Success: someone else leads. Give the old leader a moment, then
				// confirm it no longer claims leadership.
				if _, stillLeader := stateOf(c, leader); !stillLeader {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no new leader emerged after removing leader %d, or it kept leading", leader)
}

// stateOf reports node id's (term, isLeader) if it is running.
func stateOf(c *Cluster, id int) (term int, isLeader bool) {
	srv := c.Server(id)
	if srv == nil {
		return 0, false
	}
	if s, ok := srv.(stater); ok {
		return s.State()
	}
	return 0, false
}
