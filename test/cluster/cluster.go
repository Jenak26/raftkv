// Package cluster is the in-process test harness for spinning up an N-node
// cluster on the deterministic simulated network (internal/transport/memnet)
// and subjecting it to faults — partitions, disconnects, and crash/restart —
// all reproducibly from a seed.
//
// It is deliberately decoupled from the Raft implementation: callers supply a
// NodeFactory that builds whatever transport.Server should run on each node.
// Phase 1 tests pass a simple test double; from Phase 2 onward the factory wraps
// raft.Make, and the same harness gains "find the leader" helpers. Keeping the
// dependency this way round means the harness (and the network beneath it) is
// fully testable before a single line of Raft exists — the whole point of
// building the test infrastructure first.
package cluster

import (
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport"
	"github.com/janak/raftkv/internal/transport/memnet"
)

// NodeFactory constructs the server that runs on one node. It receives the
// node's id, the full list of peer ids, the transport it uses to reach peers,
// its durable Persister (which survives crash/restart), and the shared clock.
//
// Crucially, on a restart the harness passes the *same* Persister the node had
// before it crashed, so a real raft.Make will reload currentTerm/votedFor/log
// from it — exactly as a real process reloads from disk.
type NodeFactory func(id int, peers []int, tr transport.Transport, persister storage.Persister, clk clock.Clock) transport.Server

// Cluster owns the simulated network, the shared deterministic clock, and the
// per-node persisters and servers.
type Cluster struct {
	t          *testing.T
	n          int
	ids        []int
	clk        *clock.MockClock
	net        *memnet.Network
	factory    NodeFactory
	persisters map[int]storage.Persister
	servers    map[int]transport.Server
}

// New builds and starts an n-node cluster seeded by seed, constructing each node
// with factory. Nodes are numbered 0..n-1 and start fully connected.
func New(t *testing.T, n int, seed int64, factory NodeFactory) *Cluster {
	t.Helper()
	clk := clock.NewMockClock(time.Unix(0, 0))
	c := &Cluster{
		t:          t,
		n:          n,
		clk:        clk,
		net:        memnet.New(seed, clk),
		factory:    factory,
		persisters: make(map[int]storage.Persister, n),
		servers:    make(map[int]transport.Server, n),
	}
	for id := 0; id < n; id++ {
		c.ids = append(c.ids, id)
	}
	for _, id := range c.ids {
		c.persisters[id] = storage.NewInMemoryPersister()
		c.start(id)
	}
	return c
}

// start constructs the node id from its existing persister and registers it on
// the network.
func (c *Cluster) start(id int) {
	srv := c.factory(id, c.ids, c.net.Transport(id), c.persisters[id], c.clk)
	c.servers[id] = srv
	c.net.AddNode(id, srv)
}

// Transport returns the transport a client (or test) uses to reach the cluster
// as node from.
func (c *Cluster) Transport(from int) transport.Transport { return c.net.Transport(from) }

// Server returns the currently-running server for node id. After a Restart this
// is the new instance, not the crashed one.
func (c *Cluster) Server(id int) transport.Server { return c.servers[id] }

// Clock returns the cluster's deterministic clock.
func (c *Cluster) Clock() *clock.MockClock { return c.clk }

// Advance moves simulated time forward by d for the whole cluster.
func (c *Cluster) Advance(d time.Duration) { c.clk.Advance(d) }

// Crash kills node id's running server (its in-RAM state is lost) while keeping
// its Persister, simulating a process death.
func (c *Cluster) Crash(id int) {
	c.net.Crash(id)
	delete(c.servers, id)
}

// Restart rebuilds node id from its preserved Persister and re-attaches it to
// the network, simulating a process restart that reloads durable state.
func (c *Cluster) Restart(id int) {
	srv := c.factory(id, c.ids, c.net.Transport(id), c.persisters[id], c.clk)
	c.servers[id] = srv
	c.net.Restart(id, srv)
}

// Disconnect isolates node id from the network without crashing it.
func (c *Cluster) Disconnect(id int) { c.net.Disconnect(id) }

// Connect restores a disconnected node's connectivity.
func (c *Cluster) Connect(id int) { c.net.Connect(id) }

// Partition splits the cluster into isolated groups (see memnet.Network.Partition).
func (c *Cluster) Partition(groups ...[]int) { c.net.Partition(groups...) }

// Heal removes all partitions, restoring full connectivity.
func (c *Cluster) Heal() { c.net.Heal() }

// SetReliable toggles seed-driven message loss and latency for the whole cluster.
func (c *Cluster) SetReliable(reliable bool) { c.net.SetReliable(reliable) }
