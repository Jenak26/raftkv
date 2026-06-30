// Package memnet is a deterministic, in-process simulation of the network that
// connects Raft nodes. It implements transport.Transport so the exact same Raft
// code that runs over real sockets in production runs over this simulation in
// tests - the foundation of the project's Deterministic Simulation Testing
// strategy (see docs/ARCHITECTURE.md).
//
// Every source of non-determinism is seed-driven: message drops, added latency,
// and reordering all come from a single seeded RNG, and all timing flows through
// an injected clock.Clock (a clock.MockClock in tests). That means any failure -
// however rare - can be replayed exactly by rerunning with the same seed.
//
// The model is synchronous request/reply, mirroring the transport.Transport
// contract: a Send* call blocks until the peer's handler returns a reply or the
// network decides the message is lost. This deliberately follows the proven
// design of MIT 6.824's labrpc rather than an asynchronous message queue,
// because Raft's RPCs are themselves request/reply.
package memnet

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/transport"
)

// endpoint is one node's presence on the network.
type endpoint struct {
	id        int
	server    transport.Server // nil once the node has crashed
	connected bool             // false once the node is isolated from the network
}

// Network is the central coordinator of the simulation. It owns every endpoint,
// the seeded RNG that drives all randomness, and the clock used for latency.
//
// A single mutex guards all mutable state. It is held only while inspecting or
// mutating that state and while consulting the RNG - never while a peer handler
// runs - so handlers may freely call back into the network.
type Network struct {
	mu    sync.Mutex
	rng   *rand.Rand
	clk   clock.Clock
	nodes map[int]*endpoint

	// reliable, when false, makes the network drop messages and add latency at
	// random (seed-driven). When true (the default) every reachable message is
	// delivered immediately.
	reliable bool

	// partitions, when non-nil, groups node ids that can reach one another.
	// Nodes in different groups (or a node in no group) cannot communicate.
	partitions map[int]int // node id -> partition group index

	rpcCount int // total Send* calls that were actually delivered to a handler
}

// New returns a fully-connected, reliable Network whose randomness is seeded by
// seed and whose latency is measured by clk.
func New(seed int64, clk clock.Clock) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		clk:      clk,
		nodes:    make(map[int]*endpoint),
		reliable: true,
	}
}

// AddNode registers a node id served by server. The node starts connected.
func (n *Network) AddNode(id int, server transport.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = &endpoint{id: id, server: server, connected: true}
}

// Transport returns the transport.Transport that node from uses to reach its
// peers over this network.
func (n *Network) Transport(from int) transport.Transport {
	return &netTransport{net: n, from: from}
}

// Crash drops a node's in-RAM server so it can neither receive nor answer RPCs,
// simulating a process death. Its Persister (held by the caller) survives, so a
// later Restart reloads from durable state - exactly like a real restart.
func (n *Network) Crash(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.nodes[id]; ok {
		e.server = nil
	}
}

// Restart re-attaches a (freshly constructed) server to a crashed node.
func (n *Network) Restart(id int, server transport.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.nodes[id]; ok {
		e.server = server
		e.connected = true
	} else {
		n.nodes[id] = &endpoint{id: id, server: server, connected: true}
	}
}

// Disconnect isolates a node from the network without crashing it: it keeps
// running but no messages flow in or out.
func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.nodes[id]; ok {
		e.connected = false
	}
}

// Connect reverses Disconnect, restoring the node's connectivity.
func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.nodes[id]; ok {
		e.connected = true
	}
}

// SetReliable toggles message loss and latency injection. true (the default)
// means a perfect network; false means seed-driven drops and delays.
func (n *Network) SetReliable(reliable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = reliable
}

// Partition splits the cluster into isolated groups. Each argument is a group of
// node ids that can talk to one another; nodes in different groups cannot. Any
// node not named in a group becomes unreachable until the next Partition or
// Heal. Call Heal to restore full connectivity.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[int]int)
	for g, group := range groups {
		for _, id := range group {
			n.partitions[id] = g
		}
	}
}

// Heal removes all partitions, restoring full connectivity among connected,
// non-crashed nodes.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = nil
}

// RPCCount returns the number of RPCs that have been delivered to a peer handler
// (dropped or unreachable messages are not counted). Useful for tests and for
// the observability work in later phases.
func (n *Network) RPCCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcCount
}

// errUnreachable is returned when a message cannot be delivered.
type errUnreachable struct {
	from, to int
	reason   string
}

func (e errUnreachable) Error() string {
	return fmt.Sprintf("memnet: %d -> %d unreachable: %s", e.from, e.to, e.reason)
}

// reachable reports whether from can currently deliver to the endpoint to. It
// must be called with n.mu held.
func (n *Network) reachable(from, to int) (*endpoint, error) {
	src, ok := n.nodes[from]
	if !ok {
		return nil, errUnreachable{from, to, "sender not on network"}
	}
	if src.server == nil {
		return nil, errUnreachable{from, to, "sender crashed"}
	}
	dst, ok := n.nodes[to]
	if !ok {
		return nil, errUnreachable{from, to, "no such peer"}
	}
	if !src.connected || !dst.connected {
		return nil, errUnreachable{from, to, "disconnected"}
	}
	if dst.server == nil {
		return nil, errUnreachable{from, to, "peer crashed"}
	}
	if n.partitions != nil {
		gs, okS := n.partitions[from]
		gd, okD := n.partitions[to]
		if !okS || !okD || gs != gd {
			return nil, errUnreachable{from, to, "partitioned"}
		}
	}
	return dst, nil
}

// dispatch is the single delivery path shared by all three RPCs. It decides
// reachability and (for unreliable networks) loss/latency under the lock, then
// invokes the peer's handler outside the lock and returns its reply.
//
// call receives the live peer server and performs the actual handler invocation;
// returning the reply lets the three typed Send* methods stay tiny.
func (n *Network) dispatch(ctx context.Context, from, to int, call func(transport.Server)) error {
	n.mu.Lock()
	dst, err := n.reachable(from, to)
	if err != nil {
		n.mu.Unlock()
		return err
	}

	var delay time.Duration
	if !n.reliable {
		// Seed-driven loss: ~10% of messages vanish on an unreliable network.
		if n.rng.Intn(1000) < 100 {
			n.mu.Unlock()
			return errUnreachable{from, to, "dropped"}
		}
		// Seed-driven latency up to ~27ms. Long delays reorder messages, which
		// is exactly the adversarial timing Raft must tolerate.
		delay = time.Duration(n.rng.Intn(27)) * time.Millisecond
	}
	server := dst.server
	n.rpcCount++
	n.mu.Unlock()

	if delay > 0 {
		select {
		case <-n.clk.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Run the handler outside the lock so handlers may call back into the
	// network without deadlocking.
	call(server)
	return nil
}

// netTransport is the per-node view of the Network returned by Transport.
type netTransport struct {
	net  *Network
	from int
}

var _ transport.Transport = (*netTransport)(nil)

func (t *netTransport) SendRequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply *raft.RequestVoteReply
	err := t.net.dispatch(ctx, t.from, to, func(s transport.Server) {
		reply = s.HandleRequestVote(args)
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *netTransport) SendAppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply *raft.AppendEntriesReply
	err := t.net.dispatch(ctx, t.from, to, func(s transport.Server) {
		reply = s.HandleAppendEntries(args)
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *netTransport) SendInstallSnapshot(ctx context.Context, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply *raft.InstallSnapshotReply
	err := t.net.dispatch(ctx, t.from, to, func(s transport.Server) {
		reply = s.HandleInstallSnapshot(args)
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}
