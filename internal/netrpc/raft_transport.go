// Package netrpc is the production transport: real node-to-node Raft RPC and the
// client-facing KV service, both over Go's stdlib net/rpc (gob on TCP).
//
// It is the deployment-time counterpart to internal/transport/memnet. Every
// correctness test runs over the deterministic simulated network; this package is
// what a real `kvserver` process uses to talk to its peers and clients. Because it
// satisfies the same raft.RPCTransport and kv.KV interfaces, no consensus or
// application code changes between simulation and production.
package netrpc

import (
	"context"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/transport"
)

// raftService adapts a transport.Server (a *raft.Raft) to net/rpc's method shape
// (func(args, *reply) error) and is registered under the service name "Raft".
type raftService struct{ h transport.Server }

func (s *raftService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = *s.h.HandleRequestVote(args)
	return nil
}

func (s *raftService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = *s.h.HandleAppendEntries(args)
	return nil
}

func (s *raftService) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	*reply = *s.h.HandleInstallSnapshot(args)
	return nil
}

// RaftTransport is the outbound (client) half: it dials peers lazily, caches one
// net/rpc client per peer, and drops a client on error so the next send re-dials.
// It satisfies raft.RPCTransport.
type RaftTransport struct {
	self    int
	addrs   map[int]string // peer id -> host:port
	timeout time.Duration  // per-call deadline (raft passes a background ctx)

	mu      sync.Mutex
	clients map[int]*rpc.Client
}

// NewRaftTransport builds a transport for node self that can reach the peers in
// addrs. A non-zero timeout bounds every call so a dead peer cannot wedge a
// replication goroutine indefinitely.
func NewRaftTransport(self int, addrs map[int]string, timeout time.Duration) *RaftTransport {
	cp := make(map[int]string, len(addrs))
	for id, a := range addrs {
		cp[id] = a
	}
	return &RaftTransport{self: self, addrs: cp, timeout: timeout, clients: map[int]*rpc.Client{}}
}

func (t *RaftTransport) SendRequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	return reply, t.call(ctx, to, "Raft.RequestVote", args, reply)
}

func (t *RaftTransport) SendAppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	return reply, t.call(ctx, to, "Raft.AppendEntries", args, reply)
}

func (t *RaftTransport) SendInstallSnapshot(ctx context.Context, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	return reply, t.call(ctx, to, "Raft.InstallSnapshot", args, reply)
}

// call invokes method on peer to, honoring whichever of ctx or the transport
// timeout fires first. On any error it drops the cached client so the connection
// is re-established on the next attempt.
func (t *RaftTransport) call(ctx context.Context, to int, method string, args, reply any) error {
	client, err := t.clientFor(to)
	if err != nil {
		return err
	}

	done := make(chan *rpc.Call, 1)
	client.Go(method, args, reply, done)

	var timeout <-chan time.Time
	if t.timeout > 0 {
		timer := time.NewTimer(t.timeout)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timeout:
		t.drop(to)
		return fmt.Errorf("netrpc: call %s to %d timed out", method, to)
	case c := <-done:
		if c.Error != nil {
			t.drop(to)
			return c.Error
		}
		return nil
	}
}

func (t *RaftTransport) clientFor(to int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		return c, nil
	}
	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("netrpc: no address for peer %d", to)
	}
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.clients[to] = c
	return c, nil
}

func (t *RaftTransport) drop(to int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		c.Close()
		delete(t.clients, to)
	}
}

// Close tears down all cached peer connections.
func (t *RaftTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
}

// Compile-time assertion that RaftTransport satisfies what raft.Make expects.
var _ raft.RPCTransport = (*RaftTransport)(nil)
