// Package transport abstracts node-to-node communication so that the Raft layer
// never touches sockets directly.
//
// Two implementations satisfy the same interface:
//   - a production transport (gRPC/TCP), and
//   - an in-memory simulation transport (internal/transport/memnet) that can
//     deterministically drop, delay, reorder, and partition messages from a
//     seed.
//
// Because both look identical to Raft, every correctness test runs against the
// simulation transport and every deployment runs against the real one - the
// foundation of the project's Deterministic Simulation Testing strategy.
package transport

import (
	"context"

	"github.com/janak/raftkv/internal/raft"
)

// Transport delivers Raft's RPCs to a peer identified by its node id. Each
// method blocks until a reply arrives or ctx is cancelled, and returns a
// non-nil error if the peer was unreachable.
type Transport interface {
	SendRequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
}

// Server is the receiving half of the RPC contract - the symmetric counterpart
// to Transport. A Raft node implements Server so that a Transport (real or
// simulated) can deliver an incoming RPC to it and hand the reply back to the
// caller. Handlers must be safe for concurrent calls and must not block
// indefinitely; the transport owns timeouts and reachability.
type Server interface {
	HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
}
