// Package storage provides durable persistence for a Raft node.
//
// Raft requires three pieces of state to survive crashes (currentTerm,
// votedFor, log[]); the Raft layer encodes them into one opaque "raft state"
// blob and saves it through a Persister before replying to any RPC that
// changed them. Snapshots are stored alongside that blob.
//
// Production will provide a file- or BoltDB-backed Persister. The in-memory
// implementation here is used by tests and by the deterministic simulation,
// where "crash" means dropping the node's in-RAM state while keeping the
// Persister, exactly as a real restart reloads from disk.
package storage

import "sync"

// Persister is the durability boundary for a single Raft node.
//
// Implementations must guarantee that SaveStateAndSnapshot is crash-atomic with
// respect to the pair it writes: after a crash, ReadRaftState and ReadSnapshot
// must return state and snapshot that were saved together (no torn pair).
type Persister interface {
	// SaveRaftState durably stores the encoded Raft state, replacing any
	// previously saved state.
	SaveRaftState(state []byte)
	// ReadRaftState returns the most recently saved Raft state, or nil if none.
	ReadRaftState() []byte
	// SaveStateAndSnapshot atomically stores Raft state and a snapshot together.
	SaveStateAndSnapshot(state, snapshot []byte)
	// ReadSnapshot returns the most recently saved snapshot, or nil if none.
	ReadSnapshot() []byte
	// RaftStateSize returns the size in bytes of the saved Raft state. The Raft
	// layer uses this to decide when to snapshot (compact the log).
	RaftStateSize() int
	// SnapshotSize returns the size in bytes of the saved snapshot.
	SnapshotSize() int
}

// InMemoryPersister is a Persister backed by RAM. It clones all data on the way
// in and out so callers can never alias and accidentally mutate stored bytes.
type InMemoryPersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

// NewInMemoryPersister returns an empty InMemoryPersister.
func NewInMemoryPersister() *InMemoryPersister { return &InMemoryPersister{} }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (p *InMemoryPersister) SaveRaftState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(state)
}

func (p *InMemoryPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

func (p *InMemoryPersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(state)
	p.snapshot = clone(snapshot)
}

func (p *InMemoryPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func (p *InMemoryPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func (p *InMemoryPersister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}

// Compile-time assertion that InMemoryPersister satisfies Persister.
var _ Persister = (*InMemoryPersister)(nil)
