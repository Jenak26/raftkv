// Package kv holds the application layer that sits on top of Raft: the
// key-value state machine and (later) the client-facing server.
//
// The StateMachine is the "replicated" in "replicated state machine": Raft
// guarantees every node applies the exact same sequence of commands, so as long
// as Apply is deterministic, every node ends up with identical state.
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// OpKind enumerates the operations a client may request.
type OpKind int

const (
	OpGet OpKind = iota
	OpPut
	OpDelete
	OpCAS // compare-and-swap: set Key to Value only if it currently equals Expected
)

// Command is a single operation to apply to the state machine. It carries
// client-session metadata (ClientID, SeqNum) so the server can deduplicate
// retried requests and achieve exactly-once semantics (Phase 5).
type Command struct {
	Kind     OpKind
	Key      string
	Value    string
	Expected string // only used by OpCAS
	ClientID int64
	SeqNum   int64

	// ReadStale, when set on an OpGet, asks for a fast read served from the local
	// state machine of whatever node receives it — possibly stale, not
	// linearizable. It is never used for writes and never travels through the log
	// (stale reads are not replicated).
	ReadStale bool
}

// Result is the outcome of applying a Command.
type Result struct {
	Value string // current/previous value (Get, failed CAS)
	Ok    bool   // Get: key existed; Put: applied; Delete: key existed; CAS: swapped
	Err   string // non-empty on error
}

// StateMachine is the deterministic application replicated by Raft. Applying
// the same Commands in the same order on every replica must produce identical
// state and identical Results.
type StateMachine interface {
	Apply(cmd Command) Result
	// Snapshot serializes the entire current state for log compaction.
	Snapshot() ([]byte, error)
	// Restore replaces the current state with one decoded from a snapshot.
	Restore(snapshot []byte) error
}

// MapStateMachine is an in-memory key-value store. It is safe for concurrent
// use, though Raft applies commands one at a time from a single goroutine.
type MapStateMachine struct {
	mu   sync.Mutex
	data map[string]string
}

// NewMapStateMachine returns an empty key-value state machine.
func NewMapStateMachine() *MapStateMachine {
	return &MapStateMachine{data: make(map[string]string)}
}

// Apply executes cmd against the store and returns its Result.
func (m *MapStateMachine) Apply(cmd Command) Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch cmd.Kind {
	case OpGet:
		v, ok := m.data[cmd.Key]
		return Result{Value: v, Ok: ok}
	case OpPut:
		m.data[cmd.Key] = cmd.Value
		return Result{Ok: true}
	case OpDelete:
		_, ok := m.data[cmd.Key]
		delete(m.data, cmd.Key)
		return Result{Ok: ok}
	case OpCAS:
		cur, ok := m.data[cmd.Key]
		if ok && cur == cmd.Expected {
			m.data[cmd.Key] = cmd.Value
			return Result{Ok: true}
		}
		return Result{Value: cur, Ok: false}
	default:
		return Result{Err: "unknown op kind"}
	}
}

// Snapshot serializes the map with gob.
func (m *MapStateMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore replaces the map with the snapshot's contents. An empty snapshot
// resets the store to empty.
func (m *MapStateMachine) Restore(snapshot []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(snapshot) == 0 {
		m.data = make(map[string]string)
		return nil
	}
	var data map[string]string
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&data); err != nil {
		return err
	}
	m.data = data
	return nil
}

// Compile-time assertion that MapStateMachine satisfies StateMachine.
var _ StateMachine = (*MapStateMachine)(nil)
