// Package linearizability records histories of client operations and checks them
// for linearizability — the gold-standard correctness test for a distributed
// store. A history is a set of operations, each with the real time it was invoked
// and the real time it returned; the checker searches for a single sequential
// ordering of those operations that (a) respects real-time order (if op A returned
// before op B was invoked, A must come before B) and (b) is legal for the data
// model. If one exists, the history is linearizable.
//
// This is a from-scratch implementation of the Wing & Gong / Lowe ("WGL")
// algorithm (the same idea as the Porcupine library), written for the learning
// value. See explain/10-failure-testing.md.
package linearizability

import (
	"sync"
	"sync/atomic"
)

// OpKind is the kind of key-value operation recorded.
type OpKind int

const (
	OpPut OpKind = iota
	OpGet
	OpCAS
	OpDelete
)

// Operation is one completed client operation with its real-time bracket. Call and
// Return are monotonic logical timestamps from the Recorder; Call < Return always,
// and operations that did not overlap in real time have disjoint [Call,Return]
// ranges that the checker uses as ordering constraints.
type Operation struct {
	ClientID int
	Kind     OpKind
	Key      string
	Value    string // Put/CAS: the new value
	Expected string // CAS: the expected current value

	// Observed result.
	GotValue string // Get: the value read ("" if absent)
	GotOk    bool   // Get: key existed; CAS: swapped; Delete: existed; Put: true

	Call   int64
	Return int64
}

// Recorder collects operations from concurrent clients with a shared monotonic
// clock, so every invoke and return gets a distinct, ordered timestamp.
type Recorder struct {
	clock int64 // atomic monotonic counter

	mu  sync.Mutex
	ops []Operation
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

func (r *Recorder) tick() int64 { return atomic.AddInt64(&r.clock, 1) }

// Handle represents an in-flight operation between its invocation and completion.
type Handle struct {
	r  *Recorder
	op Operation
}

// Begin records the invocation of an operation and returns a handle to complete.
func (r *Recorder) Begin(clientID int, kind OpKind, key, value, expected string) *Handle {
	h := &Handle{r: r, op: Operation{
		ClientID: clientID, Kind: kind, Key: key, Value: value, Expected: expected,
	}}
	h.op.Call = r.tick()
	return h
}

// End records the operation's result and return time, appending it to the history.
func (h *Handle) End(gotValue string, gotOk bool) {
	h.op.GotValue = gotValue
	h.op.GotOk = gotOk
	h.op.Return = h.r.tick()
	h.r.mu.Lock()
	h.r.ops = append(h.r.ops, h.op)
	h.r.mu.Unlock()
}

// History returns a copy of all completed operations recorded so far.
func (r *Recorder) History() []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Operation, len(r.ops))
	copy(out, r.ops)
	return out
}
