package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/raft"
)

// Errors returned by Server.Submit. All of them mean "this server could not
// complete your request — retry (elsewhere)"; the Clerk handles them transparently.
var (
	// ErrNotLeader means this server is not the current leader, so it cannot
	// propose. The client should try another server.
	ErrNotLeader = errors.New("kv: not leader")
	// ErrLostLeader means the server proposed the command but a different command
	// committed at the assigned index — leadership was lost mid-flight. Retrying is
	// safe because the dedup table makes a re-submit exactly-once.
	ErrLostLeader = errors.New("kv: lost leadership before commit")
	// ErrTimeout means the command did not commit within the wait deadline.
	ErrTimeout = errors.New("kv: commit timed out")
)

// Raft is the slice of the consensus core the KV server depends on: propose a
// command, compact the log via a snapshot, and report the persisted state size so
// the server knows when to snapshot. *raft.Raft satisfies it. Declaring an
// interface (rather than taking *raft.Raft) keeps the server unit-testable.
type Raft interface {
	Propose(command []byte) (index, term int, isLeader bool)
	Snapshot(index int, snapshot []byte)
	RaftStateSize() int
	// ReadIndex returns a commit index for a linearizable read, after confirming
	// leadership; ok is false if this node cannot serve a linearizable read now.
	ReadIndex(ctx context.Context) (index int, ok bool)
}

// session is one client's dedup memory: the highest SeqNum applied for that client
// and the Result that command produced. Fields are exported so the session table
// can be gob-encoded into a snapshot (it is part of the replicated state).
type session struct {
	Seq    int64
	Result Result
}

// opResult is what the apply loop hands back to a waiting Submit: the identity of
// the command that actually committed at the waited-on index, plus its result.
type opResult struct {
	clientID int64
	seqNum   int64
	result   Result
}

// Server is the application layer for one node: it owns the state machine, drains
// committed commands from Raft's applyCh in order, deduplicates retries, and wakes
// the client call that proposed each command.
//
// Concurrency: mu guards sessions, waiters, and lastApplied. The state machine has
// its own lock; it is only ever touched from the single apply goroutine, so its
// state stays a deterministic function of the committed log.
type Server struct {
	rf           Raft
	applyCh      <-chan raft.ApplyMsg
	sm           StateMachine
	maxRaftState int // snapshot when persisted Raft state grows past this (0 = never)

	mu          sync.Mutex
	sessions    map[int64]session     // clientID -> dedup memory
	waiters     map[int]chan opResult // log index -> the Submit blocked on it
	lastApplied int

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewServer wires a state machine to a Raft node's apply stream and starts the
// apply loop. applyCh must be the same channel passed to raft.Make for this node.
// If maxRaftState > 0, the server snapshots (compacts the log) whenever the
// persisted Raft state grows past that many bytes; 0 disables snapshotting.
func NewServer(rf Raft, applyCh <-chan raft.ApplyMsg, sm StateMachine, maxRaftState int) *Server {
	s := &Server{
		rf:           rf,
		applyCh:      applyCh,
		sm:           sm,
		maxRaftState: maxRaftState,
		sessions:     make(map[int64]session),
		waiters:      make(map[int]chan opResult),
		stopCh:       make(chan struct{}),
	}
	go s.applyLoop()
	return s
}

// Stop halts the apply loop. The underlying Raft node is stopped separately. It is
// idempotent: calling it more than once is safe.
func (s *Server) Stop() { s.stopOnce.Do(func() { close(s.stopCh) }) }

// Submit proposes cmd, waits for it to commit and apply, and returns its Result.
// It returns ErrNotLeader if this node is not the leader, ErrLostLeader if a
// different command took the assigned log index, or ErrTimeout if ctx expires
// before the command applies. All three are safe to retry: dedup makes a re-submit
// of the same (ClientID, SeqNum) exactly-once.
func (s *Server) Submit(ctx context.Context, cmd Command) (Result, error) {
	// Reads do not go through the log. A stale read is served straight from the
	// local state machine on any node; a linearizable read uses ReadIndex.
	if cmd.Kind == OpGet {
		if cmd.ReadStale {
			return s.sm.Apply(cmd), nil
		}
		return s.linearizableRead(ctx, cmd)
	}

	index, _, isLeader := s.rf.Propose(encodeCommand(cmd))
	if !isLeader {
		return Result{}, ErrNotLeader
	}

	ch := make(chan opResult, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case got := <-ch:
		if got.clientID == cmd.ClientID && got.seqNum == cmd.SeqNum {
			return got.result, nil
		}
		// A different command committed at our index: we lost leadership mid-flight.
		return Result{}, ErrLostLeader
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return Result{}, ErrTimeout
	case <-s.stopCh:
		return Result{}, ErrTimeout
	}
}

// applyLoop drains committed entries in log order, the single goroutine that
// mutates the state machine and the session table.
func (s *Server) applyLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case m := <-s.applyCh:
			switch {
			case m.CommandValid:
				s.applyCommand(m)
				s.maybeSnapshot(m.CommandIndex)
			case m.SnapshotValid:
				s.installSnapshot(m)
			}
		}
	}
}

// applyCommand decodes one committed entry, applies it (or replays a deduped
// result), records the session, and notifies any waiter on this index.
func (s *Server) applyCommand(m raft.ApplyMsg) {
	if len(m.Command) == 0 {
		// An internal Raft entry — an election no-op or a configuration change — not
		// a client command. Advance our applied index (so linearizable reads waiting
		// on it make progress) but do not decode or dedup it.
		s.mu.Lock()
		if m.CommandIndex > s.lastApplied {
			s.lastApplied = m.CommandIndex
		}
		s.mu.Unlock()
		return
	}
	cmd, err := decodeCommand(m.Command)
	if err != nil {
		// A log entry we cannot decode is a programming error, not a runtime
		// condition: every entry was encoded by encodeCommand.
		panic("kv: decode committed command: " + err.Error())
	}

	s.mu.Lock()
	var res Result
	if isMutating(cmd.Kind) && cmd.SeqNum != 0 && cmd.SeqNum <= s.sessions[cmd.ClientID].Seq {
		// Duplicate of an already-applied command: replay the stored result without
		// touching the state machine.
		res = s.sessions[cmd.ClientID].Result
	} else {
		res = s.sm.Apply(cmd)
		if isMutating(cmd.Kind) {
			s.sessions[cmd.ClientID] = session{Seq: cmd.SeqNum, Result: res}
		}
	}
	if m.CommandIndex > s.lastApplied {
		s.lastApplied = m.CommandIndex
	}
	w := s.waiters[m.CommandIndex]
	delete(s.waiters, m.CommandIndex)
	s.mu.Unlock()

	if w != nil {
		w <- opResult{clientID: cmd.ClientID, seqNum: cmd.SeqNum, result: res}
	}
}

// linearizableRead serves a read without writing to the log: it obtains a
// ReadIndex from Raft (which confirms leadership), waits until the state machine
// has applied through that index, then reads. This guarantees the read reflects
// every write that committed before the read began — linearizability — at the cost
// of one heartbeat round, not a log append.
func (s *Server) linearizableRead(ctx context.Context, cmd Command) (Result, error) {
	readIndex, ok := s.rf.ReadIndex(ctx)
	if !ok {
		return Result{}, ErrNotLeader
	}
	if err := s.waitApplied(ctx, readIndex); err != nil {
		return Result{}, err
	}
	return s.sm.Apply(cmd), nil
}

// waitApplied blocks until the state machine has applied through index, or ctx
// expires.
func (s *Server) waitApplied(ctx context.Context, index int) error {
	for {
		s.mu.Lock()
		applied := s.lastApplied
		s.mu.Unlock()
		if applied >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrTimeout
		case <-s.stopCh:
			return ErrTimeout
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// kvSnapshot is the application state captured in a snapshot: the state machine
// bytes plus the session/dedup table. Both are replicated state, so both must be
// captured — omitting the sessions would make already-applied client requests look
// new after a snapshot, breaking exactly-once.
type kvSnapshot struct {
	SM       []byte
	Sessions map[int64]session
}

// maybeSnapshot compacts the log when the persisted Raft state has grown past the
// configured threshold, capturing application state as of appliedIndex.
func (s *Server) maybeSnapshot(appliedIndex int) {
	if s.maxRaftState <= 0 || s.rf.RaftStateSize() < s.maxRaftState {
		return
	}
	s.mu.Lock()
	smBytes, err := s.sm.Snapshot()
	if err != nil {
		s.mu.Unlock()
		panic("kv: snapshot state machine: " + err.Error())
	}
	sessions := make(map[int64]session, len(s.sessions))
	for id, sess := range s.sessions {
		sessions[id] = sess
	}
	s.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kvSnapshot{SM: smBytes, Sessions: sessions}); err != nil {
		panic("kv: encode snapshot: " + err.Error())
	}
	s.rf.Snapshot(appliedIndex, buf.Bytes())
}

// installSnapshot restores the state machine and session table from a snapshot
// delivered by Raft (after an InstallSnapshot, or on restart).
func (s *Server) installSnapshot(m raft.ApplyMsg) {
	var snap kvSnapshot
	if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&snap); err != nil {
		panic("kv: decode snapshot: " + err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sm.Restore(snap.SM); err != nil {
		panic("kv: restore state machine: " + err.Error())
	}
	if snap.Sessions != nil {
		s.sessions = snap.Sessions
	} else {
		s.sessions = make(map[int64]session)
	}
	if m.SnapshotIndex > s.lastApplied {
		s.lastApplied = m.SnapshotIndex
	}
}

// isMutating reports whether an op changes state (and therefore needs dedup).
// Reads are idempotent and skip the session table.
func isMutating(k OpKind) bool { return k == OpPut || k == OpDelete || k == OpCAS }

// --- command (de)serialization for the opaque Raft log ---

func encodeCommand(cmd Command) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		panic("kv: encode command: " + err.Error())
	}
	return buf.Bytes()
}

func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd, err
}
