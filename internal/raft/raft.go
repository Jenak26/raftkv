// Package raft is the from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout). It is the heart of the project.
//
// Phase 0 established the shared vocabulary (Role, ApplyMsg). Phase 2 adds
// leader election: terms as a logical clock, randomized election timeouts for
// liveness, the RequestVote RPC and its voting rules, heartbeats (empty
// AppendEntries), and step-down on observing a higher term. Log replication,
// persistence of the log, snapshotting, and membership changes arrive in later
// phases (see plan.md).
//
// Concurrency discipline: a single mutex guards all mutable node state. The one
// rule that prevents the classic Raft deadlock is that we NEVER hold the mutex
// while making an outbound RPC — every Send* call happens after the lock is
// released, and replies are processed by re-acquiring it.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/storage"
)

// RPCTransport is the outbound half of node-to-node communication, as Raft needs
// it. It is declared here (rather than imported from the transport package) to
// avoid an import cycle: the transport package imports raft for the RPC argument
// types, so raft cannot import transport. Any concrete transport — the simulated
// memnet or a production gRPC client — structurally satisfies this interface.
type RPCTransport interface {
	SendRequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Role is the role a Raft node currently occupies. At any time a node is
// exactly one of Follower, Candidate, or Leader.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyMsg is what a Raft node sends up to the application. Each committed log
// entry produces one CommandValid message in log order; installing a snapshot
// produces one SnapshotValid message. Exactly one of the two groups of fields
// is meaningful per message.
type ApplyMsg struct {
	// Committed command.
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	// Snapshot to install.
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Default timing. Heartbeats must be well below the election timeout so a live
// leader reliably suppresses elections. Values are deliberately small so that,
// under a pumped simulated clock, tests run in milliseconds.
const (
	defaultHeartbeatInterval  = 50 * time.Millisecond
	defaultElectionTimeoutMin = 150 * time.Millisecond
	defaultElectionTimeoutMax = 300 * time.Millisecond
)

// Config is the dependency-injected construction parameters for a Raft node.
// Using a struct keeps Make's signature stable as later phases add fields.
type Config struct {
	ID        int
	Peers     []int // all node ids in the cluster, including this node
	Transport RPCTransport
	Persister storage.Persister
	Clock     clock.Clock
	ApplyCh   chan ApplyMsg
	Rand      *rand.Rand // source of randomized election timeouts (seeded for replay)

	// Optional timing overrides; zero values fall back to the defaults above.
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// Raft is a single consensus peer.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int
	tr        RPCTransport
	persister storage.Persister
	clk       clock.Clock
	applyCh   chan ApplyMsg

	rngMu sync.Mutex
	rng   *rand.Rand

	hbInterval time.Duration
	etMin      time.Duration
	etMax      time.Duration

	// Persistent state (survives restarts). The log arrives in Phase 3.
	currentTerm int
	votedFor    int // -1 means "none"

	// Volatile state.
	role      Role
	lastHeard time.Time // last time we heard from a leader or granted a vote

	dead   bool
	stopCh chan struct{}
}

// Make creates, persists-loads, and starts a Raft peer. It launches the ticker
// goroutine before returning; the node begins as a follower.
func Make(cfg Config) *Raft {
	rf := &Raft{
		id:         cfg.ID,
		peers:      append([]int(nil), cfg.Peers...),
		tr:         cfg.Transport,
		persister:  cfg.Persister,
		clk:        cfg.Clock,
		applyCh:    cfg.ApplyCh,
		rng:        cfg.Rand,
		hbInterval: orDur(cfg.HeartbeatInterval, defaultHeartbeatInterval),
		etMin:      orDur(cfg.ElectionTimeoutMin, defaultElectionTimeoutMin),
		etMax:      orDur(cfg.ElectionTimeoutMax, defaultElectionTimeoutMax),
		votedFor:   -1,
		role:       Follower,
		stopCh:     make(chan struct{}),
	}
	if rf.rng == nil {
		rf.rng = rand.New(rand.NewSource(int64(cfg.ID) + 1))
	}
	rf.readPersist(rf.persister.ReadRaftState())
	rf.lastHeard = rf.clk.Now()

	go rf.ticker()
	return rf
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

// State reports the node's current term and whether it believes it is the
// leader. (The classic Raft GetState.)
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Kill stops the node's background goroutines. Used by the test harness to
// simulate a crash; the Persister is left intact so a restart reloads it.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.dead {
		return
	}
	rf.dead = true
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.dead
}

// --- persistence (Raft Figure 2: currentTerm and votedFor must be durable) ---

type persistentState struct {
	CurrentTerm int
	VotedFor    int
}

// persist writes the durable state. Must be called with rf.mu held.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{rf.currentTerm, rf.votedFor}); err != nil {
		panic("raft: encode persistent state: " + err.Error())
	}
	rf.persister.SaveRaftState(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil && err != io.EOF {
		panic("raft: decode persistent state: " + err.Error())
	}
	rf.currentTerm = ps.CurrentTerm
	rf.votedFor = ps.VotedFor
}

// --- election timing ---

func (rf *Raft) randTimeout() time.Duration {
	rf.rngMu.Lock()
	defer rf.rngMu.Unlock()
	span := int64(rf.etMax - rf.etMin)
	return rf.etMin + time.Duration(rf.rng.Int63n(span+1))
}

// resetHeard marks "now" as the last time we heard from authority. Must hold mu.
func (rf *Raft) resetHeard() { rf.lastHeard = rf.clk.Now() }

// ticker is the single goroutine that drives elections. Each cycle it waits a
// fresh randomized timeout, then — if it is not the leader and has heard nothing
// for that long — starts an election. Randomizing the wait each cycle is what
// breaks symmetry and prevents perpetual split votes (Raft's liveness answer to
// FLP).
func (rf *Raft) ticker() {
	for {
		timeout := rf.randTimeout()
		select {
		case <-rf.stopCh:
			return
		case <-rf.clk.After(timeout):
		}

		rf.mu.Lock()
		if rf.dead {
			rf.mu.Unlock()
			return
		}
		startElection := rf.role != Leader && rf.clk.Now().Sub(rf.lastHeard) >= timeout
		rf.mu.Unlock()

		if startElection {
			rf.startElection()
		}
	}
}

// startElection transitions to candidate for a new term and solicits votes. It
// is called WITHOUT the lock held; it briefly takes the lock to mutate state and
// snapshot the request, then releases it before sending any RPC.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.dead || rf.role == Leader {
		rf.mu.Unlock()
		return
	}
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.persist()
	rf.resetHeard()

	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.id,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	peers := rf.peers
	majority := len(peers)/2 + 1
	rf.mu.Unlock()

	votes := 1 // vote for self
	for _, peer := range peers {
		if peer == rf.id {
			continue
		}
		go func(peer int) {
			reply, err := rf.tr.SendRequestVote(context.Background(), peer, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}
			// Ignore replies that are stale: we've moved on to another term or
			// are no longer a candidate. A vote only counts for the term it was
			// requested in.
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= majority {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader promotes a winning candidate and starts heartbeating. Must hold
// mu. It is idempotent across concurrent vote replies because it only acts while
// the node is still a Candidate.
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	go rf.heartbeatLoop(rf.currentTerm)
}

// stepDown reverts to follower, adopting term if it is newer. Must hold mu.
func (rf *Raft) stepDown(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = Follower
}

// heartbeatLoop runs while this node is leader for term, sending empty
// AppendEntries to suppress other elections. It sends a round immediately, then
// every heartbeat interval.
func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.dead || rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.id,
			PrevLogIndex: rf.lastLogIndex(),
			PrevLogTerm:  rf.lastLogTerm(),
			LeaderCommit: 0,
		}
		peers := rf.peers
		rf.mu.Unlock()

		for _, peer := range peers {
			if peer == rf.id {
				continue
			}
			go func(peer int) {
				reply, err := rf.tr.SendAppendEntries(context.Background(), peer, args)
				if err != nil {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
				}
			}(peer)
		}

		select {
		case <-rf.stopCh:
			return
		case <-rf.clk.After(rf.hbInterval):
		}
	}
}

// --- RPC handlers (the receiving half of transport.Server) ---

// HandleRequestVote implements the RequestVote receiver rules (Raft Figure 2).
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply := &RequestVoteReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}

	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetHeard() // granting a vote counts as hearing from authority
		reply.VoteGranted = true
	}
	return reply
}

// HandleAppendEntries implements the AppendEntries receiver rules for the
// heartbeat case (log replication is added in Phase 3).
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply := &AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}

	// A valid leader for our term exists: become/stay follower and reset timer.
	rf.role = Follower
	rf.resetHeard()
	reply.Success = true
	return reply
}

// HandleInstallSnapshot is a Phase 6 concern; it is a no-op stub for now beyond
// the mandatory term exchange so the transport.Server contract is satisfied.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	return &InstallSnapshotReply{Term: rf.currentTerm}
}

// --- log accessors (empty until Phase 3) ---

func (rf *Raft) lastLogIndex() int { return 0 }
func (rf *Raft) lastLogTerm() int  { return 0 }
