// Package raft is the from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout). It is the heart of the project.
//
// Phase 0 established the shared vocabulary (Role, ApplyMsg). Phase 2 added
// leader election: terms as a logical clock, randomized election timeouts for
// liveness, the RequestVote RPC and its voting rules, heartbeats, and step-down
// on observing a higher term. Phase 3 adds log replication: Propose, the
// AppendEntries consistency check with conflict-term fast backtracking, commit
// advancement under the Figure 8 current-term rule, and the applier that
// delivers committed entries in order. Snapshotting and membership changes
// arrive in later phases (see plan.md).
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

	// Persistent state (survives restarts). log[0] is a sentinel {Term:0,Index:0}
	// so that the first real entry is at index 1 and prevLogIndex==0 needs no
	// special case. In Phase 3 the slice index equals the entry's log index;
	// snapshots add an offset in Phase 6.
	currentTerm int
	votedFor    int // -1 means "none"
	log         []LogEntry

	// Volatile state on all servers.
	role        Role
	lastHeard   time.Time // last time we heard from a leader or granted a vote
	commitIndex int       // highest log index known to be committed
	lastApplied int       // highest log index applied to the state machine

	// Volatile state on leaders, reinitialized after each election.
	nextIndex  map[int]int // per peer: next log index to send
	matchIndex map[int]int // per peer: highest index known replicated

	applyCond *sync.Cond // signaled when commitIndex advances or on shutdown

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
		log:        []LogEntry{{Term: 0, Index: 0}}, // sentinel
		role:       Follower,
		stopCh:     make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	if rf.rng == nil {
		rf.rng = rand.New(rand.NewSource(int64(cfg.ID) + 1))
	}
	rf.readPersist(rf.persister.ReadRaftState())
	rf.lastHeard = rf.clk.Now()

	go rf.ticker()
	go rf.applier()
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
	rf.applyCond.Broadcast() // wake the applier so it can exit
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
	Log         []LogEntry
}

// persist writes the durable state. Must be called with rf.mu held.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{rf.currentTerm, rf.votedFor, rf.log}); err != nil {
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
	if len(ps.Log) > 0 {
		rf.log = ps.Log
	}
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

// becomeLeader promotes a winning candidate, initializes per-peer replication
// state, and starts the leader loop. Must hold mu. It is idempotent across
// concurrent vote replies because it only acts while still a Candidate.
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	rf.nextIndex = make(map[int]int, len(rf.peers))
	rf.matchIndex = make(map[int]int, len(rf.peers))
	for _, p := range rf.peers {
		rf.nextIndex[p] = rf.lastLogIndex() + 1
		rf.matchIndex[p] = 0
	}
	go rf.leaderLoop(rf.currentTerm)
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

// leaderLoop runs while this node is leader for term. Each heartbeat interval it
// replicates to every peer (an empty AppendEntries acts as a heartbeat),
// suppressing other elections and driving log convergence.
func (rf *Raft) leaderLoop(term int) {
	for {
		rf.mu.Lock()
		stillLeader := !rf.dead && rf.role == Leader && rf.currentTerm == term
		rf.mu.Unlock()
		if !stillLeader {
			return
		}

		rf.broadcastAppend(term)

		select {
		case <-rf.stopCh:
			return
		case <-rf.clk.After(rf.hbInterval):
		}
	}
}

// broadcastAppend fires one replication round at every peer.
func (rf *Raft) broadcastAppend(term int) {
	rf.mu.Lock()
	peers := rf.peers
	rf.mu.Unlock()
	for _, peer := range peers {
		if peer == rf.id {
			continue
		}
		go rf.replicateOne(peer, term)
	}
}

// replicateOne sends one AppendEntries to peer carrying the entries it is missing
// (from nextIndex onward), then processes the reply: on success it advances
// matchIndex/nextIndex and tries to commit; on a consistency-check failure it
// backtracks nextIndex using the conflict hint.
func (rf *Raft) replicateOne(peer, term int) {
	rf.mu.Lock()
	if rf.dead || rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	prevIndex := rf.nextIndex[peer] - 1
	if prevIndex < 0 {
		prevIndex = 0
	}
	prevTerm := rf.log[prevIndex].Term
	entries := append([]LogEntry(nil), rf.log[prevIndex+1:]...)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, err := rf.tr.SendAppendEntries(context.Background(), peer, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != term {
		return // stale reply
	}

	if reply.Success {
		match := prevIndex + len(entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.maybeAdvanceCommit()
		return
	}

	// Consistency check failed: back nextIndex up using the conflict hint.
	rf.nextIndex[peer] = rf.backtrack(reply, prevIndex)
}

// backtrack computes the new nextIndex for a peer after a failed AppendEntries,
// skipping a whole conflicting term at once. Must hold mu.
func (rf *Raft) backtrack(reply *AppendEntriesReply, prevIndex int) int {
	if reply.ConflictTerm <= 0 {
		// Follower's log is shorter than prevIndex: jump to its first free slot.
		next := reply.ConflictIndex
		if next < 1 {
			next = 1
		}
		return next
	}
	// If the leader has the conflicting term, resume just after its last entry
	// in that term; otherwise fall back to the follower's first index of it.
	for i := rf.lastLogIndex(); i >= 1; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			return i + 1
		}
	}
	if reply.ConflictIndex < 1 {
		return 1
	}
	return reply.ConflictIndex
}

// maybeAdvanceCommit advances commitIndex to the highest index replicated on a
// majority — but only for an entry from the current term (the Figure 8 rule:
// committing a prior-term entry by count alone can lose it). Must hold mu.
func (rf *Raft) maybeAdvanceCommit() {
	majority := len(rf.peers)/2 + 1
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range rf.peers {
			if p != rf.id && rf.matchIndex[p] >= n {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// Propose appends a command to the leader's log and triggers replication. It
// returns the index the command will occupy if committed, the current term, and
// whether this node is the leader (false means the caller must retry elsewhere).
func (rf *Raft) Propose(command []byte) (index, term int, isLeader bool) {
	rf.mu.Lock()
	if rf.dead || rf.role != Leader {
		t := rf.currentTerm
		rf.mu.Unlock()
		return -1, t, false
	}
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: append([]byte(nil), command...)})
	rf.matchIndex[rf.id] = index
	rf.persist()
	rf.mu.Unlock()

	go rf.broadcastAppend(term) // replicate promptly rather than waiting a tick
	return index, term, true
}

// applier delivers committed-but-not-yet-applied entries to the application via
// applyCh, strictly in log order. Sends happen outside the lock because applyCh
// may block.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.dead && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.dead {
			rf.mu.Unlock()
			return
		}
		base := rf.lastApplied + 1
		entries := append([]LogEntry(nil), rf.log[base:rf.commitIndex+1]...)
		rf.mu.Unlock()

		for _, e := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			}
		}

		rf.mu.Lock()
		if n := entries[len(entries)-1].Index; n > rf.lastApplied {
			rf.lastApplied = n
		}
		rf.mu.Unlock()
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

// HandleAppendEntries implements the AppendEntries receiver rules (Raft
// Figure 2): term check, log-matching consistency check with a conflict hint for
// fast backtracking, conflict-only truncation, append, and commit advancement.
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

	// Consistency check: our log must contain prevLogIndex with prevLogTerm.
	if args.PrevLogIndex > rf.lastLogIndex() {
		// Too short: tell the leader where our log ends.
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return reply
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Term mismatch: report the offending term and its first index so the
		// leader can skip the whole term in one round.
		ct := rf.log[args.PrevLogIndex].Term
		reply.ConflictTerm = ct
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == ct {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Merge entries, truncating only on an actual conflict (never delete
	// matching entries — that is the classic over-eager-truncation bug).
	changed := false
	for j, e := range args.Entries {
		pos := args.PrevLogIndex + 1 + j
		if pos <= rf.lastLogIndex() {
			if rf.log[pos].Term == e.Term {
				continue // already matches
			}
			rf.log = rf.log[:pos] // conflict: drop this entry and everything after
		}
		rf.log = append(rf.log, args.Entries[j:]...)
		changed = true
		break
	}
	if changed {
		rf.persist()
	}

	// Advance our commit index, but never past what we actually hold.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Broadcast()
	}

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

// --- log accessors (Phase 3: slice index == log index) ---

func (rf *Raft) lastLogIndex() int { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

// LogState reports the last log index/term and the commit index. It exists for
// tests and observability; it takes the lock and is safe to call concurrently.
func (rf *Raft) LogState() (lastIndex, lastTerm, commit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex
}
