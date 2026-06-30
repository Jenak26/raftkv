// Package raft is the from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout). It is the heart of the project.
//
// Phase 0 established the shared vocabulary (Role, ApplyMsg). Phase 2 added
// leader election: terms as a logical clock, randomized election timeouts for
// liveness, the RequestVote RPC and its voting rules, heartbeats, and step-down
// on observing a higher term. Phase 3 adds log replication: Propose, the
// AppendEntries consistency check with conflict-term fast backtracking, commit
// advancement under the Figure 8 current-term rule, and the applier that
// delivers committed entries in order. Phase 4 made the state durable
// (persist-before-reply). Phase 6 added snapshotting: the log carries an offset
// (log[0] is the snapshot anchor) and InstallSnapshot brings lagging followers
// current. Phase 7 added single-server membership changes: the configuration
// lives in the log and the current one (latest in the log) drives every quorum.
// Phase 8 added linearizable reads: a no-op committed on election lets the leader
// serve ReadIndex reads (a heartbeat round confirms leadership) without a log write.
//
// Concurrency discipline: a single mutex guards all mutable node state. The one
// rule that prevents the classic Raft deadlock is that we NEVER hold the mutex
// while making an outbound RPC - every Send* call happens after the lock is
// released, and replies are processed by re-acquiring it.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/storage"
)

// Errors returned by the membership-change API (Phase 7).
var (
	ErrNotLeader              = errors.New("raft: not leader")
	ErrConfigChangeInProgress = errors.New("raft: a configuration change is already in progress")
	ErrAlreadyMember          = errors.New("raft: server is already a member")
	ErrNotMember              = errors.New("raft: server is not a member")
)

// RPCTransport is the outbound half of node-to-node communication, as Raft needs
// it. It is declared here (rather than imported from the transport package) to
// avoid an import cycle: the transport package imports raft for the RPC argument
// types, so raft cannot import transport. Any concrete transport - the simulated
// memnet or a production gRPC client - structurally satisfies this interface.
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
	mu sync.Mutex
	id int
	// peers is the CURRENT cluster configuration (member ids including self). It is
	// dynamic from Phase 7 on: it is recomputed by refreshConfig from the latest
	// configuration entry in the log, falling back to bootstrapConfig when the log
	// holds none. All quorum decisions (votes, commit, replication targets) use it.
	peers           []int
	bootstrapConfig []int // initial membership, used until a config entry appears in the log
	tr              RPCTransport
	persister       storage.Persister
	clk             clock.Clock
	applyCh         chan ApplyMsg

	rngMu sync.Mutex
	rng   *rand.Rand

	hbInterval time.Duration
	etMin      time.Duration
	etMax      time.Duration

	// Persistent state (survives restarts). log[0] is the snapshot anchor: it is a
	// sentinel carrying {lastIncludedTerm, lastIncludedIndex}. On a fresh node it is
	// {0,0}, so the first real entry is index 1 and prevLogIndex==0 needs no special
	// case. After a snapshot (Phase 6) log[0].Index becomes lastIncludedIndex and
	// the log index of slice position k is k + log[0].Index. ALL index math goes
	// through the helpers (firstIndex/termAt/entryAt/sliceFrom), never raw log[i].
	currentTerm int
	votedFor    int // -1 means "none"
	log         []LogEntry
	snapshot    []byte // most recent snapshot bytes (state covered by log[0])

	// Volatile state on all servers.
	role        Role
	lastHeard   time.Time // last time we heard from a leader or granted a vote
	commitIndex int       // highest log index known to be committed
	lastApplied int       // highest log index applied to the state machine

	// Volatile state on leaders, reinitialized after each election.
	nextIndex  map[int]int // per peer: next log index to send
	matchIndex map[int]int // per peer: highest index known replicated
	noopIndex  int         // index of this term's election no-op; reads wait for it to commit

	// Observability counters (guarded by mu), surfaced via Metrics().
	electionsStarted int64
	leadershipWins   int64

	applyCond       *sync.Cond // signaled when commitIndex advances, a snapshot is pending, or on shutdown
	snapshotPending bool       // a snapshot is waiting to be delivered up the applyCh

	dead   bool
	stopCh chan struct{}
}

// Make creates, persists-loads, and starts a Raft peer. It launches the ticker
// goroutine before returning; the node begins as a follower.
func Make(cfg Config) *Raft {
	rf := &Raft{
		id:              cfg.ID,
		peers:           append([]int(nil), cfg.Peers...),
		bootstrapConfig: append([]int(nil), cfg.Peers...),
		tr:              cfg.Transport,
		persister:       cfg.Persister,
		clk:             cfg.Clock,
		applyCh:         cfg.ApplyCh,
		rng:             cfg.Rand,
		hbInterval:      orDur(cfg.HeartbeatInterval, defaultHeartbeatInterval),
		etMin:           orDur(cfg.ElectionTimeoutMin, defaultElectionTimeoutMin),
		etMax:           orDur(cfg.ElectionTimeoutMax, defaultElectionTimeoutMax),
		votedFor:        -1,
		log:             []LogEntry{{Term: 0, Index: 0}}, // sentinel
		role:            Follower,
		stopCh:          make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	if rf.rng == nil {
		rf.rng = rand.New(rand.NewSource(int64(cfg.ID) + 1))
	}
	rf.readPersist(rf.persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.refreshConfig() // adopt whatever configuration the persisted log implies
	// Everything the snapshot covers is durably committed, so on restart our commit
	// floor is the snapshot point (not 0). Queue the snapshot for delivery so the
	// application rebuilds its state machine before any later command is applied.
	if rf.firstIndex() > 0 {
		rf.commitIndex = rf.firstIndex()
		rf.lastApplied = rf.firstIndex()
		if len(rf.snapshot) > 0 {
			rf.snapshotPending = true
		}
	}
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

// persistWithSnapshot durably saves the trimmed Raft state and the snapshot
// together as a crash-atomic pair. Must be called with rf.mu held.
func (rf *Raft) persistWithSnapshot() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{rf.currentTerm, rf.votedFor, rf.log}); err != nil {
		panic("raft: encode persistent state: " + err.Error())
	}
	rf.persister.SaveStateAndSnapshot(buf.Bytes(), rf.snapshot)
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
// fresh randomized timeout, then - if it is not the leader and has heard nothing
// for that long - starts an election. Randomizing the wait each cycle is what
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
		// A node that is not a member of its own configuration (e.g. one that was
		// removed) must not campaign - otherwise it would time out forever and
		// disrupt the cluster with ever-higher terms.
		startElection := rf.role != Leader && rf.inConfig(rf.id) &&
			rf.clk.Now().Sub(rf.lastHeard) >= timeout
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
	rf.electionsStarted++
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
	majority := rf.majority()
	if majority == 1 {
		// Single-node cluster: the self-vote is already a majority and there are no
		// peers to ask, so we win the election immediately. Without this, promotion
		// would never happen - the votes>=majority check below only runs inside a
		// per-peer reply goroutine, of which there are none when we are the only node.
		rf.becomeLeader()
		rf.mu.Unlock()
		return
	}
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
	rf.leadershipWins++

	// Append a no-op entry for the new term. A fresh leader does not know which
	// prior-term entries are committed (the Figure 8 rule forbids committing them by
	// count alone); committing one current-term entry lifts its commitIndex to cover
	// the whole prior committed prefix (Leader Completeness). That is the
	// precondition for serving linearizable ReadIndex reads, and it also commits any
	// stranded prior-term entries. A no-op has nil Command and nil Config; the
	// applier delivers it but the application skips empty commands.
	noopIndex := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: noopIndex})
	rf.noopIndex = noopIndex
	rf.persist()

	rf.nextIndex = make(map[int]int, len(rf.peers))
	rf.matchIndex = make(map[int]int, len(rf.peers))
	for _, p := range rf.peers {
		rf.nextIndex[p] = rf.lastLogIndex() + 1
		rf.matchIndex[p] = 0
	}
	rf.matchIndex[rf.id] = noopIndex
	rf.maybeAdvanceCommit() // a single-node leader commits its no-op at once
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
	if _, ok := rf.nextIndex[peer]; !ok {
		// A member added to the configuration after this leader took office: start
		// it from the end of the log like becomeLeader would have.
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
		rf.matchIndex[peer] = 0
	}
	if rf.nextIndex[peer] <= rf.firstIndex() {
		// The entries this peer needs have been compacted into our snapshot; it
		// cannot be caught up with AppendEntries, so ship the snapshot instead.
		rf.mu.Unlock()
		rf.replicateSnapshot(peer, term)
		return
	}
	prevIndex := rf.nextIndex[peer] - 1
	prevTerm := rf.termAt(prevIndex)
	entries := rf.sliceFrom(prevIndex + 1)
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

// replicateSnapshot ships the current snapshot to a peer too far behind to catch up
// from the log (Raft Figure 13). On success the peer holds everything through
// lastIncludedIndex, so we advance its matchIndex/nextIndex accordingly.
func (rf *Raft) replicateSnapshot(peer, term int) {
	rf.mu.Lock()
	if rf.dead || rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          rf.id,
		LastIncludedIndex: rf.firstIndex(),
		LastIncludedTerm:  rf.lastIncludedTerm(),
		Config:            append([]int(nil), rf.log[0].Config...),
		Data:              append([]byte(nil), rf.snapshot...),
	}
	rf.mu.Unlock()

	reply, err := rf.tr.SendInstallSnapshot(context.Background(), peer, args)
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
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if rf.matchIndex[peer]+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
	rf.maybeAdvanceCommit()
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
	for i := rf.lastLogIndex(); i > rf.firstIndex(); i-- {
		if rf.termAt(i) == reply.ConflictTerm {
			return i + 1
		}
	}
	if reply.ConflictIndex < 1 {
		return 1
	}
	return reply.ConflictIndex
}

// maybeAdvanceCommit advances commitIndex to the highest index replicated on a
// majority - but only for an entry from the current term (the Figure 8 rule:
// committing a prior-term entry by count alone can lose it). Must hold mu.
func (rf *Raft) maybeAdvanceCommit() {
	majority := rf.majority()
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 0
		if rf.inConfig(rf.id) {
			count = 1 // self counts only if we are a member of the current config
		}
		for _, p := range rf.peers {
			if p != rf.id && rf.matchIndex[p] >= n {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			break
		}
	}
	// If a configuration that removes us has now committed, step down: we are no
	// longer part of the cluster we were leading.
	if rf.role == Leader && !rf.inConfig(rf.id) && rf.latestConfigIndex() <= rf.commitIndex {
		rf.role = Follower
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
	// Try to commit immediately. For a multi-node cluster the leader's lone
	// matchIndex is never a majority, so this is a no-op until peers ack; but for a
	// single-node cluster (majority = 1) there are no peers to ack, so this is the
	// only thing that ever advances commitIndex.
	rf.maybeAdvanceCommit()
	rf.mu.Unlock()

	go rf.broadcastAppend(term) // replicate promptly rather than waiting a tick
	return index, term, true
}

// AddServer and RemoveServer change the cluster membership by a single server,
// appending a configuration-change entry to the log. They must be called on the
// leader and only one change may be in flight at a time (the previous config entry
// must have committed). Single-server changes are safe without joint consensus
// because the old and new majorities always overlap (see
// explain/08-membership-changes.md).
func (rf *Raft) AddServer(id int) error    { return rf.reconfigure(id, true) }
func (rf *Raft) RemoveServer(id int) error { return rf.reconfigure(id, false) }

func (rf *Raft) reconfigure(id int, add bool) error {
	rf.mu.Lock()
	if rf.dead || rf.role != Leader {
		rf.mu.Unlock()
		return ErrNotLeader
	}
	// One change at a time: the latest configuration entry must have committed.
	if rf.latestConfigIndex() > rf.commitIndex {
		rf.mu.Unlock()
		return ErrConfigChangeInProgress
	}
	member := rf.inConfig(id)
	switch {
	case add && member:
		rf.mu.Unlock()
		return ErrAlreadyMember
	case !add && !member:
		rf.mu.Unlock()
		return ErrNotMember
	}

	var newConfig []int
	if add {
		newConfig = append(append([]int(nil), rf.peers...), id)
	} else {
		for _, p := range rf.peers {
			if p != id {
				newConfig = append(newConfig, p)
			}
		}
	}

	term := rf.currentTerm
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Config: newConfig})
	rf.refreshConfig() // adopt the new configuration immediately (latest-in-log rule)
	rf.matchIndex[rf.id] = index
	rf.persist()
	rf.maybeAdvanceCommit() // single-node clusters commit the change at once
	rf.mu.Unlock()

	go rf.broadcastAppend(term)
	return nil
}

// Config returns a copy of the current cluster configuration (member ids). For
// tests and observability.
func (rf *Raft) Config() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]int(nil), rf.peers...)
}

// ReadIndex returns a commit index such that any read reflecting the state machine
// applied through that index is linearizable - without writing to the log. ok is
// false if this node is not the leader, has not yet committed an entry in its
// current term (its election no-op), or cannot confirm it is still the leader.
//
// The protocol (Ongaro thesis §6.4): (1) the leader must have committed a
// current-term entry, so its commitIndex reflects all earlier commits; (2) record
// readIndex = commitIndex; (3) confirm leadership with a heartbeat round to a
// majority, proving no newer leader has superseded us as of now. The caller then
// waits until the state machine has applied through readIndex and reads. Step (3)
// is what stops a deposed-but-unaware leader from serving stale data.
func (rf *Raft) ReadIndex(ctx context.Context) (index int, ok bool) {
	rf.mu.Lock()
	if rf.role != Leader || rf.commitIndex < rf.noopIndex {
		// Not leader, or our term's no-op has not committed yet - the caller retries.
		rf.mu.Unlock()
		return 0, false
	}
	term := rf.currentTerm
	readIndex := rf.commitIndex
	rf.mu.Unlock()

	if !rf.confirmLeadership(ctx, term) {
		return 0, false
	}
	return readIndex, true
}

// confirmLeadership returns true once a majority of the current configuration has,
// in this heartbeat round, confirmed it knows no higher term than term - i.e. our
// leadership is still current. It returns false on ctx expiry or if we are no
// longer leader for term.
func (rf *Raft) confirmLeadership(ctx context.Context, term int) bool {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return false
	}
	peers := append([]int(nil), rf.peers...)
	majority := rf.majority()
	rf.mu.Unlock()

	if majority <= 1 {
		return true // single-node cluster: trivially still the leader
	}

	acks := make(chan bool, len(peers))
	for _, peer := range peers {
		if peer == rf.id {
			continue
		}
		go func(peer int) { acks <- rf.sendConfirm(peer, term) }(peer)
	}

	votes := 1 // self
	pending := len(peers) - 1
	for votes < majority && pending > 0 {
		select {
		case <-ctx.Done():
			return false
		case ok := <-acks:
			pending--
			if ok {
				votes++
			}
		}
	}
	return votes >= majority
}

// sendConfirm sends one heartbeat to peer and reports whether it confirms our
// leadership for term (it replied without a higher term). A higher term steps us
// down.
func (rf *Raft) sendConfirm(peer, term int) bool {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return false
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.id,
		PrevLogIndex: rf.lastLogIndex(),
		PrevLogTerm:  rf.lastLogTerm(),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, err := rf.tr.SendAppendEntries(context.Background(), peer, args)
	if err != nil {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return false
	}
	return rf.role == Leader && rf.currentTerm == term
}

// applier delivers committed-but-not-yet-applied entries to the application via
// applyCh, strictly in log order. Sends happen outside the lock because applyCh
// may block.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.dead && !rf.snapshotPending && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.dead {
			rf.mu.Unlock()
			return
		}

		// A pending snapshot is delivered first and in place of commands: it
		// supersedes everything through its index. lastApplied was already advanced
		// to the snapshot point when it was installed (or at Make on restart).
		if rf.snapshotPending {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      append([]byte(nil), rf.snapshot...),
				SnapshotIndex: rf.firstIndex(),
				SnapshotTerm:  rf.lastIncludedTerm(),
			}
			rf.snapshotPending = false
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		base := rf.lastApplied + 1
		entries := rf.sliceFrom(base)[:rf.commitIndex-base+1]
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

	// If prevLogIndex sits below our snapshot anchor, the leader is referring to
	// entries our snapshot already subsumes. Re-anchor the request at firstIndex():
	// drop the entries the snapshot covers, and treat the anchor as prev.
	if args.PrevLogIndex < rf.firstIndex() {
		skip := rf.firstIndex() - args.PrevLogIndex
		if skip <= len(args.Entries) {
			args.Entries = args.Entries[skip:]
		} else {
			args.Entries = nil
		}
		args.PrevLogIndex = rf.firstIndex()
		args.PrevLogTerm = rf.lastIncludedTerm()
	}

	// Consistency check: our log must contain prevLogIndex with prevLogTerm.
	if args.PrevLogIndex > rf.lastLogIndex() {
		// Too short: tell the leader where our log ends.
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return reply
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		// Term mismatch: report the offending term and its first index so the
		// leader can skip the whole term in one round.
		ct := rf.termAt(args.PrevLogIndex)
		reply.ConflictTerm = ct
		i := args.PrevLogIndex
		for i > rf.firstIndex()+1 && rf.termAt(i-1) == ct {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Merge entries, truncating only on an actual conflict (never delete
	// matching entries - that is the classic over-eager-truncation bug).
	changed := false
	for j, e := range args.Entries {
		pos := args.PrevLogIndex + 1 + j
		if pos <= rf.lastLogIndex() {
			if rf.termAt(pos) == e.Term {
				continue // already matches
			}
			rf.log = rf.log[:pos-rf.firstIndex()] // conflict: drop this entry and everything after
		}
		rf.log = append(rf.log, args.Entries[j:]...)
		changed = true
		break
	}
	if changed {
		// The log changed (entries appended and/or a conflicting tail truncated):
		// a configuration entry may have arrived or been removed, so re-adopt the
		// latest configuration the log now implies.
		rf.refreshConfig()
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

// HandleInstallSnapshot implements the InstallSnapshot receiver rules (Raft
// Figure 13): adopt the leader's snapshot when it advances our committed state,
// trimming the log and handing the snapshot up to the application via the applier.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply := &InstallSnapshotReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}

	// Valid leader for our term: stay follower and reset the election timer.
	rf.role = Follower
	rf.resetHeard()

	// Ignore a snapshot that does not advance our committed state (stale or one we
	// already have); installing it would rewind progress.
	if args.LastIncludedIndex <= rf.commitIndex {
		return reply
	}

	// Install. If we still hold the entry at LastIncludedIndex with a matching term,
	// keep the suffix after it (we may have entries the snapshot does not cover);
	// otherwise the whole log is superseded and we keep only the anchor.
	anchor := LogEntry{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex, Config: append([]int(nil), args.Config...)}
	if args.LastIncludedIndex < rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.log = append([]LogEntry{anchor}, rf.sliceFrom(args.LastIncludedIndex+1)...)
	} else {
		rf.log = []LogEntry{anchor}
	}
	rf.snapshot = append([]byte(nil), args.Data...)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.snapshotPending = true

	rf.refreshConfig() // the snapshot may carry a configuration we did not have
	rf.persistWithSnapshot()
	rf.applyCond.Broadcast() // wake the applier to deliver the snapshot upward
	return reply
}

// Snapshot is called by the application once it has durably captured all state
// through log index index. Raft discards the log prefix at or before index,
// compacting the log. It is a no-op if index is already covered or not yet
// committed.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.firstIndex() || index > rf.commitIndex {
		return
	}
	// Record the configuration in effect as of the snapshot point on the anchor, so
	// that after compaction refreshConfig still finds it (config entries below the
	// snapshot are otherwise discarded).
	anchor := LogEntry{Term: rf.termAt(index), Index: index, Config: append([]int(nil), rf.peers...)}
	rf.log = append([]LogEntry{anchor}, rf.sliceFrom(index+1)...)
	rf.snapshot = append([]byte(nil), snapshot...)
	// The application only snapshots an index it has already applied; raise the
	// applied/commit floor to match so the applier never tries to read an entry the
	// snapshot just compacted away (lastApplied can lag a batch behind the app).
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persistWithSnapshot()
}

// RaftStateSize reports the size of the persisted Raft state so the application can
// decide when to snapshot (compact the log).
func (rf *Raft) RaftStateSize() int { return rf.persister.RaftStateSize() }

// --- log accessors (offset-aware; all assume rf.mu is held) ---
//
// log[0] is the snapshot anchor carrying {lastIncludedTerm, lastIncludedIndex}.
// The log index of slice position k is k + firstIndex(). These helpers are the
// ONLY place that arithmetic lives.

// firstIndex is the log index of the snapshot anchor (lastIncludedIndex). It is
// the lowest index for which we still know a term; entries strictly below it have
// been compacted into the snapshot.
func (rf *Raft) firstIndex() int { return rf.log[0].Index }

// lastIncludedTerm is the term of the snapshot anchor.
func (rf *Raft) lastIncludedTerm() int { return rf.log[0].Term }

func (rf *Raft) lastLogIndex() int { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

// entryAt returns the log entry at log index i. It panics if i is below the
// snapshot anchor (a programming error - that entry was compacted away) or beyond
// the end.
func (rf *Raft) entryAt(i int) LogEntry { return rf.log[i-rf.firstIndex()] }

// termAt returns the term of the entry at log index i, where i must be in
// [firstIndex(), lastLogIndex()]. i == firstIndex() yields the snapshot term.
func (rf *Raft) termAt(i int) int { return rf.log[i-rf.firstIndex()].Term }

// sliceFrom returns a copy of the entries from log index i onward (i must be
// > firstIndex(), i.e. a real entry, or == lastLogIndex()+1 for an empty result).
func (rf *Raft) sliceFrom(i int) []LogEntry {
	return append([]LogEntry(nil), rf.log[i-rf.firstIndex():]...)
}

// --- cluster configuration (Phase 7; all assume rf.mu is held) ---

// majority is the smallest number of votes/replicas that constitutes a majority of
// the CURRENT configuration.
func (rf *Raft) majority() int { return len(rf.peers)/2 + 1 }

// inConfig reports whether id is a member of the current configuration.
func (rf *Raft) inConfig(id int) bool {
	for _, p := range rf.peers {
		if p == id {
			return true
		}
	}
	return false
}

// refreshConfig recomputes the current configuration (rf.peers) from the log: the
// member set of the latest configuration entry, scanning back to and including the
// snapshot anchor (log[0]), and falling back to the bootstrap configuration if the
// log holds none. It must be called after any log mutation that could add, remove,
// or truncate a configuration entry.
func (rf *Raft) refreshConfig() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Config != nil {
			rf.peers = append([]int(nil), rf.log[i].Config...)
			return
		}
	}
	rf.peers = append([]int(nil), rf.bootstrapConfig...)
}

// latestConfigIndex returns the log index of the most recent configuration entry,
// or 0 if none (the anchor counts). Used to enforce one-change-at-a-time.
func (rf *Raft) latestConfigIndex() int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Config != nil {
			return rf.log[i].Index
		}
	}
	return 0
}

// LogState reports the last log index/term and the commit index. It exists for
// tests and observability; it takes the lock and is safe to call concurrently.
func (rf *Raft) LogState() (lastIndex, lastTerm, commit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex
}

// Metrics is a point-in-time snapshot of a node's observable state, for dashboards
// and demos (the building block a Prometheus exporter would publish).
type Metrics struct {
	Term             int
	Role             string
	CommitIndex      int
	LastApplied      int
	LogEntries       int   // entries currently held in memory (post-compaction)
	FirstIndex       int   // snapshot anchor; >0 means the log has been compacted
	ConfigSize       int   // members in the current configuration
	ElectionsStarted int64 // how many elections this node has begun (term inflation signal)
	LeadershipWins   int64 // how many times it has become leader (failover churn signal)
}

// Metrics returns a snapshot of this node's state for observability.
func (rf *Raft) Metrics() Metrics {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Metrics{
		Term:             rf.currentTerm,
		Role:             rf.role.String(),
		CommitIndex:      rf.commitIndex,
		LastApplied:      rf.lastApplied,
		LogEntries:       len(rf.log) - 1, // exclude the anchor
		FirstIndex:       rf.firstIndex(),
		ConfigSize:       len(rf.peers),
		ElectionsStarted: rf.electionsStarted,
		LeadershipWins:   rf.leadershipWins,
	}
}

// FirstIndex reports the log's snapshot anchor (lastIncludedIndex): 0 means no
// snapshot has been taken; a positive value means the log prefix below it has been
// compacted away. For tests and observability.
func (rf *Raft) FirstIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.firstIndex()
}
