package raft

// This file defines the wire types for Raft's three RPCs, mirroring Figure 2 of
// the extended Raft paper. They are transport-agnostic plain structs; the
// transport layer is responsible for actually moving them between nodes.

// LogEntry is one entry in the replicated log. Command is an opaque blob — the
// Raft layer never interprets it; the application (kv) encodes and decodes it.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// RequestVoteArgs is sent by candidates to gather votes (Figure 2).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // receiver's currentTerm, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// AppendEntriesArgs is sent by the leader to replicate log entries and as a
// heartbeat (with an empty Entries slice).
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so followers can redirect clients
	PrevLogIndex int        // index of the log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntries RPC. ConflictTerm and
// ConflictIndex carry the fast-backtracking hints used to skip a whole
// conflicting term at once (an optimization implemented in Phase 3).
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshotArgs is sent by the leader to bring a follower that has fallen
// behind the log's start up to date (Phase 6).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int
}
