// Package raft will contain the from-scratch implementation of the Raft
// consensus algorithm (Ongaro & Ousterhout). It is the heart of the project.
//
// Phase 0 establishes only the shared vocabulary: the node Role, and ApplyMsg,
// the message a node delivers to the application as entries commit. Leader
// election, log replication, persistence, snapshotting, and membership changes
// arrive in later phases (see plan.md).
package raft

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
