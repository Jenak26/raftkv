# Log Replication

> Phase 3. Election decides *who* leads; replication decides *what is agreed*.
> This is where Raft actually stores data — and where its two subtlest bugs live.

## The log

Each entry is `(term, index, command)`. We keep a **sentinel** at index 0
(`{term:0, index:0}`) so the first real entry is index 1 and `prevLogIndex == 0`
needs no special case. In Phase 3 the slice position equals the log index;
snapshots add an offset in Phase 6.

`currentTerm`, `votedFor`, **and the log** are the durable state — every mutation
calls `persist()` before the node replies.

## The Log Matching Property

> If two logs contain an entry with the same index and term, then the logs are
> identical in all entries up through that index.

`AppendEntries` enforces this with a **consistency check**: the leader includes
`prevLogIndex`/`prevLogTerm` (the entry just before the new ones). A follower
accepts only if it has that exact entry. This inductively guarantees the whole
prefix matches before any new entry is appended.

### Conflict-term fast backtracking

When the check fails, walking `nextIndex` back one step per round is O(log
length). Instead the follower returns a hint:
- log too short → `ConflictTerm = -1`, `ConflictIndex =` its next free slot;
- term mismatch → `ConflictTerm =` the offending term, `ConflictIndex =` the
  **first** index of that term.

The leader then skips the entire conflicting term in one round (`backtrack` in
`raft.go`). O(number of terms) instead of O(entries).

### Truncate only on a real conflict

The follower deletes its tail **only** when an incoming entry's term differs from
what it already has at that position — never for entries that already match. The
over-eager version (truncate everything from `prevLogIndex+1`) silently deletes
committed entries that a delayed/duplicated `AppendEntries` would have you
re-append. `TestAppendEntriesTruncatesOnConflictOnly` pins this.

## Committing — and the Figure 8 trap

The leader tracks `matchIndex[peer]`. An index is committable once a **majority**
has it. But there is a notorious exception:

> A leader may only mark an entry committed by counting replicas if that entry is
> from the **leader's current term** (`maybeAdvanceCommit`'s `log[n].Term ==
> currentTerm` guard).

Committing a *previous-term* entry purely because it sits on a majority can lead
to that entry being **overwritten after it was reported committed** — the Figure 8
scenario, the canonical Raft safety bug. Prior-term entries become committed only
*transitively*, when a current-term entry above them commits.

## The applier

A dedicated goroutine waits on a `sync.Cond`; when `commitIndex > lastApplied` it
delivers entries to `applyCh` **in order**, one `ApplyMsg` per entry. Sends happen
outside the lock (the channel may block). This is the single path by which the
state machine learns of committed commands, and it is what the tests observe.

## Concurrency, again

The Phase 2 rule still rules everything: **mutate state under the lock, release it
before any `Send`, re-acquire it for the reply.** `replicateOne` snapshots its
`AppendEntries` args under the lock, sends unlocked, and re-locks to apply the
result — bailing out if the term/role moved on (a stale reply must never advance
commit).

## Tests that pin the properties

- `TestReplicatesAndAppliesInOrder` — commands replicate and apply in identical
  order on every node.
- `TestLogConvergesAfterPartition` — a partitioned old leader's *uncommitted*
  divergent entries are overwritten after heal; committed entries survive.
- `TestNoConflictingCommitsUnderChurn` — across seeds, under leader crash/restart
  churn, no two nodes ever apply different commands at the same index, and the
  cluster keeps committing.
