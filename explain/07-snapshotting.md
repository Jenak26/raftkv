# Log Compaction & Snapshotting

> Phase 6. The log cannot grow forever. Snapshotting lets the state machine's
> current state stand in for — and discard — a prefix of the log. It also forces
> the single most bug-prone change in the whole project: the **log offset**.

## What problem this solves

Every committed command stays in the log forever, so far. That means: restarts
replay millions of entries, disk fills, and a follower that has been offline for an
hour needs the leader to ship it an hour of history. A **snapshot** is the escape:
the application serializes its *entire current state* as of some committed index,
and Raft then throws away every log entry at or before that index. State replaces
history.

A snapshot is two things:

- the serialized state-machine bytes, plus
- the **`(lastIncludedIndex, lastIncludedTerm)`** the snapshot covers — the log
  position the state is current as of.

## The log offset (where the bugs live)

Until now, slice position equalled log index: `rf.log[i]` was the entry with log
index `i`, and `log[0]` was a `{Term:0, Index:0}` sentinel. After we discard a
prefix, that identity is gone. Slice position 0 now holds the
`lastIncludedIndex` entry, and:

```
logIndex = sliceIndex + lastIncludedIndex
sliceIndex = logIndex - lastIncludedIndex
```

The discipline that keeps this sane: **all index math goes through helpers**, never
raw `rf.log[i]`. We keep the sentinel trick — `log[0]` is always a real-or-synthetic
entry carrying `{lastIncludedIndex, lastIncludedTerm}` — so the common case
(`prevLogIndex == lastIncludedIndex`) needs no special branch, exactly as the old
index-0 sentinel did. On a fresh node `lastIncludedIndex == 0`, so everything
reduces to the Phase 3 behavior; the offset code is backward-compatible.

Helpers (all assume the lock is held):
- `firstIndex()` / `lastIncludedTerm()` — `log[0]`'s index and term.
- `lastLogIndex()` / `lastLogTerm()` — `log[last]`'s index and term (already
  index-valued, so unchanged).
- `entryAt(i)` — the entry at *log* index `i` (panics if compacted away).
- `termAt(i)` — term at log index `i`, with `i == firstIndex()` returning the
  snapshot term, used by the consistency check.
- `sliceFrom(i)` — entries from log index `i` onward, for replication.

Every former `rf.log[i]` is rewritten in terms of these.

## The two directions

### App → Raft: `Snapshot(index, stateBytes)`

The application decides *when* to snapshot (when the persisted Raft state grows past
a threshold) and owns the bytes. It calls `rf.Snapshot(index, stateBytes)`:

1. Ignore it if `index <= firstIndex()` (we already compacted past it) or
   `index > commitIndex` (never snapshot uncommitted state).
2. Set `log[0]` to `{Index: index, Term: termAt(index)}` and drop everything before
   it: `log = log[index-firstIndex():]`.
3. Persist the trimmed Raft state **and** the snapshot bytes **together**, via
   `SaveStateAndSnapshot`, so a crash recovers a consistent pair.

### Raft → Raft: the `InstallSnapshot` RPC (Figure 13)

When a follower is so far behind that the leader has already discarded the entries
it needs (`nextIndex[peer] <= firstIndex()`), the leader cannot send
`AppendEntries` — those entries are gone. It sends the **snapshot** instead.

- **Leader side** (`replicateOne`): if the peer's `nextIndex` is at or below
  `firstIndex()`, send `InstallSnapshot{lastIncludedIndex, lastIncludedTerm, data}`.
  On success, advance `matchIndex`/`nextIndex` to `lastIncludedIndex`.
- **Follower side** (`HandleInstallSnapshot`): the usual term check; then, if the
  snapshot is newer than what we have (`lastIncludedIndex > commitIndex`), discard
  the log (keeping any suffix that the snapshot already covers and matches),
  reset `log[0]` to the snapshot point, bump `commitIndex`/`lastApplied` to
  `lastIncludedIndex`, persist state+snapshot, and **hand the snapshot up to the
  application** so it can replace its state machine.

## Coordinating with the application (and avoiding the deadlock)

The dangerous shape is a synchronous cycle: app calls into Raft to install a
snapshot, Raft calls back into the app to restore, while a lock is held on each
side. We avoid it with a **single applier**: the snapshot travels up the same
`applyCh`, delivered by the same goroutine that delivers commands, strictly in
order.

- `HandleInstallSnapshot` does the log/persistence work under the lock, then marks
  a *pending snapshot* and signals the applier — it never touches `applyCh` itself.
- The applier loop, on waking, delivers a pending snapshot first (as
  `ApplyMsg{SnapshotValid:true, ...}`) and only then resumes delivering commands.
  Sends happen outside the lock.

The application's apply loop (the KV server) handles `SnapshotValid` by restoring
its state machine **and its session/dedup table** from the bytes — both are part of
the replicated state, so both must be in the snapshot. (Forgetting the session
table would resurrect already-applied client requests as new.)

## What breaks if you get it wrong

- **Off-by-one after truncation** — the entire bug class this phase invites. The
  cure is to never index `rf.log` raw; go through the helpers, and centralize the
  `± firstIndex()` arithmetic in exactly those helpers.
- **Snapshot + trimmed log persisted separately** — a crash between them recovers an
  inconsistent pair. Use `SaveStateAndSnapshot`.
- **Installing a stale snapshot** — one older than the follower's current state must
  be ignored, or it rewinds committed progress.
- **App↔Raft restore deadlock** — solved by single-applier delivery over `applyCh`.
- **Snapshot omits the session table** — exactly-once breaks after a snapshot, since
  the dedup memory is lost.

## Tests that pin the properties

- `TestSnapshotLogIsCompacted` — under a long workload with a size threshold, the
  persisted Raft state stays bounded (the log does not grow without limit).
- `TestSnapshotLaggingFollowerCatchesUp` — a follower kept offline past the
  snapshot point is brought current via `InstallSnapshot`, not `AppendEntries`.
- `TestSnapshotSurvivesRestart` — a node restarts from a snapshot + tail and serves
  correct reads (state and dedup table intact).
- All Phase 2–5 tests still pass — the offset refactor must be a no-op when no
  snapshot has been taken.
