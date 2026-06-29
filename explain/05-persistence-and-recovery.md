# Persistence & Crash Recovery

> Phase 4. Election and replication assume nodes that crash come back having
> *forgotten nothing they promised*. Persistence is what makes that promise real —
> and what stops a restarted node from violating Election Safety.

## What problem this solves

Raft's whole safety argument rests on the **crash-recovery** failure model: a node
may stop at any instant and later restart, but when it restarts it must remember
the commitments it already made. If a node forgets, it can actively *break* safety
— not merely lose availability.

The canonical disaster: a follower grants its vote to candidate A in term 5, then
crashes before the vote is durable. It restarts with `votedFor == none`, and now
grants a *second* vote in term 5 to candidate B. Two candidates each collect a
majority that includes this node → **two leaders in term 5** → split brain. The
fix is not clever logic; it is durability: the vote must hit stable storage
*before* the grant is sent.

## What must be durable (Raft Figure 2)

Exactly three fields, and no more:

| Field | Why it must survive a crash |
|---|---|
| `currentTerm` | The logical clock. Forgetting it lets a node re-participate in a term it already advanced past, re-issuing votes/appends under a stale identity. |
| `votedFor` | One vote per term. Forgetting it enables the double-vote split-brain above. |
| `log[]` | The committed prefix. Forgetting a committed entry loses acknowledged data — the cardinal sin. |

Everything else (`role`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`)
is **volatile**: it is safely reconstructed after restart. A recovering node always
comes back as a **Follower** with `commitIndex = 0`; it re-learns its commit index
from the next leader's `AppendEntries.LeaderCommit` and re-applies from its durable
log. Persisting volatile state would be at best wasteful and at worst wrong (a
stale `role = Leader` on disk would be a bug).

## The one ordering invariant: persist before you reply

> **Durable-write-before-reply.** Any RPC handler or transition that mutates one of
> the three durable fields must call `persist()` *before* the side effect that
> tells the outside world about it (sending a reply, sending an RPC, returning a
> commit index to a client).

A crash in the window *after* replying but *before* persisting is exactly the
split-brain bug. Because our single mutex is held across the whole handler and
`persist()` runs inside it, the encode-and-save completes before the reply value
ever leaves the function. In `raft.go` the call sites are:

- `startElection` — `currentTerm++`, `votedFor = self`, then `persist()` **before**
  any `RequestVote` is sent.
- `stepDown(term)` — on adopting a higher term it sets `votedFor = none` and
  `persist()`s before the handler returns its (stale) reply.
- `HandleRequestVote` — on granting, sets `votedFor` and `persist()`s before
  returning `VoteGranted`.
- `HandleAppendEntries` — on any log change (truncate/append) `persist()`s before
  returning `Success`.
- `Propose` — appends the new entry and `persist()`s before returning the index to
  the caller, so a client that hears "index N" is told so only after N is durable
  on the leader.

## How recovery flows

```
process restart
   │
   ▼
raft.Make(cfg)
   │  rf.readPersist(persister.ReadRaftState())
   │     └─ decodes {currentTerm, votedFor, log} back into the node
   │  role = Follower, commitIndex = 0, lastApplied = 0   (volatile, fresh)
   ▼
ticker + applier goroutines start
   │
   ▼
next AppendEntries from the live leader carries LeaderCommit
   └─ follower advances commitIndex, applier re-delivers committed entries in order
```

`readPersist` is a no-op on a truly fresh node (nil state → keep the in-memory
defaults: term 0, no vote, sentinel-only log). Decoding uses `encoding/gob`, the
mirror of `persist`'s encoder; the sentinel entry at index 0 round-trips so all the
Phase 3 index math still holds after a restart.

## The durability boundary: the `Persister`

Raft never touches a file directly. It hands an **opaque blob** to a
`storage.Persister` and asks for it back later. Two implementations:

- **`InMemoryPersister`** — used by every test and by the deterministic simulation.
  "Crash" means dropping the node's in-RAM state while keeping the persister, so a
  `Restart` reloads from it exactly as a real process reloads from disk. It clones
  on the way in and out so a caller can never alias and mutate stored bytes.
- **`FileStorage`** — the real on-disk implementation (`file_storage.go`). Its job
  is to make a save **crash-atomic**: a crash mid-write must leave either the old
  bytes or the new bytes, never a torn mix. It does this with the classic
  **write-temp → fsync → atomic rename** dance, plus an fsync of the directory so
  the rename itself is durable.

### fsync vs throughput

`fsync` is what actually pushes bytes past the OS page cache onto stable media; a
rename without it can still be lost in a power cut even though `write` returned.
But fsync is *expensive* — it is the dominant cost of a consensus round, because a
leader cannot acknowledge a commit until a majority has durably persisted the
entry. This is the central durability/throughput trade-off, and the reason
production systems **batch** many log appends into one fsync (group commit, a
Phase 10 optimization). `FileStorage` fsyncs on every save — correct, and a clean
baseline to measure batching against later.

## What breaks if you get it wrong

- **Persist after replying** → the double-vote split brain above. The subtlest of
  all: the happy path looks identical; only a crash in a microsecond-wide window
  reveals it.
- **Forget to persist on *some* mutation** (e.g. update `votedFor` but skip the
  flush) → same bug, intermittently.
- **Persist volatile state** → a node restarts believing it is still leader of an
  old term, or with a stale `commitIndex` ahead of what it can justify.
- **Non-atomic disk write** → a crash mid-save corrupts the only copy of the log; a
  decode panic on restart, or silent loss. Hence temp-file + rename.

## Tests that pin the properties

- `TestPersistsTermAndVoteAcrossRestart` (unit) — a node's `currentTerm`/`votedFor`
  survive a `Make` from the same persister, **and** a node that voted in a term,
  crashed, and restarted refuses a second conflicting vote in that same term (the
  no-double-vote / Election-Safety-across-restart property).
- `TestPersistedDataSurvivesFullClusterRestart` — commit entries, crash **every**
  node, restart them all; each node's durable log holds the committed prefix the
  instant it returns, and a fresh leader re-applies it identically (the M3 demo).
- `TestPersistUnderUnreliableChurn` — under message loss plus repeated
  crash/restart, no committed entry is ever lost or contradicted.
- `FileStorage` tests (`file_storage_test.go`) — round-trip, the state+snapshot
  pair survives reopening together, and a half-written temp file never corrupts the
  committed blob.
</content>
</invoke>
