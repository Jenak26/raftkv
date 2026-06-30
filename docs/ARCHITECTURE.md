# Architecture

How raftkv is put together, the invariants it upholds, and the concurrency model that keeps it correct.

## Layered design

```
kvctl (client)  →  KV service layer  →  Raft consensus core  →  Transport + Storage (pluggable)
```

- **KV service layer** (`internal/kv`) - the replicated state machine (an in-memory map: `Put`/`Get`/`Delete`/`CAS`), a client-session dedup table for exactly-once semantics, and the ReadIndex linearizable-read path. It turns client requests into Raft proposals and applies committed entries.
- **Raft consensus core** (`internal/raft`) - leader election, log replication, persistence, snapshotting, and single-server membership changes. Implemented from the extended Raft paper; every function maps to a figure or section.
- **Transport** (`internal/transport`, `internal/netrpc`) - an interface with two implementations: production `net/rpc` over TCP, and a deterministic in-memory simulation that can drop, delay, reorder, and partition messages from a seed.
- **Storage** (`internal/storage`) - a `Persister` interface with a file implementation (`fsync` + atomic rename) and an in-memory one used by the simulation.

The point of the interfaces is uniformity: **the same consensus code runs in production and inside the seeded simulation.**

## The concurrency model (where bugs live)

Each Raft node runs a small, fixed set of goroutines, and **one mutex guards all mutable node state**:

| Goroutine | Role |
|---|---|
| `ticker` | fires election timeouts; starts elections |
| `leaderLoop` / `replicateOne` | per-round replication + heartbeats to peers (leader only) |
| `applier` | delivers committed entries (and snapshots) to the application in log order |
| RPC handlers | created by the transport for inbound `RequestVote` / `AppendEntries` / `InstallSnapshot` |

> **The one rule that prevents the classic Raft deadlock:** never hold the mutex while making an outbound RPC. Every `Send*` happens after the lock is released; the reply is processed by re-acquiring it and re-checking term/role (a stale reply must never advance state). The test harness mirrors this discipline - it never holds its lock across a call into a node.

## Write path

```
Clerk.Put → Server.Submit → rf.Propose(cmd)
   → leader appends to log, persists, replicates (AppendEntries) to followers
   → entry committed once a majority has it (and it is from the current term - Figure 8)
   → applier delivers it; KV applies to the state machine and wakes the waiting Submit
   → Submit verifies the committed entry is *its* (ClientID, SeqNum) and returns
```

A waiter that sees a *different* command commit at its index knows it lost leadership mid-flight and tells the client to retry - which is safe because the dedup table makes the retry exactly-once.

## Read path (linearizable, no log write)

```
Clerk.Get → Server.linearizableRead → rf.ReadIndex
   → require a current-term entry committed (the election no-op)
   → readIndex = commitIndex
   → confirm leadership via one heartbeat round to a majority
   → wait until applied ≥ readIndex, then read the state machine
```

A stale read (`GetStale`) skips all of this and reads the local state machine on any node - fast, but not linearizable.

## Persistence & snapshotting

- **Durable before reply:** `currentTerm`, `votedFor`, and the log are persisted *before* any RPC reply that depends on them - the ordering that prevents a double-vote split brain after a crash.
- **Snapshots** compact the log: the application serializes its state (plus the session table) at a committed index, and Raft discards the prefix. `log[0]` becomes a snapshot anchor and all index math flows through offset-aware helpers. A follower that has fallen behind the snapshot is caught up with `InstallSnapshot` rather than `AppendEntries`.

## Membership changes

Configuration lives in the log as a special entry; the **latest configuration in the log** drives every quorum decision, adopted the moment it is appended (not when committed). Only **single-server** changes are allowed, one at a time - which is safe without joint consensus because the old and new majorities always overlap, so two leaders can never be elected in the same term.

## Safety properties upheld

The five Raft safety properties, each exercised by the test suite:

1. **Election Safety** - at most one leader per term.
2. **Leader Append-Only** - a leader never overwrites or deletes its own entries.
3. **Log Matching** - identical `(index, term)` ⇒ identical prefixes.
4. **Leader Completeness** - a committed entry is present in every future leader's log.
5. **State Machine Safety** - no two nodes ever apply different commands at the same index.

## Testing strategy

A layered pyramid, all under the race detector:

1. **Unit** - pure logic (vote rules, the consistency check, commit math, snapshot offsets).
2. **Integration** - an in-process N-node cluster over the simulated network (`test/cluster`).
3. **Property / randomized** - invariants asserted across many seeds.
4. **Linearizability** - a from-scratch Wing-Gong-Lowe checker (`test/linearizability`).
5. **Chaos / soak** - a seeded nemesis under continuous fault injection (`test/chaos`), every failure replayable from its seed.

See [`BENCHMARKS.md`](BENCHMARKS.md) for performance and [`bug-museum/`](bug-museum/) for documented bugs.
