# KV State Machine & Exactly-Once Client Semantics

> Phase 5. Raft now becomes a *database*. The consensus core agrees on an ordered
> log of commands; this layer turns that log into a key-value store and confronts
> the messy reality of clients that retry.

## What problem this solves

Phases 2–4 give us a replicated, durable, totally-ordered log. But a log is not a
database and a network is not reliable. This phase answers three questions:

1. **How does the log become state?** A deterministic state machine (`Apply`) folds
   committed commands into a key-value map. Because every replica applies the
   identical sequence (Raft's guarantee) and `Apply` is deterministic, every
   replica holds identical state — the *replicated state machine* model made real.
2. **How does a client talk to a cluster?** It doesn't know who the leader is, the
   leader can change mid-request, and any message can be lost. The client must
   discover the leader, follow leadership changes, and retry.
3. **What does a retry mean?** This is the subtle one, below.

## The duplicate problem (the heart of the phase)

The transport gives **at-least-once** delivery: a client that times out waiting for
a reply retries. But the original request may have *already committed* — the reply
just got lost (or the leader crashed *after* committing). Naively, the retry
applies the command a **second time**.

For idempotent ops (`Put`, `Delete`) a double-apply is invisible. For non-idempotent
ops it corrupts state: a "withdraw \$10" applied twice withdraws \$20; a CAS that
succeeded once reports failure the second time. We want **exactly-once semantics**
on top of at-least-once delivery.

### The fix: client sessions + a dedup table

- Every client has a unique `ClientID` and a per-client monotonically increasing
  `SeqNum`. A retry reuses the *same* `(ClientID, SeqNum)`.
- The state machine layer keeps a **session table**: `ClientID -> {lastSeq,
  lastResult}`. When a command arrives with `SeqNum <= lastSeq`, it is a duplicate:
  return the stored result **without re-applying**.

> **Crucial placement: dedup happens at *apply* time, not at propose time.** The
> session table is part of the replicated state — it is updated only inside the
> apply loop, deterministically, on every replica. If we deduped at the leader
> before proposing, replicas would disagree, and a leader change would lose the
> dedup memory. (This is also why the table must be snapshotted in Phase 6.)

Reads (`Get`) are idempotent, so they skip the session table — only mutating ops
record a result.

## The apply / wait handshake

The server cannot reply to a client until the client's command has actually
**committed and applied** (otherwise it might report success for an entry that a
leader change later discards). The mechanism:

```
client op
   │  build Command{Kind,Key,Value,ClientID,SeqNum}
   ▼
Server.Submit ── rf.Propose(encode(cmd)) ──► (index, isLeader)
   │  not leader? → return ErrNotLeader (client retries elsewhere)
   │  leader? → register a waiter channel keyed by `index`, then block
   ▼
... Raft replicates & commits ...
   ▼
apply loop  (one goroutine draining applyCh in log order)
   │  decode cmd; dedup-check against session table; Apply or reuse result
   │  update session table (mutating ops only)
   │  hand the result to the waiter registered at this index, if any
   ▼
Server.Submit wakes: is the applied command MINE (same ClientID+SeqNum)?
   │  yes → return result
   │  no  → a different command took my index (I lost leadership mid-flight)
   │        → return ErrLostLeader, client retries
   └  timeout with no apply → return ErrTimeout, client retries
```

The **identity check on wake-up** is what makes this safe across leader changes:
the entry that commits at the index we were promised might not be ours if a new
leader overwrote the uncommitted tail. We detect that and tell the client to retry,
rather than falsely reporting another client's result as our own.

## The client (Clerk)

- Holds the set of server endpoints, a random `ClientID`, and a `SeqNum` counter.
- Per operation: bump `SeqNum`, then loop over servers starting from the
  last-known leader. On `ErrNotLeader` / timeout / unreachable, advance to the next
  server. On success, remember that server as the leader and return.
- Because the same `SeqNum` is reused across all retries of one logical operation,
  the dedup table collapses any duplicates the retries cause.

We discover the leader by *trying* (round-robin) rather than threading a leader
hint through every reply — simpler, and robust to a stale hint.

## Why reads still go through the log (for now)

It is tempting to answer `Get` straight from the leader's map. That is **not
linearizable**: a partitioned-but-doesn't-know-it old leader would serve stale
data. So in Phase 5 reads are proposed through the log like writes — correct, if
not yet fast. Phase 8 (ReadIndex) makes linearizable reads cheap; Phase 5
deliberately leaves that performance on the table to keep correctness obvious.

## What breaks if you get it wrong

- **No dedup** → retried non-idempotent writes double-apply. The classic bug.
- **Dedup at propose time instead of apply time** → replicas diverge; dedup memory
  lost on leader change.
- **Reply before apply** → report success for an entry a leader change later
  discards (a lost write the client believes succeeded).
- **No identity check on wake-up** → return another client's result as your own
  after your proposal was overwritten.
- **Block forever** waiting for an apply that will never come (proposal overwritten)
  → client hangs instead of retrying. Hence the timeout.
- **Unbounded session table** → memory leak; bounded later by tracking the client's
  lowest in-flight seq / tying entries to sessions.

## Tests that pin the properties

- `TestKVBasicPutGetDeleteCAS` — a 3-node cluster serves the four ops end to end.
- `TestKVLeaderRedirectAndRetry` — a client pointed at a follower still succeeds by
  finding the leader.
- `TestKVExactlyOnceUnderDuplicateSubmit` — submitting the identical
  `(ClientID, SeqNum)` command twice mutates the state machine exactly once.
- `TestKVProgressAcrossLeaderCrash` — a client keeps succeeding while leaders are
  crashed and re-elected.
- The live path (`cmd/kvserver` + `cmd/kvctl` over net/rpc) is the M4 demo.
