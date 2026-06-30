# Linearizable Reads (ReadIndex)

> Phase 8. Writes are linearizable because they go through the log. Reads, so far,
> did too — correct but slow (a full consensus round to read a value). This phase
> makes reads *fast* while keeping them linearizable, and exposes the trade-off
> against deliberately stale reads.

## The central misconception

> "Reading from the leader is linearizable."

It is not. A leader can be **deposed without knowing it**: it was partitioned away,
a new leader was elected in a higher term and committed new writes, but the old
leader — still receiving no messages — believes it is leader and would happily
serve its now-stale state. A read from it would miss writes that already completed.
Avoiding this is the whole problem.

## What linearizable means here

A read is linearizable if it reflects **every write that completed before the read
began** (and never a write that hasn't happened). Concretely: if a client's `Put`
returned success, any later `Get` — by anyone — must see at least that value.

## ReadIndex (Ongaro thesis §6.4)

The protocol serves a read without writing to the log:

1. **Be sure the leader's commit index is current for its term.** A freshly elected
   leader does not yet know which prior-term entries are committed (the Figure 8
   rule). So on election the leader appends a **no-op** entry in its own term;
   once that commits, its `commitIndex` provably covers the entire prior committed
   prefix (Leader Completeness). Reads wait for this.
2. **Record `readIndex = commitIndex`.**
3. **Confirm leadership.** Exchange one round of heartbeats with a majority. If a
   majority still answers without a higher term, then *at this moment* no other
   leader has superseded us — so no write we don't know about has committed. This is
   the step that defeats the deposed-leader problem.
4. **Wait** until the state machine has applied through `readIndex`, then read it.

No log entry, no disk write — just one heartbeat round. In this project:

- `raft.ReadIndex(ctx)` does steps 1–3 and returns `readIndex` (or `ok=false` if not
  leader / no-op not yet committed / leadership unconfirmed). `confirmLeadership`
  runs the heartbeat round and counts a peer as confirming if it replies without a
  higher term; a single-node cluster is trivially confirmed.
- `kv.Server.linearizableRead` calls `ReadIndex`, waits for `lastApplied >=
  readIndex`, then reads the state machine.

The no-op also matters for the applier/KV boundary: the no-op (and configuration
entries) have an empty `Command`, so the KV apply loop **skips** them — they advance
the applied index but are not client operations to decode or dedup.

## Leader leases (understood, not implemented)

The heartbeat round in step 3 costs a round trip per read. A **leader lease**
avoids it: if the leader knows (from the last successful heartbeat round) that it
will remain leader for some bounded time, it can serve reads within that window with
no fresh round. The catch is **clock skew** — leases assume bounded clock drift
across machines; get that assumption wrong and you can serve stale reads. We
implement ReadIndex (no clock assumption) and document leases as the optimization.

## The read-consistency knob

To make the trade-off concrete, the client exposes two read modes:

- **`Get` (linearizable, default)** — ReadIndex; always fresh, costs a heartbeat
  round and must be served by the leader.
- **`GetStale`** — served immediately from the local state machine of whatever node
  answers (including a follower), no leadership round. Fast and scalable across
  followers, but may return slightly stale data.

This is the spectrum interviewers probe: linearizable ↔ bounded-staleness ↔
follower reads, trading consistency for latency and read throughput. Phase 10 will
benchmark the difference.

## What breaks if you get it wrong

- **"Leader read = linearizable"** → a deposed leader serves stale data. ReadIndex's
  heartbeat round is precisely the fix.
- **No election no-op** → a new leader serves reads before it knows its commit index
  for the term, potentially missing the tail of the previous term's commits.
- **Leases without a clock-skew bound** → stale reads under drift; always state the
  assumption.

## Tests that pin the properties

- `TestLinearizableReadDoesNotGrowLog` — many `Get`s leave the leader's log length
  unchanged, proving reads don't go through the log.
- `TestLinearizableReadAfterLeaderChange` — after the leader is crashed, a
  linearizable `Get` still returns the last committed write (exercises the
  no-op-then-ReadIndex path on the new leader).
- `TestStaleReadServedByFollowerLocally` — a linearizable read to a follower is
  refused; a stale read is answered from the follower's own state machine.
