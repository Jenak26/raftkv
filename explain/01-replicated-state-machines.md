# 01 — Replicated State Machines

> Written before any consensus code. The whole project is one idea applied carefully; this is that idea.

## What problem does this solve?

A single server holding our key-value data is a single point of failure: if it crashes or its disk dies, the data and the service are gone. We want the store to **survive crashes of a minority of machines** and keep serving — *fault tolerance* and *availability*.

The naive fix — "just run several copies" — immediately raises the hard question: **how do the copies stay in agreement** when clients hit different servers, messages are lost or reordered, and machines crash mid-operation? Answering that *is* the distributed systems problem.

## The model

The **Replicated State Machine (RSM)** approach:

1. Model the service as a deterministic **state machine**: given the same current state and the same input command, it always produces the same next state and output. (Our KV map is exactly this.)
2. Put `N` copies of that state machine on `N` servers.
3. Feed every copy the **exact same sequence of commands, in the exact same order**.
4. Therefore every copy goes through the exact same states and stays identical.

```
client cmds ─▶ [ CONSENSUS: agree on the ordered log ] ─▶ same log on every node
                                                          │
                  ┌──────────────────┬───────────────────┤
                  ▼                  ▼                   ▼
            state machine      state machine      state machine
              (node 0)           (node 1)           (node 2)
                  │                  │                   │
                  └──── identical state on all nodes ────┘
```

The hard part collapses to one job: **agree on the ordered sequence of commands** (the log). That single job is what Raft does. Everything above the log (the KV store) is "just" a deterministic function of the log.

## The invariants that make it work

- **Determinism of Apply.** If `Apply` ever depends on wall-clock time, map iteration order, randomness, or local state not derived from the log, replicas diverge. *Every nondeterministic input must instead be baked into a command in the log.* (This is why request IDs, timestamps, etc. get written into log entries rather than read locally.)
- **Same order everywhere.** Not just the same set of commands — the same *order*. `Put(x,1)` then `Put(x,2)` must commit in that order on all nodes.
- **Apply only committed entries, in index order.** A node never applies an entry until consensus says it's safe, and never out of order.

## Where the difficulty hides (foreshadowing later phases)

- Agreeing on order despite crashes and partitions → **leader election + log replication** (Phases 2–3).
- Not forgetting decisions across restarts → **persistence** (Phase 4).
- Clients that retry and would otherwise double-apply → **exactly-once via dedup** (Phase 5).
- Logs can't grow forever → **snapshotting** (Phase 6).
- Reads must also respect the order, or they go stale → **linearizable reads** (Phase 8).

## Why this matters for the project's thesis

Because the replicas are a deterministic function of the log, the *entire system's behavior is a function of (initial state, ordered log, command inputs)*. If I also make the network, clocks, and scheduling deterministic, then the whole distributed system becomes reproducible from a seed — which is exactly what makes [Deterministic Simulation Testing](../DIFFERENTIATION.md) possible. The RSM model isn't just how the store works; it's *why* the store is testable.

## Success check for my understanding

I can answer, without notes:
- Why must `Apply` be deterministic, and what's an example of a bug if it isn't?
- Why is agreeing on *order* (not just *set*) essential?
- How does "a majority must store an entry" prevent two nodes from disagreeing later? (→ quorum intersection, see [[00-glossary]].)

Next: [[02-leader-election]] (Phase 2).
