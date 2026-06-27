# Leader Election

> Phase 2. The first Raft safety property — *at most one leader per term* — and
> the first liveness property — *some leader is eventually elected*.

## Terms are a logical clock

A **term** is a monotonically increasing integer. Every RPC carries the sender's
term, and the universal rule is: **if you see a higher term, adopt it and step
down to follower.** That single rule (`stepDown` in `raft.go`) is what keeps the
cluster from ever having two leaders who think they're current — a stale leader
that rejoins immediately learns it is behind and yields.

## The three roles and the transitions

```
            times out, starts election
 Follower ───────────────────────────────▶ Candidate
    ▲                                          │
    │ sees higher term / valid AppendEntries   │ wins majority
    │                                          ▼
    └────────────────── steps down ──────── Leader
```

- **Follower** — passive; resets its election timer whenever it hears from a
  valid leader (`AppendEntries`) or grants a vote.
- **Candidate** — incremented its term, voted for itself, and is collecting
  votes. Wins with a **majority** (`N/2 + 1`).
- **Leader** — sends periodic empty `AppendEntries` (heartbeats) to suppress
  other elections.

## Why randomized timeouts (the answer to FLP)

FLP says no deterministic protocol can guarantee consensus in an asynchronous
network with even one faulty node. Raft sidesteps this with **randomized**
election timeouts (here, 150–300 ms): when nodes time out at different moments,
one becomes candidate first and usually wins before others wake. Fixed timeouts
would cause endless **split votes** — every node becomes a candidate at once,
nobody gets a majority, repeat. Randomness breaks the symmetry and gives
*probabilistic* liveness. Crucially, the randomness is **seeded** (`Config.Rand`),
so a "bad" interleaving replays exactly.

## The voting rules (Figure 2)

A node grants its vote iff **all** hold:
1. the candidate's term is ≥ its own,
2. it hasn't already voted for someone else **this term** (`votedFor`), and
3. the candidate's log is at least as up-to-date as its own (the up-to-date
   check — trivial now with empty logs, central in Phase 3).

Because each node votes at most once per term and a win needs a majority, **two
candidates cannot both collect a majority in the same term** — that is Election
Safety, and it is exactly what `TestElectionSafetyUnderRandomPartitions` asserts
across many seeds.

## The two hard-won implementation rules

1. **Never hold the lock across an outbound RPC.** `startElection` and
   `heartbeatLoop` mutate state under `rf.mu`, *release it*, then send. Replies
   re-acquire the lock. Holding the lock across `Send` deadlocks the moment two
   nodes RPC each other. The whole struct is built around this discipline.
2. **A vote only counts for the term it was requested in.** Vote-reply handlers
   bail out if `rf.currentTerm != term || rf.role != Candidate`. Counting a late
   reply from an old election can manufacture a phantom majority.

## What a "crash" is here

`Raft.Kill` stops the ticker and heartbeat goroutines but leaves the `Persister`
intact; `Restart` builds a fresh node that `readPersist`s `currentTerm`/`votedFor`
from it. `currentTerm` and `votedFor` are durable precisely so a restarted node
can't vote twice in one term — see `TestPersistsTermAndVoteAcrossRestart`.

## Tests that pin the properties

- `TestSingleLeaderElectedNoFaults` (3 and 5 nodes) — basic liveness.
- `TestLeaderReElectedAfterCrash` / `...AfterPartition` — recovery.
- `TestMinorityCannotElect` — no quorum, no leader.
- `TestElectionSafetyUnderRandomPartitions` — the headline: never two leaders in
  one term, across seeds, under partition churn.
