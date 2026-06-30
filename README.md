# raftkv — A Deterministically-Tested Distributed Key-Value Store

A linearizable, fault-tolerant key-value store built on a **from-scratch implementation of the Raft consensus algorithm** in Go — and tested the way real distributed databases are tested: inside a **deterministic simulation** where the network, clocks, and disk are driven by a single seed, so *any* failure can be replayed on demand.

> **Why this exists:** "I implemented Raft" is common. The goal here is rarer and harder — to *prove* the system stays linearizable under adversarial faults, replay any bug from its seed, and explain every safety and liveness property. See [`DIFFERENTIATION.md`](DIFFERENTIATION.md) for the strategy and [`plan.md`](plan.md) for the full execution plan.

---

## Status

✅ **Core complete — all 11 phases (0–10) done.** Verified linearizable under seeded fault injection; benchmarked.

| Phase | Capability | State |
|------:|---|:---:|
| 0 | Project scaffolding, core interfaces, deterministic clock, race-tested toolchain | ✅ |
| 1 | Deterministic simulated network + cluster test harness | ✅ |
| 2 | Leader election | ✅ |
| 3 | Log replication | ✅ |
| 4 | Persistence & crash recovery | ✅ |
| 5 | KV layer + exactly-once client semantics | ✅ |
| 6 | Snapshotting / log compaction | ✅ |
| 7 | Cluster membership changes | ✅ |
| 8 | Linearizable reads (ReadIndex) | ✅ |
| 9 | Jepsen-style fault testing + linearizability verification | ✅ |
| 10 | Benchmarking + observability | ✅ |

Reached **M7** (the resume-ready milestone): thousands of seeded chaos operations
verified linearizable by a from-scratch WGL checker, with real bugs found and
written up in [`explain/bug-museum/`](explain/bug-museum/). See [`plan.md`](plan.md)
for the full milestone map, [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for the cost
of consensus, and [`explain/`](explain/) for the concept-by-concept walkthrough.

---

## Architecture (target)

```
Clients (CLI / gRPC)
        │
   KV Service Layer        ← state machine, exactly-once sessions, linearizable reads
        │  Propose(cmd) / applyCh
   Raft Consensus Core      ← election, replication, persistence, snapshots, membership  (built from scratch)
        │  RequestVote / AppendEntries / InstallSnapshot
   Transport (pluggable)    ← real gRPC  |  deterministic in-memory sim network
        │
   Storage (pluggable)      ← file / BoltDB  |  in-memory (with crash injection)
```

Every source of non-determinism — time, network, disk, randomness — is behind an interface so the same Raft code runs in production and inside the seeded simulation. That is the project's core idea.

## Layout

```
cmd/            entry points (kvserver, kvctl)
internal/
  raft/         the consensus core (from scratch)
  kv/           key-value state machine + server
  transport/    Transport interface + memnet (simulated network)
  storage/      Persister interface + impls
  clock/        injectable time (real + deterministic mock)
  warmup/       throwaway Phase-0 concurrency drills (proves the -race toolchain)
test/           cluster harness, chaos/nemesis, linearizability checker
bench/          benchmark harness
explain/        learning notes (concepts, bug museum) — half the value of this repo
docs/           polished architecture / design-decision / testing / benchmark docs
```

## Build & test

Requires Go 1.26+.

```bash
go build ./...                 # compile everything
go test -race -count=1 ./...   # the real gate: tests under the race detector

# or via the task runner (Git Bash / WSL / make on Windows):
make race      # race-enabled tests
make all       # fmt check + vet + race tests
make cover     # coverage summary
```

## Design notes

- **Consensus is implemented from the [extended Raft paper](https://raft.github.io/raft.pdf), not a library** — that's the point.
- **Correctness before performance:** no optimization lands until the linearizability checker is green under chaos.
- **Every bug is a documented, seed-reproducible artifact** in [`explain/bug-museum/`](explain/bug-museum/).

## License

TBD.
