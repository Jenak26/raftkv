# raftkv

**A linearizable, fault-tolerant key-value store built on a from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go** — and tested the way real distributed databases are: inside a *deterministic simulation* where the network, clocks, and crashes are all driven by a single seed, so any bug can be replayed on demand.

[![CI](https://github.com/Jenak26/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Jenak26/raftkv/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

> "I implemented Raft" is common. The aim here is the rarer, harder thing: to **prove** the system stays linearizable under adversarial faults, **replay any failure from its seed**, and **explain every safety and liveness property**. No consensus libraries — the algorithm is built from the extended Raft paper, function by function.

---

## Quickstart — run a real 3-node cluster

Requires Go 1.26+. Open four terminals.

```bash
# Terminals 1–3: start a 3-node cluster (each node gets its own data dir)
PEERS="0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002"
go run ./cmd/kvserver -id 0 -peers "$PEERS" -data ./data/0
go run ./cmd/kvserver -id 1 -peers "$PEERS" -data ./data/1
go run ./cmd/kvserver -id 2 -peers "$PEERS" -data ./data/2
```

```bash
# Terminal 4: drive it with the CLI — it finds the leader and retries automatically
SRV="127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002"
go run ./cmd/kvctl -servers "$SRV" put color blue        # OK
go run ./cmd/kvctl -servers "$SRV" get color             # blue
go run ./cmd/kvctl -servers "$SRV" cas color blue green  # OK (swapped)
go run ./cmd/kvctl -servers "$SRV" del color             # OK (deleted)
```

Kill the leader mid-session — the cluster re-elects and the CLI keeps working. State survives restarts (it's persisted to `./data/*` with `fsync` + atomic rename).

## What makes it different

The moat isn't that the Raft code works — it's that correctness is **proven, not asserted**:

- 🎲 **Deterministic Simulation Testing.** Time, the network, and disk are behind interfaces. In production they're real (`net/rpc` over TCP, files with `fsync`); in tests they're a seeded in-memory simulation that can drop, delay, reorder, and partition messages and crash/restart nodes — all reproducibly. A flaky distributed bug becomes a one-line repro: `make chaos SEED=42`.
- 🔬 **Linearizability verified under chaos.** A from-scratch [Wing-Gong-Lowe checker](test/linearizability/) confirms that the operations clients actually observed — recorded with real-time brackets while a seeded nemesis injects partitions, crashes, and restarts — could only have come from a single correct machine. Every operation, every seed.
- 🐛 **A bug museum.** Real bugs found during development, each a case file: symptom → root cause (which Raft invariant) → fix → the test that now catches it. See [`explain/bug-museum/`](explain/bug-museum/).

## Results

| Read consistency | Latency (3-node, loopback) | |
|---|---:|---|
| Stale read (local, any node) | **~71 µs** | served from a follower's state machine |
| Linearizable read (ReadIndex) | **~143 µs** | one heartbeat round confirms leadership — no log write |
| Write (full consensus round) | **~1.2 ms** | replicate to a majority, then apply |

The ~2× linearizable-vs-stale gap and the ~8× write cost are the consistency/latency trade-offs the design deliberately exposes. Methodology and the full analysis: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Raft features implemented (from the paper, from scratch)

- ✅ **Leader election** — terms as a logical clock, randomized timeouts, Election Safety verified across thousands of seeded partition runs
- ✅ **Log replication** — `AppendEntries` with the Log Matching property, conflict-term fast backtracking, and the Figure-8 current-term commit rule
- ✅ **Persistence & crash recovery** — `currentTerm`/`votedFor`/`log` durable before any reply; crash-atomic `fsync` + rename storage
- ✅ **Snapshotting / log compaction** — `InstallSnapshot`, a log offset, and single-applier delivery that avoids the app↔Raft restore deadlock
- ✅ **Single-server membership changes** — configuration lives in the log; safe without joint consensus via overlapping majorities
- ✅ **Linearizable reads** — ReadIndex (election no-op + heartbeat confirmation), plus a fast stale-read mode
- ✅ **Exactly-once client semantics** — client sessions + a dedup table turn at-least-once delivery into exactly-once
- ⬜ Stretch: sharding, BoltDB storage, gRPC + TLS, Pre-Vote, Grafana dashboards (see [`plan.md`](plan.md))

## Architecture

```
            kvctl (CLI client)  ── finds leader, retries, exactly-once
                     │
   ┌─────────────────▼──────────────────────────────────────────────┐
   │  KV service layer   state machine · client sessions · ReadIndex │
   └─────────────────┬──────────────────────────────────────────────┘
                     │  Propose(cmd) → applyCh
   ┌─────────────────▼──────────────────────────────────────────────┐
   │  Raft consensus core (from scratch)                             │
   │  elections · replication · persistence · snapshots · membership │
   └─────────────────┬──────────────────────────────────────────────┘
                     │  RequestVote · AppendEntries · InstallSnapshot
   ┌─────────────────▼──────────────┐   ┌────────────────────────────┐
   │ Transport (interface)          │   │ Storage (interface)        │
   │ net/rpc/TCP  │  sim network ◀──┼───┼─ file (fsync) │ in-memory  │
   └────────────────────────────────┘   └────────────────────────────┘
              production │ tests              production │ tests
```

Every source of non-determinism sits behind an interface, so the **same Raft code** runs in production and inside the seeded simulation. That is the project's core idea.

## How it's tested

```bash
make race            # the real gate: every test under the race detector
make all             # gofmt check + vet + race tests
make chaos           # the nemesis + linearizability suite, sweeping seeds
make chaos SEED=42   # replay one seed exactly
make bench           # latency/throughput benchmarks
```

The test pyramid: unit tests for pure logic, an in-process cluster harness over the simulated network, property/randomized tests, the linearizability checker, and the chaos/soak suite. CI runs `-race` on every push.

## Repository layout

```
cmd/            kvserver (a node) and kvctl (the CLI client)
internal/
  raft/         the consensus core — built from scratch
  kv/           key-value state machine, server, client (Clerk)
  transport/    Transport interface + memnet (deterministic sim network)
  netrpc/       production net/rpc transport (node-to-node + client)
  storage/      Persister interface — file (fsync) and in-memory impls
  clock/        injectable time (real + deterministic mock)
test/
  cluster/      in-process N-node cluster harness
  chaos/        seeded nemesis + Jepsen-style verification
  linearizability/  from-scratch WGL linearizability checker
bench/          benchmark harness
explain/        learning notes — concepts, and the bug museum (half the value)
docs/           architecture, benchmarks, design decisions
```

## Learn the internals

The [`explain/`](explain/) folder walks through every subsystem in its own words — what problem it solves, the invariants, the message flow, and what breaks if you get it wrong:

- [Replicated state machines](explain/01-replicated-state-machines.md) · [Deterministic simulation](explain/02-deterministic-simulation.md) · [Leader election](explain/03-leader-election.md) · [Log replication](explain/04-log-replication.md)
- [Persistence & recovery](explain/05-persistence-and-recovery.md) · [KV & exactly-once](explain/06-kv-and-client-semantics.md) · [Snapshotting](explain/07-snapshotting.md)
- [Membership changes](explain/08-membership-changes.md) · [Linearizable reads](explain/09-linearizable-reads.md) · [Failure testing](explain/10-failure-testing.md)
- 🐛 [**The bug museum**](explain/bug-museum/) — real bugs, root causes, and the tests that catch them

See [`plan.md`](plan.md) for the full execution plan and [`DIFFERENTIATION.md`](DIFFERENTIATION.md) for the strategy behind it.

## License

Released under the [MIT License](LICENSE).
