# Deterministic Simulation Testing & the simulated network

> Phase 1. The contrarian move: build the *test infrastructure* before the
> system it tests.

## The problem

Distributed bugs are timing bugs. A split vote, a lost heartbeat, a stale read —
they appear only under a specific interleaving of message delays, drops, and
crashes. Run the test again and the interleaving changes, so the bug vanishes.
"Flaky" is just another word for "non-reproducible." You cannot debug what you
cannot reproduce.

## The idea

Put **every** source of non-determinism behind an interface and drive it from a
single seed:

| Non-determinism | Behind | In production | In simulation |
|---|---|---|---|
| Time | `clock.Clock` | `RealClock` | `MockClock` (advances only when told) |
| Network | `transport.Transport` | gRPC/TCP | `memnet.Network` (seeded drops/delays/partitions) |
| Disk | `storage.Persister` | file/BoltDB | `InMemoryPersister` (crash = drop RAM, keep blob) |

Because Raft sees only the interfaces, the *same Raft code* runs over real
sockets in production and inside the simulation in tests. A failing seed replays
the exact bug on demand — `make chaos SEED=0x...` (Phase 9).

## What Phase 1 built

- **`internal/transport/memnet`** — an in-process network implementing
  `transport.Transport`. Knobs, all seed-driven: `Partition`/`Heal`,
  `Disconnect`/`Connect`, `Crash`/`Restart`, and `SetReliable` (loss + latency).
  It follows MIT 6.824's *synchronous request/reply* `labrpc` model rather than
  an async message queue, because Raft's RPCs are themselves request/reply.
- **`test/cluster`** — a harness that spins up an N-node cluster on the network
  with one shared `MockClock`. It is decoupled from Raft via a `NodeFactory`, so
  it (and the network) are fully tested *before any Raft exists*. From Phase 2
  the factory wraps `raft.Make`.
- **`internal/rlog`** — the logging convention: every line carries
  `seed=… t=+… n=… term=… role=…`, timestamped against simulated time so a log is
  a replayable timeline.

## Two design subtleties worth knowing

- **Never hold the lock across a handler call.** `memnet` decides reachability
  and consults the RNG under its mutex, then releases it *before* invoking the
  peer's handler. Handlers may call back into the network without deadlocking —
  the same discipline Raft itself needs (never RPC while holding the lock).
- **Crash keeps the Persister.** `Crash` drops the in-RAM server; `Restart`
  rebuilds it from the *same* `Persister`. That is exactly how a real process
  reload behaves, and it's what makes crash-recovery testable (Phase 4).

## The success criterion, stated as a test

> "Partition node 2 away, then heal after 500 ms" — deterministically, from a
> seed.

See `test/cluster.TestDeterministicPartitionThenTimedHeal` and
`memnet.TestUnreliableDropPatternIsReproducibleFromSeed`.
