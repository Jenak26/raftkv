# 03 — Test harness data race under concurrent chaos

- **Found by:** `go test -race` on the very first run of the chaos suite
  (`test/chaos`), seeds 0 and 1.
- **Phase:** 9 (failure testing).

## Symptom

```
WARNING: DATA RACE
... testing.go: race detected during execution of test
--- FAIL: TestChaosLinearizable/seed=0
```

Linearizability itself passed on the seeds that completed (120 operations, all
linearizable) — the failure was the race detector, not a consensus violation.

## Root cause

The cluster harness, `test/cluster.Cluster`, was written for Phases 1–8, where a
single test goroutine drives it: call `Crash`, then `Restart`, then assert. Its
`servers`/`persisters` maps had **no lock**.

The chaos suite is the first thing to drive the cluster from several goroutines at
once: a **nemesis** goroutine calling `Crash`/`Restart`/`Partition` while
**client** goroutines call `Server(id)` and `Leaders()`, and the main goroutine
tears down. Concurrent read/write of the `servers` map is a data race (and, left
unchecked, a potential crash from concurrent map access).

## Fix

Add a mutex to `Cluster` guarding the maps and id list, and take it in `start`,
`Server`, `Crash`, `Restart`, `AddNode`, and `Leaders`
([`test/cluster/cluster.go`](../../test/cluster/cluster.go)). Handler calls that
take a node's own lock (`Kill`, `State`) are made *outside* the cluster lock — the
snapshot-the-map-then-act pattern — so the cluster lock is never held across a
call into a Raft node, mirroring the "never hold a lock across an RPC" discipline
of the consensus core itself.

## Regression test

The chaos suite runs under `-race`; the harness is now exercised concurrently on
every run.

## Lesson

Test infrastructure is production code for the purpose of correctness. The moment a
harness is driven concurrently it needs the same lock discipline as the system it
tests — and the race detector is the cheapest possible reviewer. It is fitting that
the suite built to find concurrency bugs found its first one in its own scaffolding.
