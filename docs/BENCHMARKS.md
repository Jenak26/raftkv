# Benchmarks — the cost of consensus

> Now that the system is *correct* (verified linearizable under chaos by the
> [`test/chaos`](../test/chaos) suite), we measure it. The goal is not a leaderboard
> number — it's to quantify the trade-offs the design deliberately exposes, and to
> know where the cost lives.

## How to run

```
make bench
# == go test -bench=. -benchmem -run='^$' ./bench/...
```

The harness (`bench/bench_test.go`) starts a **real 3-node cluster** over loopback
`net/rpc` with the real clock — the same code path a deployment uses, not the
simulated network. It warms up with one committed write (forcing an election and
committing the leader's no-op) so the first measured operation does not pay
election cost. Each benchmark then drives `b.N` operations through the `Clerk`.

## Methodology

- **Real transport, real clock.** Numbers include TCP round-trips and Go's
  scheduler — representative of a single-machine deployment, not a micro-benchmark
  of in-process function calls.
- **Sequential client.** `ns/op` is therefore per-operation *latency*; invert it
  for single-client throughput. (A parallel-client throughput harness is future
  work — see below.)
- **Warm-up excluded** via `b.ResetTimer()` after the first write.
- **Reproducible**: per-node RNGs are seeded; rerun to confirm variance is small.
- Report includes `-benchmem` (allocations), which surfaced a real inefficiency
  (below).

## Sample results

Measured on a 3-node loopback cluster, Windows, 11th-gen i7-1165G7, in-memory
persister (so these isolate consensus + transport cost, not disk):

| Benchmark | Latency (ns/op) | ≈ | Allocs/op |
|---|---:|---:|---:|
| `BenchmarkWrite` | ~1,223,000 | ~1.2 ms | 800 |
| `BenchmarkReadLinearizable` | ~143,000 | ~143 µs | 75 |
| `BenchmarkReadStale` | ~71,000 | ~71 µs | 28 |

(Your numbers will differ by machine; the *ratios* are the point.)

## What the numbers say

- **Linearizable reads cost about half a write, and ~2× a stale read.** A
  linearizable `Get` (ReadIndex) pays one heartbeat round to a majority to confirm
  leadership, but writes no log entry — so it is far cheaper than a write yet
  strictly more expensive than a local stale read. This is the
  consistency/latency knob from Phase 8 made concrete: pay ~2× latency for
  linearizability, or read stale-but-local and fast.
- **Writes are dominated by the replication round.** A write must reach a majority
  and be applied before the client hears success. At ~1.2 ms it is ~8× a
  linearizable read — consensus is not free, and this is where batching pays off.
- **The allocation spike on writes is a finding, not noise.** `BenchmarkWrite`
  shows ~5 MB/op at high `b.N`: with snapshotting disabled in the benchmark the log
  grows unbounded, and each replication round copies the entire pending suffix
  (`sliceFrom`). It illustrates exactly why **bounded logs (snapshotting, Phase 6)**
  and **batching** matter, and why a production deployment sets a snapshot
  threshold.

## Observability

Each node exposes a `raft.Metrics()` snapshot — term, role, commit/applied
indices, in-memory log size, snapshot anchor, configuration size, and counters for
elections started and leadership wins (a term-inflation / failover-churn signal).
This is the data a Prometheus exporter would publish; wiring it to a `/metrics`
endpoint and a Grafana dashboard is left as ops work.

## Optimization roadmap (future work)

These are deliberately **not yet implemented**: each one trades simplicity for
speed and must re-pass the chaos + linearizability suite before being trusted.
Correctness first.

1. **Batch** multiple client commands into one log append / `AppendEntries` —
   amortizes the consensus round over many operations; the single biggest write
   throughput win.
2. **Pipeline** `AppendEntries` (send the next batch before the previous is acked)
   to keep the replication link full.
3. **Avoid copying the whole suffix** every round (the allocation finding above):
   track per-peer progress and send only new entries, reuse buffers.
4. **Leader leases** for reads — skip the ReadIndex heartbeat within a
   time-bounded lease, closing most of the linearizable-vs-stale gap (at the cost
   of a clock-skew assumption).

Each must be gated behind a green `make chaos` run: an optimization that breaks
linearizability is not an optimization.
