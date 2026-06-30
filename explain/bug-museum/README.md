# The Bug Museum

A curated gallery of **real bugs** found while building this system — each one a self-contained case file. This is the single most valuable interview asset in the repo: "tell me about a hard bug" is asked in every senior interview, and most candidates fumble it.

Distributed-systems bugs are normally invisible and non-reproducible. Because this project runs inside a [deterministic simulation](../../DIFFERENTIATION.md), every bug here is **reproducible from a single seed** — and once fixed, that seed becomes a permanent regression test.

## Template for each entry (`NN-short-name.md`)

```
# NN — <one-line title>

- **Found by:** <which test / checker, e.g. linearizability checker on the soak suite>
- **Seed:** 0x........   (reproduce with: make chaos SEED=0x........)
- **Phase:** <when it surfaced>

## Symptom
What went wrong, as observed (checker verdict, panic, divergent logs, lost commit...).

## The trace
The exact interleaving that triggers it (link to a rendered space-time diagram if available).

## Root cause
Which Raft invariant was violated, citing the paper figure/section. Why my code violated it.

## Fix
The change (link to the commit / diff).

## Regression test
The seed/test that now guards against it.

## Lesson
The general principle I'll carry forward.
```

## Index

1. [01 — A single-node cluster never elects a leader](01-single-node-never-elects.md) — a liveness gap: the `votes >= majority` promotion check only ran on peer replies, so a lone node (majority = 1) stayed a perpetual candidate. Surfaced in Phase 5.
2. [02 — Raft delivered internal log entries to the application](02-internal-entries-delivered-to-app.md) — config-change and election-no-op entries (nil `Command`) were handed to the KV layer, which panicked decoding them. The integration of two individually-tested features was broken. Phase 7/8.
3. [03 — Test harness data race under concurrent chaos](03-harness-data-race.md) — the cluster harness, fine for single-threaded driving, raced when the chaos nemesis and clients drove it at once. Caught by `-race` on the first chaos run. Phase 9.

_(Target: 5–10 genuine bugs, especially from Phase 9 chaos.)_
