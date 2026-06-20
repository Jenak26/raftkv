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

_(empty — entries arrive starting in Phase 2, and especially Phase 9. Target: 5–10 genuine bugs.)_
