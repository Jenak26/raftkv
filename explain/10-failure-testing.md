# Failure Testing & Linearizability Verification

> Phase 9. This is what separates a toy from a credible distributed system, and
> it's the strongest interview material in the repo. We prove correctness
> *empirically*: drive the cluster under fault injection, record what clients
> actually observed, and check that the recorded history could have come from a
> single, sane, sequential machine.

## The methodology (Jepsen, made deterministic)

Four steps, on a loop:

1. **Generate** — several concurrent clients issue random `Put`/`Get`/`CAS`
   operations against the live KV cluster.
2. **Perturb** — a *nemesis* injects faults from a seed: network partitions
   (random splits), node crashes, and restarts, healing between them so the
   workload makes progress.
3. **Record** — every operation is logged with the real time it was invoked and
   the real time it returned (`test/linearizability`'s `Recorder`).
4. **Verify** — after the run, the recorded history is checked for
   **linearizability**.

Because the whole thing runs on the deterministic simulated network
(`internal/transport/memnet`), a failing run is replayable from its seed:
`make chaos SEED=<n>`. There are no "flaky" distributed bugs here — only bugs we
haven't reproduced yet.

## What "linearizable" means, and how we check it

A history is linearizable if there exists a total order of its operations that
(a) respects real time — if op A returned before op B was invoked, A precedes B —
and (b) is legal for the data model, where each operation appears to take effect
atomically at some single instant between its invocation and its response.

`test/linearizability` implements the **Wing & Gong / Lowe (WGL) algorithm** from
scratch (the same idea as the Porcupine library):

- Operations on different keys are independent, so the history is **partitioned by
  key** and each key checked against a single-register model (`Put`/`Get`/`CAS`/
  `Delete`).
- For one key, the checker searches for a valid linearization by repeatedly picking
  a **minimal** not-yet-placed operation — one that no other un-placed operation is
  forced to precede (no undone `j` with `j.Return < i.Call`) — tentatively placing
  it if the model accepts its observed result, and backtracking otherwise.
- **Memoization** on `(set-of-placed-ops, model-state)` collapses the otherwise
  exponential search; a register has very few distinct states, so this is fast for
  the modest per-key histories the workload produces.

The checker is itself unit-tested (`checker_test.go`): it must *accept* valid
sequential and concurrent histories and *reject* a stale read and an impossible
CAS. A checker you don't trust proves nothing.

## Why client retries don't corrupt the history

A subtle trap: under chaos, a client's write may time out at the transport even
though it committed. If the client gave up and the write silently took effect, a
later read would see a value with no recorded write — a *false* violation.

Phase 5's exactly-once semantics save us. The `Clerk` retries the *same*
`(ClientID, SeqNum)` until it gets a definite answer; the server's dedup table
collapses duplicates, so each logical operation commits exactly once and is recorded
exactly once, bracketed by its first invocation and its final response. Every
recorded operation corresponds to exactly one committed effect — so the history is
complete and the checker's verdict is sound.

## A bug the suite found immediately

The first time the nemesis, the clients, and the main goroutine all drove the
cluster at once, the race detector fired: the test harness's node map was not
safe for concurrent access (see [bug museum 03](bug-museum/03-harness-data-race.md)).
A separate latent bug — Raft delivering internal log entries (configuration changes,
the election no-op) to the application, which would panic trying to decode them —
was caught when the KV layer first met membership changes
([bug museum 02](bug-museum/02-internal-entries-delivered-to-app.md)).

## What breaks if you get it wrong

- **Testing only happy paths** — the interesting bugs only appear under partition +
  crash + reorder.
- **Non-reproducible chaos** — without a logged seed, a failure is a ghost. Every
  run here prints its seed.
- **Recording histories with wrong real-time boundaries** — the call/return bracket
  *is* the linearizability constraint; get it wrong and the checker lies.
- **Declaring victory after one green run** — distributed bugs are probabilistic;
  the suite sweeps many seeds, and `make chaos` runs more.

## How to run it

```
make chaos              # sweep several seeds under -race
make chaos SEED=3       # replay exactly one seed
```
