# Raft-Based Distributed Key-Value Store in Go — Master Execution Plan

> **Goal:** Build a correct, well-tested, well-documented distributed key-value store backed by a **from-scratch implementation of the Raft consensus algorithm**, optimized for *maximum learning, interview readiness, and genuine distributed-systems understanding* — not for finishing fast.
>
> **Profile assumptions (locked):**
> - Builder is **new to both Go and distributed systems** → each phase front-loads the concepts before the code.
> - Raft is implemented **from scratch, full spec** (election, replication, persistence, snapshotting, membership changes).
> - Target depth: **a single Raft group done with deep correctness** — linearizable semantics + Jepsen-style failure testing. Sharding / product layer are stretch goals.
> - Pace: **learning-quality over speed.** Calendar estimates assume ~10–15 hrs/week; compress proportionally if full-time.
>
> **The North Star:** This should be a resume centerpiece. The differentiator is not "I used Raft" — it's *"I implemented Raft from the paper, found and fixed real correctness bugs under fault injection, and can explain every safety/liveness property and the trade-offs I made."*

---

## Table of Contents

1. [Guiding Principles](#1-guiding-principles)
2. [Project Architecture](#2-project-architecture)
3. [Folder Structure](#3-folder-structure)
4. [The `explain/` Folder Strategy](#4-the-explain-folder-strategy)
5. [Learning Roadmap](#5-learning-roadmap)
6. [Development Phases](#6-development-phases)
7. [Milestones](#7-milestones)
8. [Testing Strategy](#8-testing-strategy)
9. [Failure Testing Strategy](#9-failure-testing-strategy)
10. [Benchmarking Plan](#10-benchmarking-plan)
11. [Documentation Strategy](#11-documentation-strategy)
12. [Timeline Estimates](#12-timeline-estimates)
13. [Stretch Goals](#13-stretch-goals)
14. [Interview Readiness Map](#14-interview-readiness-map)
15. [Reference Materials](#15-reference-materials)

---

## 1. Guiding Principles

These rules shape every phase. Re-read them whenever you're tempted to cut a corner.

1. **Learn the concept before writing the code.** Each phase opens with a reading/notes step. You write an `explain/` note *before* the implementation, in your own words. If you can't explain it, you can't build it correctly.
2. **The Raft paper is law.** Implement against the *Extended Raft paper* (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm"). Every function should map to a figure or section. Keep Figure 2 pinned to your wall.
3. **Tests are not optional — they're the curriculum.** Distributed systems bugs are invisible without aggressive, randomized, repeatable testing. A feature isn't "done" until it survives the test harness under fault injection.
4. **Determinism + reproducibility.** Every test failure must be reproducible from a logged seed. Never debug a "flaky" distributed test by re-running and hoping.
5. **Commit small, commit often, write why.** Each commit is a learning artifact. Commit messages explain *why*, not *what*. Your git history becomes a story you can narrate in interviews.
6. **Correctness before performance.** No optimization until the linearizability checker is green under chaos. Then benchmark, then optimize, then re-verify.
7. **Write it down.** The `explain/` folder is half the value of this project. Interviewers test understanding, not typing speed.

---

## 2. Project Architecture

### 2.1 Layered view (bottom to top)

```
┌─────────────────────────────────────────────────────────────┐
│  Clients (CLI / HTTP / gRPC)                                  │
│  - Put / Get / Delete / CAS                                   │
│  - Retries, leader redirection, request IDs (dedup)           │
└───────────────────────────┬───────────────────────────────────┘
                            │  client protocol
┌───────────────────────────▼───────────────────────────────────┐
│  KV Service Layer (the "application" / state machine)          │
│  - Translates client ops → Raft log commands                   │
│  - Applies committed commands to the state machine             │
│  - Linearizable reads (ReadIndex / lease)                      │
│  - Client session table for exactly-once semantics             │
└───────────────────────────┬───────────────────────────────────┘
                            │  Propose(cmd) / applyCh
┌───────────────────────────▼───────────────────────────────────┐
│  Raft Consensus Core (the heart — built from scratch)          │
│  - Roles: Follower / Candidate / Leader                        │
│  - Leader election + heartbeats                                │
│  - Log replication (AppendEntries)                             │
│  - Commit index advancement + safety rules                     │
│  - Persistence (currentTerm, votedFor, log[])                  │
│  - Snapshotting / log compaction (InstallSnapshot)             │
│  - Membership changes (add/remove servers)                     │
└───────────────────────────┬───────────────────────────────────┘
                            │  RPC: RequestVote / AppendEntries / InstallSnapshot
┌───────────────────────────▼───────────────────────────────────┐
│  Transport / RPC Layer (pluggable)                             │
│  - Real:    Go net/rpc or gRPC over TCP                        │
│  - Test:    in-memory network with partition/delay/drop knobs  │
└───────────────────────────┬───────────────────────────────────┘
┌───────────────────────────▼───────────────────────────────────┐
│  Persistent Storage (pluggable)                                │
│  - Raft state + log: append-only file / BoltDB                 │
│  - Snapshots: file-based                                       │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Key architectural decisions (and why)

| Decision | Choice | Why it matters for learning & interviews |
|---|---|---|
| **Consensus** | Raft from scratch | The entire point. You learn safety/liveness by implementing them. |
| **State machine** | In-memory hash map, replaceable via interface | Keeps the KV layer simple so focus stays on consensus; the `StateMachine` interface shows you understand the *replicated state machine* model. |
| **Transport** | Interface with two impls: real RPC + simulated network | The simulated network is what makes deterministic fault injection possible. This is the secret weapon of the whole project. |
| **Storage** | `Persister`/`Storage` interface | Lets you start with simple files and later swap in BoltDB without touching Raft logic. Demonstrates clean boundaries. |
| **Concurrency model** | One big mutex per Raft node initially, with disciplined lock/unlock; clearly defined goroutines (ticker, applier, per-peer replicators) | Beginners *must* start simple. Fine-grained locking is a premature optimization that causes 90% of Raft bugs. |
| **Time** | Injected clock interface (real + mock) | Lets tests control timeouts deterministically. |

### 2.3 The concurrency map (memorize this — it's where bugs live)

Each Raft node runs a small, fixed set of goroutines:

- **`ticker` goroutine** — fires election timeout / heartbeat timeout, drives state transitions.
- **`applier` goroutine** — reads committed entries off an internal channel and applies them to the state machine, sending results back to clients.
- **`replicator` goroutines** (one per peer, leader only) — push `AppendEntries` to followers and react to responses.
- **RPC handler goroutines** — created by the transport when an inbound RPC arrives.

> **Interview gold:** Being able to draw this diagram and explain which goroutine holds the lock when, and how you avoid deadlocks (never hold the lock across an RPC call), is a senior-level signal.

---

## 3. Folder Structure

```
systems-project/                      # repo root
├── CLAUDE.md                         # the spec / source of truth (see note below)
├── plan.md                           # THIS FILE — the execution plan
├── README.md                         # what it is, how to run, a demo GIF, the headline results
├── go.mod / go.sum
├── Makefile                          # make test / race / lint / bench / chaos
│
├── cmd/                              # entry points (thin binaries)
│   ├── kvserver/                     # runs a single KV+Raft node
│   │   └── main.go
│   └── kvctl/                        # CLI client (put/get/del/cas, cluster status)
│       └── main.go
│
├── internal/                         # private packages (the meat)
│   ├── raft/                         # ★ the consensus core, built from scratch
│   │   ├── raft.go                   # node struct, public API (Start/Propose/Snapshot)
│   │   ├── state.go                  # roles, term/vote/log fields, transitions
│   │   ├── election.go               # RequestVote logic + election timer
│   │   ├── replication.go            # AppendEntries (leader & follower side)
│   │   ├── commit.go                 # commitIndex/matchIndex advancement, safety checks
│   │   ├── persist.go                # encode/decode persistent state
│   │   ├── snapshot.go               # InstallSnapshot + compaction
│   │   ├── membership.go             # add/remove server (single-server changes)
│   │   ├── log.go                    # the Raft log abstraction (with snapshot offset)
│   │   ├── rpc_types.go              # request/response structs
│   │   └── raft_test.go              # unit + integration tests (see testing strategy)
│   │
│   ├── kv/                           # the application / state machine layer
│   │   ├── server.go                 # KV server: maps client ops → Raft, applies commits
│   │   ├── statemachine.go           # StateMachine interface + in-memory map impl
│   │   ├── session.go                # client session table (exactly-once dedup)
│   │   ├── readindex.go              # linearizable read path
│   │   └── kv_test.go
│   │
│   ├── transport/                    # RPC layer (pluggable)
│   │   ├── transport.go              # Transport interface
│   │   ├── grpc_transport.go         # real network impl
│   │   └── memnet/                   # ★ simulated network for deterministic tests
│   │       ├── network.go            # reliable/unreliable, partition, delay, drop, reorder
│   │       └── network_test.go
│   │
│   ├── storage/                      # persistence (pluggable)
│   │   ├── storage.go                # Persister/Storage interface
│   │   ├── file_storage.go           # append-only file impl
│   │   └── bolt_storage.go           # (later) BoltDB impl
│   │
│   └── clock/                        # injectable time (real + mock)
│       └── clock.go
│
├── test/                             # cross-cutting test harnesses
│   ├── cluster/                      # spin up N-node clusters in-process
│   ├── chaos/                        # fault-injection scenarios
│   └── linearizability/              # history recorder + checker (Porcupine-style)
│
├── bench/                            # benchmarking harness + scripts
│   ├── bench_test.go
│   └── scenarios/                    # throughput, latency, recovery-time scripts
│
├── explain/                          # ★ YOUR LEARNING NOTES (see section 4)
│   ├── 00-glossary.md
│   ├── 01-replicated-state-machines.md
│   ├── 02-leader-election.md
│   ├── 03-log-replication.md
│   ├── 04-safety-proofs.md
│   ├── 05-persistence-and-recovery.md
│   ├── 06-snapshotting.md
│   ├── 07-membership-changes.md
│   ├── 08-linearizable-reads.md
│   ├── 09-bugs-i-found.md            # the most valuable file for interviews
│   └── diagrams/                     # state machine + message flow diagrams
│
└── docs/                            # polished, outward-facing documentation
    ├── ARCHITECTURE.md
    ├── DESIGN-DECISIONS.md           # ADR-style records
    ├── TESTING.md
    ├── BENCHMARKS.md
    └── DEMO.md
```

> **On `CLAUDE.md`:** the folder is currently empty and has no `CLAUDE.md`. This plan treats your stated idea as the spec. A companion `CLAUDE.md` (the formal spec / source of truth) should be generated next so future sessions and reviewers have one canonical contract.

**Why this structure:**
- `internal/` enforces real package boundaries — interviewers notice when consensus logic isn't tangled into HTTP handlers.
- `raft/` is split by *concept* (election, replication, commit, snapshot, membership), not by arbitrary size. Each file maps to a section of the paper.
- `transport/memnet/` and `test/linearizability/` are the infrastructure that *makes the learning possible*. Investing here early pays off in every later phase.
- `explain/` is deliberately a first-class folder, not buried in a wiki.

---

## 4. The `explain/` Folder Strategy

This is the single highest-leverage habit in the whole plan. **Distributed systems interviews test explanation, not code.** The `explain/` folder forces you to convert implementation into understanding.

### Rules
1. **Write the note before the code.** Before implementing leader election, write `02-leader-election.md` in your own words: what problem it solves, the invariants, the message flow, the failure cases. If gaps appear, you've found what to study.
2. **One file per concept**, numbered to read in order.
3. **Every file answers four questions:** *What problem does this solve? What are the invariants/safety properties? How does the message flow work (with a diagram)? What breaks if I get it wrong?*
4. **`09-bugs-i-found.md` is the crown jewel.** Every real bug you hit (split votes, log divergence, lost commits, deadlocks) gets a write-up: symptom → root cause → the invariant you violated → the fix → the test that now catches it. *This is what you talk about in interviews.* Nothing signals real experience like "let me tell you about the time my commitIndex advanced incorrectly."
5. **Diagrams over prose** where possible. Hand-drawn-then-scanned or Mermaid/ASCII. A state-transition diagram and an `AppendEntries` sequence diagram are worth 1000 words.
6. **Link explanations to code.** Reference `internal/raft/election.go:NN` so the note and code stay synced.

### Suggested contents
- `00-glossary.md` — term, log, index, commit, quorum, linearizability, etc. Precise definitions.
- `01-replicated-state-machines.md` — the mental model the whole field is built on.
- `02`–`08` — one per Raft subsystem (see folder layout).
- `04-safety-proofs.md` — the five Raft safety properties (Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety) in your own words, with *why* each holds.
- `09-bugs-i-found.md` — your debugging war stories.

---

## 5. Learning Roadmap

A dependency-ordered list of what to learn, and *when*. Don't binge it all up front — learn each block right before the phase that needs it (just-in-time learning sticks better).

### Track A — Go fundamentals (before Phase 1)
- Go syntax, structs, interfaces, error handling, slices/maps semantics.
- Packages, modules, `go test`, table-driven tests.
- **Concurrency (critical):** goroutines, channels, `select`, `sync.Mutex`/`RWMutex`, `sync.WaitGroup`, `sync.Cond`, `context.Context`.
- The **race detector** (`go test -race`) — you will live in this.
- Resources: *A Tour of Go*, *Effective Go*, "Go Concurrency Patterns" (Rob Pike talk).

### Track B — Distributed systems fundamentals (parallel with Phase 0–1)
- Why distributed systems: replication for fault tolerance and availability.
- **Failure models:** crash-stop, crash-recovery, network partitions, message loss/reorder/duplication, asynchrony. (Not Byzantine — Raft assumes non-Byzantine.)
- **The replicated state machine model.**
- **CAP theorem** (and its nuances/limits), and **PACELC**.
- **Consistency models:** linearizability vs sequential vs causal vs eventual. Be able to define linearizability precisely.
- **Consensus:** the problem statement, FLP impossibility (consensus can't be *both* safe and live under pure asynchrony), and how Raft sidesteps it (randomized timeouts for liveness, never sacrificing safety).
- Quorums and majority intersection.
- Resources: MIT **6.824 / 6.5840** lectures (free on YouTube) — the single best resource and literally built around building Raft + a KV store.

### Track C — Raft, deeply (Phases 2–8)
- **Read the Extended Raft paper twice.** First for intuition, then with Figure 2 as a checklist.
- Watch the Raft author talks; play with the **raft.github.io visualization**.
- Understand each piece *before* coding it: election → replication → safety → persistence → snapshotting → membership → reads.

### Track D — Testing & verification (Phases 1, 9)
- Property-based / randomized testing concepts.
- **Linearizability checking** (Porcupine library, Jepsen/Knossos ideas).
- Deterministic simulation testing (FoundationDB / TigerBeetle philosophy) — aspirational but worth knowing the vocabulary.

### Track E — Performance & ops (Phases 10+)
- Go benchmarking (`testing.B`), `pprof`, flame graphs.
- Metrics (Prometheus), structured logging.
- Latency vs throughput, tail latency, batching, pipelining.

---

## 6. Development Phases

> Each phase follows the same template: **Why it exists · Concepts to learn · What to build · Common mistakes · Success criteria · Interview topics.** Do not start a phase's code until its `explain/` note exists and its prerequisites' success criteria are met.

---

### Phase 0 — Foundations & Scaffolding
**Why it exists:** You're new to both Go and distributed systems. Trying to learn Go *while* debugging consensus is a recipe for despair. This phase builds the muscle and the skeleton so later phases are about *ideas*, not syntax.

**Concepts to learn:**
- Go concurrency primitives end-to-end (goroutines, channels, mutexes, `sync.Cond`, `context`).
- The race detector and how to read its reports.
- The replicated state machine model; failure models; what consensus is and why it's hard (FLP at an intuition level).
- Quorums and majority intersection.

**What to build:**
- `go.mod`, `Makefile` (`test`, `race`, `lint`, `bench`, `chaos` targets), linting (`golangci-lint`), CI (GitHub Actions running `-race`).
- Package skeletons with interfaces only: `Transport`, `Storage`/`Persister`, `StateMachine`, `Clock`.
- A tiny concurrency warm-up: implement a thread-safe counter and a worker pool with `-race` clean. (Throwaway, but proves your tooling works.)
- `explain/00-glossary.md` and `explain/01-replicated-state-machines.md`.

**Common mistakes:**
- Skipping the Go concurrency drills and paying for it later in every Raft race.
- Designing elaborate interfaces before you understand the domain — keep them minimal and let them evolve.
- Not wiring `-race` into CI from day one.

**Success criteria:**
- `make test race lint` is green on an empty skeleton.
- You can explain (in `explain/01`) the replicated-state-machine model and why a majority quorum guarantees overlap.

**Interview topics prepared:** Go concurrency model; why consensus is needed; failure models; quorum intersection; "walk me through how you set up a Go project."

---

### Phase 1 — Simulated Network + Test Harness (built *before* Raft)
**Why it exists:** This is the contrarian, high-payoff move. You build the *testing infrastructure first*. A deterministic in-memory network with knobs for partitions, delays, drops, and reordering is what turns "impossible-to-debug flaky distributed bug" into "reproducible failure from seed 42." Almost every later phase depends on it.

**Concepts to learn:**
- How to model a network as a Go abstraction.
- Deterministic randomness (seeded RNG) for reproducibility.
- Test cluster orchestration in a single process.

**What to build:**
- `transport/memnet`: an in-process network connecting N nodes via channels, with controls to: enable/disable connectivity (partitions), add latency, drop/duplicate/reorder messages, and crash/restart nodes — all driven by a seeded RNG.
- `test/cluster`: helpers to spin up an N-node cluster, find the leader, disconnect/reconnect nodes, crash/restart.
- A logging convention: every node tags logs with `[node id][term][role]`; logs are timestamped and seed-stamped.

**Common mistakes:**
- Making the simulated network non-deterministic (using unseeded `rand` or wall-clock time) — then failures can't be reproduced.
- Building too little: if you can't inject partitions and restarts, you can't test the interesting cases.

**Success criteria:**
- You can write a test that says "partition node 2 away, then heal after 500ms" and have it run deterministically from a seed.
- The harness can crash and restart a node, preserving its persistent storage.

**Interview topics prepared:** "How do you test a distributed system?"; deterministic simulation testing; the value of fault injection; why reproducibility matters.

---

### Phase 2 — Leader Election
**Why it exists:** Raft's foundation. A cluster must agree on exactly one leader per term, even as nodes crash and networks partition. Election Safety is the first safety property you'll implement and defend.

**Concepts to learn:**
- Raft roles (Follower/Candidate/Leader) and the state machine between them.
- Terms as a logical clock; the rule "higher term wins, step down."
- `RequestVote` RPC; voting rules; the up-to-date log check (foreshadowing replication).
- Randomized election timeouts → why they break symmetry and provide *liveness* (this is Raft's answer to FLP).
- Heartbeats (empty `AppendEntries`).

**What to build:**
- Node state struct: `currentTerm`, `votedFor`, role, election timer.
- `ticker` goroutine driving election timeouts and heartbeats.
- `RequestVote` handler + candidate vote-collection logic.
- Step-down logic on seeing a higher term.

**Common mistakes:**
- Holding the mutex while making an outbound RPC (classic deadlock). **Rule: never call an RPC while holding the lock.**
- Forgetting to reset the election timer at the right moments (on granting a vote, on valid `AppendEntries` from current leader).
- Fixed (non-randomized) timeouts → perpetual split votes.
- Not stepping down to follower immediately when a higher term is observed.
- Counting votes incorrectly across terms (a vote is only valid for the term it was requested in).

**Success criteria:**
- A 3- and 5-node cluster elects exactly one leader and re-elects after the leader is partitioned/killed.
- Under repeated random partitions, **never two leaders in the same term** (Election Safety holds across thousands of seeded runs).
- `go test -race` clean.

**Interview topics prepared:** Leader election; terms as logical clocks; how randomized timeouts give liveness; split-brain prevention; FLP and how Raft works around it; deadlock avoidance with locks + RPCs.

---

### Phase 3 — Log Replication
**Why it exists:** Election alone doesn't store data. This phase makes the leader replicate client commands to followers and decide when an entry is *committed* (safely applied). This is the core of "agreement."

**Concepts to learn:**
- The Raft log: entries with `(term, index, command)`.
- `AppendEntries` as both heartbeat and replication.
- The **Log Matching Property** and how `prevLogIndex`/`prevLogTerm` enforce it.
- `nextIndex`/`matchIndex` per follower; the consistency-check/backtracking mechanism (and the optimization to skip a whole term).
- `commitIndex` advancement via majority `matchIndex`, **plus the critical rule: a leader only commits entries from its *current* term directly** (Figure 8 in the paper).
- The `applyCh`: committed entries flow to the state machine via the `applier` goroutine.

**What to build:**
- Leader: `Propose(command)` appends to its log and triggers replication; per-peer `replicator` goroutines.
- Follower: `AppendEntries` handler that does the consistency check, truncates conflicting entries, appends new ones, and advances its commit index.
- Commit-index advancement on the leader using `matchIndex` + the current-term rule.
- The `applier` goroutine delivering committed commands in order.

**Common mistakes:**
- Truncating the follower's log too eagerly (deleting entries that actually match — only delete on a *conflict*).
- Committing an entry from a *previous* term just because it's on a majority (the Figure 8 bug — leads to committed-then-lost entries). This is *the* subtle Raft bug; study it.
- Off-by-one errors in log indices (especially once snapshots add an offset later).
- Applying entries out of order or applying the same entry twice.
- Re-sending `AppendEntries` in a tight loop and saturating the network.

**Success criteria:**
- Commands proposed to the leader are replicated and applied **in the same order on every node**.
- Logs converge after partitions heal; divergent uncommitted entries are correctly overwritten.
- A committed entry is **never lost**, even across leader changes (verified by the test harness over many seeds).

**Interview topics prepared:** Log replication; Log Matching Property; commit rules and the Figure 8 subtlety; why you can't commit prior-term entries by count alone; how followers reconcile divergent logs.

---

### Phase 4 — Persistence & Crash Recovery
**Why it exists:** A node that forgets `currentTerm`/`votedFor`/`log` on restart can violate safety (e.g., vote twice in a term → two leaders). Durability is what makes the crash-*recovery* model safe.

**Concepts to learn:**
- Which state *must* be persisted before responding to an RPC: `currentTerm`, `votedFor`, `log[]`.
- The write-before-reply ordering (persist, *then* send the response).
- Crash-recovery semantics; idempotent recovery.
- Encoding/serialization (`encoding/gob` or protobuf); fsync trade-offs.

**What to build:**
- `storage.Persister`: durably save/load the three persistent fields.
- Hook persistence into every state mutation that requires it (term change, vote, log append/truncate).
- Recovery path: on startup, load persisted state and resume as a follower.
- Wire crash+restart into the test harness so a restarted node reloads from disk.

**Common mistakes:**
- Persisting *after* replying to an RPC (a crash in between breaks safety).
- Forgetting to persist on *every* mutation (e.g., updating `votedFor` but not flushing).
- Ignoring fsync and being surprised by data loss on real crashes (note the durability/perf trade-off even if you keep it simple).

**Success criteria:**
- Kill and restart any/all nodes mid-workload; the cluster recovers and **no committed entry is lost**.
- Election Safety still holds across restarts (no double-voting).
- Linearizability checker (Phase 9, but stub it here) stays green across crash-restart scenarios.

**Interview topics prepared:** Crash-recovery model; what Raft persists and why; write-ahead/durability ordering; fsync vs throughput trade-offs; how persistence prevents split-brain.

---

### Phase 5 — KV State Machine, Client Protocol & Exactly-Once Semantics
**Why it exists:** Now Raft becomes a *database*. This phase builds the application layer on top of the log and confronts the messy realities of client interaction: leader redirection, retries, and duplicate detection.

**Concepts to learn:**
- The `StateMachine` interface and how committed log entries drive it.
- Client/leader interaction: how clients find the leader, handle `NotLeader` redirects, and retry on timeout.
- **The duplicate problem:** a client retries, but the original request *did* commit → the command applies twice (e.g., double-increment). Solved with **client sessions + request IDs (dedup table)** to get exactly-once *semantics* on top of at-least-once delivery.
- Why naive reads from the leader's map can return **stale data** (motivates Phase 8).

**What to build:**
- `kv.StateMachine`: in-memory map supporting `Put`, `Get`, `Delete`, `CAS`.
- `kv.Server`: turns client RPCs into Raft proposals, waits for the entry to be applied, returns the result.
- Client session table: `(clientID, seqNum) → result` dedup.
- `kvctl` CLI + a real `Transport` (gRPC) so you can drive a live cluster from the terminal.
- Leader redirection and client-side retry logic.

**Common mistakes:**
- No dedup → retried writes double-apply.
- Blocking forever waiting for an apply that will never come (the entry was overwritten after a leader change) — you need to detect this and tell the client to retry.
- Letting the dedup table grow unbounded (tie it to client sessions / lowest-seq tracking).
- Serving reads directly from the map and calling it linearizable (it isn't — yet).

**Success criteria:**
- A live multi-node cluster serves `put`/`get`/`del`/`cas` via the CLI.
- Retries under failures never double-apply (verified with a counter/append workload).
- Clients transparently follow leader changes.

**Interview topics prepared:** Replicated state machines in practice; at-least-once vs exactly-once vs at-most-once; idempotency and dedup tables; client session design; why reads are subtle.

---

### Phase 6 — Log Compaction & Snapshotting
**Why it exists:** The log grows forever otherwise — restarts get slow, disk fills, and replaying millions of entries is absurd. Snapshotting lets the state machine's current state replace a prefix of the log. It also fixes the "log offset" indexing that haunts beginners.

**Concepts to learn:**
- Snapshots = serialized state machine state + the `(lastIncludedIndex, lastIncludedTerm)` it covers.
- Log truncation and the **log offset** (index 0 of your slice no longer means log index 0).
- The `InstallSnapshot` RPC: how a leader brings a *very* lagging follower up to date when it no longer has the needed log entries.
- Coordinating snapshots between the application layer (which owns the state) and Raft (which owns the log).

**What to build:**
- `Snapshot(index, stateBytes)` API: app tells Raft "I've snapshotted up to index N," Raft discards the log prefix.
- Log abstraction updated to handle the offset cleanly (all index math goes through helpers).
- `InstallSnapshot` RPC (leader and follower sides) + persistence of snapshots.
- App-side: produce and restore from snapshots.

**Common mistakes:**
- Off-by-one chaos after truncation — *every* index access must respect the offset. (Centralize index math in `log.go`.)
- Forgetting to persist the snapshot and the truncated log atomically-ish (recover consistently).
- Applying an `InstallSnapshot` that's actually *older* than what the follower already has.
- Deadlocks from the app→Raft→app callback cycle during snapshot/restore.

**Success criteria:**
- Under a long-running workload, log size stays bounded; restarts are fast.
- A node that's been offline long enough to fall behind the snapshot catches up via `InstallSnapshot`.
- All earlier correctness tests still pass (no regressions from index math).

**Interview topics prepared:** Log compaction; snapshotting design; `InstallSnapshot`; the app/consensus boundary; why naive logs don't scale; index-offset bookkeeping.

---

### Phase 7 — Cluster Membership Changes
**Why it exists:** Real clusters add/remove nodes without downtime. Done naively, you can momentarily have **two disjoint majorities → two leaders → split brain**. This phase teaches the most safety-critical configuration logic in Raft.

**Concepts to learn:**
- Why a direct "switch from old config to new config" is unsafe (overlapping majorities).
- **Single-server changes** (add or remove one at a time — the simpler approach from the Raft author's thesis) vs **joint consensus** (`C-old,new`). Implement single-server changes; *understand* joint consensus.
- Configuration entries stored *in the log* and applied as soon as seen (not when committed).
- Edge cases: removed leader steps down; new servers join as non-voting "learners" to catch up first.

**What to build:**
- `AddServer` / `RemoveServer` operations that append a configuration change entry to the log.
- Logic to use the latest config in the log (even uncommitted) for quorum decisions.
- (Recommended) non-voting learner phase so a new node catches up before counting toward quorum.

**Common mistakes:**
- Allowing two concurrent configuration changes (must be serialized — only one in flight).
- Using the committed config rather than the latest-in-log config for membership decisions.
- Adding a fresh, empty node directly to the voting set and stalling commits while it catches up.
- Not handling the leader removing itself.

**Success criteria:**
- You can grow a 3-node cluster to 5 and shrink back to 3 **while serving traffic**, with no lost commits and no split brain.
- Fault-injection during membership changes stays linearizable.

**Interview topics prepared:** The hardest Raft topic — membership changes; joint consensus vs single-server; why overlapping majorities matter; learners/non-voting members; "how does etcd/Consul reconfigure safely?"

---

### Phase 8 — Linearizable Reads
**Why it exists:** Up to now, reads either go through the log (correct but slow) or read the leader's map (fast but possibly **stale**, because a deposed leader doesn't know it). This phase delivers *fast* reads that are still linearizable — a concept interviewers love to probe.

**Concepts to learn:**
- Why a stale leader can serve stale reads (it hasn't heard about the new leader yet).
- **ReadIndex:** record the current commit index, confirm leadership via a heartbeat round to a majority, wait until applied up to that index, then read. Linearizable, no log write.
- **Leader leases:** an optimization that avoids the heartbeat round using time bounds (and its clock-skew risks).
- The spectrum: linearizable reads vs bounded-staleness vs follower reads (and the consistency/perf trade-off).

**What to build:**
- ReadIndex read path in `kv/readindex.go`.
- A read-consistency mode flag: `linearizable` (default) vs `stale` (fast, follower-served) so you can *demonstrate the trade-off* in benchmarks.

**Common mistakes:**
- Assuming "read from leader = linearizable" (the central misconception this phase fixes).
- Forgetting the leader must commit a no-op entry on election before it can safely serve ReadIndex reads (it must know its commit index for the current term).
- Implementing leases without accounting for clock skew (and not stating the assumption).

**Success criteria:**
- Linearizability checker passes with ReadIndex reads under chaos.
- Benchmarks show the latency/throughput difference between linearizable and stale reads — and you can explain it.

**Interview topics prepared:** Linearizability (define it!); why leader reads can be stale; ReadIndex vs lease reads vs follower reads; consistency/performance trade-offs; the no-op-on-election trick.

---

### Phase 9 — Failure Testing & Linearizability Verification (the credibility phase)
**Why it exists:** This is what separates a toy from a *credible* distributed system — and it's your strongest interview material. You prove correctness empirically by recording operation histories under chaos and checking them for linearizability.

**Concepts to learn:**
- Linearizability *checking*: record a history of `(invocation, response)` events with real-time ordering; a checker (e.g., **Porcupine**) searches for a valid sequential ordering.
- Jepsen's methodology: generate operations, inject nemeses (partitions, crashes, clock skew, pauses), then analyze the history.
- Property-based and randomized testing.
- Reproducibility: every failing run prints its seed.

**What to build:**
- `test/linearizability`: a history recorder + integrate **Porcupine** (or a hand-rolled checker for the learning, then Porcupine for rigor).
- `test/chaos`: a nemesis driver that, during a workload, randomly partitions/crashes/restarts/delays nodes from a seed.
- A suite of named scenarios (see Section 9) runnable via `make chaos`.
- Long-running soak test (run for minutes/hours, thousands of ops).
- Document every bug found in `explain/09-bugs-i-found.md`.

**Common mistakes:**
- Testing only happy paths.
- Non-reproducible chaos (no seed logging).
- Recording histories incorrectly (wrong real-time boundaries) → false checker verdicts.
- Declaring victory after one green run instead of thousands of seeds.

**Success criteria:**
- Thousands of randomized chaos runs pass the linearizability checker.
- At least a few **real bugs found, fixed, and written up** with the test that now catches each.
- A documented, reproducible chaos suite anyone can run with one command.

**Interview topics prepared:** *The* differentiator — "I verified linearizability under fault injection." Jepsen methodology; linearizability checking; how you debug distributed bugs; specific war stories from `09-bugs-i-found.md`.

---

### Phase 10 — Benchmarking, Observability & Tuning
**Why it exists:** Now that it's *correct*, measure it, observe it, and make it faster — then re-verify correctness. This adds the performance-engineering and ops dimension interviewers ask about for senior roles.

**Concepts to learn:**
- Throughput vs latency; tail latency (p50/p99/p999); Little's Law intuition.
- **Batching** and **pipelining** `AppendEntries` to amortize consensus overhead.
- Go profiling: `pprof`, flame graphs, allocation profiling, lock contention.
- Metrics (Prometheus) and structured logging.

**What to build:**
- `bench/`: throughput/latency harness for write-heavy, read-heavy, mixed workloads; recovery-time measurement (how fast after leader loss).
- Prometheus metrics: commit latency, log size, leader changes, RPC counts, apply lag.
- Optimizations (each gated behind re-running the chaos+linearizability suite): batch log appends, pipeline replication, tune timeouts.
- `docs/BENCHMARKS.md` with methodology, numbers, and graphs.

**Common mistakes:**
- Optimizing before correctness is verified.
- Benchmarking non-determinism / not pinning the workload, so numbers aren't comparable.
- Adding batching/pipelining and silently breaking ordering or commit rules (must re-verify).
- Reporting only averages, hiding tail latency.

**Success criteria:**
- Reproducible benchmark numbers with documented methodology.
- A measured before/after improvement from batching/pipelining, *with correctness re-verified*.
- Dashboards/metrics that visualize a leader failover live.

**Interview topics prepared:** Performance engineering; batching/pipelining in consensus; tail latency; profiling Go; observability; "how would you make Raft faster?"

---

## 7. Milestones

| Milestone | Definition of done | Demo you can show |
|---|---|---|
| **M0 — Skeleton** (end P0–P1) | CI green with `-race`; deterministic sim-network + cluster harness works. | "I can spin up a 5-node cluster in a test and partition it deterministically." |
| **M1 — It elects** (end P2) | Stable leader election under partitions/crashes; Election Safety holds over thousands of seeds. | Live: kill the leader, watch a new one get elected. |
| **M2 — It replicates** (end P3) | Logs converge; committed entries never lost across leader changes. | Propose values, kill leader, show data survives. |
| **M3 — It survives crashes** (end P4) | Crash/restart any node, no committed data lost. | Kill all nodes, restart, data intact. |
| **M4 — It's a database** (end P5) | Live cluster serves put/get/del/cas via CLI with exactly-once semantics. | Terminal demo of the KV store with retries. |
| **M5 — It scales operationally** (end P6–P7) | Bounded logs via snapshots; membership changes live without split brain. | Grow 3→5 nodes while serving traffic. |
| **M6 — It's fast *and* correct** (end P8) | Linearizable ReadIndex reads; trade-off benchmarked. | Show linearizable vs stale read latency. |
| **M7 — It's credible** (end P9) | Thousands of chaos runs pass linearizability; real bugs documented. | `make chaos` runs green; walk through `09-bugs-i-found.md`. |
| **M8 — It's measured** (end P10) | Documented benchmarks; observability; a verified optimization. | Grafana dashboard during a failover; before/after numbers. |

> **Resume-ready point:** M7. Everything after is polish/depth. M4 is the minimum "it works" demo; M7 is the "I actually understand distributed systems" proof.

---

## 8. Testing Strategy

A layered pyramid — each layer catches different bugs.

1. **Unit tests** — pure logic: vote granting rules, log consistency check, commit-index math, snapshot index arithmetic. Table-driven. Fast, run on every save.
2. **Component/integration tests (in-process cluster)** — the bread and butter. Spin up N nodes over the simulated network; assert election, replication, persistence properties. Model these on the **MIT 6.5840 Raft test suite** (`TestInitialElection`, `TestReElection`, `TestBasicAgree`, `TestFailAgree`, `TestRejoin`, `TestBackup`, `TestPersist*`, `TestSnapshot*`, etc.) — re-implement equivalents; they're a battle-tested checklist of what can go wrong.
3. **Property/randomized tests** — random operation sequences + random faults, asserting invariants (one leader per term; logs never diverge on committed entries; applied order identical across nodes).
4. **Linearizability tests** — record histories, check with Porcupine (Phase 9).
5. **Chaos/soak tests** — long runs under continuous nemesis activity (Phase 9).
6. **End-to-end tests** — real gRPC transport, real storage, real CLI driving a real cluster (Phase 5+).

**Cross-cutting rules:**
- **Always run with `-race`.** Make it the default in CI and `make test`.
- **Every test is seeded and reproducible.** Print the seed; allow `SEED=… make test` to replay.
- **Run flaky-looking tests in a loop** (`go test -run X -count=100`) — in distributed code, a 1-in-50 failure is a real bug, not flakiness.
- **No feature is "done" until its tests pass under fault injection**, not just happy path.

`docs/TESTING.md` documents how to run each layer and what each guards.

---

## 9. Failure Testing Strategy

The heart of credibility. The simulated network (Phase 1) makes all of this deterministic and reproducible.

**Failure modes to inject:**
- **Network partitions** — isolate a node, isolate a minority, isolate the leader, symmetric and asymmetric partitions.
- **Node crashes** — crash-stop and crash-recovery (restart, reload persistent state).
- **Message faults** — drop, duplicate, delay, reorder.
- **Clock issues** — skew/pauses (matters for lease reads; also simulates GC pauses).
- **Slow nodes** — a node that's alive but lagging.

**Named nemesis scenarios (each seeded, in `test/chaos/scenarios`):**
| Scenario | What it stresses |
|---|---|
| `partition-leader` | Re-election; old leader must not commit in isolation. |
| `partition-minority` | Minority can't make progress; majority continues. |
| `flapping-partitions` | Repeated heal/break; term inflation handled. |
| `crash-restart-loop` | Persistence correctness. |
| `lagging-follower` | Snapshot / `InstallSnapshot` path. |
| `membership-under-chaos` | Reconfiguration safety while faults occur. |
| `slow-disk` | Persistence latency doesn't break correctness. |
| `soak` | Hours of mixed ops + random nemeses. |

**The verification loop:**
1. Generate a random workload of client ops (with concurrency).
2. Run a nemesis from a seed alongside it.
3. Record the operation history.
4. Heal everything, drain, and assert: cluster converges, and the **history is linearizable**.
5. On failure: the seed reproduces it. Debug, fix, add a regression test, write it up in `explain/09-bugs-i-found.md`.

**Success bar:** thousands of seeded runs across all scenarios pass; the suite runs from a single `make chaos`; bugs found are documented with root cause + invariant violated + fix + catching test.

---

## 10. Benchmarking Plan

**What to measure:**
- **Throughput** — committed ops/sec for write-heavy, read-heavy (linearizable vs stale), and mixed workloads, across cluster sizes (3 vs 5 nodes).
- **Latency** — p50/p99/p999 commit latency; client-observed latency. Always report tails, not just averages.
- **Recovery time** — time-to-new-leader after leader loss; time for a lagging node to catch up via snapshot.
- **Scalability** — how throughput/latency change from 3→5→7 nodes (and *why* it gets slower, not just that it does).
- **Resource cost** — CPU, allocations (via `pprof`), log/disk growth over time.

**Methodology (document in `docs/BENCHMARKS.md`):**
- Fixed workload generators with seeds; warm-up period excluded; multiple runs reported with variance.
- Same hardware/config across compared runs; record machine specs.
- Use Go's `testing.B` for micro-benchmarks (e.g., log append, encode/decode) and a custom harness for end-to-end cluster throughput.
- Profile with `pprof`; include at least one flame graph and one lock-contention analysis.

**Optimizations to evaluate (each must re-pass the chaos+linearizability suite):**
- Batching multiple client commands per log append / `AppendEntries`.
- Pipelining `AppendEntries` (don't wait for ack before sending the next).
- Tuning heartbeat/election timeouts and their effect on recovery time vs spurious elections.
- (Stretch) parallel disk persistence / group commit.

**Deliverable:** a benchmarks doc with graphs, a clear methodology section, and a short "what I learned about the cost of consensus" narrative.

---

## 11. Documentation Strategy

Two audiences, two doc sets — keep them distinct.

**A) `explain/` — for *you* and for interview prep (learning notes).** See Section 4. Written *as you learn*, in your own words, concept-first, with the bugs log as the centerpiece.

**B) `docs/` — for *readers* (recruiters, reviewers, your future self).** Polished, outward-facing:
- `README.md` (root) — the 30-second pitch: what it is, a demo GIF/asciinema, headline results ("survives partitions; thousands of chaos runs pass linearizability; X ops/sec"), quickstart, and links into `docs/`. This is what gets you the interview — make it excellent.
- `docs/ARCHITECTURE.md` — the layered design, the concurrency map, component responsibilities, diagrams.
- `docs/DESIGN-DECISIONS.md` — **ADR-style** records: each significant decision (single-mutex vs fine-grained locking, single-server vs joint-consensus membership, ReadIndex vs lease reads) with context, options, choice, and trade-offs. *Interviewers love ADRs* — they show you reason about trade-offs.
- `docs/TESTING.md` — the test pyramid, how to run each layer, the chaos suite.
- `docs/BENCHMARKS.md` — methodology + results (Section 10).
- `docs/DEMO.md` — scripted live demos (kill the leader, grow the cluster) you can reproduce in an interview.

**Process rules:**
- Update docs *with* the code (a phase isn't done until its docs are updated) — use the `document-release`-style discipline.
- Diagrams in Mermaid or committed images; keep them in sync with code.
- Git history is documentation too: small, well-messaged commits that narrate the build.

---

## 12. Timeline Estimates

Assumes **~10–15 hrs/week** (part-time, sustained) and *learning-first* pacing. Multiply down by ~3–4× for full-time. Ranges reflect that you're new to both Go and DS — the early phases especially may run long, and that's *expected and fine*.

| Phase | Focus | Part-time estimate | Notes |
|---|---|---|---|
| P0 | Foundations & scaffolding | 1–2 weeks | Includes Go concurrency drills + DS reading. Don't rush it. |
| P1 | Sim network + test harness | 1 week | High-leverage; pays back everywhere. |
| P2 | Leader election | 1.5–2.5 weeks | First real consensus; expect to fight races. |
| P3 | Log replication | 2–3 weeks | The hardest core phase. The Figure 8 subtlety alone costs days. |
| P4 | Persistence & recovery | 1 week | Conceptually small, easy to get subtly wrong. |
| P5 | KV layer + client semantics | 1.5–2 weeks | Where it becomes a real database. |
| P6 | Snapshotting | 1.5–2 weeks | Index-offset bugs eat time; budget for it. |
| P7 | Membership changes | 1.5–2.5 weeks | Safety-critical; go slow. |
| P8 | Linearizable reads | 1 week | Conceptually rich, code is modest. |
| P9 | Failure testing & verification | 2–3 weeks | The credibility phase; also where you'll re-fix earlier bugs. |
| P10 | Benchmarking & observability | 1.5–2 weeks | Profiling + writing it all up. |
| **Total** | **Core (single group, deep)** | **~16–24 weeks (≈4–6 months part-time)** | Resume-ready at M7 (~end of P9). |

> **Reality check:** treat these as *learning budgets*, not deadlines. Going over on P3 or P7 because you genuinely understand them is a *win*, not a slip. The deliverable is understanding + a correct system, not a Gantt chart.

---

## 13. Stretch Goals

Tackle in roughly this order *after* M7. Each is independently resume-worthy.

1. **Sharded / multi-Raft KV store (6.824 Lab 4 style)** — multiple Raft groups + a shard controller that assigns shards to groups, with live shard migration. This is the single biggest step up in scope and signal ("I built a *horizontally scalable* consistent store"). Re-run linearizability across shard moves.
2. **Pluggable durable storage (BoltDB/Pebble)** — swap file storage for an embedded LSM/B-tree engine via the `Storage` interface; benchmark the difference. Shows clean abstractions + storage-engine awareness.
3. **gRPC + TLS + auth + a polished CLI/HTTP API** — make it feel like a real product; mutual TLS between nodes.
4. **Leader leases & follower reads** — beyond ReadIndex; quantify the consistency/latency trade-offs.
5. **Observability suite** — Prometheus + Grafana dashboards, distributed tracing (OpenTelemetry) of a write through consensus.
6. **Deterministic simulation testing (DST)** — push the sim-network toward FoundationDB/TigerBeetle-style full determinism; run "100 years of failures" overnight.
7. **Docker Compose / Kubernetes deployment** — a 5-node cluster you can `docker compose up`; a StatefulSet on k8s. Great for demos.
8. **Pre-vote & CheckQuorum** — production-grade election stability (prevents term inflation from a flapping partitioned node). Shows you've read beyond the basic paper.
9. **Write a blog series / talk** — turn `explain/` into public posts. Teaching is the ultimate interview prep and a strong public signal.

---

## 14. Interview Readiness Map

Quick index of "if asked about X, I built/learned it in Phase Y" — rehearse these.

| Interview theme | Where it's covered | Your one-line proof |
|---|---|---|
| Consensus / why it's hard / FLP | P0, P2 | "I implemented Raft from the paper; randomized timeouts give liveness without sacrificing safety." |
| Leader election & split-brain | P2 | "Election Safety verified over thousands of seeded partition runs." |
| Replication & commit safety | P3 | "I hit and fixed the Figure 8 prior-term commit bug." |
| Durability / crash recovery | P4 | "Persist term/vote/log before replying; survives kill-all-restart with no data loss." |
| Exactly-once semantics | P5 | "Client sessions + dedup table turn at-least-once delivery into exactly-once semantics." |
| Linearizability | P8, P9 | "ReadIndex reads, verified linearizable by Porcupine under chaos." |
| Membership / reconfiguration | P7 | "Single-server changes; grew/shrank a cluster live without split brain." |
| Testing distributed systems | P1, P9 | "Deterministic sim-network + Jepsen-style nemesis + linearizability checking, all reproducible from seeds." |
| Debugging distributed bugs | P9 | "See `explain/09-bugs-i-found.md` — here's a war story." |
| Performance / consensus cost | P10 | "Batching + pipelining gave X% throughput; I re-verified correctness after." |
| System design trade-offs | docs/DESIGN-DECISIONS.md | "ADRs: single-mutex vs fine-grained, ReadIndex vs lease, etc." |
| CAP / consistency models | P0, P5, P8 | "I can place my system on the CAP/PACELC map and explain the read-consistency knob I built." |

---

## 15. Reference Materials

- **Raft Extended Paper** — Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*. Your primary reference; Figure 2 is the contract.
- **Diego Ongaro's PhD thesis** — *Consensus: Bridging Theory and Practice* (membership changes, single-server reconfiguration, optimizations).
- **MIT 6.5840 (formerly 6.824)** — lectures + Raft/KV labs. The closest thing to a guided version of this project. Use their test suite as a correctness checklist.
- **raft.github.io** — interactive visualization + implementations list.
- **Porcupine** — Go linearizability checker (Phase 9).
- **Jepsen** (jepsen.io) — methodology and analyses; read a few reports to internalize how distributed systems break.
- **Go:** *A Tour of Go*, *Effective Go*, "Go Concurrency Patterns" (Rob Pike).
- **Inspiration codebases (read, don't copy):** `etcd/raft`, `hashicorp/raft` — *after* you've built your own, to compare engineering choices.

---

### How to use this plan
Work top-to-bottom through the phases. For each: **read → write the `explain/` note → build → test under fault injection → update docs → commit.** Don't advance until the phase's success criteria hold under `-race` and (from P9 on) the chaos suite. The goal isn't to finish — it's to *understand*, and to have the artifacts (`explain/09-bugs-i-found.md`, the chaos suite, the benchmarks, the ADRs) that prove it.
