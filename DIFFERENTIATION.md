# Making This a 10/10 Systems Project — Differentiation & Signal Strategy

> Companion to `plan.md`. `plan.md` tells you *how to build it correctly*. **This document tells you how to make it unforgettable** — what to amplify, what to cut, what to put in front of a recruiter, and the exact artifacts that turn "I built Raft" into "I want this person on my infrastructure team."

---

## 0. The Brutal Truth (read this first)

A from-scratch Raft KV store is a **commodity portfolio project**. MIT 6.5840 mints thousands a year. The bare implementation is *table stakes*, not a differentiator. If your README says "I implemented the Raft consensus algorithm," a senior engineer's reaction is "okay, so did everyone — did you actually understand it, or did you pass the test suite by trial and error?"

**The moat is not the code. The moat is provable rigor + a story only you can tell.**

Three things separate a 10/10 from a 6/10, and none of them are "the Raft code works":

1. **You can prove it's correct** — not "tests pass," but *verified linearizable under adversarial fault injection, reproducibly.*
2. **You can debug the undebuggable** — you found real, deep concurrency/consensus bugs and can narrate the hunt.
3. **You have a signature technique** — something most engineers have only *heard* of, that you actually built and can explain.

This plan's answer to all three is one unifying idea: **Deterministic Simulation Testing (DST).**

---

## 1. The Differentiator: Deterministic Simulation Testing

### What it is
DST is the technique behind **FoundationDB** (acquired by Apple, runs iCloud) and **TigerBeetle**. The whole distributed system — every node, the network, the clock, the disk, every source of randomness — runs **inside a single deterministic simulation** driven by one seed. Because *nothing* is real (no real threads racing, no real wall clock, no real sockets), the entire execution is a pure function of the seed:

> `seed → exact sequence of events → exact bug (or success)`

This gives you three superpowers no ordinary test suite has:

1. **Perfect reproducibility.** A bug that happens 1-in-50,000 runs, you reproduce *instantly and forever* by replaying its seed. No "it's flaky," no "couldn't repro."
2. **Time travel.** You run *simulated* time, not real time. Hours of partitions, crashes, and clock skew compress into milliseconds. You can run **"a thousand years of failures" overnight.**
3. **Adversarial search.** A "Sometimes" failure-injection layer makes the simulation actively hostile — dropping packets, freezing nodes, skewing clocks, reordering disk writes — and because it's seeded, every nasty scenario it finds is a permanent, replayable regression test.

### Why this is the move for *you specifically*
- You are new to distributed systems. **DST is the single best learning accelerator that exists** — it turns invisible, non-reproducible Heisenbugs into deterministic puzzles you can actually solve. It makes the whole project *learnable* instead of maddening.
- It is **rare in portfolios.** Recruiters and staff engineers have heard the FoundationDB testing talk ("Testing Distributed Systems w/ Deterministic Simulation" — a legendary Strange Loop talk). Almost no candidate has *built* it. You will.
- It produces the proof artifacts (Section 4) that make the rest of the project credible.

### The one-line pitch this unlocks
> *"I built a distributed key-value store on a from-scratch Raft, and a deterministic simulation harness that lets me replay any failure from a single seed — so I can run thousands of years of partitions, crashes, and clock skew overnight and prove the system stays linearizable."*

That sentence gets you the interview. Nobody else's bullet sounds like that.

---

## 2. How DST changes the architecture (vs. plain `plan.md`)

`plan.md` already calls for a simulated network (Phase 1) and linearizability checking (Phase 9). DST **pushes that idea all the way down** and makes it the project's organizing principle. The key architectural commitment:

> **Every source of non-determinism is injected through an interface, and in test mode all of them are driven by one seeded PRNG and a single-threaded deterministic scheduler.**

| Source of non-determinism | Production impl | Simulation impl (seeded) |
|---|---|---|
| **Concurrency / scheduling** | real goroutines | a cooperative, single-threaded **deterministic scheduler** that interleaves "logical" tasks in a seed-determined order |
| **Time / clocks** | `time.Now`, real timers | a **virtual clock** the simulation advances explicitly |
| **Network** | gRPC/TCP | in-memory message bus with seeded drop/delay/reorder/duplicate/partition |
| **Disk** | real fsync to file | simulated storage with seeded latency, reorderable writes, and **torn-write / crash-before-fsync** injection |
| **Randomness** | `crypto/rand` / `math/rand` | a single seeded `math/rand.Rand` threaded everywhere |

The hardest and most impressive part is making **concurrency itself deterministic.** In Go this means: instead of free-running goroutines with mutexes, Raft node logic runs as cooperative "turns" the simulator schedules. This is genuinely hard, genuinely educational, and genuinely rare — it's the part that makes the project a 10/10 instead of a 7/10.

> **Pragmatic note (so you don't drown):** full single-threaded determinism is the gold standard but is the steepest cliff. The plan below gives you a *staged* path: start with a deterministic *network + clock + disk* (achievable, already huge signal), then graduate to a deterministic *scheduler* as the headline stretch. Even the partial version beats 95% of portfolio projects. Don't let perfect block shippable.

---

## 3. The Signature Deliverables ("the things they remember")

A 10/10 project is remembered by its **artifacts**, not its source tree. Build these deliberately — each is a story, a screenshot, and a resume bullet.

### 3.1 The Bug Museum (`explain/bug-museum/`)
A curated gallery of **real bugs you found**, each as a self-contained case file:
- **The symptom** (what the linearizability checker reported).
- **The seed** that reproduces it (`SEED=0x8f3a... make repro`).
- **The trace** — a rendered timeline of the exact event interleaving that triggered it.
- **The root cause** — which Raft invariant was violated (cite the paper figure).
- **The fix** — the diff.
- **The regression test** — the seed is now a permanent test.

> This is the **single most valuable interview asset you will produce.** "Tell me about a hard bug" is asked in every senior interview. Most candidates fumble. You'll have a *museum*. Aim for 5–10 genuine entries (you'll hit them naturally — Raft is a bug factory, and DST surfaces them).

### 3.2 Time-Travel Debugger / Trace Viewer
Because runs are deterministic, you can **replay any seed and emit a structured event log**, then render it as:
- A **space-time / message-sequence diagram** (nodes on the X axis, simulated time on Y, messages as arrows, partitions as shaded regions, leader changes highlighted).
- ASCII for the terminal; an HTML/SVG version for the README.

A recruiter scrolling your README and seeing *"here is the exact 1-in-50,000 interleaving where my old commit logic lost a committed entry, and here's the heartbeat that should have prevented it"* — that's the moment you stop being a résumé and start being a hire.

### 3.3 The Linearizability Verdict
Integrate **Porcupine** (or hand-roll first, then Porcupine) so every chaos run ends with a machine-checked verdict: *is this history linearizable?* Surface a **headline number** in the README:

> *"✅ 2,300,000 operations across 41,000 seeded fault-injection runs — 0 linearizability violations (after fixing 7, documented in the Bug Museum)."*

The "(after fixing 7)" is **not** a weakness to hide — it's the proof you did real work. Hiding bugs reads as "never stress-tested it." Showing fixed bugs reads as "battle-hardened."

### 3.4 The Comparison Bench vs. etcd
Run the **same workload** against your store and against `etcd` (which uses production Raft). Publish: throughput, p50/p99/p999 latency, recovery time after leader loss. You will likely be slower — **that's fine and you say so**, with an honest analysis of *why* (batching, pipelining, fsync strategy, Go GC). This signals: you benchmark rigorously, you know the production landscape, and you reason about trade-offs instead of hand-waving. Nothing says "junior" like a project that's never been measured against a real system.

### 3.5 The Public Write-Up
Turn the journey into a **3–5 part blog series** (or one long technical post): "Building a Deterministic-Simulation-Tested Raft in Go." Teaching is the ultimate proof of understanding, it's a public signal a recruiter can find, and it forces clarity. The DST angle is *inherently interesting* to the infra community — it has a real shot at the front page of HN/Lobsters/r/programming, which is a referral magnet.

### 3.6 The 90-Second Demo (asciinema/GIF in the README)
A scripted, reproducible terminal demo:
1. `make cluster` → 5 nodes up.
2. Write data, read it back.
3. `kill -9` the leader → new leader elected, data intact, client never noticed.
4. Partition the cluster 3/2 → minority refuses writes (preserves safety), majority serves.
5. Heal → minority catches up via snapshot.
6. `make chaos SEED=...` → replay a Bug Museum failure live.

This is what you screen-share in an interview. Practice it until it's flawless.

---

## 4. The Scoring Rubric: 6/10 vs 10/10

Use this to self-audit. Be honest.

| Dimension | 6/10 (commodity) | **10/10 (signature)** |
|---|---|---|
| **Correctness claim** | "All tests pass" | "Verified linearizable over 41k seeded adversarial runs; 7 real bugs found & fixed" |
| **Testing** | happy-path + some partitions | full **DST**: deterministic scheduler, virtual clock, disk fault injection, replay-from-seed |
| **Debuggability** | print statements | **time-travel trace viewer**; any failure replays from a seed |
| **Bug stories** | "had some race conditions" | a curated **Bug Museum** with traces, root causes, fixes, regression seeds |
| **Performance** | never measured | benchmarked **vs. etcd** with honest trade-off analysis |
| **Scope depth** | election + replication | + persistence + snapshots + membership + linearizable reads, all chaos-verified |
| **Explanation** | "it uses Raft" | can derive every safety property; ADRs for every decision; a published write-up |
| **Recruiter artifact** | a GitHub repo | a repo with a demo GIF, a headline correctness number, and a blog series |
| **Uniqueness** | one of thousands | "the DST person" — a story nobody else in the stack has |

**The gap between these columns is almost entirely rigor and storytelling — not more features.** Resist the urge to add sharding before the correctness story is airtight. A deeply-verified single Raft group beats a shallow sharded store every time.

---

## 5. Which `plan.md` phases to amplify (and which to keep lean)

DST doesn't replace `plan.md` — it re-weights it. Spend your *extra* effort here:

| Phase | Treatment | Why |
|---|---|---|
| **P1 (Sim network + harness)** | **Massively amplify → make it the DST core.** Build the virtual clock and seeded message bus here. Add the deterministic scheduler as the headline. | This *is* the differentiator. The earlier it's solid, the easier every later phase becomes. |
| **P2–P3 (election, replication)** | Keep lean and correct. | Table stakes. Don't gold-plate. |
| **P4 (persistence)** | Amplify slightly: add **disk fault injection** (crash-before-fsync, torn writes, reordered writes) to the sim. | This is where the *juiciest* bugs live and where DST shines. Huge signal. |
| **P6 (snapshots)** | Standard. | Necessary, not headline. |
| **P7 (membership)** | Standard, but run it **under chaos** — reconfiguration-during-partition is a famous bug class. | Membership-under-fault is a great Bug Museum source. |
| **P8 (linearizable reads)** | Standard. | Needed for the linearizability story. |
| **P9 (failure testing)** | **Amplify → this is where the Bug Museum, trace viewer, and headline number are born.** Run for real, for a long time, find the bugs. | The credibility payoff. |
| **P10 (benchmarking)** | Amplify the **vs-etcd comparison** specifically. | The "I know the production landscape" signal. |

> **New mini-phase to insert (between P9 and P10): "Adversarial Hardening Week."** Point the seeded fault-injector at the system and let it run for days, fixing every linearizability violation it finds. Each fix → a Bug Museum entry. End when it can't break you across millions of ops. *This week is what you'll talk about in interviews more than any other.*

---

## 6. The Resume & Narrative Layer

### Resume bullets (use these as templates)
- *Built a distributed key-value store on a **from-scratch Raft** implementation (leader election, log replication, persistence, snapshotting, dynamic membership, linearizable reads) in Go.*
- *Engineered a **deterministic simulation testing** harness (FoundationDB/TigerBeetle-style) with a seeded scheduler, virtual clock, and disk/network fault injection — making every distributed failure **reproducible from a single seed**.*
- *Verified **linearizability** across **40k+ adversarial fault-injection runs** using Porcupine; **found and fixed 7 deep consensus/concurrency bugs**, each preserved as a replayable regression test.*
- *Benchmarked against **etcd**, analyzing throughput/tail-latency/recovery-time trade-offs and the cost of consensus.*

### The 30-second verbal pitch (memorize)
> "Most Raft projects just pass the test suite. I wanted to actually *prove* mine was correct, so I built it the way FoundationDB tests their database — a full deterministic simulation where the network, clocks, disk, and even the goroutine scheduler are driven by one seed. That let me run thousands of years of simulated partitions and crashes overnight, and any bug it found, I could replay instantly. I documented seven real consensus bugs that way, with the exact interleavings that triggered them."

### The story arc (for "tell me about a project")
1. **Tension:** distributed bugs are invisible and non-reproducible — the hardest thing in the field.
2. **Insight:** the industry's answer is determinism (FoundationDB/TigerBeetle).
3. **Action:** you built that, from scratch, around a from-scratch Raft.
4. **Proof:** the Bug Museum, the 40k-run linearizability verdict, the trace viewer.
5. **Reflection:** what consensus *actually* costs, where Raft is subtle (Figure 8, membership), what you'd do differently.

A clear tension→insight→action→proof→reflection arc is what makes interviewers *lean in*. You now have one nobody else has.

---

## 7. Anti-Patterns That Would Drop You Back to 6/10

- **Feature-chasing before correctness.** Adding sharding/gRPC/a web UI while the linearizability story is shaky. *Depth beats breadth.* A 10/10 single group > a 7/10 sharded store.
- **Hiding the bugs.** A repo with zero documented bugs reads as "never tested hard." Your fixed bugs are your credibility.
- **Averages-only benchmarks.** Always report p99/p999. Hiding tails is a junior tell.
- **Copying etcd/hashicorp internals.** Read them *after* you build yours, to compare. Copying robs you of the learning *and* the story (and you can't defend code you didn't write).
- **A README that leads with "I implemented Raft."** Lead with the *outcome and the rigor*: the demo GIF, the headline correctness number, the DST angle. The algorithm is the means, not the headline.
- **Non-reproducible "chaos."** If your fault injection uses unseeded randomness, you've built flakiness, not testing. Seed everything.
- **Perfectionism on the deterministic scheduler blocking everything else.** Ship the deterministic network+clock+disk first; graduate to scheduler determinism as the stretch headline.

---

## 8. Definition of Done for "10/10"

You can stop and call it a signature project when **all** of these are true:

- [ ] A 5-node cluster survives kill-the-leader and partitions live, demoable in <2 minutes.
- [ ] All Raft features done: election, replication, persistence, snapshotting, single-server membership, linearizable (ReadIndex) reads.
- [ ] Failures are **reproducible from a seed** (deterministic network + clock + disk at minimum).
- [ ] A **linearizability checker** gives a machine-verified verdict; README shows a headline number from a long adversarial run.
- [ ] A **Bug Museum** with ≥5 real, documented, seed-reproducible bugs.
- [ ] A **trace/space-time viewer** that renders a run from a seed (at least one rendered diagram in the README).
- [ ] A **benchmark vs. etcd** with honest trade-off analysis.
- [ ] `docs/` has ARCHITECTURE, DESIGN-DECISIONS (ADRs), TESTING, BENCHMARKS; `explain/` has the concept notes.
- [ ] A **README that sells it in 30 seconds** (demo GIF + headline number + the DST pitch).
- [ ] (Stretch, the crown) A **deterministic scheduler** making the *entire* system replayable, and/or a **published blog series**.

Hit the first 9 and you have a project that meaningfully separates you from the field. Hit the crown and you have one staff engineers will want to talk to you about.

---

### TL;DR
Building Raft well makes you *competent*. Building it inside a **deterministic simulation**, **proving** linearizability under adversarial faults, curating a **Bug Museum**, and **benchmarking against etcd** makes you *memorable*. Spend your marginal effort on **rigor and artifacts**, not extra features. The algorithm gets you to 6; the *proof and the story* get you to 10.
