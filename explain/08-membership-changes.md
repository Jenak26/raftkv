# Cluster Membership Changes

> Phase 7. Real clusters add and remove nodes without downtime. Done naively this
> is the most dangerous thing Raft can do — it can split the cluster into two
> disjoint majorities and elect two leaders. This phase is about doing it *safely*.

## What problem this solves

So far the member set is fixed at construction. But machines die and clusters grow.
We need to change membership *while the cluster keeps serving*, without ever — even
for an instant — allowing two leaders.

## Why the naive switch is unsafe

Suppose we flip every node from the old configuration `C_old` to a new one `C_new`
"at once." Because nodes apply the change at different times, there is a window
where some nodes still think the cluster is `C_old` and others think it is `C_new`.
If `C_old` and `C_new` have **disjoint majorities**, two leaders can be elected in
the same term — one by a majority of `C_old`, one by a majority of `C_new`. Split
brain. This is the entire hazard.

## The fix we implement: single-server changes

The Raft author's thesis proves a beautiful fact: **if you add or remove only one
server at a time, the old and new majorities always overlap** (they differ by one
member, so any two majorities of `C_old` and `C_new` share at least one node). That
shared node cannot vote for two different leaders in a term, so split brain is
impossible — *without* the machinery of joint consensus.

So `AddServer`/`RemoveServer` change the membership by exactly one, and the cluster
goes through a sequence of single-member steps to grow 3 → 4 → 5 or shrink back.
(Joint consensus — committing a combined `C_old,new` — is the alternative for
multi-member changes; we *understand* it but don't need it.)

## How a configuration lives in the log

A configuration change is **a log entry** like any other, except it carries a member
set (`LogEntry.Config != nil`) instead of a client command. It replicates by the
same `AppendEntries` machinery, which is what makes the change ordered and durable.

Two rules govern it:

1. **Adopt a configuration the moment it is appended to the log — not when it
   commits.** Each node uses the *latest configuration in its log* for all quorum
   decisions (who to replicate to, how to count votes and commits), even while that
   entry is uncommitted. This is essential: the node proposing the change must
   already count the new membership, or it could commit the change under the old
   majority and lose safety. In code, any log mutation (append, truncate, install
   snapshot) is followed by recomputing the current config from the log:
   `refreshConfig()` scans for the last `Config` entry, falling back to the
   bootstrap config if none.

   A corollary: if a follower **truncates** a config entry away during conflict
   resolution, it automatically reverts to the earlier configuration — because the
   config is always recomputed from whatever the log currently holds.

2. **At most one configuration change in flight.** A leader may not start a new
   change until the previous config entry has committed. Otherwise two overlapping
   single-server changes could compose into a non-overlapping jump. We enforce it
   by refusing `AddServer`/`RemoveServer` while the log's latest config entry is
   still uncommitted.

## The tricky edges

- **A leader that removes itself.** Once the new configuration (excluding the
  leader) commits, the leader steps down to follower. It has already replicated the
  change to the others, so they elect a new leader from the new set. Until it steps
  down it keeps serving, so there is no availability gap.
- **A node not in its own configuration does not start elections.** A removed node,
  left running, would otherwise time out and start elections with ever-higher terms,
  disrupting the cluster. Gating campaigns on "am I a member?" silences it. (The
  fully general defense against a removed/partitioned node disrupting the cluster is
  **Pre-Vote**, listed as a stretch goal; single-server removal plus this gate
  covers the cases this phase targets.)
- **A freshly added node is behind.** It joins as a voter but starts with an empty
  log, so the leader brings it up to date with `AppendEntries`/`InstallSnapshot`.
  Adding *one* voter never stalls commits, because a majority of the new
  configuration is the already-caught-up old members. (Catching a new node up as a
  non-voting *learner* before adding it is the production refinement; we note it but
  rely on single-at-a-time keeping liveness.)

## What breaks if you get it wrong

- **Changing more than one member at once** (or two concurrent single changes) →
  disjoint majorities → two leaders.
- **Using the committed config instead of the latest-in-log config** for quorum →
  you can commit a change under the old majority and violate the overlap guarantee.
- **Adding several fresh, empty nodes as voters at once** → no majority is caught
  up → commits stall.
- **Not stepping the removed leader down** → it keeps acting as leader of a cluster
  it is no longer part of.

## Tests that pin the properties

- `TestMembershipGrowAndShrinkWhileServing` — grow a 3-node cluster to 5 and back to
  3 while a workload streams in; assert exactly one leader throughout and no
  committed entry is ever lost or contradicted.
- `TestMembershipRejectsConcurrentChange` — a second `AddServer` is refused while
  the first is still uncommitted.
- `TestMembershipLeaderStepsDownWhenRemoved` — removing the leader yields a new
  leader from the remaining set, and the old leader becomes a follower.
