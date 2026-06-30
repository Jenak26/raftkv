# 01 - A single-node cluster never elects a leader

- **Found by:** `TestKVExactlyOnceUnderDuplicateSubmit`, which spun up a **1-node**
  KV cluster - the first test in the project to use `n=1`.
- **Seed:** n/a (deterministic; reproduces on every run of a 1-node cluster).
- **Phase:** surfaced in Phase 5 (KV layer); the bug lived in the Phase 2 election code.

## Symptom

A one-node cluster never produced a leader: `waitLeader` timed out after 3s and
`Propose` always returned `isLeader == false`. Every multi-node test (n=3, n=5) had
always passed, so the election code looked correct.

## Root cause

In `startElection`, the candidate casts its self-vote as a local `votes := 1`, then
fans out `RequestVote` RPCs; the promotion check -

```go
if votes >= majority { rf.becomeLeader() }
```

- lived **only inside the per-peer reply goroutine**. For a single-node cluster the
peer loop has nothing to iterate (the one peer is self, which is skipped), so *no
reply goroutine is ever spawned* and the check never runs. The node increments its
term, votes for itself, and sits as a candidate forever, re-electing on every
timeout.

This does not violate a Raft *safety* property - it is a **liveness** gap in an edge
configuration. Figure 2 says a candidate becomes leader upon receiving votes from a
majority of the *whole cluster*, and a candidate always implicitly counts its own
vote; with majority = 1 the election is already won at the moment of self-vote.

## Fix

Handle the self-vote-is-a-majority case before fanning out, while still holding the
lock and still a candidate ([`internal/raft/raft.go`](../../internal/raft/raft.go),
`startElection`):

```go
majority := len(peers)/2 + 1
if majority == 1 {
    rf.becomeLeader()   // self-vote already a majority; no peers to ask
    rf.mu.Unlock()
    return
}
```

## Regression test

`TestSingleNodeElectsItselfLeader` (`internal/raft/raft_test.go`): a 1-node cluster
on a MockClock must report itself leader after its first election timeout.

## Lesson

Test the boundary configurations, not just the comfortable middle. "Counting votes"
quietly assumed at least one peer reply would arrive to trigger the tally; the
degenerate cluster (N=1) exposed that the self-vote was never *acted upon*, only
counted. A majority calculation should be evaluated the instant the relevant vote is
recorded - including your own - not deferred to the arrival of someone else's reply.
