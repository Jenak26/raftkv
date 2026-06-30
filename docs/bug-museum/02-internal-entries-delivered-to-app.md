# 02 - Raft delivered internal log entries to the application

- **Found by:** reasoning through the KV apply path while building Phase 8
  (linearizable reads); it would have surfaced as a panic the first time the KV
  layer ran on a cluster that did a membership change or elected a new leader.
- **Phase:** introduced in Phase 7 (configuration entries), compounded in Phase 8
  (the election no-op); caught before it shipped.

## Symptom

`panic: kv: decode committed command` - the KV server's apply loop called
`gob.Decode` on a log entry whose `Command` was nil.

## Root cause

Two kinds of log entry are **internal to Raft**, not client commands:

- **configuration-change entries** (Phase 7) carry a member set, with `Command ==
  nil`;
- the **election no-op** (Phase 8) carries nothing at all.

The applier delivered *every* committed entry up the `applyCh` as a
`CommandValid` message. The KV layer assumed every such message was a gob-encoded
`kv.Command` and decoded it - which panics on the nil `Command` of an internal
entry. The earlier phases never hit it because their tests drove Raft directly
(the raw-`[]byte` collector never decodes), so no client-command decode ran on a
config or no-op entry.

## Fix

Internal entries are exactly those with an empty `Command`. The KV apply loop now
skips them - advancing its applied index (so linearizable reads waiting on that
index still make progress) without decoding or deduping
([`internal/kv/server.go`](../../internal/kv/server.go), `applyCommand`):

```go
if len(m.Command) == 0 {
    // Internal Raft entry (config change or election no-op): not a client command.
    s.mu.Lock()
    if m.CommandIndex > s.lastApplied { s.lastApplied = m.CommandIndex }
    s.mu.Unlock()
    return
}
```

(Keeping the entries in the `applyCh` stream - rather than filtering them inside
Raft - preserves a gap-free index space, which the test oracle relies on.)

## Regression test

`TestMembershipGrowAndShrinkWhileServing` and the Phase 8 read tests now exercise
the KV layer across configuration changes and elections; the chaos suite hammers
both continuously.

## Lesson

A log carries the consensus layer's own bookkeeping as well as the application's
data. The boundary between "Raft's entries" and "the application's commands" must
be explicit - here, "empty `Command` means not-for-you." Test the *integration* of
features (membership + application), not just each in isolation; both halves passed
their own tests while being silently incompatible.
