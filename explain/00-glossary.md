# 00 — Glossary

Precise definitions, in my own words. If I can't define it crisply here, I don't understand it well enough to implement it. Updated continuously.

| Term | Definition |
|---|---|
| **Replicated State Machine (RSM)** | A model where several servers each run the same deterministic state machine and feed it the *same sequence* of commands, so they stay identical. Consensus is what agrees on that sequence. |
| **Consensus** | Getting a group of nodes to agree on a single value / ordered sequence of values, despite crashes and network faults. Must be **safe** (never disagree) and ideally **live** (eventually decide). |
| **Raft** | An understandable consensus algorithm that elects a single leader per term and replicates a log through it. |
| **Term** | A logical clock: a monotonically increasing integer. Each term has at most one leader. Used to detect and reject stale information. |
| **Log** | An ordered, append-only sequence of entries `(term, index, command)`. The thing consensus agrees on. |
| **Index** | The position of an entry in the log (1-based in this project; index 0 is a sentinel). |
| **Committed** | An entry is committed once it is safely stored on a majority and the current leader has an entry from its own term at/after it. Committed entries are durable and will be applied by every node. |
| **Applied** | An entry is applied when it has been handed to the state machine. `lastApplied <= commitIndex` always. |
| **Quorum / Majority** | ⌊N/2⌋ + 1 nodes. Any two majorities of the same cluster share at least one node — the property that prevents two leaders or two conflicting decisions. |
| **Leader / Follower / Candidate** | The three roles. Followers are passive; a candidate stands for election; the leader handles all client writes and drives replication. |
| **Heartbeat** | An empty AppendEntries the leader sends periodically to assert authority and prevent followers from starting elections. |
| **Election timeout** | Randomized interval a follower waits without hearing from a leader before becoming a candidate. Randomization breaks symmetry → liveness. |
| **Linearizability** | The strongest single-object consistency model: every operation appears to take effect atomically at some single point between its invocation and its response, consistent with real-time order. "It behaves like one machine." |
| **Safety property** | Something bad never happens (e.g., two leaders in one term). Must hold in *every* execution. |
| **Liveness property** | Something good eventually happens (e.g., a leader is eventually elected). Raft guarantees liveness only under reasonable timing assumptions. |
| **FLP impossibility** | In a purely asynchronous network, no consensus algorithm can guarantee both safety and liveness if even one node can crash. Raft keeps safety always and gets liveness via timeouts/randomness. |
| **Snapshot** | A serialized copy of the state machine at a given index, used to discard (compact) the log prefix it covers. |
| **Deterministic Simulation Testing (DST)** | Running the whole system inside one seeded simulation (network, clocks, disk, scheduling all controlled) so any run — and any bug — is reproducible from its seed. |
| **Nemesis** | The fault-injecting component in a chaos test that partitions/crashes/delays nodes during a workload. |

> See [[01-replicated-state-machines]] for the model these terms hang off of.
