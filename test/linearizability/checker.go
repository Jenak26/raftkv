package linearizability

import (
	"fmt"
	"sort"
)

// regState is the model state of a single key: a register that may or may not hold
// a value. Operations on different keys are independent, so the checker partitions
// the history by key and checks each key's sub-history against this model — which
// keeps each search small and tractable.
type regState struct {
	val    string
	exists bool
}

// step applies op to a register state, returning whether op's observed result is
// consistent with that state and the resulting state.
func step(s regState, op Operation) (ok bool, next regState) {
	switch op.Kind {
	case OpPut:
		return true, regState{val: op.Value, exists: true}
	case OpGet:
		return op.GotValue == s.val && op.GotOk == s.exists, s
	case OpCAS:
		swapped := s.exists && s.val == op.Expected
		if op.GotOk != swapped {
			return false, s
		}
		if swapped {
			return true, regState{val: op.Value, exists: true}
		}
		return true, s
	case OpDelete:
		existed := s.exists
		if op.GotOk != existed {
			return false, s
		}
		return true, regState{exists: false}
	default:
		return false, s
	}
}

// Check reports whether the whole history is linearizable. On failure it returns
// the key whose sub-history could not be linearized, for diagnosis.
func Check(history []Operation) (ok bool, badKey string) {
	byKey := map[string][]Operation{}
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, ops := range byKey {
		if !linearizableKey(ops) {
			return false, key
		}
	}
	return true, ""
}

// linearizableKey runs the WGL search on one key's operations: find a total order
// consistent with real time whose every step is legal for the register model.
//
// The search repeatedly picks a "minimal" not-yet-linearized operation — one that
// no other un-linearized operation must precede (no undone j with j.Return <
// i.Call) — tentatively linearizes it if the model allows its observed result, and
// recurses; it backtracks otherwise. Memoizing on (set-linearized, state) collapses
// the otherwise exponential search, because a register has few distinct states.
func linearizableKey(ops []Operation) bool {
	n := len(ops)
	if n == 0 {
		return true
	}
	if n > 62 {
		// The bitmask memo uses uint64; very long single-key histories would need a
		// different representation. Panic loudly rather than check incorrectly — the
		// workload is expected to spread operations across enough keys.
		panic(fmt.Sprintf("linearizability: %d ops on one key exceeds the 62-op checker limit; use more keys", n))
	}
	// Deterministic order makes failures reproducible.
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].Call != ops[j].Call {
			return ops[i].Call < ops[j].Call
		}
		return ops[i].Return < ops[j].Return
	})

	type memoKey struct {
		done uint64
		st   regState
	}
	memo := map[memoKey]bool{}
	full := uint64(1)<<uint(n) - 1

	var search func(done uint64, st regState) bool
	search = func(done uint64, st regState) bool {
		if done == full {
			return true
		}
		mk := memoKey{done, st}
		if v, seen := memo[mk]; seen {
			return v
		}
		result := false
		for i := 0; i < n; i++ {
			if done&(1<<uint(i)) != 0 {
				continue
			}
			// i may be linearized next only if no un-done operation entirely
			// precedes it in real time.
			blocked := false
			for j := 0; j < n; j++ {
				if i == j || done&(1<<uint(j)) != 0 {
					continue
				}
				if ops[j].Return < ops[i].Call {
					blocked = true
					break
				}
			}
			if blocked {
				continue
			}
			if okStep, ns := step(st, ops[i]); okStep {
				if search(done|(1<<uint(i)), ns) {
					result = true
					break
				}
			}
		}
		memo[mk] = result
		return result
	}
	return search(0, regState{})
}
