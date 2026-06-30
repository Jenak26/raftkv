package linearizability

import "testing"

// op is a terse constructor for tests: fixed [call,return] bracket.
func op(kind OpKind, key, value, expected string, gotV string, gotOk bool, call, ret int64) Operation {
	return Operation{Kind: kind, Key: key, Value: value, Expected: expected, GotValue: gotV, GotOk: gotOk, Call: call, Return: ret}
}

func TestCheckerAcceptsSequentialHistory(t *testing.T) {
	h := []Operation{
		op(OpPut, "k", "a", "", "", true, 1, 2),
		op(OpGet, "k", "", "", "a", true, 3, 4),
		op(OpCAS, "k", "b", "a", "", true, 5, 6), // a -> b, swapped
		op(OpGet, "k", "", "", "b", true, 7, 8),
	}
	if ok, bad := Check(h); !ok {
		t.Fatalf("valid sequential history flagged non-linearizable at key %q", bad)
	}
}

func TestCheckerAcceptsValidConcurrentHistory(t *testing.T) {
	// Get overlaps the Put and observes the pre-Put (absent) state — legal, because
	// the Get can be linearized before the Put.
	h := []Operation{
		op(OpPut, "k", "a", "", "", true, 1, 4),
		op(OpGet, "k", "", "", "", false, 2, 3),
	}
	if ok, _ := Check(h); !ok {
		t.Fatal("a valid concurrent linearization was rejected")
	}
}

func TestCheckerRejectsStaleRead(t *testing.T) {
	// The Get begins strictly after the Put returned, yet sees the absent state.
	// No linearization respects this — it must be flagged.
	h := []Operation{
		op(OpPut, "k", "a", "", "", true, 1, 2),
		op(OpGet, "k", "", "", "", false, 3, 4),
	}
	if ok, _ := Check(h); ok {
		t.Fatal("a stale read (Get sees absent after Put completed) was accepted")
	}
}

func TestCheckerRejectsImpossibleCAS(t *testing.T) {
	// CAS claims it swapped a->b, but the register never held "a".
	h := []Operation{
		op(OpPut, "k", "x", "", "", true, 1, 2),
		op(OpCAS, "k", "b", "a", "", true, 3, 4), // expected "a" but value is "x"
	}
	if ok, _ := Check(h); ok {
		t.Fatal("an impossible CAS (reported swap that the state forbids) was accepted")
	}
}

func TestCheckerIndependentKeys(t *testing.T) {
	// Two keys, each internally linearizable; the checker treats them independently.
	h := []Operation{
		op(OpPut, "k1", "a", "", "", true, 1, 2),
		op(OpPut, "k2", "b", "", "", true, 1, 2),
		op(OpGet, "k1", "", "", "a", true, 3, 4),
		op(OpGet, "k2", "", "", "b", true, 3, 4),
	}
	if ok, bad := Check(h); !ok {
		t.Fatalf("independent-key history rejected at %q", bad)
	}
}
