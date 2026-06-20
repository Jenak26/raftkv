package kv

import "testing"

func TestMapStateMachinePutGetDelete(t *testing.T) {
	m := NewMapStateMachine()

	if r := m.Apply(Command{Kind: OpGet, Key: "a"}); r.Ok {
		t.Fatal("Get on missing key should report Ok=false")
	}
	if r := m.Apply(Command{Kind: OpPut, Key: "a", Value: "1"}); !r.Ok {
		t.Fatal("Put should report Ok=true")
	}
	if r := m.Apply(Command{Kind: OpGet, Key: "a"}); !r.Ok || r.Value != "1" {
		t.Fatalf("Get = %+v, want Value=1 Ok=true", r)
	}
	if r := m.Apply(Command{Kind: OpDelete, Key: "a"}); !r.Ok {
		t.Fatal("Delete of existing key should report Ok=true")
	}
	if r := m.Apply(Command{Kind: OpGet, Key: "a"}); r.Ok {
		t.Fatal("Get after Delete should report Ok=false")
	}
}

func TestMapStateMachineCAS(t *testing.T) {
	m := NewMapStateMachine()
	m.Apply(Command{Kind: OpPut, Key: "k", Value: "v1"})

	// Mismatched expected value: no swap.
	if r := m.Apply(Command{Kind: OpCAS, Key: "k", Expected: "WRONG", Value: "v2"}); r.Ok {
		t.Fatal("CAS with wrong expected should not swap")
	}
	if r := m.Apply(Command{Kind: OpGet, Key: "k"}); r.Value != "v1" {
		t.Fatalf("value changed despite failed CAS: %q", r.Value)
	}

	// Matching expected value: swap succeeds.
	if r := m.Apply(Command{Kind: OpCAS, Key: "k", Expected: "v1", Value: "v2"}); !r.Ok {
		t.Fatal("CAS with correct expected should swap")
	}
	if r := m.Apply(Command{Kind: OpGet, Key: "k"}); r.Value != "v2" {
		t.Fatalf("value = %q, want v2", r.Value)
	}
}

func TestMapStateMachineSnapshotRestore(t *testing.T) {
	src := NewMapStateMachine()
	src.Apply(Command{Kind: OpPut, Key: "x", Value: "1"})
	src.Apply(Command{Kind: OpPut, Key: "y", Value: "2"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	dst := NewMapStateMachine()
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if r := dst.Apply(Command{Kind: OpGet, Key: "x"}); r.Value != "1" {
		t.Fatalf("restored x = %q, want 1", r.Value)
	}
	if r := dst.Apply(Command{Kind: OpGet, Key: "y"}); r.Value != "2" {
		t.Fatalf("restored y = %q, want 2", r.Value)
	}
}

func TestMapStateMachineRestoreEmptyResets(t *testing.T) {
	m := NewMapStateMachine()
	m.Apply(Command{Kind: OpPut, Key: "k", Value: "v"})
	if err := m.Restore(nil); err != nil {
		t.Fatalf("Restore(nil): %v", err)
	}
	if r := m.Apply(Command{Kind: OpGet, Key: "k"}); r.Ok {
		t.Fatal("Restore(nil) should have reset the store")
	}
}
