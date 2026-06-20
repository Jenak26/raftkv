package storage

import (
	"bytes"
	"testing"
)

func TestInMemoryPersisterRoundTrip(t *testing.T) {
	p := NewInMemoryPersister()

	if p.ReadRaftState() != nil {
		t.Fatal("fresh persister should have nil raft state")
	}
	if p.RaftStateSize() != 0 || p.SnapshotSize() != 0 {
		t.Fatal("fresh persister sizes should be 0")
	}

	state := []byte("term=3,votedFor=1")
	p.SaveRaftState(state)
	if got := p.ReadRaftState(); !bytes.Equal(got, state) {
		t.Fatalf("ReadRaftState = %q, want %q", got, state)
	}
	if p.RaftStateSize() != len(state) {
		t.Fatalf("RaftStateSize = %d, want %d", p.RaftStateSize(), len(state))
	}
}

func TestInMemoryPersisterStateAndSnapshot(t *testing.T) {
	p := NewInMemoryPersister()
	state := []byte("state")
	snap := []byte("snapshot-bytes")
	p.SaveStateAndSnapshot(state, snap)

	if got := p.ReadRaftState(); !bytes.Equal(got, state) {
		t.Fatalf("ReadRaftState = %q, want %q", got, state)
	}
	if got := p.ReadSnapshot(); !bytes.Equal(got, snap) {
		t.Fatalf("ReadSnapshot = %q, want %q", got, snap)
	}
	if p.SnapshotSize() != len(snap) {
		t.Fatalf("SnapshotSize = %d, want %d", p.SnapshotSize(), len(snap))
	}
}

// TestInMemoryPersisterClonesOnWrite verifies the persister copies input, so a
// caller mutating its buffer afterward cannot corrupt persisted state.
func TestInMemoryPersisterClonesOnWrite(t *testing.T) {
	p := NewInMemoryPersister()
	state := []byte("original")
	p.SaveRaftState(state)
	state[0] = 'X' // mutate caller's buffer after saving

	if got := p.ReadRaftState(); !bytes.Equal(got, []byte("original")) {
		t.Fatalf("persisted state aliased caller buffer: got %q", got)
	}
}

// TestInMemoryPersisterClonesOnRead verifies mutating a returned slice does not
// affect the stored copy.
func TestInMemoryPersisterClonesOnRead(t *testing.T) {
	p := NewInMemoryPersister()
	p.SaveRaftState([]byte("original"))
	got := p.ReadRaftState()
	got[0] = 'X'

	if again := p.ReadRaftState(); !bytes.Equal(again, []byte("original")) {
		t.Fatalf("stored state was mutated via returned slice: got %q", again)
	}
}
