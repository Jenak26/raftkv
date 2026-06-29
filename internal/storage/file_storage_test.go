package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	if fs.ReadRaftState() != nil {
		t.Fatal("fresh storage should have nil raft state")
	}
	if fs.RaftStateSize() != 0 || fs.SnapshotSize() != 0 {
		t.Fatal("fresh storage sizes should be 0")
	}

	state := []byte("term=7,votedFor=2,log=...")
	fs.SaveRaftState(state)
	if got := fs.ReadRaftState(); !bytes.Equal(got, state) {
		t.Fatalf("ReadRaftState = %q, want %q", got, state)
	}
	if fs.RaftStateSize() != len(state) {
		t.Fatalf("RaftStateSize = %d, want %d", fs.RaftStateSize(), len(state))
	}
}

// TestFileStorageReopenReloadsState verifies durability across a "process
// restart": a fresh FileStorage over the same directory sees the prior state and
// reports its size without a read.
func TestFileStorageReopenReloadsState(t *testing.T) {
	dir := t.TempDir()
	fs1, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	state := []byte("durable-state")
	snap := []byte("durable-snapshot")
	fs1.SaveStateAndSnapshot(state, snap)

	fs2, err := NewFileStorage(dir) // simulate restart: brand-new object, same dir
	if err != nil {
		t.Fatal(err)
	}
	if got := fs2.ReadRaftState(); !bytes.Equal(got, state) {
		t.Fatalf("after reopen ReadRaftState = %q, want %q", got, state)
	}
	if got := fs2.ReadSnapshot(); !bytes.Equal(got, snap) {
		t.Fatalf("after reopen ReadSnapshot = %q, want %q", got, snap)
	}
	if fs2.RaftStateSize() != len(state) {
		t.Fatalf("after reopen RaftStateSize = %d, want %d", fs2.RaftStateSize(), len(state))
	}
	if fs2.SnapshotSize() != len(snap) {
		t.Fatalf("after reopen SnapshotSize = %d, want %d", fs2.SnapshotSize(), len(snap))
	}
}

func TestFileStorageOverwriteReplacesState(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs.SaveRaftState([]byte("first"))
	fs.SaveRaftState([]byte("second-longer"))
	if got := fs.ReadRaftState(); !bytes.Equal(got, []byte("second-longer")) {
		t.Fatalf("ReadRaftState = %q, want %q", got, "second-longer")
	}
}

// TestFileStorageAtomicWriteLeavesNoTempFiles verifies the atomic-write dance
// cleans up after itself: after a save, the directory holds only the two
// committed files, never a leftover temp file (a torn write would surface here).
func TestFileStorageAtomicWriteLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs.SaveStateAndSnapshot([]byte("s"), []byte("snap"))
	fs.SaveRaftState([]byte("s2"))

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if name != stateFileName && name != snapFileName {
			t.Errorf("unexpected leftover file in storage dir: %q", name)
		}
	}
}

// TestFileStorageStaleTempFileIgnored verifies a leftover temp file from a crashed
// mid-write (the previous process never reached the rename) does not corrupt the
// committed state: the real file still reads back cleanly.
func TestFileStorageStaleTempFileIgnored(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs.SaveRaftState([]byte("committed"))

	// Simulate a crash partway through a later write: a half-written temp file is
	// left behind, the rename never happened.
	if err := os.WriteFile(filepath.Join(dir, stateFileName+".tmp-garbage"), []byte("HALF"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := fs2.ReadRaftState(); !bytes.Equal(got, []byte("committed")) {
		t.Fatalf("stale temp file corrupted committed state: got %q", got)
	}
}
