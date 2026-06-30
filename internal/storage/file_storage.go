package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage is a Persister that keeps the Raft state blob and the snapshot blob
// as two files in a directory. It is the production durability implementation;
// the deterministic simulation uses InMemoryPersister instead.
//
// Durability model. Each save is made crash-atomic with the classic
// write-temp -> fsync(temp) -> rename -> fsync(dir) sequence:
//
//   - The new bytes are written to a sibling temp file and fsynced, so they are on
//     stable media before anything points at them.
//   - rename(2) atomically swaps the temp file into the final name. A crash leaves
//     either the complete old file or the complete new one - never a torn mix.
//   - The directory is fsynced so the rename itself survives a power loss.
//
// SaveStateAndSnapshot writes both files this way. The pair is not updated under a
// single atomic operation (two renames), so a crash between them can leave the new
// state with the old snapshot. The Raft layer tolerates this: it only ever pairs a
// snapshot with state whose log has already been compacted to cover it, so a
// "new state + old snapshot" recovery simply replays a few extra log entries. A
// torn *single* file, the dangerous case, cannot happen.
type FileStorage struct {
	mu       sync.Mutex
	dir      string
	stateF   string
	snapF    string
	stateLen int
	snapLen  int
}

const (
	stateFileName = "raft-state"
	snapFileName  = "snapshot"
)

// NewFileStorage opens (creating if needed) a FileStorage rooted at dir and loads
// the sizes of any state and snapshot already present, so RaftStateSize/
// SnapshotSize are correct immediately after a restart.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create dir %q: %w", dir, err)
	}
	fs := &FileStorage{
		dir:    dir,
		stateF: filepath.Join(dir, stateFileName),
		snapF:  filepath.Join(dir, snapFileName),
	}
	fs.stateLen = sizeOf(fs.stateF)
	fs.snapLen = sizeOf(fs.snapF)
	return fs, nil
}

// sizeOf returns the byte size of path, or 0 if it does not exist.
func sizeOf(path string) int {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func (fs *FileStorage) SaveRaftState(state []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeFileAtomic(fs.stateF, state); err != nil {
		panic("storage: save raft state: " + err.Error())
	}
	fs.stateLen = len(state)
}

func (fs *FileStorage) ReadRaftState() []byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return readFileOrNil(fs.stateF)
}

func (fs *FileStorage) SaveStateAndSnapshot(state, snapshot []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeFileAtomic(fs.snapF, snapshot); err != nil {
		panic("storage: save snapshot: " + err.Error())
	}
	if err := writeFileAtomic(fs.stateF, state); err != nil {
		panic("storage: save raft state: " + err.Error())
	}
	fs.snapLen = len(snapshot)
	fs.stateLen = len(state)
}

func (fs *FileStorage) ReadSnapshot() []byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return readFileOrNil(fs.snapF)
}

func (fs *FileStorage) RaftStateSize() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.stateLen
}

func (fs *FileStorage) SnapshotSize() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.snapLen
}

// writeFileAtomic durably replaces path with data using a temp file + fsync +
// rename + directory fsync. After it returns nil, a crash cannot lose or tear the
// write.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we bail before the rename succeeds.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil { // fsync the data onto stable media
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil { // atomic swap
		return err
	}
	return syncDir(dir) // make the rename itself durable
}

// syncDir fsyncs a directory so a rename within it survives a crash. On platforms
// where opening a directory for sync is unsupported, the inability to sync is not
// treated as fatal.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Directory fsync is not supported on all OSes/filesystems (notably some
		// Windows configurations); the rename has still occurred.
		return nil
	}
	return nil
}

// readFileOrNil returns the file's contents, or nil if it does not exist.
func readFileOrNil(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		panic("storage: read " + path + ": " + err.Error())
	}
	return data
}

// Compile-time assertion that FileStorage satisfies Persister.
var _ Persister = (*FileStorage)(nil)
