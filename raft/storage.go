package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
)

func cloneState(st PersistentState) PersistentState {
	logCopy := make([]LogEntry, len(st.Log))
	copy(logCopy, st.Log)
	st.Log = logCopy
	return st
}

// MemoryStorage survives node restarts inside one process (used by tests to
// simulate crash-recovery) but not process exit.
type MemoryStorage struct {
	mu    sync.Mutex
	state PersistentState
	saved bool
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) SaveState(st PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state, m.saved = cloneState(st), true
	return nil
}

func (m *MemoryStorage) LoadState() (PersistentState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state), m.saved, nil
}

// FileStorage writes the full state to a temp file, fsyncs it, then atomically
// renames it over the old one, so a crash mid-write never leaves a torn file.
//
// Rewriting the whole log on every change is O(log size). That is fine for a
// demo; the natural next step is an append-only WAL plus snapshots.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{path: filepath.Join(dir, "raft-state.gob")}, nil
}

func (f *FileStorage) SaveState(st PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fh.Write(buf.Bytes()); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	// fsync the directory so the rename itself is durable.
	if d, err := os.Open(filepath.Dir(f.path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

func (f *FileStorage) LoadState() (PersistentState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return PersistentState{}, false, nil
	}
	if err != nil {
		return PersistentState{}, false, err
	}
	var st PersistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return PersistentState{}, false, err
	}
	return st, true, nil
}
