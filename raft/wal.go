package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WALStorage is an append-only write-ahead log. Each SaveState call writes only
// what changed since the previous call (new entries, a truncation point, or a
// new term/vote) and fsyncs once, so the cost is O(change), not O(log size).
//
// Record layout: [len uint32][crc32 uint32][payload]. Payload starts with a type
// byte. A torn final record (crash mid-write) fails its CRC and is discarded on
// recovery, which is safe because SaveState had not returned for it.
type WALStorage struct {
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	path string

	// What is durable, so SaveState can diff against it.
	term, vote int
	terms      []int // terms[i] = term of durable entry at index i (index 0 = sentinel)
	loaded     PersistentState
	hasState   bool
}

const (
	recHardState byte = 1 // term, vote
	recTruncate  byte = 2 // drop entries with index >= n
	recEntry     byte = 3 // term, index, command
)

func NewWALStorage(dir string) (*WALStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &WALStorage{path: filepath.Join(dir, "raft.wal"), vote: -1, terms: []int{0}}
	st := PersistentState{VotedFor: -1, Log: []LogEntry{{}}}
	validEnd, err := s.replay(&st)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(validEnd); err != nil { // drop a torn tail
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	s.f, s.w = f, bufio.NewWriterSize(f, 1<<16)
	s.term, s.vote = st.CurrentTerm, st.VotedFor
	for _, e := range st.Log[1:] {
		s.terms = append(s.terms, e.Term)
	}
	s.loaded = st
	return s, nil
}

func (s *WALStorage) replay(st *PersistentState) (int64, error) {
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return off, nil // EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr)
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, nil
		}
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:]) {
			return off, nil
		}
		if err := applyRecord(st, payload); err != nil {
			return 0, fmt.Errorf("wal: record at offset %d: %w", off, err)
		}
		s.hasState = true
		off += 8 + int64(n)
	}
}

func applyRecord(st *PersistentState, p []byte) error {
	if len(p) == 0 {
		return errors.New("empty record")
	}
	body := p[1:]
	switch p[0] {
	case recHardState:
		st.CurrentTerm = int(int64(binary.LittleEndian.Uint64(body)))
		st.VotedFor = int(int64(binary.LittleEndian.Uint64(body[8:])))
	case recTruncate:
		n := int(binary.LittleEndian.Uint64(body))
		if n < 1 || n > len(st.Log) {
			return fmt.Errorf("bad truncate %d (log len %d)", n, len(st.Log))
		}
		st.Log = st.Log[:n]
	case recEntry:
		term := int(binary.LittleEndian.Uint64(body))
		idx := int(binary.LittleEndian.Uint64(body[8:]))
		if idx != len(st.Log) {
			return fmt.Errorf("entry index %d, expected %d", idx, len(st.Log))
		}
		var cmd []byte
		if body[16] == 1 {
			cmd = append([]byte(nil), body[17:]...)
		}
		st.Log = append(st.Log, LogEntry{Term: term, Index: idx, Command: cmd})
	default:
		return fmt.Errorf("unknown record type %d", p[0])
	}
	return nil
}

func (s *WALStorage) writeRecord(p []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(p)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(p))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := s.w.Write(p)
	return err
}

func (s *WALStorage) SaveState(st PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	wrote := false
	if st.CurrentTerm != s.term || st.VotedFor != s.vote {
		p := make([]byte, 17)
		p[0] = recHardState
		binary.LittleEndian.PutUint64(p[1:], uint64(int64(st.CurrentTerm)))
		binary.LittleEndian.PutUint64(p[9:], uint64(int64(st.VotedFor)))
		if err := s.writeRecord(p); err != nil {
			return err
		}
		wrote = true
	}
	// Longest prefix the durable log shares with the new log.
	common := 1
	for common < len(s.terms) && common < len(st.Log) && s.terms[common] == st.Log[common].Term {
		common++
	}
	if common < len(s.terms) {
		p := make([]byte, 9)
		p[0] = recTruncate
		binary.LittleEndian.PutUint64(p[1:], uint64(common))
		if err := s.writeRecord(p); err != nil {
			return err
		}
		wrote = true
	}
	for _, e := range st.Log[common:] {
		p := make([]byte, 18+len(e.Command))
		p[0] = recEntry
		binary.LittleEndian.PutUint64(p[1:], uint64(e.Term))
		binary.LittleEndian.PutUint64(p[9:], uint64(e.Index))
		if e.Command != nil {
			p[17] = 1
			copy(p[18:], e.Command)
		}
		if err := s.writeRecord(p); err != nil {
			return err
		}
		wrote = true
	}
	if !wrote {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.term, s.vote = st.CurrentTerm, st.VotedFor
	s.terms = s.terms[:common]
	for _, e := range st.Log[common:] {
		s.terms = append(s.terms, e.Term)
	}
	return nil
}

func (s *WALStorage) LoadState() (PersistentState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.loaded), s.hasState, nil
}

func (s *WALStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w.Flush()
	return s.f.Close()
}
