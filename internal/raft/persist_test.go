package raft

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestFilePersister(t *testing.T) {
	t.Run("loads nothing before the first save", func(t *testing.T) {
		p := NewFilePersister(filepath.Join(t.TempDir(), "raft.state"))

		data, err := p.Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if data != nil {
			t.Errorf("data: expected nil, got %q", data)
		}
	})

	t.Run("loads what was saved last", func(t *testing.T) {
		p := NewFilePersister(filepath.Join(t.TempDir(), "raft.state"))

		p.Save([]byte("first"))
		p.Save([]byte("second"))

		data, err := p.Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !slices.Equal(data, []byte("second")) {
			t.Errorf("data: expected %q, got %q", "second", data)
		}
	})

	t.Run("leaves no temporary file behind", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "raft.state")
		p := NewFilePersister(path)

		if err := p.Save([]byte("state")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("temporary file: expected none, got %v", err)
		}
	})

	t.Run("fails when its directory is missing", func(t *testing.T) {
		p := NewFilePersister(filepath.Join(t.TempDir(), "missing", "raft.state"))

		if err := p.Save([]byte("state")); err == nil {
			t.Error("save: expected an error, got none")
		}
	})
}

func TestReadPersist(t *testing.T) {
	t.Run("rejects saved state that does not decode", func(t *testing.T) {
		peers := map[uint64]string{1: "localhost:0"}
		persister := &memoryPersister{data: []byte{0xff}}

		if _, err := New(1, peers, nil, persister); err == nil {
			t.Error("new: expected an error, got none")
		}
	})
}

func TestPersistFailure(t *testing.T) {
	t.Run("panics when the save fails", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.persister = failingPersister{}

		defer func() {
			if recover() == nil {
				t.Error("persist: expected a panic, got none")
			}
		}()

		raft.persist()
	})
}

// HELPERS

type failingPersister struct{}

func (failingPersister) Save([]byte) error {
	return errors.New("disk full")
}

func (failingPersister) Load() ([]byte, error) {
	return nil, nil
}
