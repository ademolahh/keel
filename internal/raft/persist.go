package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type Persister interface {
	Save(data []byte) error

	Load() ([]byte, error)
}

type FilePersister struct {
	path string
}

func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path}
}

func (p *FilePersister) Save(data []byte) error {
	tmp := p.path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(p.path))
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}

func (p *FilePersister) Load() ([]byte, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	return data, err
}

func (r *Raft) persist() {
	data, err := protobuf.Marshal(&proto.PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Logs:        r.logs,
	})
	if err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	if err := r.persister.Save(data); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}
}

func (r *Raft) readPersist() error {
	data, err := r.persister.Load()
	if err != nil || data == nil {
		return err
	}

	var state proto.PersistentState
	if err := protobuf.Unmarshal(data, &state); err != nil {
		return err
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.logs = state.Logs

	return nil
}
