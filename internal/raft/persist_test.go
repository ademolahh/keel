package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

func TestFilePersister(t *testing.T) {
	t.Run("loads nothing before the first save", func(t *testing.T) {
		p := NewFilePersister(t.TempDir(), t.TempDir())

		state, err := p.Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if state != nil {
			t.Errorf("state: expected nil, got %v", state)
		}
	})

	t.Run("loads the last term and vote saved", func(t *testing.T) {
		p := NewFilePersister(t.TempDir(), t.TempDir())

		p.SaveState(1, nil)
		p.SaveState(2, uint64Ptr(3))

		state := load(t, p)

		if state.CurrentTerm != 2 {
			t.Errorf("term: expected 2, got %d", state.CurrentTerm)
		}

		if state.VotedFor == nil || *state.VotedFor != 3 {
			t.Errorf("voted for: expected 3, got %v", state.VotedFor)
		}
	})

	t.Run("keeps the term and vote in a file of their own", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)

		p.SaveLog(1, makeLogs())
		before, err := os.ReadFile(filepath.Join(dir, "raft.log"))
		if err != nil {
			t.Fatalf("read log: %v", err)
		}

		p.SaveState(5, uint64Ptr(2))
		after, err := os.ReadFile(filepath.Join(dir, "raft.log"))
		if err != nil {
			t.Fatalf("read log: %v", err)
		}

		if string(before) != string(after) {
			t.Error("log file: expected it unchanged by a term save")
		}
	})

	t.Run("appends entries after the ones already saved", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)

		p.SaveLog(1, makeLogs()[:2])
		p.SaveLog(3, makeLogs()[2:])

		assertLogs(t, load(t, NewFilePersister(dir, dir)).Logs, makeLogs())
	})

	t.Run("replaces entries from the index it is given", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)

		p.SaveLog(1, makeLogs())
		replacement := []*proto.LogEntry{{Term: 4, Cmd: "set x=9"}}
		p.SaveLog(3, replacement)

		want := append(makeLogs()[:2], replacement...)
		assertLogs(t, load(t, NewFilePersister(dir, dir)).Logs, want)
	})

	t.Run("keeps appending after a reload", func(t *testing.T) {
		dir := t.TempDir()
		NewFilePersister(dir, dir).SaveLog(1, makeLogs()[:3])

		p := NewFilePersister(dir, dir)
		load(t, p)
		p.SaveLog(4, makeLogs()[3:])

		assertLogs(t, load(t, NewFilePersister(dir, dir)).Logs, makeLogs())
	})

	t.Run("refuses to leave a gap in the log", func(t *testing.T) {
		p := NewFilePersister(t.TempDir(), t.TempDir())
		p.SaveLog(1, makeLogs()[:2])

		if err := p.SaveLog(4, makeLogs()[3:]); err == nil {
			t.Error("save log: expected an error, got none")
		}
	})

	t.Run("drops a partial record left by a crash", func(t *testing.T) {
		dir := t.TempDir()
		NewFilePersister(dir, dir).SaveLog(1, makeLogs()[:2])

		f, err := os.OpenFile(filepath.Join(dir, "raft.log"), os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		f.Write([]byte{9, 0, 0, 0, 1, 2})
		f.Close()

		p := NewFilePersister(dir, dir)
		assertLogs(t, load(t, p).Logs, makeLogs()[:2])

		p.SaveLog(3, makeLogs()[2:3])
		assertLogs(t, load(t, NewFilePersister(dir, dir)).Logs, makeLogs()[:3])
	})

	t.Run("saves the snapshot in its own directory", func(t *testing.T) {
		dir, snapshotDir := t.TempDir(), t.TempDir()
		p := NewFilePersister(dir, snapshotDir)
		want := &proto.Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 3, Data: []byte("state")}

		if err := p.SaveSnapshot(snapshotBytes(t, want)); err != nil {
			t.Fatalf("save snapshot: %v", err)
		}

		if got := readSnapshot(t, snapshotDir); !protobuf.Equal(got, want) {
			t.Errorf("snapshot: expected %v, got %v", want, got)
		}

		if _, err := os.Stat(filepath.Join(dir, "raft.snapshot")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("snapshot in the persist directory: expected none, got %v", err)
		}
	})

	t.Run("replaces the previous snapshot", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)
		want := &proto.Snapshot{LastIncludedIndex: 9, LastIncludedTerm: 4, Data: []byte("newer")}

		p.SaveSnapshot(snapshotBytes(t, &proto.Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 3, Data: []byte("older")}))
		p.SaveSnapshot(snapshotBytes(t, want))

		if got := readSnapshot(t, dir); !protobuf.Equal(got, want) {
			t.Errorf("snapshot: expected %v, got %v", want, got)
		}
	})

	t.Run("keeps the log's first index across a reload", func(t *testing.T) {
		dir := t.TempDir()
		NewFilePersister(dir, dir).ResetLog(4, makeLogs()[4:])

		state := load(t, NewFilePersister(dir, dir))

		if state.LogBase != 4 {
			t.Errorf("log base: expected 4, got %d", state.LogBase)
		}

		assertLogs(t, state.Logs, makeLogs()[4:])
	})

	t.Run("saves by log index after a reset", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)

		p.ResetLog(3, nil)
		if err := p.SaveLog(4, makeLogs()[3:]); err != nil {
			t.Fatalf("save log: %v", err)
		}

		state := load(t, NewFilePersister(dir, dir))
		if state.LogBase != 3 {
			t.Errorf("log base: expected 3, got %d", state.LogBase)
		}

		assertLogs(t, state.Logs, makeLogs()[3:])
	})

	t.Run("refuses to save at or before the log's first index", func(t *testing.T) {
		p := NewFilePersister(t.TempDir(), t.TempDir())
		p.ResetLog(4, nil)

		if err := p.SaveLog(4, makeLogs()[3:]); err == nil {
			t.Error("save log: expected an error, got none")
		}
	})

	t.Run("loads no snapshot before the first save", func(t *testing.T) {
		data, err := NewFilePersister(t.TempDir(), t.TempDir()).LoadSnapshot()
		if err != nil || data != nil {
			t.Errorf("snapshot: expected none, got %q (err %v)", data, err)
		}
	})

	t.Run("loads the saved snapshot", func(t *testing.T) {
		dir := t.TempDir()
		want := snapshotBytes(t, &proto.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 3, Data: []byte("state")})
		NewFilePersister(dir, dir).SaveSnapshot(want)

		got, err := NewFilePersister(dir, dir).LoadSnapshot()
		if err != nil {
			t.Fatalf("load snapshot: %v", err)
		}

		if string(got) != string(want) {
			t.Error("snapshot: expected the saved bytes back")
		}
	})

	t.Run("rejects a log file shorter than its header", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "raft.log"), []byte{1, 2, 3}, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}

		if _, err := NewFilePersister(dir, dir).Load(); err == nil {
			t.Error("load: expected an error, got none")
		}
	})

	t.Run("rejects a state file that does not decode", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "raft.state"), []byte{0xff}, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}

		if _, err := NewFilePersister(dir, dir).Load(); err == nil {
			t.Error("load: expected an error, got none")
		}
	})

	t.Run("leaves no temporary file behind", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)

		if err := p.SaveState(1, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := os.Stat(filepath.Join(dir, "raft.state.tmp")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("temporary file: expected none, got %v", err)
		}
	})

	t.Run("fails when its directory is missing", func(t *testing.T) {
		p := NewFilePersister(filepath.Join(t.TempDir(), "missing"), filepath.Join(t.TempDir(), "missing"))

		if err := p.SaveState(1, nil); err == nil {
			t.Error("save state: expected an error, got none")
		}

		if err := p.SaveLog(1, makeLogs()); err == nil {
			t.Error("save log: expected an error, got none")
		}

		if err := p.SaveSnapshot(nil); err == nil {
			t.Error("save snapshot: expected an error, got none")
		}
	})
}

func TestReadPersist(t *testing.T) {
	t.Run("fails when the saved state cannot be loaded", func(t *testing.T) {
		peers := map[uint64]string{1: "localhost:0"}

		if _, err := New(1, peers, nil, failingPersister{}); err == nil {
			t.Error("new: expected an error, got none")
		}
	})
}

func TestPersistFailure(t *testing.T) {
	t.Run("panics when saving the term fails", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.persister = failingPersister{}

		defer func() {
			if recover() == nil {
				t.Error("persist: expected a panic, got none")
			}
		}()

		raft.persistState()
	})

	t.Run("panics when saving the log fails", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.persister = failingPersister{}
		raft.logs = makeLogs()

		defer func() {
			if recover() == nil {
				t.Error("persist: expected a panic, got none")
			}
		}()

		raft.persistLog(1)
	})
}

// HELPERS

type failingPersister struct{}

func (failingPersister) SaveState(uint64, *uint64) error {
	return errors.New("disk full")
}

func (failingPersister) SaveLog(uint64, []*proto.LogEntry) error {
	return errors.New("disk full")
}

func (failingPersister) Load() (*proto.PersistentState, error) {
	return nil, errors.New("disk unreadable")
}

func (failingPersister) SaveSnapshot([]byte) error {
	return errors.New("disk full")
}

func (failingPersister) ResetLog(uint64, []*proto.LogEntry) error {
	return errors.New("disk full")
}

func (failingPersister) Sync() error {
	return errors.New("disk full")
}

func (failingPersister) LoadSnapshot() ([]byte, error) {
	return nil, errors.New("disk unreadable")
}

func load(t *testing.T, p Persister) *proto.PersistentState {
	t.Helper()

	state, err := p.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if state == nil {
		t.Fatal("load: expected saved state, got none")
	}

	return state
}

func snapshotBytes(t *testing.T, s *proto.Snapshot) []byte {
	t.Helper()

	data, err := protobuf.Marshal(s)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	return data
}

func readSnapshot(t *testing.T, dir string) *proto.Snapshot {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, "raft.snapshot"))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	var snapshot proto.Snapshot
	if err := protobuf.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	return &snapshot
}

func assertLogs(t *testing.T, got, want []*proto.LogEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("log size: expected %d, got %d", len(want), len(got))
	}

	for i := range want {
		if !protobuf.Equal(got[i], want[i]) {
			t.Errorf("entry %d: expected %v, got %v", i+1, want[i], got[i])
		}
	}
}
