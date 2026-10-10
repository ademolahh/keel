package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type Persister interface {
	SaveState(term uint64, votedFor *uint64) error

	SaveLog(from uint64, entries []*proto.LogEntry) error

	Load() (*proto.PersistentState, error)

	ResetLog(base uint64, entries []*proto.LogEntry) error

	SaveSnapshot(data []byte) error

	LoadSnapshot() ([]byte, error)

	Sync() error
}

type FilePersister struct {
	statePath    string
	logPath      string
	snapshotPath string

	logMu   sync.Mutex
	log     *os.File
	base    uint64
	offsets []int64
	size    int64
}

const (
	logHeader    = 8
	recordHeader = 8
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func OpenFilePersister(dir, snapshotDir string) (*FilePersister, error) {
	p := &FilePersister{
		statePath:    filepath.Join(dir, "raft.state"),
		logPath:      filepath.Join(dir, "raft.log"),
		snapshotPath: filepath.Join(snapshotDir, "raft.snapshot"),
	}

	if _, err := os.Stat(p.logPath); errors.Is(err, os.ErrNotExist) {
		if err := writeAtomic(p.logPath, binary.LittleEndian.AppendUint64(nil, 0)); err != nil {
			return nil, err
		}
	}

	if _, err := p.readLog(); err != nil {
		return nil, err
	}

	if err := p.openLog(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *FilePersister) openLog() error {
	f, err := os.OpenFile(p.logPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	p.logMu.Lock()
	old := p.log
	p.log = f
	p.logMu.Unlock()

	if old != nil {
		old.Close()
	}

	return nil
}

func (p *FilePersister) Close() error {
	return p.log.Close()
}

func (p *FilePersister) SaveState(term uint64, votedFor *uint64) error {
	data, err := protobuf.Marshal(&proto.PersistentState{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return err
	}

	return writeAtomic(p.statePath, data)
}

func (p *FilePersister) SaveSnapshot(data []byte) error {
	return writeAtomic(p.snapshotPath, data)
}

func (p *FilePersister) LoadSnapshot() ([]byte, error) {
	data, err := os.ReadFile(p.snapshotPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	return data, err
}

func (p *FilePersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	if from <= p.base || from-1-p.base > uint64(len(p.offsets)) {
		return fmt.Errorf("save log from %d: entries %d to %d saved",
			from, p.base+1, p.base+uint64(len(p.offsets)))
	}

	pos := from - 1 - p.base

	start := p.size
	if pos < uint64(len(p.offsets)) {
		start = p.offsets[pos]
	}

	buf, added, err := encodeRecords(start, entries)
	if err != nil {
		return err
	}

	if err := p.log.Truncate(start); err != nil {
		return err
	}

	if _, err := p.log.WriteAt(buf, start); err != nil {
		return err
	}

	p.offsets = append(p.offsets[:pos], added...)
	p.size = start + int64(len(buf))

	return nil
}

func (p *FilePersister) ResetLog(base uint64, entries []*proto.LogEntry) error {
	records, offsets, err := encodeRecords(logHeader, entries)
	if err != nil {
		return err
	}

	buf := binary.LittleEndian.AppendUint64(nil, base)
	buf = append(buf, records...)

	if err := writeAtomic(p.logPath, buf); err != nil {
		return err
	}

	p.base, p.offsets, p.size = base, offsets, int64(len(buf))

	return p.openLog()
}

func encodeRecords(start int64, entries []*proto.LogEntry) ([]byte, []int64, error) {
	var buf []byte
	offsets := make([]int64, 0, len(entries))

	for _, e := range entries {
		data, err := protobuf.Marshal(e)
		if err != nil {
			return nil, nil, err
		}

		//  [length][crc][data]
		offsets = append(offsets, start+int64(len(buf)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
		buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(data, castagnoli))
		buf = append(buf, data...)
	}

	return buf, offsets, nil
}

func (p *FilePersister) Sync() error {
	p.logMu.Lock()
	f := p.log
	p.logMu.Unlock()

	// a closed file was replaced by ResetLog, which fsyncs everything it keeps
	if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}

	return nil
}

func (p *FilePersister) Load() (*proto.PersistentState, error) {
	data, err := os.ReadFile(p.statePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	var state proto.PersistentState
	if err := protobuf.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	logs, err := p.readLog()
	if err != nil {
		return nil, err
	}

	if data == nil && p.base == 0 && len(logs) == 0 {
		return nil, nil
	}

	state.Logs = logs
	state.LogBase = p.base

	return &state, nil
}

func (p *FilePersister) readLog() ([]*proto.LogEntry, error) {
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		return nil, err
	}

	if len(data) < logHeader {
		return nil, fmt.Errorf("log %s: header is %d bytes, want %d", p.logPath, len(data), logHeader)
	}

	var (
		logs    []*proto.LogEntry
		offsets []int64
		pos     int64 = logHeader
	)

	for {
		rest := data[pos:]
		if len(rest) < recordHeader {
			if len(rest) == 0 {
				slog.Debug("read log", "path", p.logPath, "entries", len(logs))
			} else {
				slog.Warn("partial record header", "path", p.logPath, "offset", pos, "bytes", len(rest))
			}
			break
		}

		n := binary.LittleEndian.Uint32(rest)
		sum := binary.LittleEndian.Uint32(rest[4:])

		if uint64(len(rest)-recordHeader) < uint64(n) {
			slog.Warn("partial record body", "path", p.logPath, "offset", pos,
				"want", n, "have", len(rest)-recordHeader)
			break
		}

		body := rest[recordHeader : recordHeader+n]
		if crc32.Checksum(body, castagnoli) != sum {
			slog.Warn("record checksum mismatch", "path", p.logPath, "offset", pos)
			break
		}

		var e proto.LogEntry
		if err := protobuf.Unmarshal(body, &e); err != nil {
			slog.Warn("record does not decode", "path", p.logPath, "offset", pos, "err", err)
			break
		}

		logs = append(logs, &e)
		offsets = append(offsets, pos)
		pos += recordHeader + int64(n)
	}

	if pos < int64(len(data)) {
		slog.Warn("dropping partial log record", "path", p.logPath, "bytes", int64(len(data))-pos)

		if err := os.Truncate(p.logPath, pos); err != nil {
			return nil, err
		}
	}

	p.base, p.offsets, p.size = binary.LittleEndian.Uint64(data), offsets, pos

	return logs, nil
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"

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

	if err := os.Rename(tmp, path); err != nil {
		return err
	}

	return syncDir(path)
}

func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}

func (r *Raft) persistState() {
	if err := r.persister.SaveState(r.currentTerm, r.votedFor); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}
}

func (r *Raft) persistLog(from uint64) {
	if err := r.persister.SaveLog(from, r.entriesFrom(from)); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	if from <= r.syncedIndex {
		r.syncedIndex = from - 1
		r.logGen++
	}

	r.notifySync()
}

func (r *Raft) resetLog() {
	if err := r.persister.ResetLog(r.lastIncludedIndex, r.logs); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	r.logGen++
	r.markSynced(r.lastIndex())
	r.advanceCommit()
}

func (r *Raft) readPersist() error {
	state, err := r.persister.Load()
	if err != nil {
		return err
	}

	saved, err := r.persister.LoadSnapshot()
	if err != nil {
		return err
	}

	if state != nil {
		r.currentTerm = state.CurrentTerm
		r.votedFor = state.VotedFor
		r.lastIncludedIndex = state.LogBase

		if state.Logs != nil {
			r.setEntries(state.Logs)
		}
	}

	if saved == nil {
		if r.lastIncludedIndex != 0 {
			return fmt.Errorf("log starts after entry %d but there is no snapshot", r.lastIncludedIndex)
		}

		return nil
	}

	var snapshot proto.Snapshot
	if err := protobuf.Unmarshal(saved, &snapshot); err != nil {
		return err
	}

	index := snapshot.LastIncludedIndex
	if index < r.lastIncludedIndex {
		return fmt.Errorf("snapshot ends at entry %d but the log starts after %d", index, r.lastIncludedIndex)
	}

	if index > r.lastIncludedIndex {
		if index <= r.lastIndex() && r.termAt(index) == snapshot.LastIncludedTerm {
			r.setEntries(slices.Clone(r.entriesFrom(index + 1)))
		} else {
			r.setEntries(nil)
		}
	}

	r.lastIncludedIndex = index
	r.lastIncludedTerm = snapshot.LastIncludedTerm
	r.commitIndex = index
	r.snapshot = &snapshot
	r.pendingSnapshot = &snapshot

	return nil
}
