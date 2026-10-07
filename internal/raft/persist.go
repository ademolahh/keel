package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type Persister interface {
	SaveState(term uint64, votedFor *uint64) error

	SaveLog(from uint64, entries []*proto.LogEntry) error

	Load() (*proto.PersistentState, error)
}

type FilePersister struct {
	statePath string
	logPath   string

	offsets []int64
	size    int64
	indexed bool
}

const recordHeader = 8

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func NewFilePersister(dir string) *FilePersister {
	return &FilePersister{
		statePath: filepath.Join(dir, "raft.state"),
		logPath:   filepath.Join(dir, "raft.log"),
	}
}

func (p *FilePersister) SaveState(term uint64, votedFor *uint64) error {
	data, err := protobuf.Marshal(&proto.PersistentState{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return err
	}

	return writeAtomic(p.statePath, data)
}

func (p *FilePersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	if !p.indexed {
		if _, err := p.readLog(); err != nil {
			return err
		}
	}

	if from == 0 || from-1 > uint64(len(p.offsets)) {
		return fmt.Errorf("save log from %d: %d entries saved", from, len(p.offsets))
	}

	start := p.size
	if from-1 < uint64(len(p.offsets)) {
		start = p.offsets[from-1]
	}

	var buf []byte
	added := make([]int64, 0, len(entries))

	for _, e := range entries {
		data, err := protobuf.Marshal(e)
		if err != nil {
			return err
		}

		//  [length][crc][data]
		added = append(added, start+int64(len(buf)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
		buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(data, castagnoli))
		buf = append(buf, data...)
	}

	_, statErr := os.Stat(p.logPath)
	created := errors.Is(statErr, os.ErrNotExist)

	if err := p.writeLog(start, buf); err != nil {
		p.indexed = false
		return err
	}

	if created {
		if err := syncDir(p.logPath); err != nil {
			return err
		}
	}

	p.offsets = append(p.offsets[:from-1], added...)
	p.size = start + int64(len(buf))

	return nil
}

func (p *FilePersister) writeLog(start int64, buf []byte) error {
	f, err := os.OpenFile(p.logPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Truncate(start); err != nil {
		return err
	}

	if _, err := f.WriteAt(buf, start); err != nil {
		return err
	}

	return f.Sync()
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

	if data == nil && logs == nil {
		return nil, nil
	}

	state.Logs = logs

	return &state, nil
}

func (p *FilePersister) readLog() ([]*proto.LogEntry, error) {
	data, err := os.ReadFile(p.logPath)
	if errors.Is(err, os.ErrNotExist) {
		p.offsets, p.size, p.indexed = nil, 0, true
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var (
		logs    []*proto.LogEntry
		offsets []int64
		pos     int64
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

	p.offsets, p.size, p.indexed = offsets, pos, true

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
	if err := r.persister.SaveLog(from, r.logs[from-1:]); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}
}

func (r *Raft) readPersist() error {
	state, err := r.persister.Load()
	if err != nil || state == nil {
		return err
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor

	if state.Logs != nil {
		r.logs = state.Logs
	}

	return nil
}
