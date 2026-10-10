package raft

import (
	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type logState struct {
	logs              []*proto.LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	logBytes          int

	syncedIndex uint64
	logGen      uint64
	syncCh      chan struct{}
	synced      broadcast
}

func (l *logState) lastIndex() uint64 {
	return l.lastIncludedIndex + uint64(len(l.logs))
}

func (l *logState) termAt(index uint64) uint64 {
	if index == l.lastIncludedIndex {
		return l.lastIncludedTerm
	}

	return l.logs[index-l.lastIncludedIndex-1].Term
}

func (l *logState) entriesFrom(index uint64) []*proto.LogEntry {
	return l.logs[index-l.lastIncludedIndex-1:]
}

func (l *logState) addEntries(entries ...*proto.LogEntry) {
	l.logs = append(l.logs, entries...)
	for _, e := range entries {
		l.logBytes += protobuf.Size(e)
	}
}

func (l *logState) truncateFrom(index uint64) {
	cut := index - l.lastIncludedIndex - 1
	for _, e := range l.logs[cut:] {
		l.logBytes -= protobuf.Size(e)
	}

	l.logs = l.logs[:cut]
}

func (l *logState) setEntries(entries []*proto.LogEntry) {
	l.logs = entries
	l.logBytes = 0
	for _, e := range entries {
		l.logBytes += protobuf.Size(e)
	}
}

func (l *logState) markSynced(index uint64) {
	l.syncedIndex = index
	l.synced.notify()
}
