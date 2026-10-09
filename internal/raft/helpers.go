package raft

import (
	"context"
	"maps"
	"math/rand"
	"slices"
	"time"

	"github.com/ademolahh/keel/proto"
)

type Status struct {
	Term     uint64
	VotedFor *uint64
	Logs     []*proto.LogEntry
}

type Stats struct {
	Term          uint64
	State         RaftState
	CommitIndex   uint64
	LastApplied   uint64
	LogEntries    uint64
	LeaderChanges uint64

	MatchIndex map[uint64]uint64
}

func (r *Raft) SetSnapshotThreshold(bytes int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.snapshotThreshold = bytes
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Leader
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := Status{
		Term: r.currentTerm,
		Logs: slices.Clone(r.logs),
	}

	if r.votedFor != nil {
		s.VotedFor = new(*r.votedFor)
	}

	return s
}

func (r *Raft) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := Stats{
		Term:          r.currentTerm,
		State:         r.state,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		LogEntries:    uint64(len(r.logs)),
		LeaderChanges: r.leaderChanges,
	}

	if r.state == Leader {
		s.MatchIndex = maps.Clone(r.matchIndex)
	}

	return s
}

func (r *Raft) CaughtUp() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastApplied >= r.commitIndex
}

func (r *Raft) Leader() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderId, r.leaderId != 0
}

func (r *Raft) lastIndex() uint64 {
	return r.lastIncludedIndex + uint64(len(r.logs))
}

func (r *Raft) termAt(index uint64) uint64 {
	if index == r.lastIncludedIndex {
		return r.lastIncludedTerm
	}

	return r.logs[index-r.lastIncludedIndex-1].Term
}

func (r *Raft) entriesFrom(index uint64) []*proto.LogEntry {
	return r.logs[index-r.lastIncludedIndex-1:]
}

func (r *Raft) waitApplied(ctx context.Context, index uint64) error {
	for {
		r.mu.Lock()
		done := r.lastApplied >= index
		applied := r.applied
		r.mu.Unlock()

		if done {
			return nil
		}

		select {
		case <-applied:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Raft) waitSynced(ctx context.Context, index uint64) error {
	for {
		r.mu.Lock()
		done := r.syncedIndex >= index
		synced := r.synced
		r.mu.Unlock()

		if done {
			return nil
		}

		select {
		case <-synced:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Raft) notifySync() {
	select {
	case r.syncCh <- struct{}{}:
	default:
	}
}

func (r *Raft) markSynced(index uint64) {
	r.syncedIndex = index
	close(r.synced)
	r.synced = make(chan struct{})
}

func (r *Raft) advanceCommit() {
	if r.state != Leader {
		return
	}

	acked := make(map[uint64]uint64, len(r.peers))
	for _, p := range r.peers {
		acked[p.id] = r.matchIndex[p.id]
	}

	n := match(acked, r.syncedIndex)[majority(len(r.peers))-1]

	if n > r.commitIndex && r.termAt(n) == r.currentTerm {
		r.commitIndex = n
		r.notifyCommit()
		r.log.Debug("committed", "index", n)
	}
}

func (r *Raft) notifyCommit() {
	select {
	case r.commitCh <- struct{}{}:
	default:
	}
}

func match(matchIndex map[uint64]uint64, leader uint64) []uint64 {
	index := []uint64{}
	for _, idx := range matchIndex {
		index = append(index, idx)
	}

	index = append(index, leader)

	slices.SortFunc(index, func(a, b uint64) int {
		return int(b - a)
	})

	return index
}

func majority(size int) int {
	return ((size + 1) / 2) + 1
}

func getMatchingTermIndex(logs []*proto.LogEntry, term uint64, prevLogIndex int) *uint64 {
	if prevLogIndex > len(logs) {
		prevLogIndex = len(logs)
	}

	for i := prevLogIndex - 1; i >= 0; i-- {
		if logs[i].Term == term {
			return new(uint64(i) + 1)
		}
	}
	return nil
}

const (
	electionTimeoutMin = 100 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)

	return electionTimeoutMin + time.Duration(rand.Int63n(spread+1))
}
