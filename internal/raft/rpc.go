package raft

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// save before the reply leaves whenever the term or vote changed
	dirty := false
	defer func() {
		if dirty {
			r.persistState()
		}
	}()

	if r.currentTerm > req.Term {
		return &proto.RequestVoteResponse{
			VoteGranted: false,
			Term:        r.currentTerm,
		}, nil
	}

	// candidate has the higher term
	if r.currentTerm < req.Term {
		r.currentTerm = req.Term
		r.votedFor = nil
		r.state = Follower
		r.leaderId = 0
		dirty = true
		r.log.Debug("adopted newer term", "term", req.Term, "from", "vote request")
	}

	lastLogIndex := r.lastIndex()

	if lastLogIndex != 0 {
		lastLogTerm := r.termAt(lastLogIndex)

		// if terms are not the same, the one with higher term is the latest
		if lastLogTerm > req.LastLogTerm {
			return &proto.RequestVoteResponse{
				VoteGranted: false,
				Term:        r.currentTerm,
			}, nil
		}

		// if terms are the same, the one with higher index is latest
		if req.LastLogTerm == lastLogTerm && lastLogIndex > req.LastLogIndex {
			return &proto.RequestVoteResponse{
				VoteGranted: false,
				Term:        r.currentTerm,
			}, nil
		}
	}

	// at this point, the terms are equal
	// only one candidate per term
	if r.votedFor != nil && *r.votedFor != req.CandidateId {
		return &proto.RequestVoteResponse{
			VoteGranted: false,
			Term:        r.currentTerm,
		}, nil
	}

	r.votedFor = &req.CandidateId
	dirty = true
	r.log.Debug("granted vote", "candidate", req.CandidateId, "term", r.currentTerm)
	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.RequestVoteResponse{
		VoteGranted: true,
		Term:        r.currentTerm,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context,
	req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// save before the reply leaves whenever the term or log changed
	stateDirty := false
	var logFrom uint64
	defer func() {
		if stateDirty {
			r.persistState()
		}

		if logFrom != 0 {
			r.persistLog(logFrom)
		}
	}()

	no := &proto.AppendEntriesResponse{Term: r.currentTerm, Success: false}

	if req.Term < r.currentTerm {
		return no, nil
	}

	// only the leader of a term at least as new as ours sends this
	if r.leaderId != req.LeaderId {
		r.leaderChanges++
	}
	r.leaderId = req.LeaderId
	r.state = Follower

	prevLogIndex, prevLogTerm, entries := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	if prevLogIndex < r.lastIncludedIndex {
		skip := min(r.lastIncludedIndex-prevLogIndex, uint64(len(entries)))
		entries = entries[skip:]
		prevLogIndex, prevLogTerm = r.lastIncludedIndex, r.lastIncludedTerm
	}

	followerLogIndex := r.lastIndex()
	if prevLogIndex != 0 {
		// leader - {term: 1, index: 1} {term: 2, index: 2} {term: 3, index: 3}
		// follower - {term: 1, index: 1} {term: 2, index: 2}
		if prevLogIndex > followerLogIndex {
			r.log.Debug("rejected append", "reason", "missing entry", "prev_log_index", prevLogIndex)
			return &proto.AppendEntriesResponse{
				Term:    r.currentTerm,
				Success: false,
				Hint:    &followerLogIndex,
			}, nil
		}

		// leader - {term: 1, index: 1} {term: 2, index: 2}
		// follower - {term: 1, index: 1} {term: 3, index: 2}

		// OR
		// leader - {term: 1, index: 1} {term: 2, index: 2} {term: 2, index: 3}
		// follower - {term: 1, index: 1} {term: 2, index: 2} {term: 3, index: 3} {term: 3, index: 4}
		if r.termAt(prevLogIndex) != prevLogTerm {
			r.log.Debug("rejected append", "reason", "term mismatch", "prev_log_index", prevLogIndex)
			index := new(r.lastIncludedIndex)
			if i := getMatchingTermIndex(r.logs, prevLogTerm, int(prevLogIndex-r.lastIncludedIndex)); i != nil {
				*index += *i
			}

			return &proto.AppendEntriesResponse{
				Term:    r.currentTerm,
				Success: false,
				Hint:    index,
			}, nil
		}
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = nil
		stateDirty = true
		r.log.Debug("adopted newer term", "term", req.Term, "from", "append entries")
	}

	// if log and term is the same, then all entry store the same command
	// if log and term is the same, the logs are identical in all preceeding entries

	for i, entry := range entries {
		index := prevLogIndex + uint64(i) + 1

		if index <= r.lastIndex() {
			if r.termAt(index) == entry.Term {
				continue
			}

			r.logs = r.logs[:index-r.lastIncludedIndex-1]
		}

		r.logs = append(r.logs, entries[i:]...)
		logFrom = index
		break
	}

	if req.LeaderCommit > r.commitIndex {
		lastNew := prevLogIndex + uint64(len(entries))
		if n := min(req.LeaderCommit, lastNew); n > r.commitIndex {
			r.commitIndex = n
			r.notifyCommit()
		}
	}

	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func (r *Raft) InstallSnapshot(ctx context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		return &proto.InstallSnapshotResponse{
			Term: r.currentTerm,
		}, nil
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = nil
		r.persistState()
		r.log.Debug("adopted newer term", "term", req.Term, "from", "install snapshot")
	}

	if r.leaderId != req.LeaderId {
		r.leaderChanges++
	}
	r.leaderId = req.LeaderId
	r.state = Follower
	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	reply := &proto.InstallSnapshotResponse{Term: r.currentTerm}

	if req.Offset == 0 {
		r.snapshotChunks = nil
	}

	if req.Offset != uint64(len(r.snapshotChunks)) {
		r.log.Debug("rejected snapshot chunk", "offset", req.Offset, "have", len(r.snapshotChunks))
		return reply, nil
	}

	r.snapshotChunks = append(r.snapshotChunks, req.Data...)

	if !req.Done {
		return reply, nil
	}

	snapshot := &proto.Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              r.snapshotChunks,
	}
	r.snapshotChunks = nil

	if snapshot.LastIncludedIndex <= r.commitIndex {
		return reply, nil
	}

	data, err := protobuf.Marshal(snapshot)
	if err != nil {
		r.log.Error("snapshot not saved", "index", snapshot.LastIncludedIndex, "err", err)
		return reply, nil
	}

	if err := r.persister.SaveSnapshot(data); err != nil {
		r.log.Error("snapshot not saved", "index", snapshot.LastIncludedIndex, "err", err)
		return reply, nil
	}

	if snapshot.LastIncludedIndex <= r.lastIndex() && r.termAt(snapshot.LastIncludedIndex) == snapshot.LastIncludedTerm {
		r.logs = slices.Clone(r.entriesFrom(snapshot.LastIncludedIndex + 1))
	} else {
		r.logs = nil
	}

	r.lastIncludedIndex = snapshot.LastIncludedIndex
	r.lastIncludedTerm = snapshot.LastIncludedTerm
	r.commitIndex = snapshot.LastIncludedIndex
	r.snapshot = snapshot
	r.pendingSnapshot = snapshot
	r.notifyCommit()

	if err := r.persister.ResetLog(snapshot.LastIncludedIndex, r.logs); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	r.log.Info("installed snapshot", "index", snapshot.LastIncludedIndex, "term", snapshot.LastIncludedTerm,
		"kept_entries", len(r.logs))

	return reply, nil
}
