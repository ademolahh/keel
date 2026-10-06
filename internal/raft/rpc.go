package raft

import (
	"context"
	"time"

	"github.com/ademolahh/keel/proto"
)

func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// save before the reply leaves whenever the term or vote changed
	dirty := false
	defer func() {
		if dirty {
			r.persist()
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

	lastLogIndex := len(r.logs)

	if lastLogIndex != 0 {
		// if terms are not the same, the one with higher term is the latest
		if r.logs[lastLogIndex-1].Term > req.LastLogTerm {
			return &proto.RequestVoteResponse{
				VoteGranted: false,
				Term:        r.currentTerm,
			}, nil
		}

		// if terms are the same, the one with higher index is latest
		if req.LastLogTerm == r.logs[lastLogIndex-1].Term && lastLogIndex > int(req.LastLogIndex) {
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
	dirty := false
	defer func() {
		if dirty {
			r.persist()
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

	followerLogIndex := uint64(len(r.logs))
	if req.PrevLogIndex != 0 {
		// leader - {term: 1, index: 1} {term: 2, index: 2} {term: 3, index: 3}
		// follower - {term: 1, index: 1} {term: 2, index: 2}
		if req.PrevLogIndex > followerLogIndex {
			r.log.Debug("rejected append", "reason", "missing entry", "prev_log_index", req.PrevLogIndex)
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
		if r.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			r.log.Debug("rejected append", "reason", "term mismatch", "prev_log_index", req.PrevLogIndex)
			index := getMatchingTermIndex(r.logs, req.PrevLogTerm, int(req.PrevLogIndex))
			if index == nil {
				index = new(uint64(0))
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
		dirty = true
		r.log.Debug("adopted newer term", "term", req.Term, "from", "append entries")
	}

	// if log and term is the same, then all entry store the same command
	// if log and term is the same, the logs are identical in all preceeding entries

	for i, entry := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1

		if index <= uint64(len(r.logs)) {
			if r.logs[index-1].Term == entry.Term {
				continue
			}

			r.logs = r.logs[:index-1]
		}

		r.logs = append(r.logs, req.Entries[i:]...)
		dirty = true
		break
	}

	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, uint64(len(r.logs)))
	}

	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func (r *Raft) InstallSnapshot(ctx context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	panic("")
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
