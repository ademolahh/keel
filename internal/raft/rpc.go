package raft

import (
	"context"
	"time"

	"github.com/ademolahh/raftkv/proto"
)

func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

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

	no := &proto.AppendEntriesResponse{Term: r.currentTerm, Success: false}

	if req.Term < r.currentTerm {
		return no, nil
	}

	// only the leader of a term at least as new as ours sends this
	r.leaderId = req.LeaderId

	followerLogIndex := uint64(len(r.logs))
	if req.PrevLogIndex != 0 {
		// leader - {term: 1, index: 1} {term: 2, index: 2} {term: 3, index: 3}
		// follower - {term: 1, index: 1} {term: 2, index: 2}
		if req.PrevLogIndex > followerLogIndex {
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

	newIndex := req.PrevLogIndex + 1

	if len(req.Entries) > 0 && followerLogIndex >= newIndex {
		nextEntry := r.logs[newIndex-1]

		if nextEntry.Term != req.Entries[0].Term {
			r.logs = r.logs[:newIndex-1]
		}
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = Follower
	}

	// if log and term is the same, then all entry store the same command
	// if log and term is the same, the logs are identical in all preceeding entries

	// replicated it
	if len(req.Entries) > 0 {
		r.logs = append(r.logs[:newIndex-1], req.Entries...)
	}

	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, uint64(len(r.logs)))
	}

	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
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
