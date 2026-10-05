package raft

import (
	"context"
	"testing"

	"github.com/ademolahh/keel/proto"
)

func TestRequestVote(t *testing.T) {
	t.Run("grants a vote after a heartbeat brings a newer term", func(t *testing.T) {
		raft, _ := newRaft(t, 3, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 1
		raft.votedFor = uint64Ptr(1)

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{Term: 2, LeaderId: 5})

		res, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 2, CandidateId: 5})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !res.VoteGranted {
			t.Error("vote granted: expected true, got false")
		}
	})

	t.Run("rejects a candidate whose term is behind", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 5
		raft.state = Follower

		deadline := raft.electionDeadline

		resp, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 4, CandidateId: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resp.VoteGranted {
			t.Errorf("vote granted: expected false, got true")
		}

		if resp.Term != 5 {
			t.Errorf("response term: expected 5, got %d", resp.Term)
		}

		if raft.currentTerm != 5 {
			t.Errorf("current term: expected 5, got %d", raft.currentTerm)
		}

		if raft.votedFor != nil {
			t.Errorf("voted for: expected nil, got %d", *raft.votedFor)
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		if reset := !raft.electionDeadline.Equal(deadline); reset {
			t.Errorf("election deadline reset: expected false, got true")
		}
	})

	t.Run("adopts a higher term and grants the vote", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 5
		raft.votedFor = uint64Ptr(1)
		raft.state = Candidate

		deadline := raft.electionDeadline

		resp, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 6, CandidateId: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !resp.VoteGranted {
			t.Errorf("vote granted: expected true, got false")
		}

		if resp.Term != 6 {
			t.Errorf("response term: expected 6, got %d", resp.Term)
		}

		if raft.currentTerm != 6 {
			t.Errorf("current term: expected 6, got %d", raft.currentTerm)
		}

		if raft.votedFor == nil || *raft.votedFor != 2 {
			t.Errorf("voted for: expected 2, got %v", raft.votedFor)
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		if reset := !raft.electionDeadline.Equal(deadline); !reset {
			t.Errorf("election deadline reset: expected true, got false")
		}
	})

	t.Run("grants the first vote of a term", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 5
		raft.state = Follower

		deadline := raft.electionDeadline

		resp, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 5, CandidateId: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !resp.VoteGranted {
			t.Errorf("vote granted: expected true, got false")
		}

		if resp.Term != 5 {
			t.Errorf("response term: expected 5, got %d", resp.Term)
		}

		if raft.currentTerm != 5 {
			t.Errorf("current term: expected 5, got %d", raft.currentTerm)
		}

		if raft.votedFor == nil || *raft.votedFor != 2 {
			t.Errorf("voted for: expected 2, got %v", raft.votedFor)
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		if reset := !raft.electionDeadline.Equal(deadline); !reset {
			t.Errorf("election deadline reset: expected true, got false")
		}
	})

	t.Run("rejects a second candidate in the same term", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 5
		raft.votedFor = uint64Ptr(3)
		raft.state = Follower

		deadline := raft.electionDeadline

		resp, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 5, CandidateId: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resp.VoteGranted {
			t.Errorf("vote granted: expected false, got true")
		}

		if resp.Term != 5 {
			t.Errorf("response term: expected 5, got %d", resp.Term)
		}

		if raft.currentTerm != 5 {
			t.Errorf("current term: expected 5, got %d", raft.currentTerm)
		}

		if raft.votedFor == nil || *raft.votedFor != 3 {
			t.Errorf("voted for: expected 3, got %v", raft.votedFor)
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		if reset := !raft.electionDeadline.Equal(deadline); reset {
			t.Errorf("election deadline reset: expected false, got true")
		}
	})

	t.Run("grants a repeat request from the same candidate", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 5
		raft.votedFor = uint64Ptr(2)
		raft.state = Follower

		deadline := raft.electionDeadline

		resp, err := raft.RequestVote(context.Background(),
			&proto.RequestVoteRequest{Term: 5, CandidateId: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !resp.VoteGranted {
			t.Errorf("vote granted: expected true, got false")
		}

		if resp.Term != 5 {
			t.Errorf("response term: expected 5, got %d", resp.Term)
		}

		if raft.currentTerm != 5 {
			t.Errorf("current term: expected 5, got %d", raft.currentTerm)
		}

		if raft.votedFor == nil || *raft.votedFor != 2 {
			t.Errorf("voted for: expected 2, got %v", raft.votedFor)
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		if reset := !raft.electionDeadline.Equal(deadline); !reset {
			t.Errorf("election deadline reset: expected true, got false")
		}
	})

	t.Run("rejects a longer log whose last term is behind", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.logs = append(raft.logs, makeLogs()...)
		raft.currentTerm = 3

		req := &proto.RequestVoteRequest{Term: 3, LastLogIndex: uint64(len(raft.logs)) + 1}
		res, err := raft.RequestVote(context.Background(), req)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if res.VoteGranted {
			t.Errorf("expected no vote, but go a vote")
		}

		if res.Term != raft.currentTerm {
			t.Errorf("term: expected %d, got %d", raft.currentTerm, res.Term)
		}
	})

	t.Run("rejects a candidate whose last log term is behind", func(t *testing.T) {
		raft, _ := newRaft(t, 1, 1)

		var actualTerm uint64 = 3

		raft.logs = append(raft.logs, makeLogs()...)
		raft.currentTerm = actualTerm

		req := &proto.RequestVoteRequest{Term: actualTerm,
			LastLogTerm: actualTerm, LastLogIndex: uint64(len(raft.logs)) - 1}

		res, err := raft.RequestVote(context.Background(), req)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if res.VoteGranted {
			t.Errorf("expected no vote, but go a vote")
		}

	})

}

func TestAppendEntries(t *testing.T) {
	t.Run("rejects an append from a stale leader", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 5
		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 1})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res.Term != 5 {
			t.Errorf("actual term: expected 5, got %d", res.Term)
		}

		if res.Success {
			t.Errorf("expected append to failed")
		}
	})
	t.Run("rejects when there is no entry at prevLogIndex", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 1
		raft.logs = append(raft.logs, makeLogs()...)

		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 1, PrevLogIndex: uint64(len(raft.logs)) + 1})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if res.Success {
			t.Errorf("expected append to failed")
		}

		if res.Term != 1 {
			t.Errorf("actual term: expected 1, got %d", res.Term)
		}
	})

	t.Run("rejects when the term at prevLogIndex differs", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 1
		raft.logs = append(raft.logs, makeLogs()...)

		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 1, PrevLogIndex: uint64(len(raft.logs))})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if res.Success {
			t.Errorf("expected append to failed")
		}

		if res.Term != raft.currentTerm {
			t.Errorf("actual term: expected 1, got %d", res.Term)
		}

	})

	t.Run("truncates entries that conflict with the new ones", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.currentTerm = 1
		raft.logs = append(raft.logs, makeLogs()...)
		logSize := len(raft.logs)

		log := []*proto.LogEntry{{Term: 3, Cmd: "c"}}
		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1,
				Entries: log})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !res.Success {
			t.Errorf("expected append to succeed")
		}

		if raft.currentTerm != 3 {
			t.Errorf("actual term: expected 3, got %d", raft.currentTerm)
		}

		if len(raft.logs) != 3 && logSize < len(raft.logs) {
			t.Errorf("log size: expected 3, got %d", len(raft.logs))
		}

	})

	t.Run("keeps entries it already holds", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()

		res, err := raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, Entries: makeLogs()[2:4],
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !res.Success {
			t.Error("expected append to succeed")
		}

		if len(raft.logs) != 5 {
			t.Errorf("log size: expected 5, got %d", len(raft.logs))
		}
	})

	t.Run("appends new entries after prevLogIndex", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 1
		raft.logs = makeLogs()[:2]

		res, err := raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, Entries: makeLogs()[2:],
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !res.Success {
			t.Error("expected append to succeed")
		}

		if len(raft.logs) != 5 {
			t.Fatalf("log size: expected 5, got %d", len(raft.logs))
		}

		if raft.logs[4].Cmd != "set e=5" {
			t.Errorf("last entry: expected %q, got %q", "set e=5", raft.logs[4].Cmd)
		}
	})

	t.Run("turns a candidate of the same term into a follower", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 2
		raft.state = Candidate
		raft.votedFor = uint64Ptr(1)

		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 2, LeaderId: 3})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !res.Success {
			t.Error("expected append to succeed")
		}

		if raft.state != Follower {
			t.Errorf("state: expected %s, got %s", Follower.String(), raft.state.String())
		}

		// same term, so the vote it cast for itself still stands
		if raft.votedFor == nil || *raft.votedFor != 1 {
			t.Errorf("voted for: expected 1, got %v", raft.votedFor)
		}
	})

	t.Run("follows the leader's commit index", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, PrevLogIndex: 5, PrevLogTerm: 3, LeaderCommit: 3,
		})

		if raft.commitIndex != 3 {
			t.Errorf("commit index: expected 3, got %d", raft.commitIndex)
		}
	})

	t.Run("caps the commit index at its last entry", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 1
		raft.logs = makeLogs()[:2]

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 1, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 5,
		})

		if raft.commitIndex != 2 {
			t.Errorf("commit index: expected 2, got %d", raft.commitIndex)
		}
	})

	t.Run("hints its log length when prevLogIndex is past its log", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()

		res, _ := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 3, PrevLogIndex: 8, PrevLogTerm: 3})

		if res.Hint == nil || *res.Hint != 5 {
			t.Errorf("hint: expected 5, got %v", res.Hint)
		}
	})

	t.Run("hints the last position of prevLogTerm on a term mismatch", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()

		// position 4 holds term 3; the last term 2 entry is at position 3
		res, _ := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 3, PrevLogIndex: 4, PrevLogTerm: 2})

		if res.Hint == nil || *res.Hint != 3 {
			t.Errorf("hint: expected 3, got %v", res.Hint)
		}
	})

	t.Run("hints zero when it holds no entry of prevLogTerm", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 5
		raft.logs = makeLogs()

		res, _ := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 5, PrevLogIndex: 4, PrevLogTerm: 4})

		if res.Hint == nil || *res.Hint != 0 {
			t.Errorf("hint: expected 0, got %v", res.Hint)
		}
	})
}

func TestInstallSnapshot(t *testing.T) {}

func TestGetMatchingTermIndex(t *testing.T) {
	// makeLogs holds terms 1, 1, 2, 3, 3 at positions 1 to 5

	t.Run("returns the highest position holding the term", func(t *testing.T) {
		got := getMatchingTermIndex(makeLogs(), 1, 5)

		if got == nil || *got != 2 {
			t.Errorf("index: expected 2, got %v", got)
		}
	})

	t.Run("ignores positions above prevLogIndex", func(t *testing.T) {
		got := getMatchingTermIndex(makeLogs(), 3, 3)

		if got != nil {
			t.Errorf("index: expected nil, got %d", *got)
		}
	})

	t.Run("finds the term at the first position", func(t *testing.T) {
		got := getMatchingTermIndex(makeLogs(), 1, 1)

		if got == nil || *got != 1 {
			t.Errorf("index: expected 1, got %v", got)
		}
	})

	t.Run("clamps prevLogIndex to the log length", func(t *testing.T) {
		got := getMatchingTermIndex(makeLogs(), 3, 99)

		if got == nil || *got != 5 {
			t.Errorf("index: expected 5, got %v", got)
		}
	})

	t.Run("returns nil when prevLogIndex is zero", func(t *testing.T) {
		got := getMatchingTermIndex(makeLogs(), 1, 0)

		if got != nil {
			t.Errorf("index: expected nil, got %d", *got)
		}
	})

}
