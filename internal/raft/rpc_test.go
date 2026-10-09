package raft

import (
	"context"
	"testing"
	"time"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
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

	t.Run("caps the commit index at the last entry the leader sent", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()[:4]

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 4,
		})

		if raft.commitIndex != 2 {
			t.Errorf("commit index: expected 2, got %d", raft.commitIndex)
		}
	})

	t.Run("never lowers the commit index", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()
		raft.commitIndex = 3

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 5,
		})

		if raft.commitIndex != 3 {
			t.Errorf("commit index: expected 3, got %d", raft.commitIndex)
		}
	})

	t.Run("replies only after the new entries are synced", func(t *testing.T) {
		gate := make(chan struct{})
		persister := &slowPersister{memoryPersister: &memoryPersister{}}
		raft, err := New(1, map[uint64]string{1: "localhost:0", 2: "localhost:0"}, &recorder{}, persister)
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		persister.gate = gate
		go raft.RunSync()
		t.Cleanup(raft.Kill)

		done := make(chan *proto.AppendEntriesResponse, 1)
		go func() {
			res, _ := raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
				Term: 1, LeaderId: 2, Entries: makeLogs()[:2],
			})
			done <- res
		}()

		select {
		case <-done:
			t.Fatal("reply: expected none before the sync")
		case <-time.After(50 * time.Millisecond):
		}

		close(gate)

		select {
		case res := <-done:
			if !res.Success {
				t.Error("append: expected success after the sync")
			}
		case <-time.After(time.Second):
			t.Fatal("reply: expected one after the sync")
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

func TestInstallSnapshot(t *testing.T) {
	t.Run("rejects a snapshot from a stale leader", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3

		res, _ := raft.InstallSnapshot(context.Background(), snapshotRequest(2, 4, 2, "state"))

		if res.Term != 3 {
			t.Errorf("reply term: expected 3, got %d", res.Term)
		}

		if raft.lastIncludedIndex != 0 || raft.persister.(*memoryPersister).snapshot != nil {
			t.Error("snapshot: expected none installed")
		}
	})

	t.Run("adopts a newer term and follows the sender", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 1
		raft.votedFor = uint64Ptr(1)
		raft.state = Candidate

		raft.InstallSnapshot(context.Background(), snapshotRequest(4, 3, 2, "state"))

		if raft.currentTerm != 4 || raft.votedFor != nil {
			t.Errorf("term and vote: expected 4 and none, got %d and %v", raft.currentTerm, raft.votedFor)
		}

		if raft.state != Follower || raft.leaderId != 2 {
			t.Errorf("state: expected a follower of 2, got %s following %d", raft.state, raft.leaderId)
		}
	})

	t.Run("waits for the last chunk before installing", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3

		first := snapshotRequest(3, 4, 3, "sta")
		first.Done = false
		raft.InstallSnapshot(context.Background(), first)

		if raft.lastIncludedIndex != 0 {
			t.Fatalf("last included index: expected 0 before the last chunk, got %d", raft.lastIncludedIndex)
		}

		last := snapshotRequest(3, 4, 3, "te")
		last.Offset = 3
		raft.InstallSnapshot(context.Background(), last)

		if got := string(savedSnapshot(t, raft).Data); got != "state" {
			t.Errorf("snapshot data: expected %q, got %q", "state", got)
		}
	})

	t.Run("ignores a chunk at the wrong offset", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3

		chunk := snapshotRequest(3, 4, 3, "te")
		chunk.Offset = 3
		raft.InstallSnapshot(context.Background(), chunk)

		if raft.lastIncludedIndex != 0 || len(raft.snapshotChunks) != 0 {
			t.Error("snapshot: expected the chunk to be dropped")
		}
	})

	t.Run("discards the whole log when it lacks the last included entry", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()[:2]

		raft.InstallSnapshot(context.Background(), snapshotRequest(3, 4, 3, "state"))

		if len(raft.logs) != 0 {
			t.Errorf("log size: expected 0, got %d", len(raft.logs))
		}

		if raft.lastIndex() != 4 || raft.termAt(4) != 3 {
			t.Errorf("last entry: expected index 4 term 3, got index %d term %d", raft.lastIndex(), raft.termAt(raft.lastIndex()))
		}

		if raft.commitIndex != 4 || raft.pendingSnapshot == nil {
			t.Errorf("commit index and restore: expected 4 and a pending restore, got %d and %v",
				raft.commitIndex, raft.pendingSnapshot)
		}

		if raft.snapshot == nil || raft.snapshot.LastIncludedIndex != 4 {
			t.Errorf("snapshot in memory: expected one through 4, got %v", raft.snapshot)
		}

		if saved := savedSnapshot(t, raft); saved.LastIncludedIndex != 4 || saved.LastIncludedTerm != 3 {
			t.Errorf("saved snapshot: expected index 4 term 3, got index %d term %d",
				saved.LastIncludedIndex, saved.LastIncludedTerm)
		}

		if logs := raft.persister.(*memoryPersister).state.Logs; len(logs) != 0 {
			t.Errorf("saved log size: expected 0, got %d", len(logs))
		}
	})

	t.Run("keeps the entries after a matching last included entry", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()

		raft.InstallSnapshot(context.Background(), snapshotRequest(3, 3, 2, "state"))

		assertEntries(t, raft.logs, makeLogs()[3:])

		if raft.lastIndex() != 5 {
			t.Errorf("last index: expected 5, got %d", raft.lastIndex())
		}

		assertEntries(t, raft.persister.(*memoryPersister).state.Logs, makeLogs()[3:])
	})

	t.Run("ignores a snapshot it has already committed past", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.logs = makeLogs()
		raft.commitIndex = 4

		raft.InstallSnapshot(context.Background(), snapshotRequest(3, 3, 2, "state"))

		if raft.lastIncludedIndex != 0 || len(raft.logs) != 5 {
			t.Errorf("log: expected it untouched, got base %d and %d entries", raft.lastIncludedIndex, len(raft.logs))
		}
	})

	t.Run("restores the state machine through Apply", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		sm := &recorder{}
		raft.stateMachine = sm
		raft.currentTerm = 3

		go raft.Apply()
		t.Cleanup(raft.Kill)

		raft.InstallSnapshot(context.Background(), snapshotRequest(3, 4, 3, "state"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := raft.waitApplied(ctx, 4); err != nil {
			t.Fatalf("wait: %v", err)
		}

		sm.mu.Lock()
		defer sm.mu.Unlock()

		if string(sm.restored) != "state" {
			t.Errorf("restored: expected %q, got %q", "state", sm.restored)
		}
	})
}

func TestLogOffset(t *testing.T) {
	t.Run("appends after the last included entry", func(t *testing.T) {
		raft := followerWithSnapshot(t)
		entry := &proto.LogEntry{Term: 3, Cmd: "set x=1"}

		res, _ := raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, LeaderId: 2, PrevLogIndex: 4, PrevLogTerm: 3, Entries: []*proto.LogEntry{entry},
		})

		if !res.Success {
			t.Fatal("append: expected success")
		}

		assertEntries(t, raft.logs, []*proto.LogEntry{entry})

		if raft.lastIndex() != 5 {
			t.Errorf("last index: expected 5, got %d", raft.lastIndex())
		}
	})

	t.Run("skips entries the snapshot already covers", func(t *testing.T) {
		raft := followerWithSnapshot(t)
		entry := &proto.LogEntry{Term: 3, Cmd: "set x=1"}

		res, _ := raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
			Entries: append(makeLogs()[2:4], entry),
		})

		if !res.Success {
			t.Fatal("append: expected success")
		}

		assertEntries(t, raft.logs, []*proto.LogEntry{entry})
	})

	t.Run("compares votes against the last included entry", func(t *testing.T) {
		raft := followerWithSnapshot(t)

		res, _ := raft.RequestVote(context.Background(), &proto.RequestVoteRequest{
			Term: 4, CandidateId: 2, LastLogIndex: 6, LastLogTerm: 2,
		})

		if res.VoteGranted {
			t.Error("vote: expected a candidate with an older last term to be refused")
		}
	})
}

func snapshotRequest(term, index, lastTerm uint64, data string) *proto.InstallSnapshotRequest {
	return &proto.InstallSnapshotRequest{
		Term: term, LeaderId: 2, LastIncludedIndex: index, LastIncludedTerm: lastTerm,
		Data: []byte(data), Done: true,
	}
}

func savedSnapshot(t *testing.T, raft *Raft) *proto.Snapshot {
	t.Helper()

	data := raft.persister.(*memoryPersister).snapshot
	if data == nil {
		t.Fatal("snapshot: expected one saved, got none")
	}

	var snapshot proto.Snapshot
	if err := protobuf.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	return &snapshot
}

func followerWithSnapshot(t *testing.T) *Raft {
	t.Helper()

	raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
	raft.currentTerm = 3
	raft.InstallSnapshot(context.Background(), snapshotRequest(3, 4, 3, "state"))

	return raft
}

func assertEntries(t *testing.T, got, want []*proto.LogEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("entries: expected %d, got %d", len(want), len(got))
	}

	for i := range want {
		if !protobuf.Equal(got[i], want[i]) {
			t.Errorf("entry %d: expected %v, got %v", i, want[i], got[i])
		}
	}
}

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
