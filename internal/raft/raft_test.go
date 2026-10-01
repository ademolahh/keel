package raft

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
)

// lastApplied <= commitIndex <= last log index - invariant

const (
	DEFAULT_CLUSTER_SIZE        = 5
	DEFAULT_LEADER_ID    uint64 = 1
)

func TestNew(t *testing.T) {
	currTime := time.Now()
	raft, peers := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

	if raft.id != 1 {
		t.Errorf("node %d: expected %d, go %d", raft.id, 1, raft.id)
	}

	if len(raft.peers) != len(peers)-1 {
		t.Errorf("node %d: expected %d, go %d", raft.id, len(peers)-1, len(raft.peers))
	}

	for _, peer := range raft.peers {
		_, ok := peers[peer.id]
		if !ok {
			t.Errorf("node %d: peer %d does not exist", raft.id, peer.id)
		}
	}

	duration := raft.electionDeadline.Sub(currTime)
	if duration > 300*time.Millisecond || duration < 100*time.Millisecond {
		t.Errorf("duration is expected to within 100-300ms, buy got %d", duration)
	}

}

func TestGetState(t *testing.T) {
	raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
	if raft.state != Follower {
		t.Errorf("state: expected Follower, got: %s", raft.state.String())
	}
}

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

func TestStartElection(t *testing.T) {
	t.Run("wins with a majority of votes", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

		_, currentTerm, _ := snapshot(raft)

		raft.StartElection()

		waitForLeader(t, raft, 500*time.Millisecond)

		_, term, votedFor := snapshot(raft)

		if currentTerm+1 != term {
			t.Errorf("term: expected %d, got %d ", currentTerm+1, raft.currentTerm)
		}

		if votedFor == nil || *votedFor != raft.id {
			t.Errorf("voted for: expected %d, got %v", raft.id, votedFor)
		}
	})

	t.Run("steps down when a peer reports a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.currentTerm = 2

		var actualCurrentTerm uint64 = 5

		for id, node := range nodes {
			if id == DEFAULT_LEADER_ID {
				continue
			}

			node.raft.currentTerm = actualCurrentTerm
		}

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Follower)

		_, term, _ := snapshot(raft)

		if actualCurrentTerm != term {
			t.Errorf("term: expected %d, got %d ", actualCurrentTerm, term)
		}
	})

	t.Run("wins while two peers are down", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		nodes[3].stop()
		nodes[4].stop()

		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Leader)

	})

	t.Run("stays a candidate when a majority is down", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("stays a candidate when votes arrive after the timeout", func(t *testing.T) {
		delayDuration := 500 * time.Millisecond
		delay := map[uint64]time.Duration{
			2: delayDuration,
			3: delayDuration,
			4: delayDuration,
		}

		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, delay)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("delayed peers vote in the newer term", func(t *testing.T) {
		const lag = 4 * time.Second
		lagging := []uint64{3, 4}

		delay := make(map[uint64]time.Duration, len(lagging))

		for _, id := range lagging {
			delay[id] = lag
		}

		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, delay)

		nodes[DEFAULT_LEADER_ID].raft.StartElection()
		waitForLeader(t, nodes[DEFAULT_LEADER_ID].raft, lag/4)
		_, firstTerm, _ := snapshot(nodes[DEFAULT_LEADER_ID].raft)

		nodes[5].raft.StartElection()
		waitForLeader(t, nodes[5].raft, lag/4)
		_, secondTerm, _ := snapshot(nodes[5].raft)

		if firstTerm >= secondTerm {
			t.Errorf("term: expected %d, got %d", secondTerm, firstTerm)
		}

		for _, id := range lagging {
			r := nodes[id].raft

			ok := waitFor(t, 2*lag, func() bool {
				_, term, votedFor := snapshot(r)
				return term == secondTerm && votedFor != nil && *votedFor == 5
			})

			if !ok {
				_, term, votedFor := snapshot(r)
				t.Errorf("server %d: expected term %d and vote for 5, got term %d, votedFor %v",
					r.id, secondTerm, term, votedFor)
			}
		}
	})

	t.Run("wins when every node already holds entries", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)

		for _, n := range nodes {
			n.raft.mu.Lock()
			n.raft.logs = makeLogs()
			n.raft.currentTerm = 3
			n.raft.mu.Unlock()
		}

		raft := nodes[DEFAULT_LEADER_ID].raft
		raft.StartElection()

		waitForLeader(t, raft, raft.voteTimeout)
	})
}

func TestRunElectionTimer(t *testing.T) {
	nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)

	t.Cleanup(func() {
		for _, node := range nodes {
			node.raft.Kill()
		}
	})

	for _, node := range nodes {
		go node.raft.RunElectionTimer()
	}

	// keep checking until there is a leader

	deadline := time.Now().Add(3 * time.Second)

	// term -> leader id -> bool
	leaderPerTerm := map[uint64]map[uint64]bool{}

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			node.raft.mu.Lock()
			raft := node.raft
			isLeader := raft.state == Leader
			term := raft.currentTerm
			node.raft.mu.Unlock()
			if isLeader {
				if leaderPerTerm[term] == nil {
					leaderPerTerm[term] = make(map[uint64]bool)
				}
				leaderPerTerm[term][raft.id] = true
			}

		}
		time.Sleep(10 * time.Millisecond)
	}

	for term, v := range leaderPerTerm {
		if len(v) > 1 {
			t.Errorf("term %d has multiple leaders: %v", term, len(v))
		}
	}
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
}

func TestAppend(t *testing.T) {
	t.Run("appends the command to the leader's log", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.initNextIndex()
		r.initMatchIndex()
		logSize := len(r.logs)

		r.Append("set a=1")

		if len(r.logs) != logSize+1 {
			t.Errorf("log size: expected %d, got %d", logSize+1, len(r.logs))
		}
	})

	t.Run("replicates the entry to a majority of peers", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.initNextIndex()
		r.initMatchIndex()
		size := len(nodes)

		res := r.Append("set a = 1")

		if !res {
			t.Errorf("entry failed to commit")
		}

		// time.Sleep(1 * time.Second)

		cmdEntered := 0
		for _, node := range nodes {
			node.raft.mu.Lock()
			len := len(node.raft.logs)
			node.raft.mu.Unlock()

			if len >= 1 {
				cmdEntered += 1

			}
		}

		majority := majority(size - 1)
		if cmdEntered < majority {
			t.Errorf("command entered: expected %d, received: %d", majority, cmdEntered)
		}
	})

	t.Run("catches up a follower that is far behind", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		const STALE_ID uint64 = 3
		const LATEST_TERM uint64 = 3
		nodes[DEFAULT_LEADER_ID].raft.state = Leader

		logs := makeLogs()

		for _, node := range nodes {
			if node.raft.id == STALE_ID {
				continue
			}

			node.raft.logs = append(node.raft.logs, logs...)
			node.raft.currentTerm = LATEST_TERM
		}

		nodes[DEFAULT_LEADER_ID].raft.nextIndex[1] = uint64(len(logs)) + 1
		nodes[STALE_ID].raft.currentTerm = 1
		nodes[STALE_ID].raft.logs = append(nodes[STALE_ID].raft.logs, logs[0])
		nodes[DEFAULT_LEADER_ID].raft.initNextIndex()
		nodes[DEFAULT_LEADER_ID].raft.initMatchIndex()

		res := nodes[DEFAULT_LEADER_ID].raft.Append("set a=1")
		if !res {
			t.Fatal("append failed")
		}

		time.Sleep(1 * time.Second)

		nodes[STALE_ID].raft.mu.Lock()
		len := len(nodes[STALE_ID].raft.logs)
		nodes[STALE_ID].raft.mu.Unlock()

		if len != 6 {
			t.Errorf("stale log: expected 6, got %d", len)
		}
	})

	t.Run("returns false when a majority does not respond", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.initNextIndex()
		r.initMatchIndex()

		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		if r.Append("set a=1") {
			t.Error("append: expected false, got true")
		}
	})
}

func TestHeartBeat(t *testing.T) {
	t.Run("does nothing when not the leader", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		follower := nodes[2].raft

		follower.mu.Lock()
		deadline := follower.electionDeadline
		follower.mu.Unlock()

		nodes[DEFAULT_LEADER_ID].raft.HeartBeat()

		time.Sleep(100 * time.Millisecond)

		follower.mu.Lock()
		reset := !follower.electionDeadline.Equal(deadline)
		follower.mu.Unlock()

		if reset {
			t.Error("election deadline: expected unchanged, got reset")
		}
	})

	t.Run("resets every follower's election deadline", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.initNextIndex()
		leader.initMatchIndex()

		deadlines := make(map[uint64]time.Time)
		for id, n := range nodes {
			if id == DEFAULT_LEADER_ID {
				continue
			}

			n.raft.mu.Lock()
			deadlines[id] = n.raft.electionDeadline
			n.raft.mu.Unlock()
		}

		leader.HeartBeat()

		for id, before := range deadlines {
			r := nodes[id].raft

			ok := waitFor(t, time.Second, func() bool {
				r.mu.Lock()
				defer r.mu.Unlock()

				return !r.electionDeadline.Equal(before)
			})

			if !ok {
				t.Errorf("node %d: expected election deadline to be reset", id)
			}
		}
	})

	t.Run("steps down when a follower reports a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.currentTerm = 1
		leader.initNextIndex()
		leader.initMatchIndex()

		for id, n := range nodes {
			if id == DEFAULT_LEADER_ID {
				continue
			}

			n.raft.mu.Lock()
			n.raft.currentTerm = 5
			n.raft.mu.Unlock()
		}

		leader.HeartBeat()

		waitForState(t, leader, time.Second, Follower)

		if _, term, _ := snapshot(leader); term != 5 {
			t.Errorf("term: expected 5, got %d", term)
		}
	})
}

func TestLeader(t *testing.T) {
	t.Run("knows no leader before hearing from one", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		if id, ok := raft.Leader(); ok {
			t.Errorf("leader: expected none, got %d", id)
		}

		if raft.IsLeader() {
			t.Error("is leader: expected false, got true")
		}
	})

	t.Run("learns the leader from AppendEntries", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{Term: 1, LeaderId: 3})

		if id, ok := raft.Leader(); !ok || id != 3 {
			t.Errorf("leader: expected 3, got %d (known %t)", id, ok)
		}
	})

	t.Run("forgets the leader when an election starts", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.leaderId = 3

		raft.StartElection()

		if id, ok := raft.Leader(); ok {
			t.Errorf("leader: expected none, got %d", id)
		}
	})

	t.Run("reports itself as leader after winning", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()
		waitForLeader(t, raft, raft.voteTimeout)

		if !raft.IsLeader() {
			t.Error("is leader: expected true, got false")
		}

		if id, ok := raft.Leader(); !ok || id != DEFAULT_LEADER_ID {
			t.Errorf("leader: expected %d, got %d (known %t)", DEFAULT_LEADER_ID, id, ok)
		}
	})
}

func TestApply(t *testing.T) {
	t.Run("applies committed entries in log order", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		sm := &recorder{}
		raft.stateMachine = sm
		raft.logs = makeLogs()
		raft.commitIndex = 3

		go raft.Apply()
		t.Cleanup(raft.Kill)

		want := []string{"set a=1", "set b=2", "set c=3"}

		waitFor(t, time.Second, func() bool { return len(sm.applied()) >= len(want) })

		// give Apply the chance to wrongly run past the commit index
		time.Sleep(50 * time.Millisecond)

		if got := sm.applied(); !slices.Equal(got, want) {
			t.Errorf("applied: expected %v, got %v", want, got)
		}
	})
}

func TestMajority(t *testing.T) {
	// majority takes the number of peers, which excludes this node

	t.Run("needs three of four nodes", func(t *testing.T) {
		if got := majority(3); got != 3 {
			t.Errorf("majority: expected 3, got %d", got)
		}
	})

	t.Run("needs three of five nodes", func(t *testing.T) {
		if got := majority(4); got != 3 {
			t.Errorf("majority: expected 3, got %d", got)
		}
	})
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

func TestRandomElectionTimeout(t *testing.T) {
	randomTime := randomElectionTimeout()
	if randomTime > 300*time.Millisecond || randomTime < 100*time.Millisecond {
		t.Errorf("duration is expected to within 100-300ms, buy got %d", randomTime)
	}
}

func TestRaftState(t *testing.T) {
	var (
		a           = Leader
		b           = Follower
		c           = Candidate
		u RaftState = 99
	)

	if a.String() != "Leader" {
		t.Errorf("expected Leader, got %s", a.String())
	}

	if b.String() != "Follower" {
		t.Errorf("expected Follower, got %s", b.String())
	}

	if c.String() != "Candidate" {
		t.Errorf("expected Candidate, got %s", c.String())
	}

	if u.String() != "Unknown" {
		t.Errorf("expected Unknown, got %s", u.String())
	}
}

// HELPERS
// recorder is a state machine that remembers the commands applied to it.
type recorder struct {
	mu   sync.Mutex
	cmds []string
}

func (s *recorder) Apply(cmd string) any {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cmds = append(s.cmds, cmd)
	return nil
}

func (s *recorder) applied() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.cmds...)
}

func uint64Ptr(v uint64) *uint64 {
	return new(v)
}

func (r *Raft) initNextIndex() {
	for _, peer := range r.peers {
		r.nextIndex[peer.id] = uint64(len(r.logs)) + 1
	}
}

func (r *Raft) initMatchIndex() {
	for _, peer := range r.peers {
		r.matchIndex[peer.id] = 0
	}
}

// newRaft builds a single node with n-1 peers whose addresses nothing is
// serving, for tests that call the handlers in process.
func newRaft(t *testing.T, id uint64, n int) (*Raft, map[uint64]string) {
	t.Helper()

	peers := make(map[uint64]string, n)
	for i := 1; i <= n; i++ {
		peers[uint64(i)] = "localhost:0"
	}

	if _, ok := peers[id]; !ok {
		t.Fatalf("id %d is not in the cluster", id)
	}

	raft := New(id, peers, &kv.KV{})
	if raft == nil {
		t.Fatal("raft initialization failed")
	}

	return raft, peers
}

func cluster(t *testing.T, n int, delays map[uint64]time.Duration) (map[uint64]*node, map[uint64]string) {
	t.Helper()

	peers := make(map[uint64]string, n)

	nodes := make(map[uint64]*node)

	listeners := make(map[uint64]net.Listener, n)

	for i := 1; i <= n; i++ {
		lst, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}

		id := uint64(i)
		listeners[id] = lst
		peers[id] = lst.Addr().String()
	}

	for id, lst := range listeners {
		r := New(id, peers, &kv.KV{})

		var opts []grpc.ServerOption
		if d, ok := delays[id]; ok {
			opts = append(opts, delay(d))
		}

		s := start(t, r, lst, opts...)
		nodes[id] = &node{raft: r, stop: s}
	}

	return nodes, peers
}

func start(t *testing.T, raft *Raft, lis net.Listener, opt ...grpc.ServerOption) func() {
	t.Helper()

	server := grpc.NewServer(opt...)
	proto.RegisterRaftServer(server, raft)

	var once sync.Once
	stop := func() { once.Do(server.Stop) }
	t.Cleanup(stop)

	go func() {
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			fmt.Printf("node %d serve: %v", raft.id, err)
		}
	}()

	return stop
}

func snapshot(r *Raft) (RaftState, uint64, *uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state, r.currentTerm, r.votedFor
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(30 * time.Millisecond)
	}

	return cond()
}

func waitForLeader(t *testing.T, r *Raft, timeout time.Duration) {
	t.Helper()

	waitForState(t, r, timeout, Leader)
}

func waitForState(t *testing.T, r *Raft, timeout time.Duration, want RaftState) {
	t.Helper()

	ok := waitFor(t, timeout, func() bool {
		state, _, _ := snapshot(r)
		return state == want
	})

	if !ok {
		state, term, _ := snapshot(r)
		t.Errorf("server %d: expected %s within %v, got %s (term %d)",
			r.id, want, timeout, state, term)
	}
}

func delay(duration time.Duration) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any,
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		resp, err = handler(ctx, req)
		time.Sleep(duration)
		return resp, err
	})
}

func makeLogs() []*proto.LogEntry {
	return []*proto.LogEntry{
		{Term: 1, Cmd: "set a=1"},
		{Term: 1, Cmd: "set b=2"},
		{Term: 2, Cmd: "set c=3"},
		{Term: 3, Cmd: "set d=4"},
		{Term: 3, Cmd: "set e=5"},
	}
}

type node struct {
	raft *Raft
	stop func()
}
