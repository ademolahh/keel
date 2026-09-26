package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ademolahh/raftkv/proto"
	"google.golang.org/grpc"
)

func TestNew(t *testing.T) {
	raft, peers := newRaft(t, 1, 5)
	currTime := time.Now()

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
	raft, _ := newRaft(t, 1, 5)
	if raft.state != Follower {
		t.Errorf("state: expected Follower, got: %s", raft.state.String())
	}
}

func TestRequestVote(t *testing.T) {
	t.Run("rejects a candidate whose term is behind", func(t *testing.T) {
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

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
			LastLogTerm: actualTerm - 1, LastLogIndex: uint64(len(raft.logs)) - 1}

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
		nodes, _ := cluster(t, 5, nil)
		raft := nodes[1].raft

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
		nodes, _ := cluster(t, 5, nil)
		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.currentTerm = 2

		var actualCurrentTerm uint64 = 5

		for id, node := range nodes {
			if id == candidateId {
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
		nodes, _ := cluster(t, 5, nil)
		nodes[3].stop()
		nodes[4].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Leader)

	})

	t.Run("stays a candidate when a majority is down", func(t *testing.T) {
		nodes, _ := cluster(t, 5, nil)
		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

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

		nodes, _ := cluster(t, 5, delay)
		raft := nodes[1].raft

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

		nodes, _ := cluster(t, 5, delay)

		nodes[1].raft.StartElection()
		waitForLeader(t, nodes[1].raft, lag/4)
		_, firstTerm, _ := snapshot(nodes[1].raft)

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
				t.Errorf("server %d: expected term %d and vote for 5, got term %d, votedFor %d",
					r.id, secondTerm, term, *votedFor)
			}
		}
	})
}

func TestRunElectionTimer(t *testing.T) {
	nodes, _ := cluster(t, 5, nil)

	t.Cleanup(func() {
		for _, node := range nodes {
			node.raft.Kill()
		}
	})

	for _, node := range nodes {
		go node.raft.RunElectionTimer()
	}

	// keep checking call until all are done
	// else timeout at somepoint
}

func TestAppendEntries(t *testing.T) {
	t.Run("rejects an append from a stale leader", func(t *testing.T) {
		raft, _ := newRaft(t, 1, 5)
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
		raft, _ := newRaft(t, 1, 5)
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
		raft, _ := newRaft(t, 1, 5)

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
		raft, _ := newRaft(t, 1, 5)

		raft.currentTerm = 1
		raft.logs = append(raft.logs, makeLogs()...)
		logSize := len(raft.logs)

		log := []*proto.LogEntry{{Term: 3, Cmd: "c"}}
		res, err := raft.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1,
				Entries: log})

		fmt.Println("result", len(raft.logs))

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
}

func TestRandomElectionTimeout(t *testing.T) {
	randomTime := randomElectionTimeout()
	if randomTime > 300*time.Millisecond || randomTime < 100*time.Millisecond {
		t.Errorf("duration is expected to within 100-300ms, buy got %d", randomTime)
	}
}

// HELPERS
func uint64Ptr(v uint64) *uint64 {
	return new(v)
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

	raft := New(id, peers)
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
		r := New(id, peers)

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
