package raft

import (
	"context"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ademolahh/raftkv/proto"
	"google.golang.org/grpc"
)

func TestNew(t *testing.T) {
	nodes, peers := cluster(t, 5, nil)
	raft := nodes[1].raft
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
	nodes, _ := cluster(t, 5, nil)
	raft := nodes[1].raft
	if raft.state != Follower {
		t.Errorf("state: expected Follower, got: %s", raft.state.String())
	}
}

func TestRequestVote(t *testing.T) {
	tests := []struct {
		name        string
		currentTerm uint64
		votedFor    *uint64
		state       RaftState
		request     *proto.RequestVoteRequest

		wantGranted  bool
		wantTerm     uint64
		wantVotedFor *uint64
		wantState    RaftState
		wantDeadline bool
	}{
		{
			name:        "candidate is stale",
			currentTerm: 5,
			state:       Follower,
			request:     &proto.RequestVoteRequest{Term: 4, CandidateId: 2},

			wantGranted:  false,
			wantTerm:     5,
			wantVotedFor: nil,
			wantState:    Follower,
			wantDeadline: false,
		},
		{
			name:        "candidate has a higher term",
			currentTerm: 5,
			votedFor:    uint64Ptr(1),
			state:       Candidate,
			request:     &proto.RequestVoteRequest{Term: 6, CandidateId: 2},

			wantGranted:  true,
			wantTerm:     6,
			wantVotedFor: uint64Ptr(2),
			wantState:    Follower,
			wantDeadline: true,
		},
		{
			name:        "first vote of the current term",
			currentTerm: 5,
			state:       Follower,
			request:     &proto.RequestVoteRequest{Term: 5, CandidateId: 2},

			wantGranted:  true,
			wantTerm:     5,
			wantVotedFor: uint64Ptr(2),
			wantState:    Follower,
			wantDeadline: true,
		},
		{
			name:        "already voted for another candidate",
			currentTerm: 5,
			votedFor:    uint64Ptr(3),
			state:       Follower,
			request:     &proto.RequestVoteRequest{Term: 5, CandidateId: 2},

			wantGranted:  false,
			wantTerm:     5,
			wantVotedFor: uint64Ptr(3),
			wantState:    Follower,
			wantDeadline: false,
		},
		{
			name:        "same candidate asks twice in the same term",
			currentTerm: 5,
			votedFor:    uint64Ptr(2),
			state:       Follower,
			request:     &proto.RequestVoteRequest{Term: 5, CandidateId: 2},

			wantGranted:  true,
			wantTerm:     5,
			wantVotedFor: uint64Ptr(2),
			wantState:    Follower,
			wantDeadline: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes, _ := cluster(t, 5, nil)
			raft := nodes[1].raft

			raft.currentTerm = tc.currentTerm
			raft.votedFor = tc.votedFor
			raft.state = tc.state

			deadline := raft.electionDeadline

			resp, err := raft.RequestVote(context.Background(), tc.request)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if resp.VoteGranted != tc.wantGranted {
				t.Errorf("vote granted: expected %t, got %t", tc.wantGranted, resp.VoteGranted)
			}

			if resp.Term != tc.wantTerm {
				t.Errorf("response term: expected %d, got %d", tc.wantTerm, resp.Term)
			}

			if raft.currentTerm != tc.wantTerm {
				t.Errorf("current term: expected %d, got %d", tc.wantTerm, raft.currentTerm)
			}

			switch {
			case tc.wantVotedFor == nil && raft.votedFor != nil:
				t.Errorf("voted for: expected nil, got %d", *raft.votedFor)
			case tc.wantVotedFor != nil && raft.votedFor == nil:
				t.Errorf("voted for: expected %d, got nil", *tc.wantVotedFor)
			case tc.wantVotedFor != nil && *raft.votedFor != *tc.wantVotedFor:
				t.Errorf("voted for: expected %d, got %d", *tc.wantVotedFor, *raft.votedFor)
			}

			if raft.state != tc.wantState {
				t.Errorf("state: expected %s, got %s", tc.wantState.String(), raft.state.String())
			}

			if reset := !raft.electionDeadline.Equal(deadline); reset != tc.wantDeadline {
				t.Errorf("election deadline reset: expected %t, got %t", tc.wantDeadline, reset)
			}
		})
	}
}

func TestStartElection(t *testing.T) {
	t.Run("becomes leader", func(t *testing.T) {
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

	t.Run("candidate is term stale", func(t *testing.T) {
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

	t.Run("count crashed peers as no vote", func(t *testing.T) {
		nodes, _ := cluster(t, 5, nil)
		nodes[3].stop()
		nodes[4].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Leader)

	})

	t.Run("remains candidate when majority is down", func(t *testing.T) {
		nodes, _ := cluster(t, 5, nil)
		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("slow vote response is not counted", func(t *testing.T) {
		delayDuration := 500 * time.Millisecond
		delay := map[uint64]time.Duration{
			2: delayDuration,
			3: delayDuration,
			4: delayDuration,
		}

		nodes, _ := cluster(t, 5, delay)
		raft := nodes[1].raft
		raft.voteTimeout = 2 * time.Second

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("lagging peer adopts newer term and votes", func(t *testing.T) {
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
		if err := server.Serve(lis); err != nil {
			log.Printf("node %d serve: %v", raft.id, err)
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

type node struct {
	raft *Raft
	stop func()
}
