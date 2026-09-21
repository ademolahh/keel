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
	raft, peers := newTestRaft(t, 1, 5)
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
	raft, _ := newTestRaft(t, 1, 3)
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
			raft, _ := newTestRaft(t, 1, 5)
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
		nodes, _ := cluster(t, 5)
		raft := nodes[1].raft

		_, currentTerm, _ := snapshot(raft)

		raft.StartElection()

		if state := waitForState(t, raft, Leader); state != Leader {
			t.Errorf("state: expected Leader, got %s", raft.state.String())
		}

		_, term, votedFor := snapshot(raft)

		if currentTerm+1 != term {
			t.Errorf("term: expected %d, got %d ", currentTerm+1, raft.currentTerm)
		}

		if votedFor == nil || *votedFor != raft.id {
			t.Errorf("voted for: expected %d, got %v", raft.id, votedFor)
		}
	})

	t.Run("candidate is term stale", func(t *testing.T) {
		nodes, _ := cluster(t, 5)
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

		if state := waitForState(t, raft, Follower); state != Follower {
			t.Errorf("state: expected Follower, got %s", raft.state.String())
		}

		_, term, _ := snapshot(raft)

		if actualCurrentTerm != term {
			t.Errorf("term: expected %d, got %d ", actualCurrentTerm, term)
		}
	})

	t.Run("count crashed peers as no vote", func(t *testing.T) {
		nodes, _ := cluster(t, 5)
		nodes[3].stop()
		nodes[4].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.StartElection()

		if state := waitForState(t, raft, Leader); state != Leader {
			t.Errorf("state: expected Leader, got %s", raft.state.String())
		}
	})

	t.Run("remains candidate when majority is down", func(t *testing.T) {
		nodes, _ := cluster(t, 5)
		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		var candidateId uint64 = 1
		raft := nodes[candidateId].raft

		raft.StartElection()

		if state := waitForState(t, raft, Candidate); state != Candidate {
			t.Errorf("state: expected Candidate, got %s", raft.state.String())
		}
	})

}

func TestRandomElectionTimeout(t *testing.T) {
	randomTime := randomElectionTimeout()
	if randomTime > 300*time.Millisecond || randomTime < 100*time.Millisecond {
		t.Errorf("duration is expected to within 100-300ms, buy got %d", randomTime)
	}

}

func makePeer(n int) map[uint64]string {
	peers := make(map[uint64]string, n)
	for i := range n {
		peers[uint64(i+1)] = "localhost:0"
	}

	return peers
}

func uint64Ptr(v uint64) *uint64 {
	return new(v)
}

func newTestRaft(t *testing.T, id uint64, n int) (*Raft, map[uint64]string) {
	t.Helper()

	peers := makePeer(n)
	if _, ok := peers[id]; !ok {
		t.Fatalf("id %d is not in the cluster", id)
	}

	raft := New(id, peers)
	if raft == nil {
		t.Fatal("raft initialization failed")
	}

	return raft, peers
}

func cluster(t *testing.T, n int) (map[uint64]*node, map[uint64]string) {
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
		s := start(t, r, lst)

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

func waitForState(t *testing.T, r *Raft, want RaftState) RaftState {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		state, _, _ := snapshot(r)
		if state == want || time.Now().After(deadline) {
			return state
		}

		time.Sleep(5 * time.Millisecond)
	}

}

type node struct {
	raft *Raft
	stop func()
}
