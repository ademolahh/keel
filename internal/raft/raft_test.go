package raft

import (
	"context"
	"fmt"
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

func TestRandomElectionTimeout(t *testing.T) {
	randomTime := randomElectionTimeout()
	if randomTime > 300*time.Millisecond || randomTime < 100*time.Millisecond {
		t.Errorf("duration is expected to within 100-300ms, buy got %d", randomTime)
	}

}

func makePeer(n int) map[uint64]string {
	peers := make(map[uint64]string, n)
	for i := range n {
		peers[uint64(i+1)] = fmt.Sprintf("localhost:%v", 50000+i+1)
	}

	return peers
}

func uint64Ptr(v uint64) *uint64 {
	return &v
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

func run(peers map[uint64]string) {
	var wg sync.WaitGroup
	wg.Add(len(peers))

	for id, addr := range peers {
		go func() {
			fmt.Println("id", id)
			defer wg.Done()
			raft := New(id, peers)
			start(raft, addr)
		}()
	}

	wg.Wait()
}

func start(raft *Raft, addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	server := grpc.NewServer()
	proto.RegisterRaftServer(server, raft)

	if err := server.Serve(lis); err == nil {
		log.Printf("node %d serve: %v", raft.id, err)
	}
}
