package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ademolahh/raftkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftState int

const (
	_ RaftState = iota
	Leader
	Follower
	Candidate
)

func (s RaftState) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

type Raft struct {
	mu          sync.Mutex
	currentTerm uint64
	votedFor    *uint64
	id          uint64

	state RaftState

	peers []peer

	electionDeadline time.Time

	proto.UnimplementedRaftServer
}

type peer struct {
	id     uint64
	client proto.RaftClient
}

func New(id uint64, peerClient map[uint64]string) *Raft {
	var peers []peer

	for pid, addr := range peerClient {
		if pid == id {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := proto.NewRaftClient(conn)

		peers = append(peers, peer{id: pid, client: client})
	}

	return &Raft{
		id:               id,
		peers:            peers,
		state:            Follower,
		electionDeadline: time.Now().Add(randomElectionTimeout())}
}

func (r *Raft) CheckElection() {
	for {
		r.mu.Lock()
		timemout := r.electionDeadline
		state := r.state
		r.mu.Unlock()

		if time.Since(timemout) > 0 && state != Leader {
			r.StartElection()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (r *Raft) StartElection() {
	r.mu.Lock()
	r.currentTerm += 1
	term := r.currentTerm
	r.state = Candidate
	id := r.id

	r.votedFor = &id
	r.electionDeadline = time.Now().Add(randomElectionTimeout())
	votes := 1

	peers := append([]peer(nil), r.peers...)

	r.mu.Unlock()

	req := &proto.RequestVoteRequest{
		Term:        term,
		CandidateId: id,
	}

	majority := (len(peers) / 2) + 1

	for _, v := range peers {
		if v.id == id {
			continue
		}

		go func(client proto.RaftClient) {
			res, err := client.RequestVote(context.Background(), req)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if res.Term > r.currentTerm {
				r.currentTerm = res.Term
				r.state = Follower
				r.votedFor = nil
			}

			if r.state != Candidate || r.currentTerm != term {
				return
			}

			if res.VoteGranted {
				votes += 1
				if votes >= majority {
					r.state = Leader
					return
				}
			}
		}(v.client)

	}

}

func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	no := &proto.RequestVoteResponse{VoteGranted: false, Term: r.currentTerm}

	if r.currentTerm > req.Term {
		return no, nil
	}

	// candidate has the higher term
	if r.currentTerm < req.Term {
		r.currentTerm = req.Term
		r.votedFor = nil
		r.state = Follower
	}

	// at this point, the terms are equal
	// only one candidate per term
	if r.votedFor != nil && *r.votedFor != req.CandidateId {
		return no, nil
	}

	r.votedFor = &req.CandidateId
	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	yes := &proto.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm}

	return yes, nil
}

func (r *Raft) GetState() (string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state.String(), r.currentTerm
}

func randomElectionTimeout() time.Duration {
	minimum, maximum := 100, 300
	r := rand.Intn(maximum-minimum+1) + minimum

	return time.Duration(r) * time.Millisecond
}
