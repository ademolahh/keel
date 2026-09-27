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

	logs []*proto.LogEntry

	electionDeadline time.Time
	voteTimeout      time.Duration

	killOnce sync.Once
	done     chan struct{}

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
		logs:             []*proto.LogEntry{},
		voteTimeout:      5 * time.Second,
		done:             make(chan struct{}),
		electionDeadline: time.Now().Add(randomElectionTimeout())}
}

func (r *Raft) Kill() {
	r.killOnce.Do(func() { close(r.done) })
}

func (r *Raft) RunElectionTimer() {
	ticker := time.NewTicker(10 * time.Millisecond)

	for range ticker.C {
		select {
		case <-r.done:
			return
		case <-ticker.C:
		}

		r.mu.Lock()
		deadline := r.electionDeadline
		isLeader := r.state == Leader
		r.mu.Unlock()

		if time.Since(deadline) > 0 && !isLeader {
			r.StartElection()
		}

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

		go func(client proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), r.voteTimeout)
			defer cancel()

			res, err := client.RequestVote(ctx, req)
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
					// initialize next index to next index after the last log
					//
					// r.emptyAppend()
					return
				}
			}
		}(v.client)
	}

}

func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentTerm > req.Term {
		return &proto.RequestVoteResponse{
			VoteGranted: false,
			Term:        r.currentTerm}, nil
	}

	// candidate has the higher term
	if r.currentTerm < req.Term {
		r.currentTerm = req.Term
		r.votedFor = nil
		r.state = Follower
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
			Term:        r.currentTerm}, nil
	}

	r.votedFor = &req.CandidateId
	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.RequestVoteResponse{
		VoteGranted: true,
		Term:        r.currentTerm}, nil
}

func (r *Raft) AppendEntries(ctx context.Context,
	req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	no := &proto.AppendEntriesResponse{Term: r.currentTerm, Success: false}

	if req.Term < r.currentTerm {
		return no, nil
	}

	followerLogIndex := len(r.logs)
	if req.PrevLogIndex != 0 {
		if int(req.PrevLogIndex) > followerLogIndex {
			// custom the error such that the leader retries with a lesser index
			// until they match
			return no, nil
		}

		// contains, but the log doesn't match
		if r.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return no, nil
		}
	}

	newIndex := int(req.PrevLogIndex) + 1

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

	r.electionDeadline = time.Now().Add(randomElectionTimeout())
	return &proto.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func randomElectionTimeout() time.Duration {
	minimum, maximum := 100, 300
	r := rand.Intn(maximum-minimum+1) + minimum

	return time.Duration(r) * time.Millisecond
}
