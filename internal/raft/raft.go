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
	//
	currentTerm uint64
	votedFor    *uint64
	id          uint64
	logs        []*proto.LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	peers []peer
	state RaftState

	electionDeadline time.Time
	voteTimeout      time.Duration

	killOnce sync.Once
	done     chan struct{}
	mu       sync.Mutex

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
		nextIndex:        make(map[uint64]uint64),
		done:             make(chan struct{}),
		electionDeadline: time.Now().Add(randomElectionTimeout())}
}

func (r *Raft) Kill() {
	r.killOnce.Do(func() { close(r.done) })
}

func (r *Raft) InitNextIndex() {
	for _, peer := range r.peers {
		r.nextIndex[peer.id] = uint64(len(r.logs)) + 1
	}
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

	majority := majority(len(peers))

	for _, peer := range peers {

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
					r.nextIndex[peer.id] = uint64(len(r.logs)) + 1
					r.InitNextIndex()
					// initialize next index to next index after the last log
					//
					// r.emptyAppend()
					return
				}
			}
		}(peer.client)
	}
}

func (r *Raft) Append(cmd string) bool {
	ch := make(chan bool, 1)

	r.mu.Lock()
	term := r.currentTerm
	leaderId := r.id
	logSize := len(r.logs) - 1
	peerSize := len(r.peers)

	lastLog := &proto.LogEntry{}

	if logSize > 0 {
		lastLog = r.logs[len(r.logs)-1]
	}

	entry := []*proto.LogEntry{{Term: term, Cmd: cmd}}

	r.logs = append(r.logs, entry...)
	r.mu.Unlock()

	majority := majority(peerSize)

	var count int = 1
	for _, peer := range r.peers {

		go func(client proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			prevLogIndex := max(r.nextIndex[peer.id]-1, 0)

			e := []*proto.LogEntry{{Term: term, Cmd: cmd}}
			req := &proto.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  lastLog.Term,
				Entries:      e,
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res, err := client.AppendEntries(ctx, req)
				if err != nil {
					continue
				}

				if res.Hint != nil && *res.Hint > 0 {
					hint := *res.Hint
					r.mu.Lock()
					req.PrevLogIndex = hint
					req.PrevLogTerm = r.logs[hint-1].Term
					req.Entries = append([]*proto.LogEntry(nil), r.logs[req.PrevLogIndex:]...)
					r.mu.Unlock()
					continue
				}

				if res.Success {
					r.mu.Lock()
					count++

					if count >= majority {
						select {
						case ch <- true:
						default:
						}
					}

					r.mu.Unlock()
					break
				}
			}
		}(peer.client)
	}

	return <-ch
}

func majority(size int) int {
	return (size / 2) + 1
}

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

	r.electionDeadline = time.Now().Add(randomElectionTimeout())

	return &proto.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func randomElectionTimeout() time.Duration {
	minimum, maximum := 100, 300
	r := rand.Intn(maximum-minimum+1) + minimum

	return time.Duration(r) * time.Millisecond
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
