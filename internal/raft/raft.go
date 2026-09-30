package raft

import (
	"context"
	"math/rand"
	"slices"
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
		matchIndex:       make(map[uint64]uint64),
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
					r.initNextIndex()
					r.initMatchIndex()
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
	peerSize := len(r.peers)
	commitIndex := r.commitIndex

	lastLog := &proto.LogEntry{}

	if len(r.logs)-1 > 0 {
		lastLog = r.logs[len(r.logs)-1]
	}

	entry := []*proto.LogEntry{{Term: term, Cmd: cmd}}

	r.logs = append(r.logs, entry...)
	newLogSize := uint64(len(r.logs))
	r.mu.Unlock()

	majority := majority(peerSize)

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
				LeaderCommit: commitIndex,
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
					sentIndex := req.PrevLogIndex + uint64(len(req.Entries))
					r.nextIndex[peer.id] = sentIndex + 1
					r.matchIndex[peer.id] = sentIndex

					idx := match(r.matchIndex, newLogSize)

					n := idx[majority-1]

					if n > r.commitIndex && r.logs[n-1].Term == r.currentTerm {
						select {
						case ch <- true:
							r.commitIndex = n
						default:
						}
					}

					r.mu.Unlock()
					break
				}
			}
		}(peer.client)
	}

	select {
	case <-ch:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

func match(matchIndex map[uint64]uint64, leader uint64) []uint64 {
	index := []uint64{}
	for _, idx := range matchIndex {
		index = append(index, idx)
	}

	index = append(index, leader)

	slices.SortFunc(index, func(a, b uint64) int {
		return int(b - a)
	})

	return index
}

func (r *Raft) Apply() {
	ticker := time.NewTicker(10 * time.Millisecond)

	for range ticker.C {
		if r.commitIndex > r.lastApplied {
			r.lastApplied = r.lastApplied + 1

			_ = r.logs[r.lastApplied].Cmd
			// r.stateMachine.Apply
		}
	}
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

func majority(size int) int {
	return (size / 2) + 1
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
