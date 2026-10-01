package raft

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/ademolahh/keel/proto"
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

type StateMachine interface {
	Apply(cmd string) any
}

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

	// leaderId is the leader this node last heard from in its current term,
	// or 0 when it does not know one.
	leaderId uint64

	electionDeadline time.Time
	voteTimeout      time.Duration

	killOnce sync.Once
	done     chan struct{}
	mu       sync.Mutex

	stateMachine StateMachine

	proto.UnimplementedRaftServer
}

type peer struct {
	id     uint64
	client proto.RaftClient
}

func New(id uint64, peerClient map[uint64]string, stateMachine StateMachine) *Raft {
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
		stateMachine:     stateMachine,
		electionDeadline: time.Now().Add(randomElectionTimeout())}
}

func (r *Raft) Kill() {
	r.killOnce.Do(func() { close(r.done) })
}

func (r *Raft) RunElectionTimer() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

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
	r.leaderId = 0
	id := r.id

	r.votedFor = &id
	r.electionDeadline = time.Now().Add(randomElectionTimeout())
	votes := 1

	peers := append([]peer(nil), r.peers...)
	logs := append([]*proto.LogEntry(nil), r.logs...)
	lastLogIndex := uint64(len(logs))
	lastLogTerm := uint64(0)

	if lastLogIndex > 0 {
		lastLogTerm = logs[len(logs)-1].Term
	}

	r.mu.Unlock()

	req := &proto.RequestVoteRequest{
		Term:         term,
		CandidateId:  id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
				r.leaderId = 0
			}

			if r.state != Candidate || r.currentTerm != term {
				return
			}

			if res.VoteGranted {
				votes += 1
				if votes >= majority {
					r.state = Leader
					r.leaderId = r.id

					for _, p := range r.peers {
						r.nextIndex[p.id] = uint64(len(r.logs)) + 1
						r.matchIndex[p.id] = 0

						go func() {
							ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
							defer cancel()

							r.replicate(ctx, p, nil)
						}()
					}
					return
				}
			}
		}(peer.client)
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Leader
}

func (r *Raft) Leader() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderId, r.leaderId != 0
}

func (r *Raft) Append(cmd string) bool {
	r.mu.Lock()
	r.logs = append(r.logs, &proto.LogEntry{Term: r.currentTerm, Cmd: cmd})
	peers := append([]peer(nil), r.peers...)
	r.mu.Unlock()

	committed := make(chan bool, 1)

	for _, p := range peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r.replicate(ctx, p, committed)
		}()
	}

	select {
	case <-committed:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

func (r *Raft) HeartBeat() {
	r.mu.Lock()
	peers := append([]peer(nil), r.peers...)
	state := r.state
	r.mu.Unlock()

	if state != Leader {
		return
	}

	for _, p := range peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			r.replicate(ctx, p, nil)
		}()
	}
}

func (r *Raft) Apply() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
		}

		r.mu.Lock()

		if r.commitIndex <= r.lastApplied {
			r.mu.Unlock()
			continue
		}

		r.lastApplied = r.lastApplied + 1
		cmd := r.logs[r.lastApplied-1].Cmd // where lastApplied = r.lastApplied-1
		r.mu.Unlock()

		r.stateMachine.Apply(cmd)

	}
}

func (r *Raft) replicate(ctx context.Context, p peer, committed chan<- bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r.mu.Lock()
		currentTerm := r.currentTerm
		prevLogIndex := max(r.nextIndex[p.id], 1) - 1

		var prevLogTerm uint64
		if prevLogIndex > 0 {
			prevLogTerm = r.logs[prevLogIndex-1].Term
		}

		req := &proto.AppendEntriesRequest{
			Term:         r.currentTerm,
			LeaderId:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      append([]*proto.LogEntry(nil), r.logs[prevLogIndex:]...),
			LeaderCommit: r.commitIndex,
		}

		majority := majority(len(r.peers))
		r.mu.Unlock()

		res, err := p.client.AppendEntries(ctx, req)
		if err != nil {
			continue
		}

		r.mu.Lock()
		if res.Term > currentTerm {
			r.state = Follower
			r.currentTerm = res.Term
			r.votedFor = nil
			r.leaderId = 0
			r.mu.Unlock()
			return
		}

		if res.Hint != nil {
			r.nextIndex[p.id] = *res.Hint + 1
			r.mu.Unlock()
			continue
		}

		if res.Success {
			sentIndex := req.PrevLogIndex + uint64(len(req.Entries))
			r.nextIndex[p.id] = sentIndex + 1
			r.matchIndex[p.id] = sentIndex

			n := match(r.matchIndex, uint64(len(r.logs)))[majority-1]

			if n > r.commitIndex && r.logs[n-1].Term == r.currentTerm {
				r.commitIndex = n

				if committed != nil {
					select {
					case committed <- true:
					default:
					}
				}
			}
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
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

func majority(size int) int {
	return ((size + 1) / 2) + 1
}

func randomElectionTimeout() time.Duration {
	minimum, maximum := 100, 300
	r := rand.Intn(maximum-minimum+1) + minimum

	return time.Duration(r) * time.Millisecond
}
