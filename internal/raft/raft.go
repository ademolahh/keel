package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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
	id uint64

	// persistent
	currentTerm uint64
	votedFor    *uint64
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
	persister    Persister
	log          *slog.Logger

	proto.UnimplementedRaftServer
}

type peer struct {
	id     uint64
	client proto.RaftClient
}

type Status struct {
	Term     uint64
	VotedFor *uint64
	Logs     []*proto.LogEntry
}

func New(id uint64, peerClient map[uint64]string, stateMachine StateMachine, persister Persister) (*Raft, error) {
	var peers []peer

	for pid, addr := range peerClient {
		if pid == id {
			continue
		}

		conn, err := grpc.NewClient("passthrough:///"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  50 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   100 * time.Millisecond,
				},
			}),
		)

		if err != nil {
			slog.Error("dropping peer", "node", id, "peer", pid, "addr", addr, "err", err)
			continue
		}

		slog.Debug("peer client created", "node", id, "peer", pid, "addr", addr)

		client := proto.NewRaftClient(conn)

		peers = append(peers, peer{id: pid, client: client})
	}

	r := &Raft{
		id:               id,
		peers:            peers,
		state:            Follower,
		logs:             []*proto.LogEntry{},
		voteTimeout:      5 * time.Second,
		nextIndex:        make(map[uint64]uint64),
		matchIndex:       make(map[uint64]uint64),
		done:             make(chan struct{}),
		stateMachine:     stateMachine,
		persister:        persister,
		log:              slog.Default().With("node", id),
		electionDeadline: time.Now().Add(randomElectionTimeout())}

	if err := r.readPersist(); err != nil {
		return nil, err
	}

	r.log.Info("node ready", "term", r.currentTerm, "log_length", len(r.logs))

	return r, nil
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

func (r *Raft) RunHeartbeat() {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.HeartBeat()
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
	r.persist()
	r.log.Info("starting election", "term", term)
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
				r.log.Debug("vote request failed", "peer", peer.id, "term", term, "err", err)
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if res.Term > r.currentTerm {
				r.currentTerm = res.Term
				r.state = Follower
				r.votedFor = nil
				r.leaderId = 0
				r.persist()
				r.log.Info("stepping down", "term", res.Term, "reason", "higher term in vote reply")
			}

			if r.state != Candidate || r.currentTerm != term {
				return
			}

			if res.VoteGranted {
				votes += 1
				if votes >= majority {
					r.state = Leader
					r.leaderId = r.id
					r.log.Info("became leader", "term", term)

					for _, p := range r.peers {
						r.nextIndex[p.id] = uint64(len(r.logs)) + 1
						r.matchIndex[p.id] = 0

						go func() {
							ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
							defer cancel()

							r.replicate(ctx, p)
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

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := Status{
		Term: r.currentTerm,
		Logs: append([]*proto.LogEntry(nil), r.logs...),
	}

	if r.votedFor != nil {
		s.VotedFor = new(*r.votedFor)
	}

	return s
}

func (r *Raft) CaughtUp() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastApplied >= r.commitIndex
}

func (r *Raft) Leader() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderId, r.leaderId != 0
}

func (r *Raft) Append(cmd string) bool {
	r.mu.Lock()
	term := r.currentTerm
	r.logs = append(r.logs, &proto.LogEntry{Term: term, Cmd: cmd})
	r.persist()
	index := uint64(len(r.logs))
	peers := append([]peer(nil), r.peers...)
	r.mu.Unlock()

	for _, p := range peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r.replicate(ctx, p)
		}()
	}

	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-timeout:
			return false
		default:
		}

		r.mu.Lock()
		committed := r.commitIndex >= index
		ours := committed && r.logs[index-1].Term == term
		r.mu.Unlock()

		if committed {
			return ours
		}

	}

	return false
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

			r.replicate(ctx, p)
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
		r.log.Debug("applied", "index", r.lastApplied)
		cmd := r.logs[r.lastApplied-1].Cmd
		r.mu.Unlock()

		r.stateMachine.Apply(cmd)

	}
}

const retryDelay = 10 * time.Millisecond

func (r *Raft) replicate(ctx context.Context, p peer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r.mu.Lock()
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
			r.log.Debug("append failed", "peer", p.id, "err", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}

			continue
		}

		r.mu.Lock()
		if res.Term > r.currentTerm {
			r.state = Follower
			r.currentTerm = res.Term
			r.votedFor = nil
			r.leaderId = 0
			r.persist()
			r.log.Info("stepping down", "term", res.Term, "reason", "higher term in append reply")
			r.mu.Unlock()
			return
		}

		if res.Hint != nil {
			r.nextIndex[p.id] = *res.Hint + 1
			r.log.Debug("backing off", "peer", p.id, "next_index", *res.Hint+1)
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
				r.log.Debug("committed", "index", n)
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

const (
	electionTimeoutMin = 100 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)

	return electionTimeoutMin + time.Duration(rand.Int63n(spread+1))
}
