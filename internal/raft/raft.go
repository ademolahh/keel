package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	protobuf "google.golang.org/protobuf/proto"
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
	Snapshot() ([]byte, error)
	Restore(data []byte) error
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

	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotChunks    []byte
	pendingSnapshot   *proto.Snapshot
	snapshot          *proto.Snapshot

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	peers []peer
	state RaftState

	leaderId      uint64
	leaderChanges uint64

	electionDeadline time.Time
	voteTimeout      time.Duration

	killOnce sync.Once
	done     chan struct{}
	mu       sync.Mutex

	commitCh chan struct{}
	applied  chan struct{}

	stateMachine StateMachine
	persister    Persister
	log          *slog.Logger

	proto.UnimplementedRaftServer
}

type peer struct {
	id     uint64
	client proto.RaftClient
}

func New(id uint64, peerClient map[uint64]string, stateMachine StateMachine, persister Persister, opts ...grpc.DialOption) (*Raft, error) {
	var peers []peer

	for pid, addr := range peerClient {
		if pid == id {
			continue
		}

		conn, err := grpc.NewClient("passthrough:///"+addr, append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  50 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   100 * time.Millisecond,
				},
			}),
		}, opts...)...)

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
		commitCh:         make(chan struct{}, 1),
		applied:          make(chan struct{}),
		stateMachine:     stateMachine,
		persister:        persister,
		log:              slog.Default().With("node", id),
		electionDeadline: time.Now().Add(randomElectionTimeout()),
	}

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
	r.persistState()
	r.log.Info("starting election", "term", term)
	r.electionDeadline = time.Now().Add(randomElectionTimeout())
	votes := 1

	peers := slices.Clone(r.peers)
	lastLogIndex := r.lastIndex()
	lastLogTerm := r.termAt(lastLogIndex)

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
				r.persistState()
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
					r.leaderChanges++
					r.log.Info("became leader", "term", term)

					for _, p := range r.peers {
						r.nextIndex[p.id] = r.lastIndex() + 1
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

var (
	ErrNotLeader = errors.New("not the leader")
	ErrNoQuorum  = errors.New("could not reach a majority")
)

func (r *Raft) Append(cmd string) bool {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}

	term := r.currentTerm
	r.logs = append(r.logs, &proto.LogEntry{Term: term, Cmd: cmd})
	index := r.lastIndex()
	r.persistLog(index)
	peers := slices.Clone(r.peers)
	r.mu.Unlock()

	for _, p := range peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r.replicate(ctx, p)
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := r.waitApplied(ctx, index); err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return index >= r.lastIncludedIndex && r.termAt(index) == term
}

func (r *Raft) Read() error {
	if !r.IsLeader() {
		return ErrNotLeader
	}

	if !r.Append("") {
		return ErrNoQuorum
	}

	return nil
}

func (r *Raft) HeartBeat() {
	r.mu.Lock()
	peers := slices.Clone(r.peers)
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

const snapshotThreshold = 1024

func (r *Raft) Snapshot() {
	r.mu.Lock()
	index, base := r.lastApplied, r.lastIncludedIndex

	if r.pendingSnapshot != nil || index <= base {
		r.mu.Unlock()
		return
	}

	size := protobuf.Size(&proto.PersistentState{Logs: r.logs[:index-base]})
	r.mu.Unlock()

	if size <= snapshotThreshold {
		return
	}

	data, err := r.stateMachine.Snapshot()
	if err != nil {
		r.log.Error("snapshot failed", "index", index, "err", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pendingSnapshot != nil || index <= r.lastIncludedIndex {
		return
	}

	term := r.termAt(index)

	snapshot := &proto.Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: data}

	saved, err := protobuf.Marshal(snapshot)
	if err != nil {
		r.log.Error("snapshot failed", "index", index, "err", err)
		return
	}

	if err := r.persister.SaveSnapshot(saved); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	r.snapshot = snapshot
	r.logs = slices.Clone(r.entriesFrom(index + 1))
	r.lastIncludedIndex = index
	r.lastIncludedTerm = term

	if err := r.persister.ResetLog(index, r.logs); err != nil {
		panic(fmt.Sprintf("persist: %v", err))
	}

	r.log.Info("saved snapshot", "index", index, "term", term, "log_bytes", size)
}

func (r *Raft) Apply() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-r.commitCh:
		case <-ticker.C:
		}

		r.mu.Lock()

		if snapshot := r.pendingSnapshot; snapshot != nil {
			r.pendingSnapshot = nil
			r.mu.Unlock()

			if err := r.stateMachine.Restore(snapshot.Data); err != nil {
				panic(fmt.Sprintf("restore snapshot: %v", err))
			}

			r.mu.Lock()
			r.lastApplied = max(r.lastApplied, snapshot.LastIncludedIndex)
			r.log.Info("restored snapshot", "index", snapshot.LastIncludedIndex, "term", snapshot.LastIncludedTerm)
			close(r.applied)
			r.applied = make(chan struct{})
			r.mu.Unlock()
			continue
		}

		if r.commitIndex <= r.lastApplied {
			r.mu.Unlock()
			continue
		}

		first := r.lastApplied + 1
		base := r.lastIncludedIndex
		entries := slices.Clone(r.logs[r.lastApplied-base : r.commitIndex-base])
		r.mu.Unlock()

		for _, e := range entries {
			if e.Cmd != "" {
				r.stateMachine.Apply(e.Cmd)
			}
		}

		r.mu.Lock()
		r.lastApplied = first + uint64(len(entries)) - 1
		r.log.Debug("applied", "from", first, "to", r.lastApplied)
		close(r.applied)
		r.applied = make(chan struct{})
		r.mu.Unlock()

		r.Snapshot()
	}
}

const retryDelay = 10 * time.Millisecond

func (r *Raft) replicate(ctx context.Context, p peer) {
	r.mu.Lock()
	term := r.currentTerm
	r.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r.mu.Lock()
		if r.id != r.leaderId || term != r.currentTerm {
			r.mu.Unlock()
			return
		}

		prevLogIndex := max(r.nextIndex[p.id], 1) - 1

		if prevLogIndex < r.lastIncludedIndex {
			r.mu.Unlock()

			if !r.sendSnapshot(ctx, p, term) {
				return
			}

			continue
		}

		req := &proto.AppendEntriesRequest{
			Term:         r.currentTerm,
			LeaderId:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  r.termAt(prevLogIndex),
			Entries:      slices.Clone(r.entriesFrom(prevLogIndex + 1)),
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
			r.persistState()
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

		if r.id != r.leaderId || term != r.currentTerm {
			r.mu.Unlock()
			return
		}

		if res.Success {
			sentIndex := req.PrevLogIndex + uint64(len(req.Entries))
			r.nextIndex[p.id] = sentIndex + 1
			r.matchIndex[p.id] = sentIndex

			n := match(r.matchIndex, r.lastIndex())[majority-1]

			if n > r.commitIndex && r.termAt(n) == r.currentTerm {
				r.commitIndex = n
				r.notifyCommit()
				r.log.Debug("committed", "index", n)
			}
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

const snapshotChunkSize = 32 * 1024

func (r *Raft) sendSnapshot(ctx context.Context, p peer, term uint64) bool {
	r.mu.Lock()
	snapshot := r.snapshot
	r.mu.Unlock()

	if snapshot == nil {
		r.log.Error("no snapshot to send", "peer", p.id)
		return false
	}

	for offset := 0; ; {
		end := min(offset+snapshotChunkSize, len(snapshot.Data))

		req := &proto.InstallSnapshotRequest{
			Term:              term,
			LeaderId:          r.id,
			LastIncludedIndex: snapshot.LastIncludedIndex,
			LastIncludedTerm:  snapshot.LastIncludedTerm,
			Offset:            uint64(offset),
			Data:              snapshot.Data[offset:end],
			Done:              end == len(snapshot.Data),
		}

		res, err := p.client.InstallSnapshot(ctx, req)
		if err != nil {
			r.log.Debug("snapshot send failed", "peer", p.id, "err", err)

			select {
			case <-ctx.Done():
				return false
			case <-time.After(retryDelay):
			}

			return true
		}

		r.mu.Lock()
		if res.Term > r.currentTerm {
			r.state = Follower
			r.currentTerm = res.Term
			r.votedFor = nil
			r.leaderId = 0
			r.persistState()
			r.log.Info("stepping down", "term", res.Term, "reason", "higher term in snapshot reply")
			r.mu.Unlock()
			return false
		}

		if r.id != r.leaderId || term != r.currentTerm {
			r.mu.Unlock()
			return false
		}
		r.mu.Unlock()

		if req.Done {
			break
		}

		offset = end
	}

	r.mu.Lock()
	r.matchIndex[p.id] = max(r.matchIndex[p.id], snapshot.LastIncludedIndex)
	r.nextIndex[p.id] = snapshot.LastIncludedIndex + 1
	r.mu.Unlock()

	r.log.Info("sent snapshot", "peer", p.id, "index", snapshot.LastIncludedIndex)

	return true
}
