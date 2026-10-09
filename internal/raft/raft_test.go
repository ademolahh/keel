package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
	protobuf "google.golang.org/protobuf/proto"
)

// lastApplied <= commitIndex <= last log index - invariant

const (
	DEFAULT_CLUSTER_SIZE        = 5
	DEFAULT_LEADER_ID    uint64 = 1
)

func TestNew(t *testing.T) {
	currTime := time.Now()
	raft, peers := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

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
	if duration > electionTimeoutMax || duration < electionTimeoutMin {
		t.Errorf("duration: expected within %v-%v, got %v", electionTimeoutMin, electionTimeoutMax, duration)
	}

}

func TestGetState(t *testing.T) {
	raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
	if raft.state != Follower {
		t.Errorf("state: expected Follower, got: %s", raft.state.String())
	}
}

func TestStartElection(t *testing.T) {
	t.Run("wins with a majority of votes", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

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

	t.Run("steps down when a peer reports a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.currentTerm = 2

		var actualCurrentTerm uint64 = 5

		for id, node := range nodes {
			if id == DEFAULT_LEADER_ID {
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

	t.Run("wins while two peers are down", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		nodes[3].stop()
		nodes[4].stop()

		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Leader)

	})

	t.Run("stays a candidate when a majority is down", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("stays a candidate when votes arrive after the timeout", func(t *testing.T) {
		delayDuration := 500 * time.Millisecond
		delay := map[uint64]time.Duration{
			2: delayDuration,
			3: delayDuration,
			4: delayDuration,
		}

		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, delay)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()

		waitForState(t, raft, raft.voteTimeout, Candidate)

	})

	t.Run("delayed peers vote in the newer term", func(t *testing.T) {
		const lag = 4 * time.Second
		lagging := []uint64{3, 4}

		delay := make(map[uint64]time.Duration, len(lagging))

		for _, id := range lagging {
			delay[id] = lag
		}

		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, delay)

		nodes[DEFAULT_LEADER_ID].raft.StartElection()
		waitForLeader(t, nodes[DEFAULT_LEADER_ID].raft, lag/4)
		_, firstTerm, _ := snapshot(nodes[DEFAULT_LEADER_ID].raft)

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
				t.Errorf("server %d: expected term %d and vote for 5, got term %d, votedFor %v",
					r.id, secondTerm, term, votedFor)
			}
		}
	})

	t.Run("wins when every node already holds entries", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)

		for _, n := range nodes {
			n.raft.mu.Lock()
			n.raft.logs = makeLogs()
			n.raft.currentTerm = 3
			n.raft.mu.Unlock()
		}

		raft := nodes[DEFAULT_LEADER_ID].raft
		raft.StartElection()

		waitForLeader(t, raft, raft.voteTimeout)
	})
}

func TestRunElectionTimer(t *testing.T) {
	nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)

	t.Cleanup(func() {
		for _, node := range nodes {
			node.raft.Kill()
		}
	})

	for _, node := range nodes {
		go node.raft.RunElectionTimer()
	}

	// keep checking until there is a leader

	deadline := time.Now().Add(3 * time.Second)

	// term -> leader id -> bool
	leaderPerTerm := map[uint64]map[uint64]bool{}

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			node.raft.mu.Lock()
			raft := node.raft
			isLeader := raft.state == Leader
			term := raft.currentTerm
			node.raft.mu.Unlock()
			if isLeader {
				if leaderPerTerm[term] == nil {
					leaderPerTerm[term] = make(map[uint64]bool)
				}
				leaderPerTerm[term][raft.id] = true
			}

		}
		time.Sleep(10 * time.Millisecond)
	}

	for term, v := range leaderPerTerm {
		if len(v) > 1 {
			t.Errorf("term %d has multiple leaders: %v", term, len(v))
		}
	}
}

func TestAppend(t *testing.T) {
	t.Run("appends the command to the leader's log", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.leaderId = r.id
		r.initNextIndex()
		r.initMatchIndex()
		logSize := len(r.logs)

		r.Append("set a=1")

		if len(r.logs) != logSize+1 {
			t.Errorf("log size: expected %d, got %d", logSize+1, len(r.logs))
		}
	})

	t.Run("replicates the entry to a majority of peers", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.leaderId = r.id
		r.initNextIndex()
		r.initMatchIndex()
		size := len(nodes)

		res := r.Append("set a = 1")

		if !res {
			t.Errorf("entry failed to commit")
		}

		// time.Sleep(1 * time.Second)

		cmdEntered := 0
		for _, node := range nodes {
			node.raft.mu.Lock()
			len := len(node.raft.logs)
			node.raft.mu.Unlock()

			if len >= 1 {
				cmdEntered += 1

			}
		}

		majority := majority(size - 1)
		if cmdEntered < majority {
			t.Errorf("command entered: expected %d, received: %d", majority, cmdEntered)
		}
	})

	t.Run("catches up a follower that is far behind", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		const STALE_ID uint64 = 3
		const LATEST_TERM uint64 = 3
		nodes[DEFAULT_LEADER_ID].raft.state = Leader
		nodes[DEFAULT_LEADER_ID].raft.leaderId = nodes[DEFAULT_LEADER_ID].raft.id

		logs := makeLogs()

		for _, node := range nodes {
			if node.raft.id == STALE_ID {
				continue
			}

			node.raft.logs = append(node.raft.logs, logs...)
			node.raft.currentTerm = LATEST_TERM
		}

		nodes[DEFAULT_LEADER_ID].raft.nextIndex[1] = uint64(len(logs)) + 1
		nodes[STALE_ID].raft.currentTerm = 1
		nodes[STALE_ID].raft.logs = append(nodes[STALE_ID].raft.logs, logs[0])
		nodes[DEFAULT_LEADER_ID].raft.initNextIndex()
		nodes[DEFAULT_LEADER_ID].raft.initMatchIndex()

		res := nodes[DEFAULT_LEADER_ID].raft.Append("set a=1")
		if !res {
			t.Fatal("append failed")
		}

		time.Sleep(1 * time.Second)

		nodes[STALE_ID].raft.mu.Lock()
		len := len(nodes[STALE_ID].raft.logs)
		nodes[STALE_ID].raft.mu.Unlock()

		if len != 6 {
			t.Errorf("stale log: expected 6, got %d", len)
		}
	})

	t.Run("returns false when a majority does not respond", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.state = Leader
		r.leaderId = r.id
		r.initNextIndex()
		r.initMatchIndex()

		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		if r.Append("set a=1") {
			t.Error("append: expected false, got true")
		}
	})

	t.Run("returns true when another round commits the entry", func(t *testing.T) {
		raft, _ := newRaft(t, DEFAULT_LEADER_ID, DEFAULT_CLUSTER_SIZE)
		raft.state = Leader
		raft.leaderId = raft.id
		go raft.Apply()
		t.Cleanup(raft.Kill)

		// stand in for a heartbeat committing the entry; no peer is reachable,
		// so Append's own rounds never can
		go waitFor(t, time.Second, func() bool {
			raft.mu.Lock()
			defer raft.mu.Unlock()

			if len(raft.logs) == 0 {
				return false
			}

			raft.commitIndex = uint64(len(raft.logs))
			return true
		})

		if !raft.Append("set a=1") {
			t.Error("append: expected true, got false")
		}
	})

	t.Run("returns false when a later leader replaced its entry", func(t *testing.T) {
		raft, _ := newRaft(t, DEFAULT_LEADER_ID, DEFAULT_CLUSTER_SIZE)
		raft.state = Leader
		raft.leaderId = raft.id
		go raft.Apply()
		t.Cleanup(raft.Kill)

		// stand in for a newer leader overwriting the entry and committing its
		// own at the same position
		go waitFor(t, time.Second, func() bool {
			raft.mu.Lock()
			defer raft.mu.Unlock()

			if len(raft.logs) == 0 {
				return false
			}

			index := len(raft.logs)
			raft.logs[index-1] = &proto.LogEntry{Term: raft.currentTerm + 1, Cmd: "set b=2"}
			raft.commitIndex = uint64(index)
			return true
		})

		if raft.Append("set a=1") {
			t.Error("append: expected false, got true")
		}
	})

	t.Run("reports a write as applied even when a snapshot compacts it first", func(t *testing.T) {
		raft, err := New(1, map[uint64]string{1: "localhost:0"}, &recorder{}, &memoryPersister{})
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		raft.SetSnapshotThreshold(1)
		manualLeader(raft, 1)

		go raft.Apply()
		go raft.RunSync()
		t.Cleanup(raft.Kill)

		const writes = 50

		var wg sync.WaitGroup
		failed := make(chan int, writes)
		for i := range writes {
			wg.Go(func() {
				if !raft.Append(fmt.Sprintf("set k=%d", i)) {
					failed <- i
				}
			})
		}
		wg.Wait()
		close(failed)

		if n := len(failed); n != 0 {
			t.Errorf("appends reported failed: expected 0 of %d, got %d", writes, n)
		}
	})

	t.Run("refuses a command when it is not the leader", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		if raft.Append("set a=1") {
			t.Error("append: expected false, got true")
		}

		if len(raft.logs) != 0 {
			t.Errorf("log size: expected 0, got %d", len(raft.logs))
		}
	})

	t.Run("returns only after the entry is applied", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		sm := &recorder{}
		r.mu.Lock()
		r.stateMachine = sm
		r.state = Leader
		r.leaderId = r.id
		r.initNextIndex()
		r.initMatchIndex()
		r.mu.Unlock()

		if !r.Append("set a=1") {
			t.Fatal("append: expected true, got false")
		}

		if got := sm.applied(); !slices.Equal(got, []string{"set a=1"}) {
			t.Errorf("applied: expected [set a=1], got %v", got)
		}
	})
}

func TestRead(t *testing.T) {
	t.Run("refuses when it is not the leader", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		if err := raft.Read(context.Background()); !errors.Is(err, ErrNotLeader) {
			t.Errorf("read: expected %v, got %v", ErrNotLeader, err)
		}
	})

	t.Run("commits an empty entry when it has none from its term", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		r := manualLeader(nodes[DEFAULT_LEADER_ID].raft, 1)

		if err := r.Read(readContext(t)); err != nil {
			t.Fatalf("read: unexpected error: %v", err)
		}

		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.logs) != 1 || r.logs[0].Cmd != "" || r.lastApplied != 1 {
			t.Errorf("log: expected one applied empty entry, got %v (applied %d)", r.logs, r.lastApplied)
		}
	})

	t.Run("adds nothing to the log once it has committed in its term", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		r := manualLeader(nodes[DEFAULT_LEADER_ID].raft, 1)

		for range 3 {
			if err := r.Read(readContext(t)); err != nil {
				t.Fatalf("read: unexpected error: %v", err)
			}
		}

		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.logs) != 1 {
			t.Errorf("log size: expected 1, got %d", len(r.logs))
		}
	})

	t.Run("shares one round of calls between concurrent reads", func(t *testing.T) {
		calls := &callCounter{}
		nodes, _ := clusterWithOptions(t, 3, map[uint64][]grpc.ServerOption{
			2: {calls.interceptor(20 * time.Millisecond)},
			3: {calls.interceptor(20 * time.Millisecond)},
		})
		r := manualLeader(nodes[DEFAULT_LEADER_ID].raft, 1)

		if err := r.Read(readContext(t)); err != nil {
			t.Fatalf("first read: unexpected error: %v", err)
		}

		// let the first read's calls finish before counting
		time.Sleep(100 * time.Millisecond)
		before, _ := calls.stats()

		const reads = 20

		var wg sync.WaitGroup
		for range reads {
			wg.Go(func() {
				if err := r.Read(readContext(t)); err != nil {
					t.Errorf("read: unexpected error: %v", err)
				}
			})
		}
		wg.Wait()

		after, _ := calls.stats()
		t.Logf("%d concurrent reads took %d calls to the followers", reads, after-before)

		if after-before == 0 || after-before > 6 {
			t.Errorf("calls to the followers: expected 1 to 6 for %d reads, got %d", reads, after-before)
		}
	})

	t.Run("fails when a leader that already committed in its term loses its majority", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		r := nodes[DEFAULT_LEADER_ID].raft
		r.mu.Lock()
		r.state = Leader
		r.leaderId = r.id
		r.currentTerm = 1
		r.logs = []*proto.LogEntry{{Term: 1}}
		r.commitIndex = 1
		r.initNextIndex()
		r.initMatchIndex()
		r.mu.Unlock()

		nodes[3].stop()
		nodes[4].stop()
		nodes[5].stop()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		if err := r.Read(ctx); !errors.Is(err, ErrNoQuorum) {
			t.Errorf("read: expected %v, got %v", ErrNoQuorum, err)
		}
	})
}

func TestReplicate(t *testing.T) {
	t.Run("waits a full election timeout after stepping down", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.mu.Lock()
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 1
		leader.electionDeadline = time.Now().Add(-time.Second)
		leader.initNextIndex()
		leader.initMatchIndex()
		leader.mu.Unlock()

		follower := nodes[2].raft
		follower.mu.Lock()
		follower.currentTerm = 2
		follower.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		before := time.Now()
		leader.replicate(ctx, peerOf(leader, 2))

		assertFreshDeadline(t, leader, before)
	})

	t.Run("keeps a newer term when a late reply carries an older one", func(t *testing.T) {
		const lag = 200 * time.Millisecond

		nodes, _ := cluster(t, 3, map[uint64]time.Duration{2: lag})
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 1
		leader.initNextIndex()
		leader.initMatchIndex()

		follower := nodes[2].raft
		follower.mu.Lock()
		follower.currentTerm = 2
		follower.mu.Unlock()

		var p peer
		for _, q := range leader.peers {
			if q.id == 2 {
				p = q
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			leader.replicate(ctx, p)
			close(done)
		}()

		// the request goes out at term 1 and node 2 holds its reply, which
		// carries term 2, for lag; meanwhile the leader moves on to term 3
		time.Sleep(lag / 4)
		leader.mu.Lock()
		leader.currentTerm = 3
		leader.mu.Unlock()

		<-done

		if _, term, _ := snapshot(leader); term != 3 {
			t.Errorf("term: expected 3, got %d", term)
		}
	})

	t.Run("stops retrying once it loses leadership", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.initNextIndex()
		leader.initMatchIndex()

		// every call to node 2 fails, so replicate keeps retrying
		nodes[2].stop()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			leader.replicate(ctx, peerOf(leader, 2))
			close(done)
		}()

		time.Sleep(50 * time.Millisecond)
		leader.mu.Lock()
		leader.state = Follower
		leader.leaderId = 0
		leader.mu.Unlock()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("replicate: expected to stop after losing leadership, still retrying")
		}
	})

	t.Run("stops retrying once the term moves on", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 1
		leader.initNextIndex()
		leader.initMatchIndex()

		nodes[2].stop()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			leader.replicate(ctx, peerOf(leader, 2))
			close(done)
		}()

		// still the leader, but of a later term than the one replicate began in
		time.Sleep(50 * time.Millisecond)
		leader.mu.Lock()
		leader.currentTerm = 3
		leader.mu.Unlock()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("replicate: expected to stop after the term changed, still retrying")
		}
	})

	t.Run("ignores a reply that arrives after it stepped down", func(t *testing.T) {
		const lag = 200 * time.Millisecond

		nodes, _ := cluster(t, 3, map[uint64]time.Duration{2: lag})
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 3
		leader.logs = makeLogs()
		leader.initMatchIndex()
		leader.nextIndex[2] = 1

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			leader.replicate(ctx, peerOf(leader, 2))
			close(done)
		}()

		// node 2 accepts all five entries but holds its reply for lag
		time.Sleep(lag / 4)
		leader.mu.Lock()
		leader.state = Follower
		leader.leaderId = 0
		leader.mu.Unlock()

		<-done

		leader.mu.Lock()
		match := leader.matchIndex[2]
		leader.mu.Unlock()

		if match != 0 {
			t.Errorf("match index: expected 0, got %d", match)
		}
	})
}

func TestRunHeartbeat(t *testing.T) {
	t.Run("keeps resetting a follower's election deadline", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.initNextIndex()
		leader.initMatchIndex()

		go leader.RunHeartbeat()
		t.Cleanup(leader.Kill)

		follower := nodes[2].raft

		for i := range 2 {
			follower.mu.Lock()
			before := follower.electionDeadline
			follower.mu.Unlock()

			ok := waitFor(t, time.Second, func() bool {
				follower.mu.Lock()
				defer follower.mu.Unlock()

				return !follower.electionDeadline.Equal(before)
			})

			if !ok {
				t.Fatalf("heartbeat %d: expected election deadline to be reset", i+1)
			}
		}
	})
}

func TestKill(t *testing.T) {
	loops := []struct {
		name string
		run  func(*Raft)
	}{
		{"stops the election timer", (*Raft).RunElectionTimer},
		{"stops the heartbeat loop", (*Raft).RunHeartbeat},
		{"stops the apply loop", (*Raft).Apply},
	}

	for _, loop := range loops {
		t.Run(loop.name, func(t *testing.T) {
			raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

			done := make(chan struct{})
			go func() {
				loop.run(raft)
				close(done)
			}()

			raft.Kill()

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("loop: expected to return after Kill, still running")
			}
		})
	}

	t.Run("can be called more than once", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.Kill()
		raft.Kill()
	})
}

func TestHeartBeat(t *testing.T) {
	t.Run("does nothing when not the leader", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		follower := nodes[2].raft

		follower.mu.Lock()
		deadline := follower.electionDeadline
		follower.mu.Unlock()

		nodes[DEFAULT_LEADER_ID].raft.HeartBeat()

		time.Sleep(100 * time.Millisecond)

		follower.mu.Lock()
		reset := !follower.electionDeadline.Equal(deadline)
		follower.mu.Unlock()

		if reset {
			t.Error("election deadline: expected unchanged, got reset")
		}
	})

	t.Run("resets every follower's election deadline", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.initNextIndex()
		leader.initMatchIndex()

		deadlines := make(map[uint64]time.Time)
		for id, n := range nodes {
			if id == DEFAULT_LEADER_ID {
				continue
			}

			n.raft.mu.Lock()
			deadlines[id] = n.raft.electionDeadline
			n.raft.mu.Unlock()
		}

		leader.HeartBeat()

		for id, before := range deadlines {
			r := nodes[id].raft

			ok := waitFor(t, time.Second, func() bool {
				r.mu.Lock()
				defer r.mu.Unlock()

				return !r.electionDeadline.Equal(before)
			})

			if !ok {
				t.Errorf("node %d: expected election deadline to be reset", id)
			}
		}
	})

	t.Run("steps down when a follower reports a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 1
		leader.initNextIndex()
		leader.initMatchIndex()

		for id, n := range nodes {
			if id == DEFAULT_LEADER_ID {
				continue
			}

			n.raft.mu.Lock()
			n.raft.currentTerm = 5
			n.raft.mu.Unlock()
		}

		leader.HeartBeat()

		waitForState(t, leader, time.Second, Follower)

		if _, term, _ := snapshot(leader); term != 5 {
			t.Errorf("term: expected 5, got %d", term)
		}
	})
}

func TestLeader(t *testing.T) {
	t.Run("knows no leader before hearing from one", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		if id, ok := raft.Leader(); ok {
			t.Errorf("leader: expected none, got %d", id)
		}

		if raft.IsLeader() {
			t.Error("is leader: expected false, got true")
		}
	})

	t.Run("learns the leader from AppendEntries", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		raft.AppendEntries(context.Background(), &proto.AppendEntriesRequest{Term: 1, LeaderId: 3})

		if id, ok := raft.Leader(); !ok || id != 3 {
			t.Errorf("leader: expected 3, got %d (known %t)", id, ok)
		}
	})

	t.Run("forgets the leader when an election starts", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.leaderId = 3

		raft.StartElection()

		if id, ok := raft.Leader(); ok {
			t.Errorf("leader: expected none, got %d", id)
		}
	})

	t.Run("reports itself as leader after winning", func(t *testing.T) {
		nodes, _ := cluster(t, DEFAULT_CLUSTER_SIZE, nil)
		raft := nodes[DEFAULT_LEADER_ID].raft

		raft.StartElection()
		waitForLeader(t, raft, raft.voteTimeout)

		if !raft.IsLeader() {
			t.Error("is leader: expected true, got false")
		}

		if id, ok := raft.Leader(); !ok || id != DEFAULT_LEADER_ID {
			t.Errorf("leader: expected %d, got %d (known %t)", DEFAULT_LEADER_ID, id, ok)
		}
	})
}

func TestApply(t *testing.T) {
	t.Run("applies committed entries in log order", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		sm := &recorder{}
		raft.stateMachine = sm
		raft.logs = makeLogs()
		raft.commitIndex = 3

		go raft.Apply()
		t.Cleanup(raft.Kill)

		want := []string{"set a=1", "set b=2", "set c=3"}

		waitFor(t, time.Second, func() bool { return len(sm.applied()) >= len(want) })

		// give Apply the chance to wrongly run past the commit index
		time.Sleep(50 * time.Millisecond)

		if got := sm.applied(); !slices.Equal(got, want) {
			t.Errorf("applied: expected %v, got %v", want, got)
		}
	})

	t.Run("applies every waiting entry when woken", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		sm := &recorder{}
		raft.stateMachine = sm

		const n = 100
		for i := range n {
			raft.logs = append(raft.logs, &proto.LogEntry{Term: 1, Cmd: fmt.Sprintf("set k=%d", i)})
		}

		go raft.Apply()
		t.Cleanup(raft.Kill)

		raft.mu.Lock()
		raft.commitIndex = n
		raft.notifyCommit()
		raft.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		if err := raft.waitApplied(ctx, n); err != nil {
			t.Fatalf("wait: %v (applied %d of %d)", err, len(sm.applied()), n)
		}
	})

	t.Run("skips empty entries", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		sm := &recorder{}
		raft.stateMachine = sm
		raft.logs = []*proto.LogEntry{{Term: 1, Cmd: ""}, {Term: 1, Cmd: "set a=1"}}
		raft.commitIndex = 2

		go raft.Apply()
		t.Cleanup(raft.Kill)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := raft.waitApplied(ctx, 2); err != nil {
			t.Fatalf("wait: %v", err)
		}

		if got := sm.applied(); !slices.Equal(got, []string{"set a=1"}) {
			t.Errorf("applied: expected [set a=1], got %v", got)
		}
	})
}

func TestSnapshot(t *testing.T) {
	t.Run("saves a snapshot and drops the entries it covers once the applied log passes 1KB", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.stateMachine = &recorder{}
		raft.SetSnapshotThreshold(1024)
		raft.logs = bigLogs(30, 2)
		raft.commitIndex = 30

		go raft.Apply()
		t.Cleanup(raft.Kill)

		ok := waitFor(t, time.Second, func() bool {
			raft.mu.Lock()
			defer raft.mu.Unlock()

			return raft.lastIncludedIndex == 30
		})
		if !ok {
			t.Fatal("snapshot: expected one through entry 30")
		}

		raft.mu.Lock()
		defer raft.mu.Unlock()

		if len(raft.logs) != 0 || raft.lastIncludedTerm != 2 {
			t.Errorf("log: expected no entries after term 2, got %d after term %d", len(raft.logs), raft.lastIncludedTerm)
		}

		if saved := savedSnapshot(t, raft); saved.LastIncludedIndex != 30 || saved.LastIncludedTerm != 2 {
			t.Errorf("saved snapshot: expected index 30 term 2, got index %d term %d",
				saved.LastIncludedIndex, saved.LastIncludedTerm)
		}

		if base := raft.persister.(*memoryPersister).state.LogBase; base != 30 {
			t.Errorf("saved log: expected it to start after 30, got %d", base)
		}

		if raft.snapshot == nil || raft.snapshot.LastIncludedIndex != 30 {
			t.Errorf("snapshot in memory: expected one through 30, got %v", raft.snapshot)
		}
	})

	t.Run("waits for the configured threshold", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.stateMachine = &recorder{}
		raft.SetSnapshotThreshold(4096)
		raft.logs = bigLogs(30, 2)
		raft.commitIndex = 30

		go raft.Apply()
		t.Cleanup(raft.Kill)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := raft.waitApplied(ctx, 30); err != nil {
			t.Fatalf("wait: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		raft.mu.Lock()
		defer raft.mu.Unlock()

		if raft.lastIncludedIndex != 0 {
			t.Errorf("snapshot: expected none below 4096 bytes, got one through %d", raft.lastIncludedIndex)
		}
	})

	t.Run("snapshots sooner with a lower threshold", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.stateMachine = &recorder{}
		raft.SetSnapshotThreshold(100)
		raft.logs = bigLogs(10, 1)
		raft.commitIndex = 10

		go raft.Apply()
		t.Cleanup(raft.Kill)

		ok := waitFor(t, time.Second, func() bool {
			raft.mu.Lock()
			defer raft.mu.Unlock()

			return raft.lastIncludedIndex == 10
		})
		if !ok {
			t.Error("snapshot: expected one through 10 above 100 bytes")
		}
	})

	t.Run("keeps the log while it is 1KB or less", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.stateMachine = &recorder{}
		raft.SetSnapshotThreshold(1024)
		raft.logs = bigLogs(10, 1)
		raft.commitIndex = 10

		go raft.Apply()
		t.Cleanup(raft.Kill)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := raft.waitApplied(ctx, 10); err != nil {
			t.Fatalf("wait: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		raft.mu.Lock()
		defer raft.mu.Unlock()

		if raft.lastIncludedIndex != 0 || len(raft.logs) != 10 {
			t.Errorf("log: expected 10 entries and no snapshot, got %d after %d", len(raft.logs), raft.lastIncludedIndex)
		}
	})
}

func TestSendSnapshot(t *testing.T) {
	t.Run("brings a follower behind the snapshot up to date", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)

		store := kv.NewKV()
		store.Apply(`{"Op":"set","key":"a","value":"1"}`)
		data, err := store.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}

		r := nodes[DEFAULT_LEADER_ID].raft
		r.mu.Lock()
		r.state = Leader
		r.leaderId = r.id
		r.currentTerm = 3
		r.lastIncludedIndex, r.lastIncludedTerm = 4, 3
		r.logs = []*proto.LogEntry{{Term: 3, Cmd: "set e=5"}}
		r.commitIndex, r.lastApplied = 4, 4
		r.snapshot = &proto.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 3, Data: data}
		for _, p := range r.peers {
			r.nextIndex[p.id] = 1
			r.matchIndex[p.id] = 0
		}
		r.mu.Unlock()

		if !r.Append("set f=6") {
			t.Fatal("append: expected true, got false")
		}

		for _, id := range []uint64{2, 3} {
			follower := nodes[id].raft

			ok := waitFor(t, time.Second, func() bool {
				follower.mu.Lock()
				defer follower.mu.Unlock()

				return follower.lastIncludedIndex == 4 && follower.lastIndex() == 6
			})
			if !ok {
				t.Errorf("node %d: expected the snapshot through 4 and entries to 6", id)
				continue
			}

			waitFor(t, time.Second, func() bool {
				_, found := follower.stateMachine.(*kv.KV).Get("a")
				return found
			})

			if value, _ := follower.stateMachine.(*kv.KV).Get("a"); value != "1" {
				t.Errorf("node %d: expected a=1 from the snapshot, got %q", id, value)
			}
		}
	})
}

func TestStepDown(t *testing.T) {
	t.Run("waits a full election timeout after a vote reply with a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		for _, id := range []uint64{2, 3} {
			n := nodes[id].raft
			n.mu.Lock()
			n.currentTerm = 5
			n.mu.Unlock()
		}

		candidate := nodes[DEFAULT_LEADER_ID].raft
		before := time.Now()
		candidate.StartElection()

		ok := waitFor(t, time.Second, func() bool {
			_, term, _ := snapshot(candidate)
			return term == 5
		})
		if !ok {
			t.Fatal("term: expected the candidate to adopt term 5")
		}

		assertFreshDeadline(t, candidate, before)
	})

	t.Run("waits a full election timeout after a snapshot reply with a higher term", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		leader := nodes[DEFAULT_LEADER_ID].raft
		leader.mu.Lock()
		leader.state = Leader
		leader.leaderId = leader.id
		leader.currentTerm = 1
		leader.electionDeadline = time.Now().Add(-time.Second)
		leader.snapshot = &proto.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 1}
		leader.mu.Unlock()

		follower := nodes[2].raft
		follower.mu.Lock()
		follower.currentTerm = 2
		follower.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		before := time.Now()
		if leader.sendSnapshot(ctx, peerOf(leader, 2), 1) {
			t.Fatal("send: expected false after stepping down")
		}

		assertFreshDeadline(t, leader, before)
	})
}

func TestRunSync(t *testing.T) {
	t.Run("covers many writes with one sync", func(t *testing.T) {
		persister := &slowPersister{memoryPersister: &memoryPersister{}, delay: 20 * time.Millisecond}
		raft, err := New(1, map[uint64]string{1: "localhost:0"}, &recorder{}, persister)
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		raft.state = Leader
		raft.leaderId = raft.id

		go raft.Apply()
		go raft.RunSync()
		t.Cleanup(raft.Kill)

		const writes = 50

		var wg sync.WaitGroup
		for i := range writes {
			wg.Go(func() {
				if !raft.Append(fmt.Sprintf("set k=%d", i)) {
					t.Errorf("append %d: expected true, got false", i)
				}
			})
		}
		wg.Wait()

		t.Logf("%d writes took %d syncs", writes, persister.syncs())

		if syncs := persister.syncs(); syncs >= writes/2 {
			t.Errorf("syncs: expected far fewer than %d, got %d", writes, syncs)
		}
	})

	t.Run("does not count the leader before its entries are synced", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.mu.Lock()
		defer raft.mu.Unlock()

		raft.state = Leader
		raft.leaderId = raft.id
		raft.currentTerm = 1
		raft.logs = []*proto.LogEntry{{Term: 1, Cmd: "set a=1"}}
		raft.matchIndex = map[uint64]uint64{2: 1, 3: 0, 4: 0, 5: 1}

		raft.advanceCommit()
		if raft.commitIndex != 0 {
			t.Errorf("commit index before the sync: expected 0, got %d", raft.commitIndex)
		}

		raft.markSynced(1)
		raft.advanceCommit()
		if raft.commitIndex != 1 {
			t.Errorf("commit index after the sync: expected 1, got %d", raft.commitIndex)
		}
	})

	t.Run("lowers the synced index when synced entries are rewritten", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.mu.Lock()
		defer raft.mu.Unlock()

		raft.logs = makeLogs()
		raft.markSynced(5)

		raft.logs = append(raft.logs[:2], &proto.LogEntry{Term: 4, Cmd: "set x=9"})
		raft.persistLog(3)

		if raft.syncedIndex != 2 {
			t.Errorf("synced index: expected 2, got %d", raft.syncedIndex)
		}
	})
}

func TestReplicators(t *testing.T) {
	t.Run("sends each follower one call at a time and batches writes", func(t *testing.T) {
		calls := &callCounter{}
		nodes, _ := clusterWithOptions(t, 3, map[uint64][]grpc.ServerOption{
			2: {calls.interceptor(5 * time.Millisecond)},
		})

		r := nodes[DEFAULT_LEADER_ID].raft
		r.mu.Lock()
		r.state = Leader
		r.leaderId = r.id
		r.initNextIndex()
		r.initMatchIndex()
		r.mu.Unlock()

		const writes = 50

		var wg sync.WaitGroup
		for i := range writes {
			wg.Go(func() {
				if !r.Append(fmt.Sprintf("set k=%d", i)) {
					t.Errorf("append %d: expected true, got false", i)
				}
			})
		}
		wg.Wait()

		total, most := calls.stats()
		t.Logf("%d writes reached node 2 in %d calls", writes, total)

		if most != 1 {
			t.Errorf("calls in flight to node 2: expected at most 1, got %d", most)
		}

		if total >= writes {
			t.Errorf("calls to node 2: expected fewer than %d, got %d", writes, total)
		}
	})

	t.Run("stops the replicators of an earlier term", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.mu.Lock()
		raft.state = Leader
		raft.leaderId = raft.id
		raft.currentTerm = 1
		raft.wakeReplicators()
		old := raft.wake

		raft.currentTerm = 2
		raft.wakeReplicators()
		raft.mu.Unlock()

		for id, wake := range old {
			if !closed(wake) {
				t.Errorf("peer %d: expected the term 1 wake channel closed", id)
			}
		}
	})
}

func TestBecomeLeader(t *testing.T) {
	t.Run("adds an empty entry for its term", func(t *testing.T) {
		nodes, _ := cluster(t, 3, nil)
		r := nodes[DEFAULT_LEADER_ID].raft

		r.StartElection()
		waitForLeader(t, r, time.Second)

		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.logs) != 1 || r.logs[0].Cmd != "" || r.logs[0].Term != r.currentTerm {
			t.Errorf("log: expected one empty entry for term %d, got %v", r.currentTerm, r.logs)
		}
	})
}

func TestStatus(t *testing.T) {
	t.Run("returns copies the caller cannot change the node through", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.currentTerm = 3
		raft.votedFor = uint64Ptr(2)
		raft.logs = makeLogs()

		s := raft.Status()

		if s.Term != 3 {
			t.Errorf("term: expected 3, got %d", s.Term)
		}

		if s.VotedFor == nil || *s.VotedFor != 2 {
			t.Errorf("voted for: expected 2, got %v", s.VotedFor)
		}

		if len(s.Logs) != 5 {
			t.Errorf("log size: expected 5, got %d", len(s.Logs))
		}

		*s.VotedFor = 4
		s.Logs[0] = &proto.LogEntry{Term: 9}

		if *raft.votedFor != 2 {
			t.Errorf("node's vote: expected 2, got %d", *raft.votedFor)
		}

		if raft.logs[0].Term != 1 {
			t.Errorf("node's first entry term: expected 1, got %d", raft.logs[0].Term)
		}
	})

	t.Run("reports no vote before the node votes", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)

		if s := raft.Status(); s.VotedFor != nil {
			t.Errorf("voted for: expected nil, got %d", *s.VotedFor)
		}
	})
}

func TestCaughtUp(t *testing.T) {
	t.Run("is false while committed entries wait to be applied", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.logs = makeLogs()
		raft.commitIndex = 3
		raft.lastApplied = 1

		if raft.CaughtUp() {
			t.Error("caught up: expected false, got true")
		}
	})

	t.Run("is true once every committed entry is applied", func(t *testing.T) {
		raft, _ := newRaft(t, 1, DEFAULT_CLUSTER_SIZE)
		raft.logs = makeLogs()
		raft.commitIndex = 3
		raft.lastApplied = 3

		if !raft.CaughtUp() {
			t.Error("caught up: expected true, got false")
		}
	})
}

func TestMatch(t *testing.T) {
	t.Run("lists every index highest first, the leader's included", func(t *testing.T) {
		got := match(map[uint64]uint64{2: 1, 3: 4, 4: 0, 5: 2}, 5)
		want := []uint64{5, 4, 2, 1, 0}

		if !slices.Equal(got, want) {
			t.Errorf("match: expected %v, got %v", want, got)
		}
	})
}

func TestMajority(t *testing.T) {
	// majority takes the number of peers, which excludes this node

	t.Run("needs three of four nodes", func(t *testing.T) {
		if got := majority(3); got != 3 {
			t.Errorf("majority: expected 3, got %d", got)
		}
	})

	t.Run("needs three of five nodes", func(t *testing.T) {
		if got := majority(4); got != 3 {
			t.Errorf("majority: expected 3, got %d", got)
		}
	})
}

func TestPersist(t *testing.T) {
	t.Run("restores term, vote and log after a restart", func(t *testing.T) {
		persister := NewFilePersister(t.TempDir(), t.TempDir())
		peers := map[uint64]string{1: "localhost:0", 2: "localhost:0"}

		before, err := New(1, peers, &kv.KV{}, persister)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		go before.RunSync()
		t.Cleanup(before.Kill)

		before.RequestVote(context.Background(), &proto.RequestVoteRequest{Term: 3, CandidateId: 2})
		before.AppendEntries(context.Background(),
			&proto.AppendEntriesRequest{Term: 3, LeaderId: 2, Entries: makeLogs()[:2]})

		after, err := New(1, peers, &kv.KV{}, persister)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if after.currentTerm != 3 {
			t.Errorf("term: expected 3, got %d", after.currentTerm)
		}

		if after.votedFor == nil || *after.votedFor != 2 {
			t.Errorf("voted for: expected 2, got %v", after.votedFor)
		}

		if len(after.logs) != 2 {
			t.Errorf("log size: expected 2, got %d", len(after.logs))
		}
	})
}

func TestRestart(t *testing.T) {
	peers := map[uint64]string{1: "localhost:0", 2: "localhost:0"}

	t.Run("loads the snapshot and the log after it", func(t *testing.T) {
		dir := t.TempDir()

		before, err := New(1, peers, &kv.KV{}, NewFilePersister(dir, dir))
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		go before.RunSync()
		t.Cleanup(before.Kill)

		before.InstallSnapshot(context.Background(), snapshotRequest(3, 4, 3, "state"))
		entry := &proto.LogEntry{Term: 3, Cmd: "set x=1"}
		before.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
			Term: 3, LeaderId: 2, PrevLogIndex: 4, PrevLogTerm: 3, Entries: []*proto.LogEntry{entry},
		})

		sm := &recorder{}
		after, err := New(1, peers, sm, NewFilePersister(dir, dir))
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		if after.lastIncludedIndex != 4 || after.lastIncludedTerm != 3 || after.commitIndex != 4 {
			t.Errorf("snapshot: expected index 4 term 3 commit 4, got index %d term %d commit %d",
				after.lastIncludedIndex, after.lastIncludedTerm, after.commitIndex)
		}

		assertEntries(t, after.logs, []*proto.LogEntry{entry})

		if after.snapshot == nil || after.snapshot.LastIncludedIndex != 4 {
			t.Errorf("snapshot in memory: expected one through 4, got %v", after.snapshot)
		}

		go after.Apply()
		t.Cleanup(after.Kill)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := after.waitApplied(ctx, 4); err != nil {
			t.Fatalf("wait: %v", err)
		}

		sm.mu.Lock()
		defer sm.mu.Unlock()

		if string(sm.restored) != "state" {
			t.Errorf("restored: expected %q, got %q", "state", sm.restored)
		}
	})

	t.Run("keeps entries after a matching snapshot saved just before a crash", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)
		p.SaveLog(1, makeLogs())
		p.SaveSnapshot(snapshotBytes(t, &proto.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2}))

		raft, err := New(1, peers, &kv.KV{}, NewFilePersister(dir, dir))
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		assertEntries(t, raft.logs, makeLogs()[3:])

		if raft.lastIndex() != 5 {
			t.Errorf("last index: expected 5, got %d", raft.lastIndex())
		}
	})

	t.Run("drops the log when the snapshot saved just before a crash does not match it", func(t *testing.T) {
		dir := t.TempDir()
		p := NewFilePersister(dir, dir)
		p.SaveLog(1, makeLogs())
		p.SaveSnapshot(snapshotBytes(t, &proto.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 3}))

		raft, err := New(1, peers, &kv.KV{}, NewFilePersister(dir, dir))
		if err != nil {
			t.Fatalf("new: %v", err)
		}

		if len(raft.logs) != 0 || raft.lastIndex() != 3 {
			t.Errorf("log: expected no entries after 3, got %d after %d", len(raft.logs), raft.lastIncludedIndex)
		}
	})

	t.Run("fails when the log starts after an entry and there is no snapshot", func(t *testing.T) {
		dir := t.TempDir()
		NewFilePersister(dir, dir).ResetLog(4, nil)

		if _, err := New(1, peers, &kv.KV{}, NewFilePersister(dir, dir)); err == nil {
			t.Error("new: expected an error, got none")
		}
	})
}

func TestRandomElectionTimeout(t *testing.T) {
	randomTime := randomElectionTimeout()
	if randomTime > electionTimeoutMax || randomTime < electionTimeoutMin {
		t.Errorf("duration: expected within %v-%v, got %v", electionTimeoutMin, electionTimeoutMax, randomTime)
	}
}

func TestRaftState(t *testing.T) {
	var (
		a           = Leader
		b           = Follower
		c           = Candidate
		u RaftState = 99
	)

	if a.String() != "Leader" {
		t.Errorf("expected Leader, got %s", a.String())
	}

	if b.String() != "Follower" {
		t.Errorf("expected Follower, got %s", b.String())
	}

	if c.String() != "Candidate" {
		t.Errorf("expected Candidate, got %s", c.String())
	}

	if u.String() != "Unknown" {
		t.Errorf("expected Unknown, got %s", u.String())
	}
}

// HELPERS

// memoryPersister keeps the state in memory. Cluster nodes can outlive their
// test briefly, and this way their late saves never hit a removed directory.
type memoryPersister struct {
	mu       sync.Mutex
	state    *proto.PersistentState
	snapshot []byte
}

func (p *memoryPersister) SaveSnapshot(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.snapshot = slices.Clone(data)
	return nil
}

func (p *memoryPersister) Sync() error {
	return nil
}

func (p *memoryPersister) LoadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.snapshot, nil
}

func (p *memoryPersister) ResetLog(base uint64, entries []*proto.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == nil {
		p.state = &proto.PersistentState{}
	}

	p.state.LogBase = base
	p.state.Logs = slices.Clone(entries)
	return nil
}

func (p *memoryPersister) SaveState(term uint64, votedFor *uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == nil {
		p.state = &proto.PersistentState{}
	}

	p.state.CurrentTerm, p.state.VotedFor = term, votedFor
	return nil
}

func (p *memoryPersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == nil {
		p.state = &proto.PersistentState{}
	}

	keep := max(0, min(int(from-1)-int(p.state.LogBase), len(p.state.Logs)))
	p.state.Logs = append(p.state.Logs[:keep:keep], entries...)
	return nil
}

func (p *memoryPersister) Load() (*proto.PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == nil {
		return nil, nil
	}

	return protobuf.Clone(p.state).(*proto.PersistentState), nil
}

// recorder is a state machine that remembers the commands applied to it.
type recorder struct {
	mu       sync.Mutex
	cmds     []string
	restored []byte
}

func (s *recorder) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.restored = slices.Clone(data)
	return nil
}

func (s *recorder) Apply(cmd string) any {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cmds = append(s.cmds, cmd)
	return nil
}

func (s *recorder) Snapshot() ([]byte, error) {
	return []byte(""), nil
}

func (s *recorder) applied() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.cmds)
}

func manualLeader(r *Raft, term uint64) *Raft {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = Leader
	r.leaderId = r.id
	r.currentTerm = term
	r.initNextIndex()
	r.initMatchIndex()

	return r
}

func readContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	return ctx
}

// closed reports whether ch is closed, skipping any value still buffered.
func closed(ch chan struct{}) bool {
	for {
		select {
		case _, open := <-ch:
			if !open {
				return true
			}
		default:
			return false
		}
	}
}

// callCounter counts the AppendEntries calls a server handles and the most it
// handles at once.
type callCounter struct {
	mu       sync.Mutex
	inFlight int
	most     int
	total    int
}

func (c *callCounter) interceptor(delay time.Duration) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any,
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != proto.Raft_AppendEntries_FullMethodName {
			return handler(ctx, req)
		}

		c.mu.Lock()
		c.inFlight++
		c.total++
		c.most = max(c.most, c.inFlight)
		c.mu.Unlock()

		time.Sleep(delay)
		res, err := handler(ctx, req)

		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()

		return res, err
	})
}

func (c *callCounter) stats() (total, most int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.total, c.most
}

// slowPersister takes delay to sync and counts the syncs, and holds a sync
// until release is closed when gate is set.
type slowPersister struct {
	*memoryPersister

	delay time.Duration
	gate  chan struct{}

	mu    sync.Mutex
	count int
}

func (p *slowPersister) Sync() error {
	if p.gate != nil {
		<-p.gate
	}

	time.Sleep(p.delay)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.count++
	return nil
}

func (p *slowPersister) syncs() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.count
}

func assertFreshDeadline(t *testing.T, r *Raft, before time.Time) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Leader {
		t.Errorf("state: expected to have stepped down, got %s", r.state)
	}

	if r.electionDeadline.Before(before.Add(electionTimeoutMin)) {
		t.Errorf("election deadline: expected at least %v after stepping down, got %v",
			electionTimeoutMin, r.electionDeadline.Sub(before))
	}
}

func bigLogs(n int, term uint64) []*proto.LogEntry {
	logs := make([]*proto.LogEntry, n)
	for i := range logs {
		logs[i] = &proto.LogEntry{Term: term, Cmd: fmt.Sprintf("set key-%02d=%s", i, strings.Repeat("v", 28))}
	}

	return logs
}

func uint64Ptr(v uint64) *uint64 {
	return new(v)
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

// newRaft builds a single node with n-1 peers whose addresses nothing is
// serving, for tests that call the handlers in process.
func newRaft(t *testing.T, id uint64, n int) (*Raft, map[uint64]string) {
	t.Helper()

	peers := make(map[uint64]string, n)
	for i := 1; i <= n; i++ {
		peers[uint64(i)] = "localhost:0"
	}

	if _, ok := peers[id]; !ok {
		t.Fatalf("id %d is not in the cluster", id)
	}

	raft, err := New(id, peers, &kv.KV{}, &memoryPersister{})
	if err != nil {
		t.Fatalf("raft initialization failed: %v", err)
	}

	go raft.RunSync()
	t.Cleanup(raft.Kill)

	return raft, peers
}

func cluster(t *testing.T, n int, delays map[uint64]time.Duration) (map[uint64]*node, map[uint64]string) {
	t.Helper()

	opts := make(map[uint64][]grpc.ServerOption, len(delays))
	for id, d := range delays {
		opts[id] = []grpc.ServerOption{delay(d)}
	}

	return clusterWithOptions(t, n, opts)
}

func clusterWithOptions(t *testing.T, n int, serverOpts map[uint64][]grpc.ServerOption) (map[uint64]*node, map[uint64]string) {
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
		r, err := New(id, peers, &kv.KV{}, &memoryPersister{})
		if err != nil {
			t.Fatalf("raft initialization failed: %v", err)
		}

		s := start(t, r, lst, serverOpts[id]...)
		nodes[id] = &node{raft: r, stop: s}

		go r.Apply()
		go r.RunSync()
		t.Cleanup(r.Kill)
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
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			fmt.Printf("node %d serve: %v", raft.id, err)
		}
	}()

	return stop
}

func peerOf(r *Raft, id uint64) peer {
	for _, p := range r.peers {
		if p.id == id {
			return p
		}
	}

	panic(fmt.Sprintf("node %d has no peer %d", r.id, id))
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

func makeLogs() []*proto.LogEntry {
	return []*proto.LogEntry{
		{Term: 1, Cmd: "set a=1"},
		{Term: 1, Cmd: "set b=2"},
		{Term: 2, Cmd: "set c=3"},
		{Term: 3, Cmd: "set d=4"},
		{Term: 3, Cmd: "set e=5"},
	}
}

type node struct {
	raft *Raft
	stop func()
}
