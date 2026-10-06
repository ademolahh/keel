package metrics

import (
	"strconv"
	"strings"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	node *raft.Raft

	term          *prometheus.Desc
	state         *prometheus.Desc
	commitIndex   *prometheus.Desc
	lastApplied   *prometheus.Desc
	logEntries    *prometheus.Desc
	leaderChanges *prometheus.Desc
	matchIndex    *prometheus.Desc
}

func newRaftMetrics() raftMetrics {
	return raftMetrics{
		term: prometheus.NewDesc("raft_term",
			"Current term of this node.", nil, nil),
		state: prometheus.NewDesc("raft_state",
			"1 for the role this node holds, 0 for the others.", []string{"state"}, nil),
		commitIndex: prometheus.NewDesc("raft_commit_index",
			"Highest log index known to be committed.", nil, nil),
		lastApplied: prometheus.NewDesc("raft_last_applied",
			"Highest log index applied to the store.", nil, nil),
		logEntries: prometheus.NewDesc("raft_log_entries",
			"Number of entries in the log.", nil, nil),
		leaderChanges: prometheus.NewDesc("raft_leader_changes_total",
			"Times this node learned of a new leader.", nil, nil),
		matchIndex: prometheus.NewDesc("raft_peer_match_index",
			"Highest log index known to be replicated on each peer, reported by the leader only.", []string{"peer"}, nil),
	}
}

func (m raftMetrics) describe(ch chan<- *prometheus.Desc) {
	ch <- m.term
	ch <- m.state
	ch <- m.commitIndex
	ch <- m.lastApplied
	ch <- m.logEntries
	ch <- m.leaderChanges
	ch <- m.matchIndex
}

func (m raftMetrics) collect(ch chan<- prometheus.Metric) {
	if m.node == nil {
		return
	}

	s := m.node.Stats()

	ch <- prometheus.MustNewConstMetric(m.term, prometheus.GaugeValue, float64(s.Term))

	for _, state := range []raft.RaftState{raft.Leader, raft.Follower, raft.Candidate} {
		value := 0.0
		if s.State == state {
			value = 1
		}

		ch <- prometheus.MustNewConstMetric(m.state, prometheus.GaugeValue, value,
			strings.ToLower(state.String()))
	}

	ch <- prometheus.MustNewConstMetric(m.commitIndex, prometheus.GaugeValue, float64(s.CommitIndex))
	ch <- prometheus.MustNewConstMetric(m.lastApplied, prometheus.GaugeValue, float64(s.LastApplied))
	ch <- prometheus.MustNewConstMetric(m.logEntries, prometheus.GaugeValue, float64(s.LogEntries))
	ch <- prometheus.MustNewConstMetric(m.leaderChanges, prometheus.CounterValue, float64(s.LeaderChanges))

	for peer, index := range s.MatchIndex {
		ch <- prometheus.MustNewConstMetric(m.matchIndex, prometheus.GaugeValue, float64(index),
			strconv.FormatUint(peer, 10))
	}
}
