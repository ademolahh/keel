package metrics

import (
	"strconv"
	"strings"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) SetRaft(r *raft.Raft) {
	gauge := func(name, help string, value func(raft.Stats) float64) {
		m.factory.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return value(r.Stats()) })
	}

	gauge("raft_term", "Current term of this node.",
		func(s raft.Stats) float64 { return float64(s.Term) })
	gauge("raft_commit_index", "Highest log index known to be committed.",
		func(s raft.Stats) float64 { return float64(s.CommitIndex) })
	gauge("raft_last_applied", "Highest log index applied to the store.",
		func(s raft.Stats) float64 { return float64(s.LastApplied) })
	gauge("raft_log_entries", "Number of entries in the log.",
		func(s raft.Stats) float64 { return float64(s.LogEntries) })

	for _, state := range []raft.RaftState{raft.Leader, raft.Follower, raft.Candidate} {
		m.factory.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "raft_state",
			Help:        "1 for the role this node holds, 0 for the others.",
			ConstLabels: prometheus.Labels{"state": strings.ToLower(state.String())},
		}, func() float64 {
			if r.Stats().State == state {
				return 1
			}
			return 0
		})
	}

	m.factory.NewCounterFunc(prometheus.CounterOpts{
		Name: "raft_leader_changes_total",
		Help: "Times this node learned of a new leader.",
	}, func() float64 { return float64(r.Stats().LeaderChanges) })

	m.registry.MustRegister(matchIndex{
		node: r,
		desc: prometheus.NewDesc("raft_peer_match_index",
			"Highest log index known to be replicated on each peer, reported by the leader only.", []string{"peer"}, nil),
	})
}

// matchIndex reports one series per peer, and only while this node leads,
// which a GaugeFunc cannot express.
type matchIndex struct {
	node *raft.Raft
	desc *prometheus.Desc
}

func (m matchIndex) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.desc
}

func (m matchIndex) Collect(ch chan<- prometheus.Metric) {
	for peer, index := range m.node.Stats().MatchIndex {
		ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, float64(index),
			strconv.FormatUint(peer, 10))
	}
}
