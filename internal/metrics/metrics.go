package metrics

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RaftCollector struct {
	raft     *raft.Raft
	registry *prometheus.Registry

	requests *prometheus.CounterVec
	inFlight *prometheus.GaugeVec
	duration *prometheus.HistogramVec

	term        *prometheus.Desc
	state       *prometheus.Desc
	commitIndex *prometheus.Desc
	matchIndex  *prometheus.Desc
}

func New(r *raft.Raft) *RaftCollector {
	c := &RaftCollector{
		raft:     r,
		registry: prometheus.NewRegistry(),

		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "keel_http_requests_total",
			Help: "HTTP requests handled, by route, method and status code.",
		}, []string{"route", "method", "code"}),

		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "keel_http_requests_in_flight",
			Help: "HTTP requests being handled, by route.",
		}, []string{"route"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "keel_http_request_duration_seconds",
			Help:    "Time to handle an HTTP request, by route, method and status code.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "code"}),

		term: prometheus.NewDesc("raft_term",
			"Current term of this node.", nil, nil),
		state: prometheus.NewDesc("raft_state",
			"1 for the role this node holds, 0 for the others.", []string{"state"}, nil),
		commitIndex: prometheus.NewDesc("raft_commit_index",
			"Highest log index known to be committed.", nil, nil),
		matchIndex: prometheus.NewDesc("raft_peer_match_index",
			"Highest log index known to be replicated on each peer, reported by the leader only.", []string{"peer"}, nil),
	}

	c.registry.MustRegister(c, collectors.NewGoCollector())

	return c
}

func (c *RaftCollector) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

func (c *RaftCollector) Instrument(route string, next http.Handler) http.Handler {
	labels := prometheus.Labels{"route": route}

	return promhttp.InstrumentHandlerInFlight(c.inFlight.With(labels),
		promhttp.InstrumentHandlerDuration(c.duration.MustCurryWith(labels),
			promhttp.InstrumentHandlerCounter(c.requests.MustCurryWith(labels), next)))
}

func (c *RaftCollector) Describe(ch chan<- *prometheus.Desc) {
	c.requests.Describe(ch)
	c.inFlight.Describe(ch)
	c.duration.Describe(ch)

	ch <- c.term
	ch <- c.state
	ch <- c.commitIndex
	ch <- c.matchIndex
}

func (c *RaftCollector) Collect(ch chan<- prometheus.Metric) {
	c.requests.Collect(ch)
	c.inFlight.Collect(ch)
	c.duration.Collect(ch)

	s := c.raft.Stats()

	ch <- prometheus.MustNewConstMetric(c.term, prometheus.GaugeValue, float64(s.Term))

	for _, state := range []raft.RaftState{raft.Leader, raft.Follower, raft.Candidate} {
		value := 0.0
		if s.State == state {
			value = 1
		}

		ch <- prometheus.MustNewConstMetric(c.state, prometheus.GaugeValue, value,
			strings.ToLower(state.String()))
	}

	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(s.CommitIndex))

	for peer, index := range s.MatchIndex {
		ch <- prometheus.MustNewConstMetric(c.matchIndex, prometheus.GaugeValue, float64(index),
			strconv.FormatUint(peer, 10))
	}
}
