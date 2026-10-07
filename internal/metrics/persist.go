package metrics

import (
	"time"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"github.com/prometheus/client_golang/prometheus"
)

type persistMetrics struct {
	duration prometheus.Histogram
}

func newPersistMetrics() persistMetrics {
	return persistMetrics{
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_persist_duration_seconds",
			Help:    "Time to save the term and vote, or log entries, to disk, fsync included.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
	}
}

func (c *RaftCollector) Persister(p raft.Persister) raft.Persister {
	return &timedPersister{Persister: p, duration: c.persist.duration}
}

type timedPersister struct {
	raft.Persister
	duration prometheus.Histogram
}

func (p *timedPersister) SaveState(term uint64, votedFor *uint64) error {
	start := time.Now()
	err := p.Persister.SaveState(term, votedFor)
	p.duration.Observe(time.Since(start).Seconds())

	return err
}

func (p *timedPersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	start := time.Now()
	err := p.Persister.SaveLog(from, entries)
	p.duration.Observe(time.Since(start).Seconds())

	return err
}

func (m persistMetrics) describe(ch chan<- *prometheus.Desc) {
	m.duration.Describe(ch)
}

func (m persistMetrics) collect(ch chan<- prometheus.Metric) {
	m.duration.Collect(ch)
}
