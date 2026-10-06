package metrics

import (
	"time"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
)

type persistMetrics struct {
	duration prometheus.Histogram
}

func newPersistMetrics() persistMetrics {
	return persistMetrics{
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_persist_duration_seconds",
			Help:    "Time to save the term, vote and log to disk, fsync included.",
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

func (p *timedPersister) Save(data []byte) error {
	start := time.Now()
	err := p.Persister.Save(data)
	p.duration.Observe(time.Since(start).Seconds())

	return err
}

func (m persistMetrics) describe(ch chan<- *prometheus.Desc) {
	m.duration.Describe(ch)
}

func (m persistMetrics) collect(ch chan<- prometheus.Metric) {
	m.duration.Collect(ch)
}
