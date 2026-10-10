package metrics

import (
	"time"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type persistMetrics struct {
	duration prometheus.Histogram
}

func newPersistMetrics(f promauto.Factory) persistMetrics {
	return persistMetrics{
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_persist_duration_seconds",
			Help:    "Time to save the term and vote, or log entries, to disk, fsync included.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
	}
}

func (m *Metrics) Persister(p raft.Persister) raft.Persister {
	return &timedPersister{Persister: p, duration: m.persist.duration}
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

func (p *timedPersister) Sync() error {
	start := time.Now()
	err := p.Persister.Sync()
	p.duration.Observe(time.Since(start).Seconds())

	return err
}

func (p *timedPersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	start := time.Now()
	err := p.Persister.SaveLog(from, entries)
	p.duration.Observe(time.Since(start).Seconds())

	return err
}
