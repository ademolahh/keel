package metrics

import (
	"net/http"

	"github.com/ademolahh/keel/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RaftCollector struct {
	registry *prometheus.Registry

	http    httpMetrics
	rpc     rpcMetrics
	persist persistMetrics
	raft    raftMetrics
}

func New() *RaftCollector {
	c := &RaftCollector{
		registry: prometheus.NewRegistry(),
		http:     newHTTPMetrics(),
		rpc:      newRPCMetrics(),
		persist:  newPersistMetrics(),
		raft:     newRaftMetrics(),
	}

	c.registry.MustRegister(c, collectors.NewGoCollector())

	return c
}

func (c *RaftCollector) SetRaft(r *raft.Raft) {
	c.raft.node = r
}

func (c *RaftCollector) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

func (c *RaftCollector) Describe(ch chan<- *prometheus.Desc) {
	c.http.describe(ch)
	c.rpc.describe(ch)
	c.persist.describe(ch)
	c.raft.describe(ch)
}

func (c *RaftCollector) Collect(ch chan<- prometheus.Metric) {
	c.http.collect(ch)
	c.rpc.collect(ch)
	c.persist.collect(ch)
	c.raft.collect(ch)
}
