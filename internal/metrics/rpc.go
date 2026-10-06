package metrics

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type rpcMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newRPCMetrics() rpcMetrics {
	return rpcMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raft_grpc_requests_total",
			Help: "RPCs sent to peers, by peer, method and status code.",
		}, []string{"peer", "method", "code"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raft_grpc_request_duration_seconds",
			Help:    "Time for a peer to answer an RPC, by peer and method.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"peer", "method"}),
	}
}

func (c *RaftCollector) Interceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)

		peer := strings.TrimPrefix(cc.Target(), "passthrough:///")
		name := path.Base(method)

		c.rpc.duration.WithLabelValues(peer, name).Observe(time.Since(start).Seconds())
		c.rpc.requests.WithLabelValues(peer, name, status.Code(err).String()).Inc()

		return err
	}
}

func (m rpcMetrics) describe(ch chan<- *prometheus.Desc) {
	m.requests.Describe(ch)
	m.duration.Describe(ch)
}

func (m rpcMetrics) collect(ch chan<- prometheus.Metric) {
	m.requests.Collect(ch)
	m.duration.Collect(ch)
}
