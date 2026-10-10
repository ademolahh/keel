package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry
	factory  promauto.Factory

	http    httpMetrics
	rpc     rpcMetrics
	persist persistMetrics
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())

	factory := promauto.With(registry)

	return &Metrics{
		registry: registry,
		factory:  factory,
		http:     newHTTPMetrics(factory),
		rpc:      newRPCMetrics(factory),
		persist:  newPersistMetrics(factory),
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
