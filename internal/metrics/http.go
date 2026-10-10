package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type httpMetrics struct {
	requests *prometheus.CounterVec
	inFlight *prometheus.GaugeVec
	duration *prometheus.HistogramVec
}

func newHTTPMetrics(f promauto.Factory) httpMetrics {
	return httpMetrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "keel_http_requests_total",
			Help: "HTTP requests handled, by route, method and status code.",
		}, []string{"route", "method", "code"}),

		inFlight: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "keel_http_requests_in_flight",
			Help: "HTTP requests being handled, by route.",
		}, []string{"route"}),

		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "keel_http_request_duration_seconds",
			Help:    "Time to handle an HTTP request, by route, method and status code.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "code"}),
	}
}

func (m *Metrics) Instrument(route string, next http.Handler) http.Handler {
	labels := prometheus.Labels{"route": route}

	return promhttp.InstrumentHandlerInFlight(m.http.inFlight.With(labels),
		promhttp.InstrumentHandlerDuration(m.http.duration.MustCurryWith(labels),
			promhttp.InstrumentHandlerCounter(m.http.requests.MustCurryWith(labels), next)))
}
