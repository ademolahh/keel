package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type httpMetrics struct {
	requests *prometheus.CounterVec
	inFlight *prometheus.GaugeVec
	duration *prometheus.HistogramVec
}

func newHTTPMetrics() httpMetrics {
	return httpMetrics{
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
	}
}

func (c *RaftCollector) Instrument(route string, next http.Handler) http.Handler {
	labels := prometheus.Labels{"route": route}

	return promhttp.InstrumentHandlerInFlight(c.http.inFlight.With(labels),
		promhttp.InstrumentHandlerDuration(c.http.duration.MustCurryWith(labels),
			promhttp.InstrumentHandlerCounter(c.http.requests.MustCurryWith(labels), next)))
}

func (m httpMetrics) describe(ch chan<- *prometheus.Desc) {
	m.requests.Describe(ch)
	m.inFlight.Describe(ch)
	m.duration.Describe(ch)
}

func (m httpMetrics) collect(ch chan<- prometheus.Metric) {
	m.requests.Collect(ch)
	m.inFlight.Collect(ch)
	m.duration.Collect(ch)
}
