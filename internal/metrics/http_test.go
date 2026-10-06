package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInstrument(t *testing.T) {
	t.Run("counts a request by route, method and status code", func(t *testing.T) {
		c := New()
		h := c.Instrument("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))

		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/get", nil))

		if got := testutil.ToFloat64(c.http.requests.WithLabelValues("/get", "get", "404")); got != 1 {
			t.Errorf("requests: expected 1, got %v", got)
		}
	})

	t.Run("times the request", func(t *testing.T) {
		c := New()
		h := c.Instrument("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/get", nil))

		if got := sampleCount(t, c.http.duration.WithLabelValues("/get", "get", "200")); got != 1 {
			t.Errorf("timed requests: expected 1, got %d", got)
		}
	})

	t.Run("holds a request in flight only while it runs", func(t *testing.T) {
		c := New()
		gauge := c.http.inFlight.WithLabelValues("/get")

		var during float64
		h := c.Instrument("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			during = testutil.ToFloat64(gauge)
		}))

		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/get", nil))

		if during != 1 {
			t.Errorf("in flight during: expected 1, got %v", during)
		}

		if after := testutil.ToFloat64(gauge); after != 0 {
			t.Errorf("in flight after: expected 0, got %v", after)
		}
	})
}
