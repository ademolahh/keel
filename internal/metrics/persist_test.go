package metrics

import (
	"errors"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPersister(t *testing.T) {
	t.Run("saves and loads through the wrapped persister", func(t *testing.T) {
		c := New()
		p := c.Persister(&stubPersister{})

		if err := p.Save([]byte("state")); err != nil {
			t.Fatalf("save: unexpected error: %v", err)
		}

		data, err := p.Load()
		if err != nil {
			t.Fatalf("load: unexpected error: %v", err)
		}

		if !slices.Equal(data, []byte("state")) {
			t.Errorf("loaded: expected %q, got %q", "state", data)
		}
	})

	t.Run("times each save", func(t *testing.T) {
		c := New()
		p := c.Persister(&stubPersister{})

		p.Save([]byte("a"))
		p.Save([]byte("b"))

		if got := sampleCount(t, c.persist.duration); got != 2 {
			t.Errorf("timed saves: expected 2, got %d", got)
		}
	})

	t.Run("returns the wrapped persister's error", func(t *testing.T) {
		c := New()
		failure := errors.New("disk full")
		p := c.Persister(&stubPersister{err: failure})

		if err := p.Save([]byte("state")); !errors.Is(err, failure) {
			t.Errorf("error: expected %v, got %v", failure, err)
		}
	})
}

// HELPERS

type stubPersister struct {
	data []byte
	err  error
}

func (p *stubPersister) Save(data []byte) error {
	if p.err != nil {
		return p.err
	}

	p.data = data
	return nil
}

func (p *stubPersister) Load() ([]byte, error) {
	return p.data, nil
}

// sampleCount is how many observations o has recorded.
func sampleCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()

	var m dto.Metric
	if err := o.(prometheus.Metric).Write(&m); err != nil {
		t.Fatalf("read metric: %v", err)
	}

	return m.GetHistogram().GetSampleCount()
}
