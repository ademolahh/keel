package metrics

import (
	"errors"
	"testing"

	"github.com/ademolahh/keel/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPersister(t *testing.T) {
	t.Run("saves and loads through the wrapped persister", func(t *testing.T) {
		c := New()
		p := c.Persister(&stubPersister{})

		if err := p.SaveState(3, nil); err != nil {
			t.Fatalf("save state: unexpected error: %v", err)
		}

		if err := p.SaveLog(1, []*proto.LogEntry{{Term: 3, Cmd: "set a=1"}}); err != nil {
			t.Fatalf("save log: unexpected error: %v", err)
		}

		state, err := p.Load()
		if err != nil {
			t.Fatalf("load: unexpected error: %v", err)
		}

		if state.CurrentTerm != 3 || len(state.Logs) != 1 {
			t.Errorf("loaded: expected term 3 and 1 entry, got term %d and %d entries",
				state.CurrentTerm, len(state.Logs))
		}
	})

	t.Run("times each save", func(t *testing.T) {
		c := New()
		p := c.Persister(&stubPersister{})

		p.SaveState(1, nil)
		p.SaveLog(1, nil)

		if got := sampleCount(t, c.persist.duration); got != 2 {
			t.Errorf("timed saves: expected 2, got %d", got)
		}
	})

	t.Run("returns the wrapped persister's error", func(t *testing.T) {
		c := New()
		failure := errors.New("disk full")
		p := c.Persister(&stubPersister{err: failure})

		if err := p.SaveState(1, nil); !errors.Is(err, failure) {
			t.Errorf("save state error: expected %v, got %v", failure, err)
		}

		if err := p.SaveLog(1, nil); !errors.Is(err, failure) {
			t.Errorf("save log error: expected %v, got %v", failure, err)
		}
	})
}

// HELPERS

type stubPersister struct {
	state proto.PersistentState
	err   error
}

func (p *stubPersister) SaveState(term uint64, votedFor *uint64) error {
	if p.err != nil {
		return p.err
	}

	p.state.CurrentTerm, p.state.VotedFor = term, votedFor
	return nil
}

func (p *stubPersister) SaveLog(from uint64, entries []*proto.LogEntry) error {
	if p.err != nil {
		return p.err
	}

	p.state.Logs = append(p.state.Logs[:from-1], entries...)
	return nil
}

func (p *stubPersister) Load() (*proto.PersistentState, error) {
	return &p.state, nil
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
