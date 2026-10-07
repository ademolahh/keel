package metrics

import (
	"strings"
	"testing"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRaftMetrics(t *testing.T) {
	t.Run("reports nothing before a node is set", func(t *testing.T) {
		c := New()

		if n := testutil.CollectAndCount(c, "raft_term", "raft_state"); n != 0 {
			t.Errorf("raft metrics: expected none, got %d", n)
		}
	})

	t.Run("reports the node's state", func(t *testing.T) {
		c := New()
		c.SetRaft(newNode(t, 3, 2))

		// a follower reports no match index
		expected := `
# HELP raft_term Current term of this node.
# TYPE raft_term gauge
raft_term 3
# HELP raft_state 1 for the role this node holds, 0 for the others.
# TYPE raft_state gauge
raft_state{state="candidate"} 0
raft_state{state="follower"} 1
raft_state{state="leader"} 0
# HELP raft_commit_index Highest log index known to be committed.
# TYPE raft_commit_index gauge
raft_commit_index 0
# HELP raft_last_applied Highest log index applied to the store.
# TYPE raft_last_applied gauge
raft_last_applied 0
# HELP raft_log_entries Number of entries in the log.
# TYPE raft_log_entries gauge
raft_log_entries 2
# HELP raft_leader_changes_total Times this node learned of a new leader.
# TYPE raft_leader_changes_total counter
raft_leader_changes_total 0
`

		err := testutil.CollectAndCompare(c, strings.NewReader(expected),
			"raft_term", "raft_state", "raft_commit_index", "raft_last_applied",
			"raft_log_entries", "raft_leader_changes_total", "raft_peer_match_index")
		if err != nil {
			t.Error(err)
		}
	})
}

func TestHandler(t *testing.T) {
	t.Run("serves the go runtime metrics with the rest", func(t *testing.T) {
		c := New()
		c.SetRaft(newNode(t, 1, 0))

		families, err := c.registry.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}

		names := make(map[string]bool)
		for _, f := range families {
			names[f.GetName()] = true
		}

		for _, want := range []string{"go_goroutines", "raft_term"} {
			if !names[want] {
				t.Errorf("metric %s: expected, got none", want)
			}
		}
	})
}

// HELPERS

// newNode builds a lone node that starts at term, holding entries log entries.
func newNode(t *testing.T, term uint64, entries int) *raft.Raft {
	t.Helper()

	logs := make([]*proto.LogEntry, entries)
	for i := range logs {
		logs[i] = &proto.LogEntry{Term: term, Cmd: "set a=1"}
	}

	persister := raft.NewFilePersister(t.TempDir())
	if err := persister.SaveState(term, nil); err != nil {
		t.Fatalf("save state: %v", err)
	}

	if err := persister.SaveLog(1, logs); err != nil {
		t.Fatalf("save log: %v", err)
	}

	r, err := raft.New(1, map[uint64]string{1: "localhost:0"}, kv.NewKV(), persister)
	if err != nil {
		t.Fatalf("raft: %v", err)
	}

	return r
}
