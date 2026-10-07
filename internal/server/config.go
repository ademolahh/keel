package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ademolahh/keel/internal/metrics"
	"github.com/ademolahh/keel/internal/raft"
	"google.golang.org/grpc"
)

func newRaft(id uint64, sm raft.StateMachine, m *metrics.RaftCollector) (*raft.Raft, error) {
	peers, err := parsePeers(os.Getenv("PEERS"))
	if err != nil {
		return nil, err
	}

	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("id %d is not in PEERS", id)
	}

	dir := os.Getenv("DATA_DIR")
	if dir == "" {
		dir = "."
	}

	persistDir := filepath.Join(dir, "persist")
	if err := os.MkdirAll(persistDir, 0o700); err != nil {
		return nil, err
	}

	persister := m.Persister(raft.NewFilePersister(persistDir))

	return raft.New(id, peers, sm, persister, grpc.WithChainUnaryInterceptor(m.Interceptor()))
}

func parsePeers(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)

	for pair := range strings.SplitSeq(s, ",") {
		idStr, addr, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q: expected id=address", pair)
		}

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", pair, err)
		}

		peers[id] = addr
	}

	return peers, nil
}
