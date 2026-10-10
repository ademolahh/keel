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
	snapshotDir := filepath.Join(dir, "snapshot")

	for _, d := range []string{persistDir, snapshotDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}

	files, err := raft.OpenFilePersister(persistDir, snapshotDir)
	if err != nil {
		return nil, err
	}

	persister := m.Persister(files)

	r, err := raft.New(id, peers, sm, persister, grpc.WithChainUnaryInterceptor(m.Interceptor()))
	if err != nil {
		return nil, err
	}

	if v := os.Getenv("SNAPSHOT_THRESHOLD"); v != "" {
		bytes, err := strconv.Atoi(v)
		if err != nil || bytes <= 0 {
			return nil, fmt.Errorf("SNAPSHOT_THRESHOLD: want a positive number of bytes, got %q", v)
		}

		r.SetSnapshotThreshold(bytes)
	}

	return r, nil
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
