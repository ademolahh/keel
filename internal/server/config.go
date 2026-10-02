package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ademolahh/keel/internal/raft"
)

func newRaft(id uint64, sm raft.StateMachine) (*raft.Raft, error) {
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

	persister := raft.NewFilePersister(filepath.Join(dir, fmt.Sprintf("raft-%d.state", id)))

	return raft.New(id, peers, sm, persister)
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
