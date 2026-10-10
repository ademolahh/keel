package server

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ademolahh/keel/internal/metrics"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	RaftAddr string
	HTTPAddr string
}

type Config struct {
	ID                uint64
	Peers             map[uint64]Peer
	DataDir           string
	LogLevel          slog.Level
	SnapshotThreshold int
}

func LoadConfig() (Config, error) {
	cfg := Config{
		DataDir:           os.Getenv("DATA_DIR"),
		LogLevel:          slog.LevelInfo,
		SnapshotThreshold: raft.DefaultSnapshotThreshold,
	}

	id, err := strconv.ParseUint(os.Getenv("ID"), 10, 64)
	if err != nil {
		return Config{}, fmt.Errorf("ID: %w", err)
	}
	cfg.ID = id

	if cfg.Peers, err = parsePeers(os.Getenv("PEERS")); err != nil {
		return Config{}, fmt.Errorf("PEERS: %w", err)
	}

	if _, ok := cfg.Peers[id]; !ok {
		return Config{}, fmt.Errorf("PEERS: no entry for ID %d", id)
	}

	if cfg.DataDir == "" {
		cfg.DataDir = "."
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("LOG_LEVEL: %w", err)
		}
	}

	if v := os.Getenv("SNAPSHOT_THRESHOLD"); v != "" {
		bytes, err := strconv.Atoi(v)
		if err != nil || bytes <= 0 {
			return Config{}, fmt.Errorf("SNAPSHOT_THRESHOLD: want a positive number of bytes, got %q", v)
		}
		cfg.SnapshotThreshold = bytes
	}

	return cfg, nil
}

func (c Config) Self() Peer {
	return c.Peers[c.ID]
}

func (c Config) addrs(pick func(Peer) string) map[uint64]string {
	addrs := make(map[uint64]string, len(c.Peers))
	for id, p := range c.Peers {
		addrs[id] = pick(p)
	}

	return addrs
}

func parsePeers(s string) (map[uint64]Peer, error) {
	peers := make(map[uint64]Peer)

	for entry := range strings.SplitSeq(s, ",") {
		idStr, addrs, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("%q: want id=host:raft-port:http-port", entry)
		}

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", entry, err)
		}

		cut := strings.LastIndex(addrs, ":")
		host, raftPort, ok := strings.Cut(addrs[:max(cut, 0)], ":")
		if cut < 0 || !ok || !isPort(raftPort) || !isPort(addrs[cut+1:]) {
			return nil, fmt.Errorf("%q: want id=host:raft-port:http-port", entry)
		}

		peers[id] = Peer{
			RaftAddr: net.JoinHostPort(host, raftPort),
			HTTPAddr: net.JoinHostPort(host, addrs[cut+1:]),
		}
	}

	return peers, nil
}

func isPort(s string) bool {
	n, err := strconv.ParseUint(s, 10, 16)
	return err == nil && n > 0
}

func newRaft(cfg Config, sm raft.StateMachine, m *metrics.RaftCollector) (*raft.Raft, error) {
	persistDir := filepath.Join(cfg.DataDir, "persist")
	snapshotDir := filepath.Join(cfg.DataDir, "snapshot")

	for _, d := range []string{persistDir, snapshotDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}

	files, err := raft.OpenFilePersister(persistDir, snapshotDir)
	if err != nil {
		return nil, err
	}

	raftAddrs := cfg.addrs(func(p Peer) string { return p.RaftAddr })
	clients, err := dialPeers(cfg.ID, raftAddrs, grpc.WithChainUnaryInterceptor(m.Interceptor()))
	if err != nil {
		return nil, err
	}

	r, err := raft.New(cfg.ID, clients, sm, m.Persister(files))
	if err != nil {
		return nil, err
	}

	r.SetSnapshotThreshold(cfg.SnapshotThreshold)

	return r, nil
}

func dialPeers(id uint64, addrs map[uint64]string, opts ...grpc.DialOption) (map[uint64]proto.RaftClient, error) {
	clients := make(map[uint64]proto.RaftClient, len(addrs))

	for pid, addr := range addrs {
		if pid == id {
			continue
		}

		conn, err := grpc.NewClient("passthrough:///"+addr, append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  50 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   100 * time.Millisecond,
				},
			}),
		}, opts...)...)
		if err != nil {
			return nil, fmt.Errorf("peer %d at %s: %w", pid, addr, err)
		}

		clients[pid] = proto.NewRaftClient(conn)
	}

	return clients, nil
}
