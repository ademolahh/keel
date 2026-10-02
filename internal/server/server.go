package server

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
)

func Serve() error {
	id, err := strconv.ParseUint(os.Getenv("ID"), 10, 64)
	if err != nil {
		return err
	}

	kv := kv.NewKV()
	raft, err := newRaft(id, kv)
	if err != nil {
		return err
	}

	port := os.Getenv("PORT")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", port, err)
	}

	server := grpc.NewServer()
	proto.RegisterRaftServer(server, raft)

	go func() {
		if err := server.Serve(listener); err != nil {
			slog.Error("grpc server stopped", "err", err)
		}
	}()
	go raft.RunElectionTimer()
	go raft.Apply()

	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			raft.HeartBeat()
		}
	}()

	httpPort := os.Getenv("HTTP_PORT")

	mux := http.NewServeMux()
	httpPeers, err := parsePeers(os.Getenv("HTTP_PEERS"))
	if err != nil {
		return err
	}

	raftHandler := New(raft, kv, httpPeers)

	mux.HandleFunc("/set", raftHandler.LeaderOnly(raftHandler.Set))
	mux.HandleFunc("/delete", raftHandler.LeaderOnly(raftHandler.Delete))
	mux.HandleFunc("/get", raftHandler.Get)
	mux.HandleFunc("/leader", raftHandler.Leader)
	mux.HandleFunc("/state", raftHandler.State)

	s := http.Server{
		Addr:    httpPort,
		Handler: mux,
	}

	slog.Info("serving", "node", id, "grpc_port", port, "http_addr", httpPort)

	return s.ListenAndServe()
}

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
