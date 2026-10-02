package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
)

func Serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	id, err := strconv.ParseUint(os.Getenv("ID"), 10, 64)
	if err != nil {
		return err
	}

	httpPeers, err := parsePeers(os.Getenv("HTTP_PEERS"))
	if err != nil {
		return err
	}

	kv := kv.NewKV()
	raft, err := newRaft(id, kv)
	if err != nil {
		return err
	}

	grpcPort := os.Getenv("PORT")
	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterRaftServer(grpcServer, raft)

	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil {
			slog.Error("grpc server stopped", "err", err)
		}
	}()
	go raft.RunElectionTimer()
	go raft.Apply()

	stopHeartbeat := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				raft.HeartBeat()
			}
		}
	}()

	httpPort := os.Getenv("HTTP_PORT")

	mux := http.NewServeMux()
	raftHandler := New(raft, kv, httpPeers)

	mux.HandleFunc("/set", raftHandler.LeaderOnly(raftHandler.Set))
	mux.HandleFunc("/delete", raftHandler.LeaderOnly(raftHandler.Delete))
	mux.HandleFunc("/get", raftHandler.Get)
	mux.HandleFunc("/leader", raftHandler.Leader)
	mux.HandleFunc("/state", raftHandler.State)

	httpServer := &http.Server{
		Addr:    httpPort,
		Handler: mux,
	}

	httpErr := make(chan error, 1)
	go func() {
		httpErr <- httpServer.ListenAndServe()
	}()

	slog.Info("serving", "node", id, "grpc_port", grpcPort, "http_addr", httpPort)

	select {
	case err = <-httpErr:
	case <-ctx.Done():
		stop()
		slog.Info("shutting down", "node", id)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "err", err)
	}

	close(stopHeartbeat)
	raft.Kill()
	grpcServer.GracefulStop()

	slog.Info("stopped", "node", id)

	return err
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
