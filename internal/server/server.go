package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	grpcServer, err := startGRPC(grpcPort, raft)
	if err != nil {
		return err
	}

	go raft.RunElectionTimer()
	go raft.Apply()
	go raft.RunHeartbeat()

	httpPort := os.Getenv("HTTP_PORT")
	httpServer, httpErr := startHTTP(httpPort, New(raft, kv, httpPeers))

	slog.Info("serving", "node", id, "grpc_port", grpcPort, "http_addr", httpPort)

	select {
	case err = <-httpErr:
	case <-ctx.Done():
		stop()
		slog.Info("shutting down", "node", id)
	}

	shutdown(httpServer, raft, grpcServer)

	slog.Info("stopped", "node", id)

	return err
}

func startGRPC(port string, r *raft.Raft) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", port, err)
	}

	server := grpc.NewServer()
	proto.RegisterRaftServer(server, r)

	go func() {
		if err := server.Serve(listener); err != nil {
			slog.Error("grpc server stopped", "err", err)
		}
	}()

	return server, nil
}

func startHTTP(addr string, h *RaftHandler) (*http.Server, <-chan error) {
	server := &http.Server{
		Addr:    addr,
		Handler: routes(h),
	}

	errs := make(chan error, 1)
	go func() {
		errs <- server.ListenAndServe()
	}()

	return server, errs
}

func shutdown(httpServer *http.Server, r *raft.Raft, grpcServer *grpc.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("http shutdown", "err", err)
	}

	r.Kill()
	grpcServer.GracefulStop()
}
