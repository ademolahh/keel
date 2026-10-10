package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/metrics"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
	"google.golang.org/grpc"
)

func Serve(cfg Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	id := cfg.ID

	kv := kv.NewKV()
	m := metrics.New()
	raft, err := newRaft(cfg, kv, m)
	if err != nil {
		return err
	}
	m.SetRaft(raft)

	grpcAddr := ":" + port(cfg.Self().RaftAddr)
	grpcServer, err := startGRPC(grpcAddr, raft)
	if err != nil {
		return err
	}

	raft.Start()

	httpAddr := ":" + port(cfg.Self().HTTPAddr)
	httpPeers := cfg.addrs(func(p Peer) string { return p.HTTPAddr })
	httpServer, httpErr := startHTTP(httpAddr, New(raft, kv, httpPeers), m)

	slog.Info("serving", "node", id, "grpc_addr", grpcAddr, "http_addr", httpAddr)

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

func port(addr string) string {
	_, p, _ := net.SplitHostPort(addr)
	return p
}

func startGRPC(addr string, r *raft.Raft) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
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

func startHTTP(addr string, h *RaftHandler, m *metrics.RaftCollector) (*http.Server, <-chan error) {
	server := &http.Server{
		Addr:    addr,
		Handler: routes(h, m),
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

	r.Stop()
	grpcServer.GracefulStop()
}
