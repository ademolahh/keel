package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ademolahh/keel/internal/server"
)

func main() {
	cfg, err := server.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}

	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		path := "/healthz"
		if len(os.Args) > 2 {
			path = os.Args[2]
		}

		os.Exit(healthcheck(cfg, path))
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))

	if err := server.Serve(cfg); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
