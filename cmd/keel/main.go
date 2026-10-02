package main

import (
	"log/slog"
	"os"

	"github.com/ademolahh/keel/internal/server"
)

func main() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	if err := server.Serve(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
