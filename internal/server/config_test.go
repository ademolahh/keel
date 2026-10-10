package server

import (
	"log/slog"
	"testing"

	"github.com/ademolahh/keel/internal/raft"
)

func TestLoadConfig(t *testing.T) {
	t.Run("reads every setting", func(t *testing.T) {
		t.Setenv("ID", "2")
		t.Setenv("PEERS", "1=raft-1:7000:8080,2=raft-2:7001:8081")
		t.Setenv("DATA_DIR", "/data")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("SNAPSHOT_THRESHOLD", "1024")

		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("load: %v", err)
		}

		if cfg.ID != 2 || cfg.DataDir != "/data" || cfg.LogLevel != slog.LevelDebug || cfg.SnapshotThreshold != 1024 {
			t.Errorf("config: got %+v", cfg)
		}

		if self := cfg.Self(); self.RaftAddr != "raft-2:7001" || self.HTTPAddr != "raft-2:8081" {
			t.Errorf("own addresses: got %+v", self)
		}
	})

	t.Run("uses defaults for optional settings", func(t *testing.T) {
		t.Setenv("ID", "1")
		t.Setenv("PEERS", "1=localhost:7000:8080")
		t.Setenv("DATA_DIR", "")
		t.Setenv("LOG_LEVEL", "")
		t.Setenv("SNAPSHOT_THRESHOLD", "")

		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("load: %v", err)
		}

		if cfg.DataDir != "." || cfg.LogLevel != slog.LevelInfo || cfg.SnapshotThreshold != raft.DefaultSnapshotThreshold {
			t.Errorf("defaults: got %+v", cfg)
		}
	})

	bad := []struct {
		name string
		env  map[string]string
	}{
		{"an ID missing from PEERS", map[string]string{"ID": "3"}},
		{"a peer without both ports", map[string]string{"PEERS": "1=raft-1:7000"}},
		{"a port that is not a number", map[string]string{"PEERS": "1=raft-1:7000:http"}},
		{"an unknown log level", map[string]string{"LOG_LEVEL": "loud"}},
		{"a snapshot threshold of zero", map[string]string{"SNAPSHOT_THRESHOLD": "0"}},
	}

	for _, tc := range bad {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			t.Setenv("ID", "1")
			t.Setenv("PEERS", "1=raft-1:7000:8080")
			t.Setenv("LOG_LEVEL", "")
			t.Setenv("SNAPSHOT_THRESHOLD", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			if _, err := LoadConfig(); err == nil {
				t.Error("load: expected an error, got none")
			}
		})
	}
}
