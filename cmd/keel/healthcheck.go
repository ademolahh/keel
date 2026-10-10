package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ademolahh/keel/internal/server"
)

func healthcheck(cfg server.Config, path string) int {
	_, port, err := net.SplitHostPort(cfg.Self().HTTPAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PEERS: %v\n", err)
		return 1
	}

	client := http.Client{Timeout: 2 * time.Second}

	res, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%s: %s\n", path, res.Status)
		return 1
	}

	return 0
}
