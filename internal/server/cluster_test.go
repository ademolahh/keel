package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/metrics"
	"github.com/ademolahh/keel/internal/raft"
)

func TestCluster(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes)
	followers := others(nodes, leader)

	t.Run("a write through a follower is forwarded and read back through another", func(t *testing.T) {
		if res := post(t, followers[0], "/set", `{"key":"a","value":"1"}`); res.StatusCode != http.StatusCreated {
			t.Fatalf("set: expected 201, got %d", res.StatusCode)
		}

		if value := getValue(t, followers[1], "a"); value != "1" {
			t.Errorf("get: expected 1, got %q", value)
		}
	})

	t.Run("a delete removes the key", func(t *testing.T) {
		post(t, leader, "/set", `{"key":"gone","value":"1"}`)

		if res := post(t, followers[0], "/delete", `{"key":"gone"}`); res.StatusCode != http.StatusNoContent {
			t.Fatalf("delete: expected 204, got %d", res.StatusCode)
		}

		if res := get(t, leader, "/get?key=gone"); res.StatusCode != http.StatusNotFound {
			t.Errorf("get: expected 404, got %d", res.StatusCode)
		}
	})

	t.Run("a retried write is applied once", func(t *testing.T) {
		post(t, leader, "/set", `{"key":"r","value":"1","client_id":"c1","seq":1}`)
		post(t, leader, "/set", `{"key":"r","value":"2","client_id":"c1","seq":2}`)

		if res := post(t, followers[0], "/set", `{"key":"r","value":"1","client_id":"c1","seq":1}`); res.StatusCode != http.StatusCreated {
			t.Fatalf("retry: expected 201, got %d", res.StatusCode)
		}

		if value := getValue(t, leader, "r"); value != "2" {
			t.Errorf("get: expected the newer write 2, got %q", value)
		}
	})

	t.Run("a request with the wrong method is rejected", func(t *testing.T) {
		if res := get(t, leader, "/set"); res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("GET /set: expected 405, got %d", res.StatusCode)
		}

		if res := post(t, leader, "/get?key=a", ""); res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST /get: expected 405, got %d", res.StatusCode)
		}
	})

	t.Run("a get without a key is rejected", func(t *testing.T) {
		if res := get(t, leader, "/get"); res.StatusCode != http.StatusBadRequest {
			t.Errorf("get: expected 400, got %d", res.StatusCode)
		}
	})

	t.Run("every node reports the leader", func(t *testing.T) {
		for _, n := range nodes {
			var body struct {
				ID      uint64 `json:"id"`
				Address string `json:"address"`
			}
			decode(t, get(t, n, "/leader"), &body)

			if body.ID != leader.id || body.Address != leader.httpAddr {
				t.Errorf("node %d: expected leader %d at %s, got %+v", n.id, leader.id, leader.httpAddr, body)
			}
		}
	})

	t.Run("only the leader passes the leader health check", func(t *testing.T) {
		if res := get(t, leader, "/leader/healthz"); res.StatusCode != http.StatusOK {
			t.Errorf("leader: expected 200, got %d", res.StatusCode)
		}

		for _, f := range followers {
			if res := get(t, f, "/leader/healthz"); res.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("node %d: expected 503, got %d", f.id, res.StatusCode)
			}
		}
	})

	t.Run("every node is healthy and ready", func(t *testing.T) {
		for _, n := range nodes {
			for _, path := range []string{"/healthz", "/readyz"} {
				if res := get(t, n, path); res.StatusCode != http.StatusOK {
					t.Errorf("node %d %s: expected 200, got %d", n.id, path, res.StatusCode)
				}
			}
		}
	})

	t.Run("state and metrics are served", func(t *testing.T) {
		var state struct {
			Term uint64 `json:"term"`
		}
		decode(t, get(t, leader, "/state"), &state)

		if state.Term == 0 {
			t.Error("state: expected a term above 0")
		}

		res := get(t, leader, "/metrics")
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusOK || !strings.Contains(string(body), "raft_term") {
			t.Errorf("metrics: expected 200 with raft_term, got %d", res.StatusCode)
		}
	})
}

func TestClusterWithoutMajority(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes)

	for _, f := range others(nodes, leader) {
		f.stop()
	}

	t.Run("a write that cannot commit answers 503 without Retry-After", func(t *testing.T) {
		res := post(t, leader, "/set", `{"key":"a","value":"1"}`)
		body, _ := io.ReadAll(res.Body)

		if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") != "" {
			t.Errorf("expected 503 without Retry-After, got %d %v", res.StatusCode, res.Header)
		}

		if !strings.Contains(string(body), "may still apply") {
			t.Errorf("body: expected it to say the write may still apply, got %q", body)
		}
	})

	t.Run("a read that cannot confirm leadership answers 503 with Retry-After", func(t *testing.T) {
		res := get(t, leader, "/get?key=a")

		if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") == "" {
			t.Errorf("expected 503 with Retry-After, got %d %v", res.StatusCode, res.Header)
		}
	})
}

func TestNoLeader(t *testing.T) {
	h := New(idleNode(t), kv.NewKV(), nil)

	cases := []struct {
		name    string
		handler http.HandlerFunc
		retry   bool
	}{
		{"/leader", h.Leader, true},
		{"/readyz", h.Readyz, false},
		{"/set", h.LeaderOnly(h.Set), true},
	}

	for _, tc := range cases {
		t.Run(tc.name+" answers 503", func(t *testing.T) {
			res := serveRequest(tc.handler, http.MethodPost, tc.name, `{"key":"a","value":"1"}`)

			if res.Code != http.StatusServiceUnavailable {
				t.Errorf("status: expected 503, got %d", res.Code)
			}

			if got := res.Header().Get("Retry-After") != ""; got != tc.retry {
				t.Errorf("Retry-After: expected present %v, got %v", tc.retry, got)
			}
		})
	}
}

type testNode struct {
	id       uint64
	raft     *raft.Raft
	httpAddr string
	stop     func()
}

func startCluster(t *testing.T, n int) []*testNode {
	t.Helper()

	peers := make(map[uint64]Peer, n)
	for id := uint64(1); id <= uint64(n); id++ {
		peers[id] = Peer{RaftAddr: freeAddr(t), HTTPAddr: freeAddr(t)}
	}

	nodes := make([]*testNode, 0, n)
	for id := uint64(1); id <= uint64(n); id++ {
		cfg := Config{
			ID:                id,
			Peers:             peers,
			DataDir:           t.TempDir(),
			LogLevel:          slog.LevelError,
			SnapshotThreshold: raft.DefaultSnapshotThreshold,
		}

		store := kv.NewKV()
		m := metrics.New()

		r, err := newRaft(cfg, store, m)
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		m.SetRaft(r)

		grpcServer, err := startGRPC(cfg.Self().RaftAddr, r)
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}

		r.Start()

		httpPeers := cfg.addrs(func(p Peer) string { return p.HTTPAddr })
		httpServer, _ := startHTTP(cfg.Self().HTTPAddr, New(r, store, httpPeers), m)

		var once sync.Once
		stop := func() { once.Do(func() { shutdown(httpServer, r, grpcServer) }) }
		t.Cleanup(stop)

		nodes = append(nodes, &testNode{id: id, raft: r, httpAddr: cfg.Self().HTTPAddr, stop: stop})
	}

	return nodes
}

func waitForLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.IsLeader() && n.raft.CaughtUp() && allReady(nodes) {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("cluster: expected a leader within 10s")
	return nil
}

func allReady(nodes []*testNode) bool {
	for _, n := range nodes {
		if _, ok := n.raft.Leader(); !ok {
			return false
		}
	}

	return true
}

func others(nodes []*testNode, leader *testNode) []*testNode {
	var rest []*testNode
	for _, n := range nodes {
		if n != leader {
			rest = append(rest, n)
		}
	}

	return rest
}

func idleNode(t *testing.T) *raft.Raft {
	t.Helper()

	files, err := raft.OpenFilePersister(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("persister: %v", err)
	}
	t.Cleanup(func() { files.Close() })

	r, err := raft.New(1, nil, kv.NewKV(), files)
	if err != nil {
		t.Fatalf("raft: %v", err)
	}

	return r
}

func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	return l.Addr().String()
}

func serveRequest(h http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	res := httptest.NewRecorder()
	h(res, httptest.NewRequest(method, path, strings.NewReader(body)))

	return res
}

var client = &http.Client{Timeout: 5 * time.Second}

func post(t *testing.T, n *testNode, path, body string) *http.Response {
	t.Helper()

	res, err := client.Post("http://"+n.httpAddr+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { res.Body.Close() })

	return res
}

func get(t *testing.T, n *testNode, path string) *http.Response {
	t.Helper()

	res, err := client.Get("http://" + n.httpAddr + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	t.Cleanup(func() { res.Body.Close() })

	return res
}

func getValue(t *testing.T, n *testNode, key string) string {
	t.Helper()

	res := get(t, n, "/get?key="+key)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get %s: expected 200, got %d", key, res.StatusCode)
	}

	var body struct {
		Value string `json:"value"`
	}
	decode(t, res, &body)

	return body.Value
}

func decode(t *testing.T, res *http.Response, v any) {
	t.Helper()

	if err := json.NewDecoder(res.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}
