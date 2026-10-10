package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/raft"
	"github.com/ademolahh/keel/proto"
)

func TestLeaderOnly(t *testing.T) {
	t.Run("forwards a follower's request to the leader", func(t *testing.T) {
		var forwarded string
		leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			forwarded = r.Header.Get(forwardedHeader)
			w.WriteHeader(http.StatusCreated)
		}))
		defer leader.Close()

		h := followerOf(t, 2, leader.Listener.Addr().String())
		res := serve(h, "/set", nil)

		if res.Code != http.StatusCreated {
			t.Errorf("status: expected the leader's 201, got %d", res.Code)
		}

		if forwarded == "" {
			t.Errorf("leader: expected the %s header on the forwarded request", forwardedHeader)
		}
	})

	t.Run("does not forward a request twice", func(t *testing.T) {
		h := followerOf(t, 2, "127.0.0.1:1")
		res := serve(h, "/set", http.Header{forwardedHeader: {"1"}})

		if res.Code != http.StatusServiceUnavailable || res.Header().Get("Retry-After") == "" {
			t.Errorf("expected 503 with Retry-After, got %d %v", res.Code, res.Header())
		}
	})

	t.Run("answers 503 when the leader cannot be reached", func(t *testing.T) {
		h := followerOf(t, 2, "127.0.0.1:1")
		res := serve(h, "/set", nil)

		if res.Code != http.StatusServiceUnavailable || res.Header().Get("Retry-After") == "" {
			t.Errorf("expected 503 with Retry-After, got %d %v", res.Code, res.Header())
		}
	})
}

func followerOf(t *testing.T, leaderID uint64, leaderAddr string) *RaftHandler {
	t.Helper()

	files, err := raft.OpenFilePersister(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("persister: %v", err)
	}
	t.Cleanup(func() { files.Close() })

	store := kv.NewKV()
	node, err := raft.New(1, nil, store, files)
	if err != nil {
		t.Fatalf("raft: %v", err)
	}

	res, err := node.AppendEntries(context.Background(), &proto.AppendEntriesRequest{Term: 1, LeaderId: leaderID})
	if err != nil || !res.Success {
		t.Fatalf("hear from leader: %v %v", res, err)
	}

	return New(node, store, map[uint64]string{leaderID: leaderAddr})
}

func serve(h *RaftHandler, path string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"key":"a","value":"1"}`))
	for k, v := range header {
		req.Header[k] = v
	}

	res := httptest.NewRecorder()
	h.LeaderOnly(h.Set)(res, req)

	return res
}
