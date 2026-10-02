package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/raft"
)

type RaftHandler struct {
	raft  *raft.Raft
	store *kv.KV

	peers map[uint64]string
}

func New(raft *raft.Raft, store *kv.KV, peers map[uint64]string) *RaftHandler {
	return &RaftHandler{raft: raft, store: store, peers: peers}
}

func (h *RaftHandler) LeaderOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.raft.IsLeader() {
			next(w, r)
			return
		}

		id, ok := h.raft.Leader()
		addr, known := h.peers[id]
		if !ok || !known {
			slog.Debug("no leader to redirect to", "path", r.URL.Path)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
			return
		}

		slog.Debug("redirecting to leader", "leader", id, "path", r.URL.Path)
		http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}
}

func (h *RaftHandler) Leader(w http.ResponseWriter, r *http.Request) {
	id, ok := h.raft.Leader()
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		ID      uint64 `json:"id"`
		Address string `json:"address"`
	}{id, h.peers[id]})
}

func (h *RaftHandler) State(w http.ResponseWriter, r *http.Request) {
	s := h.raft.Status()

	type entry struct {
		Term uint64 `json:"term"`
		Cmd  string `json:"cmd"`
	}

	logs := make([]entry, len(s.Logs))
	for i, l := range s.Logs {
		logs[i] = entry{Term: l.Term, Cmd: l.Cmd}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Term     uint64  `json:"term"`
		VotedFor *uint64 `json:"voted_for"`
		Logs     []entry `json:"logs"`
	}{s.Term, s.VotedFor, logs})
}

func (h *RaftHandler) Set(w http.ResponseWriter, r *http.Request) {
	var cmd kv.Cmd

	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd.Op = "set"

	data, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !h.raft.Append(string(data)) {
		slog.Warn("write not committed", "op", "set", "key", cmd.Key)
		http.Error(w, "set was not committed", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *RaftHandler) Delete(w http.ResponseWriter, r *http.Request) {
	var cmd kv.Cmd

	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd.Op = "delete"

	data, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !h.raft.Append(string(data)) {
		slog.Warn("write not committed", "op", "delete", "key", cmd.Key)
		http.Error(w, "delete was not committed", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *RaftHandler) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	value, ok := h.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
}
