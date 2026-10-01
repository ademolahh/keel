package server

import (
	"encoding/json"
	"net/http"

	"github.com/ademolahh/raftkv/internal/kv"
	"github.com/ademolahh/raftkv/internal/raft"
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
			w.Header().Set("Retry-After", "1")
			http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
			return
		}

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
