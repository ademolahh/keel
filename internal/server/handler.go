package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ademolahh/keel/internal/kv"
	"github.com/ademolahh/keel/internal/metrics"
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

func (h *RaftHandler) Set(w http.ResponseWriter, r *http.Request) {
	var cmd kv.Cmd

	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if cmd.ClientID != "" && cmd.Seq == 0 {
		http.Error(w, "seq must be 1 or more when client_id is set", http.StatusBadRequest)
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

	if cmd.ClientID != "" && cmd.Seq == 0 {
		http.Error(w, "seq must be 1 or more when client_id is set", http.StatusBadRequest)
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

	if err := h.raft.Read(); err != nil {
		slog.Warn("read not confirmed", "key", key, "err", err)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "read could not be confirmed by a majority", http.StatusServiceUnavailable)
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

func (h *RaftHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok\n"))
}

func (h *RaftHandler) Readyz(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.raft.Leader(); !ok {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}

	if !h.raft.CaughtUp() {
		http.Error(w, "applying committed entries", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("ok\n"))
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

func routes(h *RaftHandler, m *metrics.RaftCollector) http.Handler {
	mux := http.NewServeMux()

	handle := func(route string, handler http.HandlerFunc) {
		mux.Handle(route, m.Instrument(route, handler))
	}

	handle("/set", h.LeaderOnly(h.Set))
	handle("/delete", h.LeaderOnly(h.Delete))
	handle("/get", h.LeaderOnly(h.Get))
	handle("/leader", h.Leader)
	handle("/state", h.State)
	handle("/healthz", h.Healthz)
	handle("/readyz", h.Readyz)
	mux.Handle("/metrics", m.Handler())

	return mux
}
