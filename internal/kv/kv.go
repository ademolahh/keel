package kv

import (
	"encoding/json"
	"sync"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type KV struct {
	Data map[string]string
	mu   sync.RWMutex

	sessions map[string]uint64
}

type Cmd struct {
	Op    string
	Key   string `json:"key"`
	Value string `json:"value"`

	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

func NewKV() *KV {
	return &KV{Data: make(map[string]string), sessions: make(map[string]uint64)}
}

func (kv *KV) Apply(command string) {
	var cmd Cmd
	if err := json.Unmarshal([]byte(command), &cmd); err != nil {
		return
	}

	if cmd.Op != "set" && cmd.Op != "delete" {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.ClientID != "" && cmd.Seq > 0 {
		if cmd.Seq <= kv.sessions[cmd.ClientID] {
			return
		}

		kv.sessions[cmd.ClientID] = cmd.Seq
	}

	if cmd.Op == "set" {
		kv.Data[cmd.Key] = cmd.Value
	} else {
		delete(kv.Data, cmd.Key)
	}
}

func (kv *KV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.Data[key]
	return value, ok
}

func (kv *KV) Restore(data []byte) error {
	var snapshot proto.KVSnapshot
	if err := protobuf.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Data = snapshot.Data
	if kv.Data == nil {
		kv.Data = make(map[string]string)
	}

	kv.sessions = snapshot.Sessions
	if kv.sessions == nil {
		kv.sessions = make(map[string]uint64)
	}

	return nil
}

func (kv *KV) Snapshot() ([]byte, error) {
	defer kv.mu.RUnlock()
	kv.mu.RLock()
	return protobuf.Marshal(&proto.KVSnapshot{Data: kv.Data, Sessions: kv.sessions})
}
