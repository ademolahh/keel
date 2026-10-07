package kv

import (
	"encoding/json"
	"fmt"
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

func (kv *KV) Apply(command string) any {
	var cmd Cmd
	if err := json.Unmarshal([]byte(command), &cmd); err != nil {
		return err
	}

	if kv.duplicate(cmd) {
		return nil
	}

	switch cmd.Op {
	case "set":
		kv.set(cmd.Key, cmd.Value)
	case "delete":
		kv.delete(cmd.Key)
	case "get":
		value, ok := kv.get(cmd.Key)
		if ok {
			return value
		}
		return fmt.Errorf("not found")
	}

	return nil
}

func (kv *KV) duplicate(cmd Cmd) bool {
	if cmd.ClientID == "" || cmd.Seq == 0 || (cmd.Op != "set" && cmd.Op != "delete") {
		return false
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.Seq <= kv.sessions[cmd.ClientID] {
		return true
	}

	if kv.sessions == nil {
		kv.sessions = make(map[string]uint64)
	}

	kv.sessions[cmd.ClientID] = cmd.Seq
	return false
}

func (kv *KV) set(key, value string) {
	defer kv.mu.Unlock()
	kv.mu.Lock()
	kv.Data[key] = value
}

func (kv *KV) Get(key string) (string, bool) {
	return kv.get(key)
}

func (kv *KV) get(key string) (string, bool) {
	defer kv.mu.RUnlock()
	kv.mu.RLock()
	value, ok := kv.Data[key]
	return value, ok
}

func (kv *KV) Snapshot() ([]byte, error) {
	defer kv.mu.RUnlock()
	kv.mu.RLock()
	return protobuf.Marshal(&proto.KVSnapshot{Data: kv.Data, Sessions: kv.sessions})
}

func (kv *KV) delete(key string) string {
	defer kv.mu.Unlock()
	kv.mu.Lock()
	value, ok := kv.Data[key]
	if !ok {
		return ""
	}
	delete(kv.Data, key)

	return value
}
