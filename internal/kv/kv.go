package kv

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type KV struct {
	Data  map[string]string
	mu    sync.RWMutex
	apply chan string
}

type Cmd struct {
	Op    string
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewKV() *KV {
	return &KV{Data: make(map[string]string)}
}

func (kv *KV) Apply(command string) any {
	var cmd Cmd
	if err := json.Unmarshal([]byte(command), &cmd); err != nil {
		return err
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

func (kv *KV) set(key, value string) error {
	defer kv.mu.Unlock()
	kv.mu.Lock()
	kv.Data[key] = value
	return nil
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
	return protobuf.Marshal(&proto.KVSnapshot{Data: kv.Data})
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
