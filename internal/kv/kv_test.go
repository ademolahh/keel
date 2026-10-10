package kv

import (
	"encoding/json"
	"testing"

	"github.com/ademolahh/keel/proto"
	protobuf "google.golang.org/protobuf/proto"
)

func TestSet(t *testing.T) {
	t.Run("stores the value under the key", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(command(t, "set", "a", "1"))

		if value := kv.Data["a"]; value != "1" {
			t.Errorf("value: expected 1, got %q", value)
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("returns the value of a stored key", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))

		if value, ok := kv.Get("a"); !ok || value != "1" {
			t.Errorf("value: expected 1, got %q (found %t)", value, ok)
		}
	})

	t.Run("reports a missing key", func(t *testing.T) {
		kv := NewKV()

		if value, ok := kv.Get("a"); ok {
			t.Errorf("value: expected none, got %q", value)
		}
	})
}

func TestApply(t *testing.T) {
	t.Run("sets a key", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(command(t, "set", "a", "1"))

		if value, ok := kv.Get("a"); !ok || value != "1" {
			t.Errorf("value: expected 1, got %q (found %t)", value, ok)
		}
	})

	t.Run("deletes a key", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))

		kv.Apply(command(t, "delete", "a", ""))

		if value, ok := kv.Get("a"); ok {
			t.Errorf("value: expected none, got %q", value)
		}
	})

	t.Run("ignores deleting a missing key", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(command(t, "delete", "a", ""))

		if len(kv.Data) != 0 {
			t.Errorf("size: expected 0, got %d", len(kv.Data))
		}
	})

	t.Run("ignores an unknown op", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))

		kv.Apply(command(t, "rename", "a", "2"))

		if value, _ := kv.Get("a"); value != "1" {
			t.Errorf("value: expected 1, got %q", value)
		}
	})

	t.Run("ignores malformed json", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))
		kv.Apply("{")

		if value, _ := kv.Get("a"); value != "1" || len(kv.Data) != 1 {
			t.Errorf("data: expected only a=1, got %v", kv.Data)
		}
	})
}

func TestDuplicate(t *testing.T) {
	t.Run("skips a write whose sequence was already applied", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(clientCommand(t, "set", "a", "1", "c1", 1))
		kv.Apply(clientCommand(t, "set", "a", "2", "c1", 2))
		kv.Apply(clientCommand(t, "set", "a", "1", "c1", 1))

		if value := kv.Data["a"]; value != "2" {
			t.Errorf("value: expected 2, got %q", value)
		}
	})

	t.Run("applies a retried delete only once", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(clientCommand(t, "delete", "a", "", "c1", 1))
		kv.Apply(clientCommand(t, "set", "a", "1", "c2", 1))
		kv.Apply(clientCommand(t, "delete", "a", "", "c1", 1))

		if value, ok := kv.Data["a"]; !ok || value != "1" {
			t.Errorf("value: expected 1, got %q (found %t)", value, ok)
		}
	})

	t.Run("tracks each client separately", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(clientCommand(t, "set", "a", "1", "c1", 5))
		kv.Apply(clientCommand(t, "set", "b", "2", "c2", 1))

		if value := kv.Data["b"]; value != "2" {
			t.Errorf("value: expected 2, got %q", value)
		}
	})

	t.Run("applies every write without a client id", func(t *testing.T) {
		kv := NewKV()

		kv.Apply(command(t, "set", "a", "1"))
		kv.Apply(command(t, "set", "a", "2"))
		kv.Apply(command(t, "set", "a", "1"))

		if value := kv.Data["a"]; value != "1" {
			t.Errorf("value: expected 1, got %q", value)
		}
	})

	t.Run("keeps the sessions in a snapshot", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(clientCommand(t, "set", "a", "1", "c1", 3))

		data, err := kv.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}

		var snap proto.KVSnapshot
		if err := protobuf.Unmarshal(data, &snap); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if seq := snap.Sessions["c1"]; seq != 3 {
			t.Errorf("session c1: expected 3, got %d", seq)
		}
	})
}

func TestRestore(t *testing.T) {
	t.Run("replaces the data and sessions with the snapshot's", func(t *testing.T) {
		source := NewKV()
		source.Apply(clientCommand(t, "set", "a", "1", "c1", 2))

		data, err := source.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}

		kv := NewKV()
		kv.Apply(command(t, "set", "b", "2"))

		if err := kv.Restore(data); err != nil {
			t.Fatalf("restore: %v", err)
		}

		if value, ok := kv.Get("a"); !ok || value != "1" {
			t.Errorf("a: expected 1, got %q (found %t)", value, ok)
		}

		if _, ok := kv.Get("b"); ok {
			t.Error("b: expected it gone after the restore")
		}

		kv.Apply(clientCommand(t, "set", "a", "9", "c1", 2))
		if value, _ := kv.Get("a"); value != "1" {
			t.Errorf("a after a retried write: expected 1, got %q", value)
		}
	})

	t.Run("rejects data that does not decode", func(t *testing.T) {
		if err := NewKV().Restore([]byte{0xff}); err == nil {
			t.Error("restore: expected an error, got none")
		}
	})
}

// HELPERS
func clientCommand(t *testing.T, op, key, value, client string, seq uint64) string {
	t.Helper()

	data, err := json.Marshal(Cmd{Op: op, Key: key, Value: value, ClientID: client, Seq: seq})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(data)
}

func command(t *testing.T, op, key, value string) string {
	t.Helper()

	data, err := json.Marshal(Cmd{Op: op, Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(data)
}
