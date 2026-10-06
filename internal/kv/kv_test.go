package kv

import (
	"encoding/json"
	"testing"
)

func TestSet(t *testing.T) {
	t.Run("stores the value under the key", func(t *testing.T) {
		kv := NewKV()

		kv.set("a", "1")

		if value := kv.Data["a"]; value != "1" {
			t.Errorf("value: expected 1, got %q", value)
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("returns the value of a stored key", func(t *testing.T) {
		kv := NewKV()
		kv.set("a", "1")

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

		if res := kv.Apply(command(t, "set", "a", "1")); res != nil {
			t.Fatalf("apply: expected nil, got %v", res)
		}

		if value, ok := kv.Get("a"); !ok || value != "1" {
			t.Errorf("value: expected 1, got %q (found %t)", value, ok)
		}
	})

	t.Run("deletes a key", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))

		if res := kv.Apply(command(t, "delete", "a", "")); res != nil {
			t.Fatalf("apply: expected nil, got %v", res)
		}

		if value, ok := kv.Get("a"); ok {
			t.Errorf("value: expected none, got %q", value)
		}
	})

	t.Run("ignores deleting a missing key", func(t *testing.T) {
		kv := NewKV()

		if res := kv.Apply(command(t, "delete", "a", "")); res != nil {
			t.Fatalf("apply: expected nil, got %v", res)
		}

		if len(kv.Data) != 0 {
			t.Errorf("size: expected 0, got %d", len(kv.Data))
		}
	})

	t.Run("ignores an unknown op", func(t *testing.T) {
		kv := NewKV()
		kv.Apply(command(t, "set", "a", "1"))

		if res := kv.Apply(command(t, "rename", "a", "2")); res != nil {
			t.Fatalf("apply: expected nil, got %v", res)
		}

		if value, _ := kv.Get("a"); value != "1" {
			t.Errorf("value: expected 1, got %q", value)
		}
	})

	t.Run("returns an error for malformed json", func(t *testing.T) {
		kv := NewKV()

		if _, ok := kv.Apply("{").(error); !ok {
			t.Error("apply: expected an error, got none")
		}
	})
}

// HELPERS
func command(t *testing.T, op, key, value string) string {
	t.Helper()

	data, err := json.Marshal(Cmd{Op: op, Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(data)
}
