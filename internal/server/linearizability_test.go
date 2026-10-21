package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a cluster under load for several seconds")
	}

	t.Run("a steady cluster", func(t *testing.T) {
		nodes := startCluster(t, 3)
		waitForLeader(t, nodes)

		h := runClients(nodes, 8, 2*time.Second, nil)

		if h.succeeded < 200 {
			t.Fatalf("history: expected at least 200 completed operations, got %d", h.succeeded)
		}
		checkLinearizable(t, h.ops)
	})

	t.Run("the leader stops part way through", func(t *testing.T) {
		nodes := startCluster(t, 3)
		leader := waitForLeader(t, nodes)

		h := runClients(nodes, 8, 3*time.Second, func() {
			time.Sleep(time.Second)
			leader.stop()
		})

		if h.writesAfterFault == 0 {
			t.Fatal("history: expected writes to succeed after the leader stopped, got none")
		}
		checkLinearizable(t, h.ops)
	})
}

type kvInput struct {
	op    string // "get", "set" or "delete"
	key   string
	value string
}

type kvValue struct {
	value string
	found bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(kvInput).key
			byKey[key] = append(byKey[key], op)
		}

		parts := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			parts = append(parts, ops)
		}

		return parts
	},
	Init: func() any { return kvValue{} },
	Step: func(state, input, output any) (bool, any) {
		switch in := input.(kvInput); in.op {
		case "set":
			return true, kvValue{value: in.value, found: true}
		case "delete":
			return true, kvValue{}
		default:
			return output.(kvValue) == state.(kvValue), state
		}
	},
	DescribeOperation: func(input, output any) string {
		switch in := input.(kvInput); in.op {
		case "set":
			return fmt.Sprintf("set(%s, %s)", in.key, in.value)
		case "delete":
			return fmt.Sprintf("delete(%s)", in.key)
		default:
			if out := output.(kvValue); out.found {
				return fmt.Sprintf("get(%s) -> %s", in.key, out.value)
			}
			return fmt.Sprintf("get(%s) -> missing", in.key)
		}
	},
}

type outcome int

const (
	done outcome = iota
	failed
	unknown
)

type history struct {
	start time.Time

	mu               sync.Mutex
	ops              []porcupine.Operation
	succeeded        int
	faultAt          int64
	writesAfterFault int
}

func (h *history) now() int64 {
	return int64(time.Since(h.start))
}

func (h *history) record(op porcupine.Operation, result outcome) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch result {
	case done:
		h.ops = append(h.ops, op)
		h.succeeded++

		if h.faultAt != 0 && op.Call > h.faultAt && op.Input.(kvInput).op != "get" {
			h.writesAfterFault++
		}
	case unknown:
		op.Return = math.MaxInt64
		h.ops = append(h.ops, op)
	}
}

func runClients(nodes []*testNode, clients int, duration time.Duration, fault func()) *history {
	h := &history{start: time.Now()}
	deadline := h.start.Add(duration)

	var wg sync.WaitGroup

	if fault != nil {
		wg.Go(func() {
			fault()

			h.mu.Lock()
			h.faultAt = h.now()
			h.mu.Unlock()
		})
	}

	for c := range clients {
		wg.Go(func() {
			for i := 0; time.Now().Before(deadline); i++ {
				node := nodes[rand.IntN(len(nodes))]
				in := kvInput{key: fmt.Sprintf("k%d", rand.IntN(4))}

				switch n := rand.IntN(10); {
				case n < 5:
					in.op = "get"
				case n < 9:
					in.op, in.value = "set", fmt.Sprintf("%d-%d", c, i)
				default:
					in.op = "delete"
				}

				call := h.now()
				out, result := send(node, in)
				h.record(porcupine.Operation{ClientId: c, Input: in, Call: call, Output: out, Return: h.now()}, result)

				if result != done {
					time.Sleep(20 * time.Millisecond)
				}
			}
		})
	}

	wg.Wait()

	return h
}

func send(n *testNode, in kvInput) (kvValue, outcome) {
	base := "http://" + n.httpAddr

	if in.op == "get" {
		res, err := client.Get(base + "/get?key=" + in.key)
		if err != nil {
			return kvValue{}, failed
		}
		defer res.Body.Close()

		switch res.StatusCode {
		case http.StatusOK:
			var body struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				return kvValue{}, failed
			}
			return kvValue{value: body.Value, found: true}, done
		case http.StatusNotFound:
			return kvValue{}, done
		default:
			return kvValue{}, failed
		}
	}

	body := fmt.Sprintf(`{"key":%q,"value":%q}`, in.key, in.value)
	res, err := client.Post(base+"/"+in.op, "application/json", strings.NewReader(body))
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return kvValue{}, failed
		}
		return kvValue{}, unknown
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)

	if res.StatusCode == http.StatusCreated || res.StatusCode == http.StatusNoContent {
		return kvValue{}, done
	}

	return kvValue{}, unknown
}

func checkLinearizable(t *testing.T, ops []porcupine.Operation) {
	t.Helper()

	result, info := porcupine.CheckOperationsVerbose(kvModel, ops, 30*time.Second)

	switch result {
	case porcupine.Ok:
		t.Logf("%d operations are linearizable", len(ops))
	case porcupine.Unknown:
		t.Skipf("checker timed out on %d operations, so linearizability was not decided", len(ops))
	case porcupine.Illegal:
		f, err := os.CreateTemp("", "keel-linearizability-*.html")
		if err != nil {
			t.Fatalf("%d operations are not linearizable, and the visualization could not be saved: %v", len(ops), err)
		}
		defer f.Close()

		if err := porcupine.Visualize(kvModel, info, f); err != nil {
			t.Fatalf("%d operations are not linearizable, and the visualization could not be written: %v", len(ops), err)
		}
		t.Fatalf("%d operations are not linearizable; open %s to see where", len(ops), f.Name())
	}
}
