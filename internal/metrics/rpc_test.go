package metrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestInterceptor(t *testing.T) {
	t.Run("counts an RPC by peer, method and status code", func(t *testing.T) {
		c := New()

		err := c.Interceptor()(context.Background(), "/Raft/AppendEntries", nil, nil,
			newConn(t, "raft-2:7000"), failWith(codes.DeadlineExceeded))

		if status.Code(err) != codes.DeadlineExceeded {
			t.Errorf("error: expected DeadlineExceeded, got %v", err)
		}

		got := testutil.ToFloat64(c.rpc.requests.WithLabelValues("raft-2:7000", "AppendEntries", "DeadlineExceeded"))
		if got != 1 {
			t.Errorf("requests: expected 1, got %v", got)
		}
	})

	t.Run("times the RPC", func(t *testing.T) {
		c := New()

		c.Interceptor()(context.Background(), "/Raft/RequestVote", nil, nil,
			newConn(t, "raft-2:7000"), failWith(codes.OK))

		if got := sampleCount(t, c.rpc.duration.WithLabelValues("raft-2:7000", "RequestVote")); got != 1 {
			t.Errorf("timed RPCs: expected 1, got %d", got)
		}
	})
}

// HELPERS

// newConn makes a client to addr; nothing is dialled until a call is made.
func newConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// failWith stands in for the call, returning an error with code, or nil for OK.
func failWith(code codes.Code) grpc.UnaryInvoker {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return status.Error(code, "stub")
	}
}
