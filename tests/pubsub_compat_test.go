//go:build postgres

package integration

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
)

// Subscription counts in (P)(UN)SUBSCRIBE replies count channels and patterns
// together, and the client only leaves pub/sub mode when both are zero.
// Compares the replies with the real Redis from docker-compose.test.yml.
func TestPubSubRepliesMatchRedis(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer srv.Stop()
	defer store.Close()

	steps := []struct {
		args    []string
		replies int
	}{
		{[]string{"SUBSCRIBE", "a", "b"}, 2},
		{[]string{"PSUBSCRIBE", "p*"}, 1},
		{[]string{"UNSUBSCRIBE", "a"}, 1},
		{[]string{"UNSUBSCRIBE"}, 1}, // only b is left
		{[]string{"PING"}, 1},        // still subscribed to p*
		{[]string{"PUNSUBSCRIBE"}, 1},
		{[]string{"PING"}, 1}, // out of pub/sub mode now
	}
	run := func(addr string) []string {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Skipf("%s not reachable: %v", addr, err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		r := resp.NewReader(conn)
		var out []string
		for _, s := range steps {
			sendCommand(conn, s.args...)
			for i := 0; i < s.replies; i++ {
				v, err := r.Read()
				if err != nil {
					t.Fatalf("%s %v: %v", addr, s.args, err)
				}
				out = append(out, formatValue(v))
			}
		}
		return out
	}

	want := run(getEnvOrDefault("REDIS_ADDR", "localhost:6399"))
	got := run(addr)
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Errorf("reply %d: redis %s, postkeys %v", i, want[i], got[i:min(i+1, len(got))])
		}
	}
}

func formatValue(v resp.Value) string {
	switch v.Type {
	case resp.Array:
		s := "["
		for _, e := range v.Array {
			s += formatValue(e) + " "
		}
		return s + "]"
	case resp.Integer:
		return strconv.FormatInt(v.Num, 10)
	case resp.BulkString:
		return v.Bulk
	default:
		return v.Str
	}
}
