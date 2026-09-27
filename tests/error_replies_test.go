//go:build postgres

package integration_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Error replies must match Redis 7 exactly; clients match on them. Compares
// against the real Redis from docker-compose.test.yml.
func TestErrorRepliesMatchRedis(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	real := redis.NewClient(&redis.Options{Addr: getEnvOrDefault("REDIS_ADDR", "localhost:6399")})
	defer real.Close()
	if err := real.Ping(ctx).Err(); err != nil {
		t.Skipf("reference Redis not reachable: %v", err)
	}
	if err := real.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	cmds := [][]any{
		{"set", "s", "x"},
		{"lpush", "s", "y"},
		{"incr", "s"},
		{"expire", "s", "abc"},
		{"hincrby", "h", "f", "x"},
		{"lmpop", "0", "k", "LEFT"},
		{"exec"},
		{"discard"},
		{"get"},
		{"evalsha", "0000000000000000000000000000000000000000", "0"},
		{"eval", "return redis.call('incr', KEYS[1])", "1", "s"},
		{"eval", "return redis.call('lpush', KEYS[1], 'x')", "1", "s"},
		{"eval", "return redis.error_reply('MY custom')", "0"},
	}
	for _, c := range cmds {
		want, got := "<nil>", "<nil>"
		if err := real.Do(ctx, c...).Err(); err != nil {
			want = err.Error()
		}
		if err := ts.client.Do(ctx, c...).Err(); err != nil {
			got = err.Error()
		}
		if want != got {
			t.Errorf("%v\n  redis:    %s\n  postkeys: %s", c, want, got)
		}
	}
}
