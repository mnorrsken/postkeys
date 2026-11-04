//go:build postgres

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func TestMutationPreservesTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:5789/%s?sslmode=%s", getEnvOrDefault("PG_USER", "postgres"), getEnvOrDefault("PG_PASSWORD", "testingpassword"), getEnvOrDefault("PG_HOST", "localhost"), getEnvOrDefault("PG_DATABASE", "postgres"), getEnvOrDefault("PG_SSLMODE", "disable")))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	ref := redis.NewClient(&redis.Options{Addr: getEnvOrDefault("REDIS_ADDR", "localhost:6399")})
	defer ref.Close()
	if err := ref.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name          string
		setup, mutate []interface{}
	}{
		{"incr", []interface{}{"SET", "k", "1"}, []interface{}{"INCR", "k"}},
		{"incrby", []interface{}{"SET", "k", "1"}, []interface{}{"INCRBY", "k", 2}},
		{"decr", []interface{}{"SET", "k", "1"}, []interface{}{"DECR", "k"}},
		{"append", []interface{}{"SET", "k", "1"}, []interface{}{"APPEND", "k", "2"}},
		{"setrange", []interface{}{"SET", "k", "1"}, []interface{}{"SETRANGE", "k", 0, "2"}},
		{"incrbyfloat", []interface{}{"SET", "k", "1"}, []interface{}{"INCRBYFLOAT", "k", "1.1"}},
		{"setbit", []interface{}{"SET", "k", "1"}, []interface{}{"SETBIT", "k", 2, 1}},
		{"bitfield", []interface{}{"SET", "k", "1"}, []interface{}{"BITFIELD", "k", "SET", "u8", 0, 2}},
		{"hset", []interface{}{"HSET", "k", "a", "1"}, []interface{}{"HSET", "k", "b", "2"}},
		{"hsetnx", []interface{}{"HSET", "k", "a", "1"}, []interface{}{"HSETNX", "k", "b", "2"}},
		{"hincrby", []interface{}{"HSET", "k", "a", "1"}, []interface{}{"HINCRBY", "k", "b", 2}},
		{"hincrbyfloat", []interface{}{"HSET", "k", "a", "1"}, []interface{}{"HINCRBYFLOAT", "k", "b", "2.1"}},
		{"lpush", []interface{}{"LPUSH", "k", "a"}, []interface{}{"LPUSH", "k", "b"}},
		{"rpush", []interface{}{"RPUSH", "k", "a"}, []interface{}{"RPUSH", "k", "b"}},
		{"sadd", []interface{}{"SADD", "k", "a"}, []interface{}{"SADD", "k", "b"}},
		{"zadd", []interface{}{"ZADD", "k", 1, "a"}, []interface{}{"ZADD", "k", 2, "b"}},
		{"zincrby", []interface{}{"ZADD", "k", 1, "a"}, []interface{}{"ZINCRBY", "k", 2, "b"}},
		{"pfadd", []interface{}{"PFADD", "k", "a"}, []interface{}{"PFADD", "k", "b"}},
		{"pfmerge", []interface{}{"PFADD", "k", "a"}, []interface{}{"PFMERGE", "k", "source"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := ts.client
			if err := ref.Del(ctx, "k", "source").Err(); err != nil {
				t.Fatal(err)
			}
			if err := ref.Do(ctx, tc.setup...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := ref.PExpire(ctx, "k", time.Second).Err(); err != nil {
				t.Fatal(err)
			}
			want := ref.Do(ctx, tc.mutate...)
			if err := want.Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.Del(ctx, "k").Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.Do(ctx, tc.setup...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.PExpire(ctx, "k", time.Second).Err(); err != nil {
				t.Fatal(err)
			}
			var before time.Time
			if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key=$1", "k").Scan(&before); err != nil {
				t.Fatal(err)
			}
			got := c.Do(ctx, tc.mutate...)
			if err := got.Err(); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got.Val()) != fmt.Sprint(want.Val()) {
				t.Fatalf("result = %v; Redis = %v", got.Val(), want.Val())
			}
			if ttl := ref.PTTL(ctx, "k").Val(); ttl <= 0 || ttl > time.Second {
				t.Fatalf("Redis TTL = %v", ttl)
			}
			var after time.Time
			if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key=$1", "k").Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !before.Equal(after) {
				t.Fatalf("deadline changed from %v to %v", before, after)
			}
			// Every physical row must carry the same deadline as metadata.
			table := map[string]string{"hset": "kv_hashes", "hsetnx": "kv_hashes", "hincrby": "kv_hashes", "hincrbyfloat": "kv_hashes", "lpush": "kv_lists", "rpush": "kv_lists", "sadd": "kv_sets", "zadd": "kv_zsets", "zincrby": "kv_zsets", "pfadd": "kv_hyperloglog", "pfmerge": "kv_hyperloglog"}[tc.name]
			if table != "" {
				var mismatches int
				if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table+" d JOIN kv_meta m ON d.key=m.key WHERE d.key=$1 AND d.expires_at IS DISTINCT FROM m.expires_at", "k").Scan(&mismatches); err != nil {
					t.Fatal(err)
				}
				if mismatches != 0 {
					t.Fatalf("%d rows have an incorrect deadline", mismatches)
				}
			}
			ttl := c.PTTL(ctx, "k").Val()
			if ttl <= 0 || ttl > time.Second {
				t.Fatalf("TTL = %v", ttl)
			}
			// Expiring the key must hide every newly added collection row as well.
			time.Sleep(1100 * time.Millisecond)
			if n, err := c.Exists(ctx, "k").Result(); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatalf("key survived expiry: %d", n)
			}
			switch tc.name {
			case "hset", "hsetnx", "hincrby", "hincrbyfloat":
				if n, err := c.HLen(ctx, "k").Result(); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatalf("hash rows survived: %d", n)
				}
			case "lpush", "rpush":
				if n, err := c.LLen(ctx, "k").Result(); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatalf("list rows survived: %d", n)
				}
			case "sadd":
				if n, err := c.SCard(ctx, "k").Result(); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatalf("set rows survived: %d", n)
				}
			case "zadd", "zincrby":
				if n, err := c.ZCard(ctx, "k").Result(); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatalf("zset rows survived: %d", n)
				}
			case "pfadd", "pfmerge":
				if n, err := c.PFCount(ctx, "k").Result(); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatalf("hyperloglog survived: %d", n)
				}
			}
		})
	}
}

func TestMutationTTLNoopsAndReplacement(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	c := ts.client
	ctx := context.Background()
	for _, cmd := range [][]interface{}{{"ZADD", "missing", "XX", 1, "a"}, {"BITFIELD", "missing", "GET", "u8", 0}, {"SETRANGE", "missing", 10, ""}} {
		if err := c.Do(ctx, cmd...).Err(); err != nil {
			t.Fatal(err)
		}
		if c.Exists(ctx, "missing").Val() != 0 {
			t.Fatalf("created missing key: %v", cmd)
		}
	}
	if err := c.Do(ctx, "HSET", "h", "a", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.PExpire(ctx, "h", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(ctx, "HSETNX", "h", "a", "2").Err(); err != nil {
		t.Fatal(err)
	}
	if c.PTTL(ctx, "h").Val() <= 0 {
		t.Fatal("HSETNX lost TTL")
	}
	if err := c.Do(ctx, "INCR", "h").Err(); err == nil {
		t.Fatal("expected WRONGTYPE")
	}
	if c.PTTL(ctx, "h").Val() <= 0 {
		t.Fatal("wrong type lost TTL")
	}
	for _, cmd := range []string{"SET", "GETSET"} {
		if err := c.Set(ctx, "s", "1", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.Do(ctx, cmd, "s", "2").Err(); err != nil {
			t.Fatal(err)
		}
		if got := c.PTTL(ctx, "s").Val(); got != -1 {
			t.Fatalf("%s TTL = %v", cmd, got)
		}
	}
}

func TestConcurrentIncrementPreservesTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	c := ts.client
	ctx := context.Background()
	if err := c.Set(ctx, "counter", "0", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- c.Incr(ctx, "counter").Err() }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := c.Get(ctx, "counter").Val(); got != fmt.Sprint(n) {
		t.Fatalf("counter = %s", got)
	}
	if c.PTTL(ctx, "counter").Val() <= 0 {
		t.Fatal("lost TTL")
	}
}

func TestMutationRecreatesPhysicallyRetainedExpiredKey(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:5789/%s?sslmode=%s", getEnvOrDefault("PG_USER", "postgres"), getEnvOrDefault("PG_PASSWORD", "testingpassword"), getEnvOrDefault("PG_HOST", "localhost"), getEnvOrDefault("PG_DATABASE", "postgres"), getEnvOrDefault("PG_SSLMODE", "disable")))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, tc := range []struct {
		setup, mutation []interface{}
		result          string
	}{
		{[]interface{}{"SET", "stale", "old"}, []interface{}{"APPEND", "stale", "new"}, "new"},
		{[]interface{}{"HSET", "stale", "old", "1"}, []interface{}{"APPEND", "stale", "new"}, "new"},
		{[]interface{}{"SET", "stale", "old"}, []interface{}{"HSET", "stale", "new", "1"}, "1"},
	} {
		if err := ts.client.Del(ctx, "stale").Err(); err != nil {
			t.Fatal(err)
		}
		if err := ts.client.Do(ctx, tc.setup...).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "UPDATE kv_meta SET expires_at = NOW() - interval '1 minute' WHERE key = 'stale'"); err != nil {
			t.Fatal(err)
		}
		if err := ts.client.Do(ctx, tc.mutation...).Err(); err != nil {
			t.Fatal(err)
		}
		var got string
		if tc.mutation[0] == "HSET" {
			got = ts.client.HGet(ctx, "stale", "new").Val()
		} else {
			got = ts.client.Get(ctx, "stale").Val()
		}
		if got != tc.result {
			t.Fatalf("%v after %v: got %q", tc.mutation, tc.setup, got)
		}
		if got := ts.client.PTTL(ctx, "stale").Val(); got != -1 {
			t.Fatalf("recreated key TTL: %v", got)
		}
	}
}

func TestMutationAfterRemovingLastMember(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	c := ts.client
	ctx := context.Background()
	cases := []struct{ setup, remove, recreate []interface{} }{
		{[]interface{}{"HSET", "ghost", "a", "1"}, []interface{}{"HDEL", "ghost", "a"}, []interface{}{"INCR", "ghost"}},
		{[]interface{}{"SADD", "ghost", "a"}, []interface{}{"SREM", "ghost", "a"}, []interface{}{"SADD", "ghost", "b"}},
		{[]interface{}{"LPUSH", "ghost", "a"}, []interface{}{"LPOP", "ghost"}, []interface{}{"LPUSH", "ghost", "b"}},
		{[]interface{}{"ZADD", "ghost", 1, "a"}, []interface{}{"ZREM", "ghost", "a"}, []interface{}{"ZADD", "ghost", 2, "b"}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.setup[0]), func(t *testing.T) {
			c.Del(ctx, "ghost")
			if err := c.Do(ctx, tc.setup...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.PExpire(ctx, "ghost", time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.Do(ctx, tc.remove...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.Do(ctx, tc.recreate...).Err(); err != nil {
				t.Fatal(err)
			}
			if ttl := c.PTTL(ctx, "ghost").Val(); ttl != -1 {
				t.Fatalf("recreated TTL = %v", ttl)
			}
		})
	}
}
