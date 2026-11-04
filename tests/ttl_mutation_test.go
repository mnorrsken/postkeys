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

func mutationTestConn(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:5789/%s?sslmode=%s", getEnvOrDefault("PG_USER", "postgres"), getEnvOrDefault("PG_PASSWORD", "testingpassword"), getEnvOrDefault("PG_HOST", "localhost"), getEnvOrDefault("PG_DATABASE", "postgres"), getEnvOrDefault("PG_SSLMODE", "disable")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	return conn
}

func TestMutationPreservesTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn := mutationTestConn(t)
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
		{"linsert-before", []interface{}{"RPUSH", "k", "a"}, []interface{}{"LINSERT", "k", "BEFORE", "a", "b"}},
		{"linsert-after", []interface{}{"RPUSH", "k", "a"}, []interface{}{"LINSERT", "k", "AFTER", "a", "b"}},
		{"rpoplpush", []interface{}{"RPUSH", "k", "a"}, []interface{}{"RPOPLPUSH", "k", "k"}},
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
			if err := ref.PExpire(ctx, "k", time.Minute).Err(); err != nil {
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
			if err := c.PExpire(ctx, "k", time.Minute).Err(); err != nil {
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
			if ttl := ref.PTTL(ctx, "k").Val(); ttl <= 0 || ttl > time.Minute {
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
			table := map[string]string{"hset": "kv_hashes", "hsetnx": "kv_hashes", "hincrby": "kv_hashes", "hincrbyfloat": "kv_hashes", "lpush": "kv_lists", "rpush": "kv_lists", "linsert-before": "kv_lists", "linsert-after": "kv_lists", "rpoplpush": "kv_lists", "sadd": "kv_sets", "zadd": "kv_zsets", "zincrby": "kv_zsets", "pfadd": "kv_hyperloglog", "pfmerge": "kv_hyperloglog"}[tc.name]
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
			if ttl <= 0 || ttl > time.Minute {
				t.Fatalf("TTL = %v", ttl)
			}
			// Advance metadata and data deadlines together. Holding the advisory
			// lock blocks cleanup until commit; readers must hide rows immediately.
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('k'))"); err != nil {
				t.Fatal(err)
			}
			for _, tab := range []string{"kv_meta", "kv_strings", "kv_hashes", "kv_lists", "kv_sets", "kv_zsets", "kv_hyperloglog"} {
				if _, err := tx.Exec(ctx, "UPDATE "+tab+" SET expires_at=NOW()-interval '1 minute' WHERE key='k'"); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
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
			case "lpush", "rpush", "linsert-before", "linsert-after", "rpoplpush":
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
	conn := mutationTestConn(t)
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
			for _, check := range []struct {
				cmd  []interface{}
				want string
			}{
				{[]interface{}{"EXISTS", "ghost"}, "0"},
				{[]interface{}{"TYPE", "ghost"}, "none"},
				{[]interface{}{"PTTL", "ghost"}, "-2"},
			} {
				got, err := c.Do(ctx, check.cmd...).Result()
				if err != nil {
					t.Fatal(err)
				}
				if fmt.Sprint(got) != check.want {
					t.Fatalf("%v = %v; want %s", check.cmd, got, check.want)
				}
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

func TestHyperLogLogRenameAndExpiry(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	for _, c := range []*redis.Client{ts.client, redis.NewClient(&redis.Options{Addr: getEnvOrDefault("REDIS_ADDR", "localhost:6399")})} {
		if c != ts.client {
			defer c.Close()
		}
		if err := c.Del(ctx, "hll-from", "hll-to").Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.PFAdd(ctx, "hll-from", "a", "b").Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.PExpire(ctx, "hll-from", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.Rename(ctx, "hll-from", "hll-to").Err(); err != nil {
			t.Fatal(err)
		}
		if n, err := c.PFCount(ctx, "hll-to").Result(); err != nil || n != 2 {
			t.Fatalf("PFCOUNT after RENAME = %d, %v", n, err)
		}
		if ttl, err := c.PTTL(ctx, "hll-to").Result(); err != nil || ttl <= 0 {
			t.Fatalf("renamed TTL = %v, %v", ttl, err)
		}
		if err := c.PExpire(ctx, "hll-to", 50*time.Millisecond).Err(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(70 * time.Millisecond)
		if n, err := c.PFCount(ctx, "hll-to").Result(); err != nil || n != 0 {
			t.Fatalf("PFCOUNT after expiry = %d, %v", n, err)
		}
	}
}

func TestCollectionRemovalMetadataAndConcurrentWriter(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	cases := []struct{ setup, remove, write []interface{} }{
		{[]interface{}{"HSET", "g", "a", 1}, []interface{}{"HDEL", "g", "a"}, []interface{}{"HSET", "g", "b", 2}},
		{[]interface{}{"SADD", "g", "a"}, []interface{}{"SREM", "g", "a"}, []interface{}{"SADD", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"LPOP", "g"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"RPOP", "g"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"LREM", "g", 0, "a"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"LREM", "g", 1, "a"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"LTRIM", "g", 1, 0}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"LMPOP", 1, "g", "LEFT"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"RPUSH", "g", "a"}, []interface{}{"RPOPLPUSH", "g", "g-dst"}, []interface{}{"RPUSH", "g", "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZREM", "g", "a"}, []interface{}{"ZADD", "g", 2, "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZREMRANGEBYSCORE", "g", 0, 2}, []interface{}{"ZADD", "g", 2, "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZREMRANGEBYRANK", "g", 0, -1}, []interface{}{"ZADD", "g", 2, "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZPOPMIN", "g"}, []interface{}{"ZADD", "g", 2, "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZPOPMAX", "g"}, []interface{}{"ZADD", "g", 2, "b"}},
		{[]interface{}{"ZADD", "g", 1, "a"}, []interface{}{"ZMPOP", 1, "g", "MIN"}, []interface{}{"ZADD", "g", 2, "b"}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.remove), func(t *testing.T) {
			c := ts.client
			rounds := 6
			// These commands can legitimately remove a concurrent new member.
			if tc.remove[0] == "LTRIM" || tc.remove[0] == "ZREMRANGEBYSCORE" || tc.remove[0] == "ZREMRANGEBYRANK" {
				rounds = 1
			}
			for round := 0; round < rounds; round++ {
				if err := c.Del(ctx, "g", "g-dst").Err(); err != nil {
					t.Fatal(err)
				}
				if err := c.Do(ctx, tc.setup...).Err(); err != nil {
					t.Fatal(err)
				}
				if round == 0 {
					if err := c.Do(ctx, tc.remove...).Err(); err != nil {
						t.Fatal(err)
					}
					for _, check := range []struct {
						cmd  []interface{}
						want string
					}{
						{[]interface{}{"EXISTS", "g"}, "0"}, {[]interface{}{"TYPE", "g"}, "none"}, {[]interface{}{"PTTL", "g"}, "-2"},
					} {
						got, err := c.Do(ctx, check.cmd...).Result()
						if err != nil || fmt.Sprint(got) != check.want {
							t.Fatalf("%v=%v,%v", check.cmd, got, err)
						}
					}
				} else {
					errs := make(chan error, 2)
					go func() { errs <- c.Do(ctx, tc.remove...).Err() }()
					go func() { errs <- c.Do(ctx, tc.write...).Err() }()
					for i := 0; i < 2; i++ {
						if err := <-errs; err != nil {
							t.Fatal(err)
						}
					}
					if n, err := c.Exists(ctx, "g").Result(); err != nil || n != 1 {
						t.Fatalf("concurrent writer key vanished: %d,%v", n, err)
					}
				}
			}
		})
	}
}

func TestMutationRepairsLegacyEmptyMetadata(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn := mutationTestConn(t)
	if _, err := conn.Exec(ctx, "INSERT INTO kv_meta(key,key_type,expires_at) VALUES ('legacy-ghost','hash',NOW()+interval '1 hour')"); err != nil {
		t.Fatal(err)
	}
	if n, err := ts.client.Incr(ctx, "legacy-ghost").Result(); err != nil || n != 1 {
		t.Fatalf("legacy recreation = %d,%v", n, err)
	}
	if ttl, err := ts.client.PTTL(ctx, "legacy-ghost").Result(); err != nil || ttl != -1 {
		t.Fatalf("legacy recreation TTL = %v,%v", ttl, err)
	}
	// Older versions could leave expired rows beside newly inserted live rows.
	if err := ts.client.HSet(ctx, "mixed-legacy", "old", "1", "live", "2").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "UPDATE kv_hashes SET expires_at=NOW()-interval '1 minute' WHERE key='mixed-legacy' AND field='old'"); err != nil {
		t.Fatal(err)
	}
	if err := ts.client.HDel(ctx, "mixed-legacy", "live").Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := ts.client.Exists(ctx, "mixed-legacy").Result(); err != nil || n != 0 {
		t.Fatalf("legacy metadata survived last live member: %d,%v", n, err)
	}
	var retained int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM kv_hashes WHERE key='mixed-legacy'").Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 0 {
		t.Fatalf("%d orphaned expired rows", retained)
	}

	for _, tc := range []struct {
		kind string
		cmd  []interface{}
	}{
		{"list", []interface{}{"LMPOP", 1, "legacy-pop", "LEFT"}},
		{"zset", []interface{}{"ZMPOP", 1, "legacy-pop", "MIN"}},
	} {
		if _, err := conn.Exec(ctx, "INSERT INTO kv_meta(key,key_type,expires_at) VALUES ('legacy-pop',$1,NOW()+interval '1 hour')", tc.kind); err != nil {
			t.Fatal(err)
		}
		if err := ts.client.Do(ctx, tc.cmd...).Err(); err != redis.Nil {
			t.Fatalf("legacy empty pop: %v", err)
		}
		if n, err := ts.client.Exists(ctx, "legacy-pop").Result(); err != nil || n != 0 {
			t.Fatalf("legacy pop metadata survived: %d,%v", n, err)
		}
	}

}

func TestListMoveDestinationPreservesTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn := mutationTestConn(t)
	c := ts.client
	if err := c.RPush(ctx, "move-src", "new").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.RPush(ctx, "move-dst", "old").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.PExpire(ctx, "move-dst", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	var before time.Time
	if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key='move-dst'").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if got, err := c.RPopLPush(ctx, "move-src", "move-dst").Result(); err != nil || got != "new" {
		t.Fatalf("move=%v,%v", got, err)
	}
	var after time.Time
	var mismatches int
	if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key='move-dst'").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !before.Equal(after) {
		t.Fatalf("move changed destination expiry: %v -> %v", before, after)
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM kv_lists WHERE key='move-dst' AND expires_at IS DISTINCT FROM $1", before).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf("%d moved rows lost expiry", mismatches)
	}
	if n, err := c.Exists(ctx, "move-src").Result(); err != nil || n != 0 {
		t.Fatalf("source metadata survived: %d,%v", n, err)
	}
}

func TestNoopCollectionRemovalPreservesStateAndTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	conn := mutationTestConn(t)
	c := ts.client
	cases := []struct{ setup, remove, read []interface{} }{
		{[]interface{}{"HSET", "noop", "a", 1}, []interface{}{"HDEL", "noop", "missing"}, []interface{}{"HGETALL", "noop"}},
		{[]interface{}{"SADD", "noop", "a"}, []interface{}{"SREM", "noop", "missing"}, []interface{}{"SMEMBERS", "noop"}},
		{[]interface{}{"RPUSH", "noop", "a"}, []interface{}{"LREM", "noop", 0, "missing"}, []interface{}{"LRANGE", "noop", 0, -1}},
		{[]interface{}{"RPUSH", "noop", "a"}, []interface{}{"LREM", "noop", 1, "missing"}, []interface{}{"LRANGE", "noop", 0, -1}},
		{[]interface{}{"ZADD", "noop", 1, "a"}, []interface{}{"ZREM", "noop", "missing"}, []interface{}{"ZRANGE", "noop", 0, -1, "WITHSCORES"}},
		{[]interface{}{"ZADD", "noop", 1, "a"}, []interface{}{"ZREMRANGEBYSCORE", "noop", 10, 20}, []interface{}{"ZRANGE", "noop", 0, -1, "WITHSCORES"}},
		{[]interface{}{"ZADD", "noop", 1, "a"}, []interface{}{"ZREMRANGEBYRANK", "noop", 10, 20}, []interface{}{"ZRANGE", "noop", 0, -1, "WITHSCORES"}},
		{[]interface{}{"ZADD", "noop", 1, "a"}, []interface{}{"ZPOPMIN", "noop", 0}, []interface{}{"ZRANGE", "noop", 0, -1, "WITHSCORES"}},
		{[]interface{}{"ZADD", "noop", 1, "a"}, []interface{}{"ZPOPMAX", "noop", 0}, []interface{}{"ZRANGE", "noop", 0, -1, "WITHSCORES"}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.remove), func(t *testing.T) {
			if err := c.Del(ctx, "noop").Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.Do(ctx, tc.setup...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.PExpire(ctx, "noop", time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			want, err := c.Do(ctx, tc.read...).Result()
			if err != nil {
				t.Fatal(err)
			}
			var before, after time.Time
			if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key='noop'").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if err := c.Do(ctx, tc.remove...).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := c.Do(ctx, tc.read...).Result()
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("no-op changed state: %v -> %v", want, got)
			}
			if err := conn.QueryRow(ctx, "SELECT expires_at FROM kv_meta WHERE key='noop'").Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !before.Equal(after) {
				t.Fatalf("no-op changed expiry: %v -> %v", before, after)
			}
		})
	}
	for _, cmd := range [][]interface{}{
		{"HDEL", "wrong-removal", "a"}, {"SREM", "wrong-removal", "a"}, {"ZREM", "wrong-removal", "a"},
		{"LPOP", "wrong-removal"}, {"RPOP", "wrong-removal"}, {"LREM", "wrong-removal", 1, "a"},
		{"ZREMRANGEBYSCORE", "wrong-removal", 0, 1}, {"ZREMRANGEBYRANK", "wrong-removal", 0, -1},
		{"ZPOPMIN", "wrong-removal", 0}, {"ZPOPMAX", "wrong-removal", 0},
	} {
		if err := c.Set(ctx, "wrong-removal", "value", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		err := c.Do(ctx, cmd...).Err()
		if err == nil || err.Error() != "WRONGTYPE Operation against a key holding the wrong kind of value" {
			t.Fatalf("%v error=%v", cmd, err)
		}
		if got, err := c.Get(ctx, "wrong-removal").Result(); err != nil || got != "value" {
			t.Fatalf("wrongtype changed state: %q,%v", got, err)
		}
		if ttl, err := c.PTTL(ctx, "wrong-removal").Result(); err != nil || ttl <= 0 {
			t.Fatalf("wrongtype changed TTL: %v,%v", ttl, err)
		}
	}
	for _, cmd := range [][]interface{}{{"HDEL", "missing-noop", "a"}, {"SREM", "missing-noop", "a"}, {"ZREM", "missing-noop", "a"}, {"LPOP", "missing-noop"}, {"RPOP", "missing-noop"}, {"LREM", "missing-noop", 0, "a"}, {"ZREMRANGEBYRANK", "missing-noop", 0, -1}, {"LMPOP", 1, "missing-noop", "LEFT"}, {"ZMPOP", 1, "missing-noop", "MIN"}} {
		if err := c.Do(ctx, cmd...).Err(); err != nil && err != redis.Nil {
			t.Fatalf("%v: %v", cmd, err)
		}
		if n, err := c.Exists(ctx, "missing-noop").Result(); err != nil || n != 0 {
			t.Fatalf("no-op created missing key: %v,%v", n, err)
		}
	}
}
