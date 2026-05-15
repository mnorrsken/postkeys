//go:build postgres
// +build postgres

package integration

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
	"github.com/redis/go-redis/v9"
)

// PostgreSQL test server setup
type pgTestServer struct {
	server *server.Server
	client *redis.Client
	store  *storage.Store
	addr   string
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func newPgTestServer(t testing.TB) *pgTestServer {
	ctx := context.Background()

	// PostgreSQL connection config from environment
	cfg := storage.Config{
		Host:     getEnvOrDefault("PG_HOST", "localhost"),
		Port:     5789, // Use test port
		User:     getEnvOrDefault("PG_USER", "postgres"),
		Password: getEnvOrDefault("PG_PASSWORD", "testingpassword"),
		Database: getEnvOrDefault("PG_DATABASE", "postgres"),
		SSLMode:  getEnvOrDefault("PG_SSLMODE", "disable"),
	}

	store, err := storage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Clean up any existing data
	cleanupStore(ctx, store)

	h := handler.New(store, "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create listener: %v", err)
	}

	addr := listener.Addr().String()
	srv := server.New(addr, h)

	go func() {
		if err := srv.ServeWithListener(listener); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		store.Close()
		t.Fatalf("Failed to connect to test server: %v", err)
	}

	return &pgTestServer{
		server: srv,
		client: client,
		store:  store,
		addr:   addr,
	}
}

func cleanupStore(ctx context.Context, store *storage.Store) {
	// Use FLUSHDB equivalent - delete all keys
	keys, _ := store.Keys(ctx, "*")
	for _, key := range keys {
		store.Del(ctx, []string{key})
	}
}

func (ts *pgTestServer) Close() {
	ts.client.Close()
	ts.server.Close()
	ts.store.Close()
}

// Benchmarks against PostgreSQL
func BenchmarkPgSetGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := ts.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
		if _, err := ts.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkPgSet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_set_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := ts.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}

func BenchmarkPgGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	// Pre-populate keys
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		ts.client.Set(ctx, key, "value", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		if _, err := ts.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkPgMSet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pairs := make([]interface{}, 20)
		for j := 0; j < 10; j++ {
			pairs[j*2] = fmt.Sprintf("mset_key_%d_%d", i, j)
			pairs[j*2+1] = fmt.Sprintf("value_%d", j)
		}
		if err := ts.client.MSet(ctx, pairs...).Err(); err != nil {
			b.Fatalf("MSET failed: %v", err)
		}
	}
}

func BenchmarkPgMGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	// Pre-populate
	keys := make([]string, 10)
	pairs := make([]interface{}, 20)
	for j := 0; j < 10; j++ {
		keys[j] = fmt.Sprintf("mget_key_%d", j)
		pairs[j*2] = keys[j]
		pairs[j*2+1] = fmt.Sprintf("value_%d", j)
	}
	ts.client.MSet(ctx, pairs...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.client.MGet(ctx, keys...).Result(); err != nil {
			b.Fatalf("MGET failed: %v", err)
		}
	}
}

func BenchmarkPgIncr(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()
	ts.client.Set(ctx, "incr_key", "0", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.client.Incr(ctx, "incr_key").Result(); err != nil {
			b.Fatalf("INCR failed: %v", err)
		}
	}
}

func BenchmarkPgHSetHGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field_%d", i)
		value := fmt.Sprintf("value_%d", i)

		if err := ts.client.HSet(ctx, "bench_hash", field, value).Err(); err != nil {
			b.Fatalf("HSET failed: %v", err)
		}
		if _, err := ts.client.HGet(ctx, "bench_hash", field).Result(); err != nil {
			b.Fatalf("HGET failed: %v", err)
		}
	}
}

func BenchmarkPgLPushLPop(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("value_%d", i)
		if err := ts.client.LPush(ctx, "bench_list", value).Err(); err != nil {
			b.Fatalf("LPUSH failed: %v", err)
		}
		if _, err := ts.client.LPop(ctx, "bench_list").Result(); err != nil {
			b.Fatalf("LPOP failed: %v", err)
		}
	}
}

func BenchmarkPgSAddSMembers(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member_%d", i)
		if err := ts.client.SAdd(ctx, "bench_set", member).Err(); err != nil {
			b.Fatalf("SADD failed: %v", err)
		}
	}
	// One SMEMBERS at the end
	if _, err := ts.client.SMembers(ctx, "bench_set").Result(); err != nil {
		b.Fatalf("SMEMBERS failed: %v", err)
	}
}

func BenchmarkPgPipeline(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := ts.client.Pipeline()
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("pipe_%d_%d", i, j)
			pipe.Set(ctx, key, "value", 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}
	}
}

// ============== New-ops benchmarks ==============
//
// These exercise the v0.24+/v0.26 hot-path additions: batched type checks
// behind HGETALL, the single-RTT multi-key BLPOP, and the CTE-based ZRANGE
// variants. They guard against regressions in the work that closed the gap
// against upstream Redis on those commands.

// BenchmarkPgHGetAll measures the single-RTT HGETALL path (one query that
// fetches both type metadata and field/value pairs) against a moderately
// sized hash. The hash is populated once in setup so the benchmark itself
// only times reads.
func BenchmarkPgHGetAll(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const fields = 50
	pairs := make([]interface{}, 0, fields*2)
	for i := 0; i < fields; i++ {
		pairs = append(pairs, fmt.Sprintf("field_%d", i), fmt.Sprintf("value_%d", i))
	}
	if err := ts.client.HSet(ctx, "bench_hgetall", pairs...).Err(); err != nil {
		b.Fatalf("HSET setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := ts.client.HGetAll(ctx, "bench_hgetall").Result()
		if err != nil {
			b.Fatalf("HGETALL failed: %v", err)
		}
		if len(out) != fields {
			b.Fatalf("HGETALL returned %d fields, want %d", len(out), fields)
		}
	}
}

// BenchmarkPgHMGet measures the batched HMGET path on a subset of fields
// from a populated hash.
func BenchmarkPgHMGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const fields = 100
	pairs := make([]interface{}, 0, fields*2)
	for i := 0; i < fields; i++ {
		pairs = append(pairs, fmt.Sprintf("field_%d", i), fmt.Sprintf("value_%d", i))
	}
	if err := ts.client.HSet(ctx, "bench_hmget", pairs...).Err(); err != nil {
		b.Fatalf("HSET setup failed: %v", err)
	}

	want := make([]string, 10)
	for i := range want {
		want[i] = fmt.Sprintf("field_%d", i*7%fields)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.client.HMGet(ctx, "bench_hmget", want...).Result(); err != nil {
			b.Fatalf("HMGET failed: %v", err)
		}
	}
}

// BenchmarkPgBLPOPMultiKey hits the multi-key BLPOP fast path: keys that
// already have data should return without ever entering the LISTEN/NOTIFY
// blocking loop. Five keys exercises the array_position CTE pattern that
// generalised popMulti.
func BenchmarkPgBLPOPMultiKey(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	keys := []string{"bl1", "bl2", "bl3", "bl4", "bl5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Push to one of the keys each iteration; BLPOP should pop without
		// blocking since data is already present.
		k := keys[i%len(keys)]
		if err := ts.client.RPush(ctx, k, "v").Err(); err != nil {
			b.Fatalf("RPUSH failed: %v", err)
		}
		if _, err := ts.client.BLPop(ctx, time.Second, keys...).Result(); err != nil {
			b.Fatalf("BLPOP failed: %v", err)
		}
	}
}

// BenchmarkPgLMPOP exercises the LMPOP COUNT path — N members popped from
// the first non-empty key in a single SQL round-trip.
func BenchmarkPgLMPOP(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const popCount = 5
	const keys = 3
	keyNames := []string{"lmp1", "lmp2", "lmp3"}

	// Pre-populate enough elements that every iteration finds something to pop.
	// We use one big push per key, sized for b.N iterations distributed across
	// the keys, so the timed loop is purely LMPOP cost.
	values := make([]interface{}, 0, b.N*popCount)
	for i := 0; i < b.N*popCount; i++ {
		values = append(values, fmt.Sprintf("v%d", i))
	}
	per := len(values) / keys
	for i, k := range keyNames {
		start := i * per
		end := start + per
		if i == keys-1 {
			end = len(values)
		}
		if end > start {
			if err := ts.client.RPush(ctx, k, values[start:end]...).Err(); err != nil {
				b.Fatalf("RPUSH setup failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ts.client.Do(ctx,
			"LMPOP", fmt.Sprintf("%d", keys), keyNames[0], keyNames[1], keyNames[2],
			"LEFT", "COUNT", fmt.Sprintf("%d", popCount),
		).Result()
		if err != nil {
			b.Fatalf("LMPOP failed at i=%d: %v", i, err)
		}
	}
}

// BenchmarkPgZRange hits the single-RTT ZRANGE path on a populated sorted
// set, returning a window that's large enough to dominate fixed query cost.
func BenchmarkPgZRange(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const members = 1000
	zs := make([]redis.Z, members)
	for i := 0; i < members; i++ {
		zs[i] = redis.Z{Score: float64(i), Member: fmt.Sprintf("m%04d", i)}
	}
	if err := ts.client.ZAdd(ctx, "bench_zrange", zs...).Err(); err != nil {
		b.Fatalf("ZADD setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := ts.client.ZRange(ctx, "bench_zrange", 0, 99).Result()
		if err != nil {
			b.Fatalf("ZRANGE failed: %v", err)
		}
		if len(out) != 100 {
			b.Fatalf("ZRANGE returned %d members, want 100", len(out))
		}
	}
}

// BenchmarkPgZRangeByScore exercises the score-bound query path with WITHSCORES.
func BenchmarkPgZRangeByScore(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const members = 1000
	zs := make([]redis.Z, members)
	for i := 0; i < members; i++ {
		zs[i] = redis.Z{Score: float64(i), Member: fmt.Sprintf("m%04d", i)}
	}
	if err := ts.client.ZAdd(ctx, "bench_zrbs", zs...).Err(); err != nil {
		b.Fatalf("ZADD setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ts.client.ZRangeByScoreWithScores(ctx, "bench_zrbs", &redis.ZRangeBy{
			Min:    "100",
			Max:    "199",
			Offset: 0,
			Count:  100,
		}).Result()
		if err != nil {
			b.Fatalf("ZRANGEBYSCORE failed: %v", err)
		}
	}
}

// BenchmarkPgZRangeStore exercises the destination-writing variant: the
// CTE that powers ZRANGE feeds an INSERT into kv_zsets in a single round-trip.
func BenchmarkPgZRangeStore(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	const members = 1000
	zs := make([]redis.Z, members)
	for i := 0; i < members; i++ {
		zs[i] = redis.Z{Score: float64(i), Member: fmt.Sprintf("m%04d", i)}
	}
	if err := ts.client.ZAdd(ctx, "bench_zrs_src", zs...).Err(); err != nil {
		b.Fatalf("ZADD setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := fmt.Sprintf("bench_zrs_dst_%d", i)
		if _, err := ts.client.Do(ctx, "ZRANGESTORE", dst, "bench_zrs_src", "0", "99").Result(); err != nil {
			b.Fatalf("ZRANGESTORE failed: %v", err)
		}
	}
}
