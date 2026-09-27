//go:build postgres

package integration_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reliability tests: concurrency, blocking commands and connection lifecycle.
// These exercise the failure modes seen on long-running deployments
// (job queues with many workers, clients that disconnect while blocked).

// writeRawCommand writes cmd as a RESP array on conn.
func writeRawCommand(t *testing.T, conn net.Conn, args ...string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
}

// Sidekiq's scheduled-job poller runs this script from every process at once.
// Each job must be handed out exactly once.
func TestConcurrentEvalZPopByScoreNoDuplicates(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	const jobs = 200
	members := make([]redis.Z, jobs)
	for i := range members {
		members[i] = redis.Z{Score: float64(i), Member: fmt.Sprintf("job-%d", i)}
	}
	if err := ts.client.ZAdd(ctx, "schedule", members...).Err(); err != nil {
		t.Fatal(err)
	}

	script := redis.NewScript(`
		local key, now = KEYS[1], ARGV[1]
		local jobs = redis.call("zrange", key, "-inf", now, "byscore", "limit", 0, 1)
		if jobs[1] then
			redis.call("zrem", key, jobs[1])
			return jobs[1]
		end
	`)

	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, err := script.Run(ctx, ts.client, []string{"schedule"}, jobs).Text()
				if err == redis.Nil {
					return
				}
				if err != nil {
					t.Errorf("script: %v", err)
					return
				}
				mu.Lock()
				seen[v]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Errorf("got %d distinct jobs, want %d", len(seen), jobs)
	}
	for job, n := range seen {
		if n > 1 {
			t.Errorf("%s handed out %d times", job, n)
		}
	}
}

// Blocking commands inside MULTI must not block (Redis runs them as if the
// timeout already expired). Blocking here would hold a Postgres transaction
// and a pool connection forever.
func TestBlockingPopInMultiDoesNotBlock(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmds, err := ts.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.BLPop(ctx, 0, "empty-list")
		p.BRPop(ctx, 0, "empty-list")
		p.BLMPop(ctx, 0, "left", 1, "empty-list")
		p.BZMPop(ctx, 0, "min", 1, "empty-zset")
		p.Set(ctx, "after", "1", 0)
		return nil
	})
	if ctx.Err() != nil {
		t.Fatal("EXEC with blocking commands blocked")
	}
	if err != nil && err != redis.Nil {
		t.Fatalf("EXEC: %v", err)
	}
	for _, c := range cmds[:4] {
		if c.Err() != redis.Nil {
			t.Errorf("%s: want nil reply, got %v", c.Name(), c.Err())
		}
	}
	if v, _ := ts.client.Get(ctx, "after").Result(); v != "1" {
		t.Errorf("command after blocking pops not executed, GET after = %q", v)
	}
}

func TestBlockingPopInEvalDoesNotBlock(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := ts.client.Eval(ctx, `return redis.call("blpop", KEYS[1], 0)`, []string{"empty-list"}).Err()
	if ctx.Err() != nil {
		t.Fatal("BLPOP inside EVAL blocked")
	}
	if err != redis.Nil {
		t.Errorf("want nil reply, got %v", err)
	}
}

// A client that disconnects while blocked must stop waiting. Otherwise the
// orphaned wait pops the next pushed item and the item is lost.
func TestBlockedClientDisconnectDoesNotLoseItem(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	for _, cmd := range [][]string{
		{"BLPOP", "q-blpop", "0"},
		{"BRPOP", "q-brpop", "0"},
		{"BLMPOP", "0", "1", "q-blmpop", "LEFT"},
	} {
		key := cmd[1]
		if cmd[0] == "BLMPOP" {
			key = cmd[3]
		}
		conn, err := net.Dial("tcp", ts.addr)
		if err != nil {
			t.Fatal(err)
		}
		writeRawCommand(t, conn, cmd...)
		time.Sleep(200 * time.Millisecond) // let the server start blocking
		conn.Close()
		time.Sleep(200 * time.Millisecond) // let the server notice

		if err := ts.client.RPush(ctx, key, "item").Err(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond) // several poll intervals
		if n, _ := ts.client.LLen(ctx, key).Result(); n != 1 {
			t.Errorf("%s: disconnected client consumed the item (LLEN=%d)", cmd[0], n)
		}
	}
}

// Every pushed job must be popped exactly once by competing blocking workers.
func TestConcurrentBlockingWorkersExactlyOnce(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	const items = 300
	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := ts.client.BRPop(ctx, time.Second, "queue:default").Result()
				if err == redis.Nil {
					return
				}
				if err != nil {
					t.Errorf("BRPOP: %v", err)
					return
				}
				mu.Lock()
				seen[res[1]]++
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < items; i++ {
		if err := ts.client.LPush(ctx, "queue:default", fmt.Sprintf("job-%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()

	if len(seen) != items {
		t.Errorf("got %d distinct items, want %d", len(seen), items)
	}
	for item, n := range seen {
		if n > 1 {
			t.Errorf("%s popped %d times", item, n)
		}
	}
}

// Concurrent transactions touching the same keys in opposite order make
// Postgres detect deadlocks. EXEC must retry them instead of failing.
func TestConcurrentMultiExecOppositeKeyOrder(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	const workers, rounds = 8, 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			first, second := "acct:a", "acct:b"
			if w%2 == 1 {
				first, second = second, first
			}
			for i := 0; i < rounds; i++ {
				_, err := ts.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.IncrBy(ctx, first, 1)
					p.HIncrBy(ctx, first+":h", "n", 1)
					p.IncrBy(ctx, second, 1)
					p.HIncrBy(ctx, second+":h", "n", 1)
					return nil
				})
				if err != nil {
					t.Errorf("EXEC: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	want := int64(workers * rounds)
	for _, k := range []string{"acct:a", "acct:b"} {
		if n, _ := ts.client.Get(ctx, k).Int64(); n != want {
			t.Errorf("%s = %d, want %d", k, n, want)
		}
		if n, _ := ts.client.HGet(ctx, k+":h", "n").Int64(); n != want {
			t.Errorf("%s:h n = %d, want %d", k, n, want)
		}
	}
}

// Concurrent INCR-style Lua scripts (rate limiters) must not lose updates.
func TestConcurrentEvalCounterNoLostUpdates(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	script := redis.NewScript(`
		local v = tonumber(redis.call("get", KEYS[1]) or "0")
		redis.call("set", KEYS[1], v + 1)
		return v + 1
	`)
	const workers, rounds = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if err := script.Run(ctx, ts.client, []string{"counter"}).Err(); err != nil {
					t.Errorf("script: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n, _ := ts.client.Get(ctx, "counter").Int(); n != workers*rounds {
		t.Errorf("counter = %d, want %d", n, workers*rounds)
	}
}

// Concurrent PFADDs read, merge and write the registers; without the key lock
// taken before the read, most of them overwrite each other.
func TestConcurrentPFAddNoLostUpdates(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	const workers, per = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := ts.client.PFAdd(ctx, "hll", fmt.Sprintf("w%d-%d", w, i)).Err(); err != nil {
					t.Errorf("PFADD: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	// HyperLogLog is approximate; lost updates show up as a far lower count.
	n, _ := ts.client.PFCount(ctx, "hll").Result()
	if n < workers*per*9/10 {
		t.Errorf("PFCOUNT = %d, want about %d", n, workers*per)
	}
}

// Mixed multi-key writes, MULTI/EXEC and expiry cleanup running against each
// other must not surface deadlock errors to clients.
func TestConcurrentMixedWorkloadNoErrors(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				k := fmt.Sprintf("mix:%d", i%5)
				var err error
				switch (w + i) % 6 {
				case 0:
					err = ts.client.MSet(ctx, k+":s1", i, k+":s2", i).Err()
				case 1:
					err = ts.client.MSet(ctx, k+":s2", i, k+":s1", i).Err()
				case 2:
					err = ts.client.Del(ctx, k+":s1", k+":s2", k+":l").Err()
				case 3:
					err = ts.client.RPush(ctx, k+":l", i).Err()
				case 4:
					err = ts.client.Expire(ctx, k+":s1", time.Second).Err()
				case 5:
					_, err = ts.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
						p.Set(ctx, k+":s2", i, 0)
						p.RPush(ctx, k+":l", i)
						p.Set(ctx, k+":s1", i, 0)
						return nil
					})
				}
				if err != nil && err != redis.Nil {
					select {
					case errs <- fmt.Errorf("worker %d op %d: %w", w, (w+i)%6, err):
					default:
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Stop and DrainConnections must not wait forever for a client blocked in
// BLPOP with no timeout.
func TestShutdownWithBlockedClient(t *testing.T) {
	for _, mode := range []string{"stop", "drain"} {
		t.Run(mode, func(t *testing.T) {
			ts := newTestServer(t, "")
			defer ts.store.Close()
			defer ts.client.Close()

			conn, err := net.Dial("tcp", ts.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			writeRawCommand(t, conn, "BLPOP", "never-filled", "0")
			time.Sleep(200 * time.Millisecond)

			done := make(chan struct{})
			go func() {
				if mode == "stop" {
					ts.server.Stop()
				} else {
					ts.server.CloseListener()
					ts.server.DrainConnections(200 * time.Millisecond)
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s hung on a blocked client", mode)
			}
			if mode == "drain" {
				// The blocked client gets its nil reply before the close.
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				line, _ := bufio.NewReader(conn).ReadString('\n')
				if !strings.HasPrefix(line, "$-1") && !strings.HasPrefix(line, "*-1") {
					t.Errorf("drained BLPOP reply = %q, want nil", line)
				}
				ts.server.Stop()
			}
		})
	}
}

// A pipelined command after a blocking one must still be answered.
func TestBlockingPopFollowedByPipelinedCommand(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	conn, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeRawCommand(t, conn, "BLPOP", "pipelined", "1")
	writeRawCommand(t, conn, "PING")

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	first, _ := r.ReadString('\n')
	second, _ := r.ReadString('\n')
	if !strings.HasPrefix(first, "$-1") && !strings.HasPrefix(first, "*-1") {
		t.Errorf("BLPOP reply = %q, want nil after timeout", first)
	}
	if second != "+PONG\r\n" {
		t.Errorf("PING reply = %q, want +PONG", second)
	}
}
