//go:build postgres

package integration

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/redis/go-redis/v9"
)

// A pipeline's read timeout covers its entire reply stream. Use the existing
// test budget rather than a short default, and never replay non-idempotent
// PUBLISH commands when a reply times out.
func pubsubLoadClient(addr string, budget time.Duration, connections *atomic.Int64) *redis.Client {
	opts := &redis.Options{Addr: addr, ReadTimeout: budget, WriteTimeout: budget, ContextTimeoutEnabled: true, MaxRetries: -1}
	if connections != nil {
		opts.OnConnect = func(context.Context, *redis.Conn) error { connections.Add(1); return nil }
	}
	return redis.NewClient(opts)
}
func loadReceive(ctx context.Context, sub *redis.PubSub) (interface{}, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("load receiver requires a deadline")
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	return sub.ReceiveTimeout(ctx, timeout)
}

type loadResult struct {
	received int
	err      error
}

func receiveOrderedLoad(ctx context.Context, sub *redis.PubSub, channel string, messages int, payload func(int) string, observed *atomic.Int64) loadResult {
	result := loadResult{}
	for ordinal := 0; ordinal <= messages; ordinal++ {
		frame, err := loadReceive(ctx, sub)
		if err != nil {
			result.err = fmt.Errorf("receive ordinal %d: %w", ordinal, err)
			return result
		}
		msg, ok := frame.(*redis.Message)
		if !ok {
			result.err = fmt.Errorf("ordinal %d: unexpected frame %T", ordinal, frame)
			return result
		}
		want := "end:" + strconv.Itoa(messages)
		if ordinal < messages {
			want = payload(ordinal)
		}
		if msg.Channel != channel || msg.Pattern != "" || msg.Payload != want {
			result.err = fmt.Errorf("ordinal %d: got channel %q payload %.16q, want channel %q payload %.16q", ordinal, msg.Channel, msg.Payload, channel, want)
			return result
		}
		if ordinal < messages {
			result.received++
			observed.Add(1)
		}
	}
	return result
}
func subscribeLoad(ctx context.Context, client *redis.Client, channel string) (*redis.PubSub, error) {
	sub := client.Subscribe(ctx, channel)
	frame, err := loadReceive(ctx, sub)
	if err != nil {
		sub.Close()
		return nil, err
	}
	ack, ok := frame.(*redis.Subscription)
	if !ok || ack.Kind != "subscribe" || ack.Channel != channel {
		sub.Close()
		return nil, fmt.Errorf("unexpected subscription acknowledgement %v", frame)
	}
	return sub, nil
}
func publishOrderedLoad(ctx context.Context, publisher *redis.Client, channel string, messages int, payload func(int) string) ([]redis.Cmder, error) {
	return publisher.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < messages; i++ {
			pipe.Publish(ctx, channel, payload(i))
		}
		// The fence must follow every ordinal on the same publisher connection.
		// Readers reject duplicates/omissions even after their Nth ordinary message.
		pipe.Publish(ctx, channel, "end:"+strconv.Itoa(messages))
		return nil
	})
}
func TestPubSubSlowSubscriberDelivery(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer func() { srv.Stop(); store.Close() }()
	const budget = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	slow, err := net.DialTCP("tcp", nil, mustTCPAddress(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if err := slow.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := slow.SetReadDeadline(time.Now().Add(budget)); err != nil {
		t.Fatal(err)
	}
	const channel = "load-slow-reader"
	sendCommand(slow, "SUBSCRIBE", channel)
	if _, err := resp.NewReader(slow).Read(); err != nil {
		t.Fatal(err)
	}
	if err := slow.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	healthy := pubsubLoadClient(addr, budget, nil)
	defer healthy.Close()
	sub, err := subscribeLoad(ctx, healthy, channel)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	const messages = 1000
	padding := strings.Repeat("x", 2994)
	payload := func(i int) string { return fmt.Sprintf("%06d", i) + padding }
	var observed atomic.Int64
	received := make(chan loadResult, 1)
	go func() { received <- receiveOrderedLoad(ctx, sub, channel, messages, payload, &observed) }()
	var connections atomic.Int64
	publisher := pubsubLoadClient(addr, budget, &connections)
	defer publisher.Close()
	start := time.Now()
	if _, err := publishOrderedLoad(ctx, publisher, channel, messages, payload); err != nil {
		t.Fatalf("single-attempt publisher after %s (%d received, %d connections): %v", time.Since(start), observed.Load(), connections.Load(), err)
	}
	select {
	case result := <-received:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.received != messages {
			t.Fatalf("received %d/%d", result.received, messages)
		}
	case <-ctx.Done():
		t.Fatalf("healthy delivery %d/%d: %v", observed.Load(), messages, ctx.Err())
	}
	if connections.Load() != 1 {
		t.Fatalf("publisher used %d connections; retry/reconnect would invalidate exactly-once evidence", connections.Load())
	}
	t.Logf("ordered healthy delivery with stalled TCP reader: %d/%d plus fence in %s; publisher connections %d, retries %d", observed.Load(), messages, time.Since(start), connections.Load(), publisher.Options().MaxRetries)
}
func mustTCPAddress(t *testing.T, address string) *net.TCPAddr {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}
func TestPubSubHealthyFanoutDelivery(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer func() { srv.Stop(); store.Close() }()
	const budget = 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	const clients = 50
	const messages = 3000
	const channel = "load-healthy-fanout"
	results := make(chan loadResult, clients)
	var observed atomic.Int64
	payload := func(i int) string { return strconv.Itoa(i) }
	for i := 0; i < clients; i++ {
		client := pubsubLoadClient(addr, budget, nil)
		defer client.Close()
		sub, err := subscribeLoad(ctx, client, channel)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		go func(id int, subscription *redis.PubSub) {
			result := receiveOrderedLoad(ctx, subscription, channel, messages, payload, &observed)
			if result.err != nil {
				result.err = fmt.Errorf("client %d: %w", id, result.err)
			}
			results <- result
		}(i, sub)
	}
	var connections atomic.Int64
	publisher := pubsubLoadClient(addr, budget, &connections)
	defer publisher.Close()
	start := time.Now()
	commands, err := publishOrderedLoad(ctx, publisher, channel, messages, payload)
	if err != nil {
		t.Fatalf("single-attempt publisher after %s (%d received, %d connections): %v", time.Since(start), observed.Load(), connections.Load(), err)
	}
	for ordinal, command := range commands {
		count, err := command.(*redis.IntCmd).Result()
		if err != nil || count != clients {
			t.Fatalf("publish ordinal %d local receivers %d, error %v", ordinal, count, err)
		}
	}
	total := 0
	for i := 0; i < clients; i++ {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatal(result.err)
			}
			total += result.received
		case <-ctx.Done():
			t.Fatalf("fanout %d/%d: %v", observed.Load(), clients*messages, ctx.Err())
		}
	}
	if total != clients*messages {
		t.Fatalf("healthy fanout lost %d messages", clients*messages-total)
	}
	if connections.Load() != 1 {
		t.Fatalf("publisher used %d connections; retry/reconnect would invalidate exactly-once evidence", connections.Load())
	}
	t.Logf("ordered healthy fanout: %d/%d plus %d fences in %s; publisher connections %d, retries %d", total, clients*messages, clients, time.Since(start), connections.Load(), publisher.Options().MaxRetries)
}

func TestPubSubStopInterruptsOrdinaryRESP3Writer(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	stopped := false
	defer func() {
		if !stopped {
			srv.Stop()
		}
		store.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publisher := pubsubLoadClient(addr, 10*time.Second, nil)
	defer publisher.Close()
	if err := publisher.Set(ctx, "blocked-resp3-response", strings.Repeat("x", 8<<20), 0).Err(); err != nil {
		t.Fatal(err)
	}
	client, err := net.DialTCP("tcp", nil, mustTCPAddress(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	reader := resp.NewReader(client)
	sendCommand(client, "HELLO", "3")
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}
	sendCommand(client, "SUBSCRIBE", "stop-ordinary-writer")
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}
	sendCommand(client, "GET", "blocked-resp3-response")
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Read just the first byte, then stop draining the multi-megabyte ordinary
	// response. Its writerMu stays held while pub/sub needs the same writer.
	first := make([]byte, 1)
	if _, err := client.Read(first); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, "stop-ordinary-writer", "queued push").Result(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { srv.Stop(); close(done) }()
	select {
	case <-done:
		stopped = true
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waited behind an ordinary RESP3 response writer")
	}
}
