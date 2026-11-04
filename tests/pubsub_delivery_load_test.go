//go:build postgres

package integration

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/redis/go-redis/v9"
)

func TestPubSubSlowSubscriberDelivery(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer func() { srv.Stop(); store.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	slow, err := net.DialTCP("tcp", nil, mustTCPAddress(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if err := slow.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	sendCommand(slow, "SUBSCRIBE", "load-slow-reader")
	if _, err := resp.NewReader(slow).Read(); err != nil {
		t.Fatal(err)
	}
	healthy := redis.NewClient(&redis.Options{Addr: addr})
	defer healthy.Close()
	sub := healthy.Subscribe(ctx, "load-slow-reader")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	const messages = 1000
	received := make(chan int, 1)
	var observed atomic.Int64
	go func() {
		n := 0
		for n < messages {
			if _, err := sub.ReceiveMessage(ctx); err != nil {
				break
			}
			n++
			observed.Add(1)
		}
		received <- n
	}()
	publisher := redis.NewClient(&redis.Options{Addr: addr})
	defer publisher.Close()
	start := time.Now()
	_, err = publisher.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < messages; i++ {
			pipe.Publish(ctx, "load-slow-reader", strings.Repeat("x", 3000))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-received:
		t.Logf("healthy delivery with stalled TCP reader: %d/%d in %s", n, messages, time.Since(start))
		if n != messages {
			t.Fatalf("healthy subscriber lost %d messages", messages-n)
		}
	case <-ctx.Done():
		t.Fatalf("healthy delivery with stalled TCP reader: %d/%d in %s", observed.Load(), messages, time.Since(start))
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const clients = 50
	const messages = 3000
	results := make(chan int, clients)
	var observed atomic.Int64
	for i := 0; i < clients; i++ {
		client := redis.NewClient(&redis.Options{Addr: addr})
		defer client.Close()
		sub := client.Subscribe(ctx, "load-healthy-fanout")
		defer sub.Close()
		if _, err := sub.Receive(ctx); err != nil {
			t.Fatal(err)
		}
		go func() {
			n := 0
			for n < messages {
				if _, err := sub.ReceiveMessage(ctx); err != nil {
					break
				}
				n++
				observed.Add(1)
			}
			results <- n
		}()
	}
	publisher := redis.NewClient(&redis.Options{Addr: addr})
	defer publisher.Close()
	start := time.Now()
	if _, err := publisher.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < messages; i++ {
			pipe.Publish(ctx, "load-healthy-fanout", "x")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	total := 0
	for i := 0; i < clients; i++ {
		select {
		case n := <-results:
			total += n
		case <-ctx.Done():
			t.Fatalf("fanout timed out after %d/%d deliveries", observed.Load(), clients*messages)
		}
	}
	t.Logf("healthy fanout: %d/%d in %s", total, clients*messages, time.Since(start))
	if total != clients*messages {
		t.Fatalf("healthy fanout lost %d messages", clients*messages-total)
	}
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
	publisher := redis.NewClient(&redis.Options{Addr: addr})
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
