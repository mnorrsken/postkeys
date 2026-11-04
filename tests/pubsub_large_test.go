//go:build postgres

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPubSubLargeMessagesAcrossServers(t *testing.T) {
	srv1, store1, addr1 := newPubSubTestServer(t)
	srv2, store2, addr2 := newPubSubTestServer(t)
	defer func() { srv1.Stop(); srv2.Stop(); store1.Close(); store2.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client1 := redis.NewClient(&redis.Options{Addr: addr1})
	client2 := redis.NewClient(&redis.Options{Addr: addr2})
	defer client1.Close()
	defer client2.Close()

	channel := "large." + string([]byte{0xff, 0xfe}) + ".channel"
	pattern := "large.*"
	sub1 := client1.Subscribe(ctx, channel)
	sub2 := client2.Subscribe(ctx, channel)
	pattern1 := client1.PSubscribe(ctx, pattern)
	pattern2 := client2.PSubscribe(ctx, pattern)
	defer sub1.Close()
	defer sub2.Close()
	defer pattern1.Close()
	defer pattern2.Close()
	for _, sub := range []*redis.PubSub{sub1, sub2, pattern1, pattern2} {
		if _, err := sub.Receive(ctx); err != nil {
			t.Fatalf("subscription acknowledgement: %v", err)
		}
	}

	binary := string([]byte{0, 0xff, 0xfe, '\r', '\n', 'x'})
	messages := []string{"before", strings.Repeat(binary, 5120/len(binary)+1)[:5120], strings.Repeat(binary, 65536/len(binary)+1)[:65536], strings.Repeat(binary, 1048576/len(binary)+1)[:1048576], "after"}
	messages = append(messages, messages[2], messages[2])
	receivers := []struct {
		name    string
		sub     *redis.PubSub
		pattern string
	}{{"server 1 exact", sub1, ""}, {"server 2 exact", sub2, ""}, {"server 1 pattern", pattern1, pattern}, {"server 2 pattern", pattern2, pattern}}
	results := make(chan error, len(receivers))
	for _, item := range receivers {
		go func() {
			for i, payload := range messages {
				msg, err := item.sub.ReceiveMessage(ctx)
				if err != nil {
					results <- fmt.Errorf("%s receive %d: %w", item.name, i, err)
					return
				}
				if msg.Channel != channel || msg.Pattern != item.pattern || msg.Payload != payload {
					results <- fmt.Errorf("%s receive %d: channel/pattern/payload mismatch (payload length %d, want %d)", item.name, i, len(msg.Payload), len(payload))
					return
				}
			}
			results <- nil
		}()
	}
	for i, payload := range messages {
		count, err := client1.Publish(ctx, channel, payload).Result()
		if err != nil {
			t.Fatalf("publish %d (%d bytes): %v", i, len(payload), err)
		}
		if count != 2 {
			t.Fatalf("publish %d: local subscriber count = %d, want 2", i, count)
		}
	}
	for range receivers {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for subscribers: %v", ctx.Err())
		}
	}
}

func TestPubSubLongChannelAndMissingReference(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer func() { srv.Stop(); store.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	channel := "long." + strings.Repeat("a", 9000)
	sub := client.Subscribe(ctx, channel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscription acknowledgement: %v", err)
	}
	missing := fmt.Sprintf(`!pk1:{"r":9223372036854775807,"s":0,"n":%d,"k":"%x"}`, len(channel), sha256.Sum256([]byte(channel)))
	if _, err := store.Pool().Exec(ctx, "SELECT pg_notify($1, $2)", "postkeys_pubsub", missing); err != nil {
		t.Fatalf("notify missing reference: %v", err)
	}
	if count, err := client.Publish(ctx, channel, "still alive").Result(); err != nil || count != 1 {
		t.Fatalf("publish: count %d, error %v", count, err)
	}
	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if msg.Channel != channel || msg.Payload != "still alive" {
		t.Fatal("long channel or payload mismatch")
	}
	if count, err := client.Publish(ctx, "no-subscribers-large", strings.Repeat("x", 65536)).Result(); err != nil || count != 0 {
		t.Fatalf("publish without subscribers: count %d, error %v", count, err)
	}
}
