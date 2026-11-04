//go:build postgres

package pubsub

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mnorrsken/postkeys/internal/storage"
)

func testPubSubDatabaseURL() string {
	env := func(key, fallback string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return fallback
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(env("PG_USER", "postgres"), env("PG_PASSWORD", "testingpassword")), Host: net.JoinHostPort(env("PG_HOST", "localhost"), env("PG_PORT", "5789")), Path: "/" + env("PG_DATABASE", "postgres")}
	q := u.Query()
	q.Set("sslmode", env("PG_SSLMODE", "disable"))
	u.RawQuery = q.Encode()
	return u.String()
}

func testPubSubPool(t *testing.T, tracer pgx.QueryTracer) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	testDatabaseURL := testPubSubDatabaseURL()
	admin, err := pgx.Connect(ctx, testDatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("pubsub_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); _ = admin.Close(ctx) })
	connStr := testDatabaseURL + "&search_path=" + schema
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := storage.InitPubSubSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var persistence string
	if err := pool.QueryRow(ctx, "SELECT relpersistence::text FROM pg_class WHERE oid = 'pubsub_messages'::regclass").Scan(&persistence); err != nil {
		t.Fatal(err)
	}
	if persistence != "u" {
		t.Fatalf("transport table persistence %q, want UNLOGGED", persistence)
	}
	return pool, connStr
}

func TestPublishLargeCommitFailureRollsBackNotification(t *testing.T) {
	ctx := context.Background()
	pool, connStr := testPubSubPool(t, nil)
	_, err := pool.Exec(ctx, `CREATE FUNCTION reject_pubsub_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'deferred rejection'; END $$;
CREATE CONSTRAINT TRIGGER reject_pubsub_insert AFTER INSERT ON pubsub_messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_pubsub_insert()`)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(ctx)
	if _, err := listener.Exec(ctx, "LISTEN "+pgxIdentifier(broadcastChannel)); err != nil {
		t.Fatal(err)
	}
	hub := NewHub(pool, connStr)
	channel := fmt.Sprintf("rollback.%d", time.Now().UnixNano())
	if _, err := hub.Publish(ctx, channel, strings.Repeat("x", 8000)); err == nil {
		t.Fatal("expected commit failure")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pubsub_messages").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("got %d rows after failed commit", count)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	for {
		notification, err := listener.WaitForNotification(waitCtx)
		if err != nil {
			if waitCtx.Err() != context.DeadlineExceeded {
				t.Fatalf("waiting for notification: %v", err)
			}
			break
		}
		// LISTEN channels are database-wide, including other test schemas. Only a
		// notification for this transaction's unique channel would be a rollback leak.
		item, err := decodeDelivery(notification.Payload)
		if err == nil && (item.key == channelKey(channel) || item.channel == channel) {
			t.Fatalf("received notification after rollback: %v", notification)
		}
	}
}

type queryTrace struct{ queries chan string }

func (q *queryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	select {
	case q.queries <- data.SQL:
	default:
	}
	return ctx
}
func (*queryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func startTestHub(t *testing.T, pool *pgxpool.Pool, connStr string) *Hub {
	t.Helper()
	h := NewHub(pool, connStr)
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	return h
}
func notifyTestPayload(t *testing.T, sender *pgx.Conn, payload string) {
	t.Helper()
	if _, err := sender.Exec(context.Background(), "SELECT pg_notify($1,$2)", broadcastChannel, payload); err != nil {
		t.Fatal(err)
	}
}
func TestReferenceFetchIndependentPoolAndOrderedDelivery(t *testing.T) {
	ctx := context.Background()
	pool, connStr := testPubSubPool(t, nil)
	h := startTestHub(t, pool, connStr)
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10)}
	h.Subscribe(sub, "slow")
	independent := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 2}
	h.Subscribe(independent, "fast")
	// Store the reference first, then occupy every dedicated fetch connection.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.StorePubSubMessage(ctx, tx, "slow", "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	held := make([]*pgxpool.Conn, 0, 4)
	for i := 0; i < 4; i++ {
		c, err := h.fetchPool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	defer func() {
		for _, c := range held {
			if c != nil {
				c.Release()
			}
		}
	}()
	sender, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	notifyTestPayload(t, sender, encodeReference(id, "slow", 5))
	second, _ := encodeInline("slow", "second")
	notifyTestPayload(t, sender, second)
	fast, _ := encodeInline("fast", "independent")
	notifyTestPayload(t, sender, fast)
	awaitMessage(t, independent, "fast", "independent")
	// Occupy the sole shared client-pool connection too. Delivery must still
	// succeed when one dedicated fetch connection becomes available.
	shared, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Release()
	held[0].Release()
	held[0] = nil
	awaitMessage(t, sub, "slow", "first")
	awaitMessage(t, sub, "slow", "second")
	// A corrupt length declaration must be skipped, then release its client FIFO
	// and byte reservation so subsequent valid inline delivery can proceed.
	notifyTestPayload(t, sender, encodeReference(id, "slow", 4))
	after, _ := encodeInline("slow", "after mismatch")
	notifyTestPayload(t, sender, after)
	awaitMessage(t, sub, "slow", "after mismatch")
}
func TestStopCancelsPendingReferenceFetch(t *testing.T) {
	ctx := context.Background()
	pool, connStr := testPubSubPool(t, nil)
	h := NewHub(pool, connStr)
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	held := make([]*pgxpool.Conn, 0, 4)
	for i := 0; i < 4; i++ {
		c, err := h.fetchPool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10)}
	h.Subscribe(sub, "slow")
	independent := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 2}
	h.Subscribe(independent, "fast")
	sender, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	notifyTestPayload(t, sender, encodeReference(1, "slow", 0))
	fast, _ := encodeInline("fast", "marker")
	notifyTestPayload(t, sender, fast)
	awaitMessage(t, independent, "fast", "marker")
	stopped := make(chan struct{})
	go func() { h.Stop(); close(stopped) }()
	// Workers must exit while all fetch connections remain unavailable. Stop's
	// pool.Close also waits for borrowed test connections, so release those only
	// after proving cancellation stopped the pending worker.
	workersStopped := make(chan struct{})
	go func() { h.wg.Wait(); close(workersStopped) }()
	select {
	case <-workersStopped:
	case <-time.After(time.Second):
		t.Error("pending fetch worker ignored shutdown cancellation")
	}
	for _, c := range held {
		c.Release()
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel pending reference fetch")
	}
}
func TestUninterestedReferencesNeverReadMessageBody(t *testing.T) {
	ctx := context.Background()
	pool, connStr := testPubSubPool(t, nil)
	h := startTestHub(t, pool, connStr)
	// Install a tracer before publishing any notification. Channel sends provide
	// synchronization with workers; no worker has accessed fetchPool yet.
	cfg := h.fetchPool.Config()
	h.fetchPool.Close()
	trace := &queryTrace{queries: make(chan string, 20)}
	cfg.ConnConfig.Tracer = trace
	var err error
	h.fetchPool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10)}
	h.PSubscribe(sub, "never:*")
	// An unknown, huge binary channel conservatively reserves pattern clients,
	// then fetches metadata but skips the unmatched message body.
	channel := strings.Repeat("\x00\xff", 8000)
	if _, err := h.Publish(ctx, channel, strings.Repeat("x", 8000)); err != nil {
		t.Fatal(err)
	}
	select {
	case query := <-trace.queries:
		if !strings.HasPrefix(query, "SELECT channel FROM") {
			t.Fatalf("unexpected query: %s", query)
		}
	case <-time.After(time.Second):
		t.Fatal("no metadata query")
	}
	// A synthetic inline sentinel on the same routing key waits behind the
	// reference. Its delivery proves the worker completed its interest check;
	// no sleep or change to the reference's matching subscribers is needed.
	h.incoming <- delivery{key: channelKey(channel), channel: "never:marker", message: "done", known: true}
	awaitMessage(t, sub, "never:*\x00never:marker", "done")
	select {
	case query := <-trace.queries:
		t.Fatalf("uninterested replica fetched body: %s", query)
	default:
	}

	// Interested oversized channels still round trip all binary bytes.
	h.Subscribe(sub, channel)
	if _, err := h.Publish(ctx, channel, "binary\x00\xff"); err != nil {
		t.Fatal(err)
	}
	awaitMessage(t, sub, channel, "binary\x00\xff")
	// A corrupt fallback channel length must skip delivery and release all
	// conservative pattern reservations, just like a failed metadata query.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.StorePubSubMessage(ctx, tx, channel, "mismatch")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	sender, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	corrupt := fmt.Sprintf(`!pk1:{"r":%d,"s":8,"n":%d,"k":"%s"}`, id, len(channel)+1, channelKey(channel))
	notifyTestPayload(t, sender, corrupt)
	marker, _ := encodeInline("never:marker", "channel mismatch released")
	notifyTestPayload(t, sender, marker)
	awaitMessage(t, sub, "never:*\x00never:marker", "channel mismatch released")
}

func TestStalledSubscriberDoesNotBlockPublish(t *testing.T) {
	for _, pattern := range []bool{false, true} {
		t.Run(fmt.Sprintf("pattern=%t", pattern), func(t *testing.T) {
			pool, connStr := testPubSubPool(t, nil)
			h := NewHub(pool, connStr)
			stalled := &blockingSubscriber{entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
			if pattern {
				h.PSubscribe(stalled, "r*")
			} else {
				h.Subscribe(stalled, "route")
			}
			targets, _ := h.snapshotTargets(delivery{channel: "route", known: true}, 10)
			go h.sendTarget(targets[0], delivery{channel: "route", message: "stalled"})
			<-stalled.entered
			released := false
			defer func() {
				if !released {
					close(stalled.release)
				}
			}()
			removed := make(chan struct{})
			go func() {
				if pattern {
					h.PUnsubscribe(stalled, "r*")
				} else {
					h.Unsubscribe(stalled, "route")
				}
				close(removed)
			}()
			awaitDetachedSubscriptions(t, h, stalled.GetID())
			select {
			case <-removed:
				t.Fatal("removal returned while send still pending")
			default:
			}
			independent := &recordingSubscriber{id: 2, messages: make(chan receivedMessage, 2)}
			published := make(chan error, 1)
			go func() {
				h.Subscribe(independent, "fast")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				count, err := h.Publish(ctx, "fast", "independent")
				if err == nil && count != 1 {
					err = fmt.Errorf("publish subscriber count %d", count)
				}
				published <- err
			}()
			select {
			case err := <-published:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("PUBLISH waited on an unrelated client's writer")
			}
			close(stalled.release)
			released = true
			select {
			case <-removed:
			case <-time.After(time.Second):
				t.Fatal("unsubscribe did not finish after send released")
			}
		})
	}
}
