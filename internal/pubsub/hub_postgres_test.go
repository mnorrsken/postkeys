//go:build postgres

package pubsub

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseURL = "postgres://postgres:testingpassword@localhost:5789/postgres?sslmode=disable"

func testPubSubPool(t *testing.T, tracer pgx.QueryTracer) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
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
	if _, err := pool.Exec(ctx, `CREATE TABLE pubsub_messages (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, channel BYTEA NOT NULL, message BYTEA NOT NULL, expires_at TIMESTAMPTZ NOT NULL)`); err != nil {
		t.Fatal(err)
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
	if _, err := hub.Publish(ctx, "channel", strings.Repeat("x", 8000)); err == nil {
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
	if notification, err := listener.WaitForNotification(waitCtx); err == nil {
		t.Fatalf("received notification after rollback: %v", notification)
	} else if waitCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("waiting for notification: %v", err)
	}
}

type acquireTrace struct{ starts chan struct{} }

func (a *acquireTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (a *acquireTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (a *acquireTrace) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	select {
	case a.starts <- struct{}{}:
	default:
	}
	return ctx
}
func (a *acquireTrace) TraceAcquireEnd(context.Context, *pgxpool.Pool, pgxpool.TraceAcquireEndData) {}

func TestStopCancelsPendingReferenceFetch(t *testing.T) {
	ctx := context.Background()
	trace := &acquireTrace{starts: make(chan struct{}, 1)}
	pool, connStr := testPubSubPool(t, trace)
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	// Discard acquisition traces from setup and the held connection.
	select {
	case <-trace.starts:
	default:
	}
	hub := NewHub(pool, connStr)
	if err := hub.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	defer func() {
		select {
		case <-stopped:
		default:
			hub.Stop()
		}
	}()
	sender, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	payload := base64.StdEncoding.EncodeToString([]byte(`{"r":"1"}`))
	if _, err := sender.Exec(ctx, "SELECT pg_notify($1, $2)", broadcastChannel, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-trace.starts:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not attempt reference fetch")
	}
	if pool.Stat().AcquiredConns() != 1 {
		t.Fatal("pool connection was not held")
	}
	go func() { hub.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel pending fetch")
	}
}
