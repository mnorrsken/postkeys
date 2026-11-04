//go:build postgres

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPubSubCleanup(t *testing.T) {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig("postgres://postgres:testingpassword@localhost:5789/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("pubsub_cleanup_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := &Store{pool: pool}
	if err := s.initSchema(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO pubsub_messages(channel, message, expires_at)
 SELECT decode('00ff','hex'), decode('ff00','hex'), now() - interval '1 hour' + n * interval '1 microsecond' FROM generate_series(1,1002) n`)
	if err != nil {
		t.Fatal(err)
	}
	var lockedID int64
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, `SELECT id FROM pubsub_messages ORDER BY expires_at LIMIT 1 FOR UPDATE`).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO pubsub_messages(channel, message, expires_at) VALUES ('x','y',now() + interval '1 hour')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FlushDB(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteExpiredPubSubMessages(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pubsub_messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("after first cleanup got %d rows, want locked, one expired and one live", count)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteExpiredPubSubMessages(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pubsub_messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("after second cleanup got %d rows, want one live", count)
	}
}
