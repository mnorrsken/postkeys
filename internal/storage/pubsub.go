package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InitPubSubSchema initializes the ephemeral transport schema used by Store and
// isolated transport tests. Rows may disappear after PostgreSQL crash recovery.
func InitPubSubSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `CREATE UNLOGGED TABLE IF NOT EXISTS pubsub_messages (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 channel BYTEA NOT NULL, message BYTEA NOT NULL, expires_at TIMESTAMPTZ NOT NULL);
 CREATE INDEX IF NOT EXISTS idx_pubsub_messages_expires ON pubsub_messages(expires_at);`)
	return err
}

// StorePubSubMessage stores a body in the transaction that will issue NOTIFY.
func StorePubSubMessage(ctx context.Context, tx pgx.Tx, channel, message string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO pubsub_messages (channel, message, expires_at)
 VALUES ($1, $2, clock_timestamp() + interval '5 minutes') RETURNING id`, []byte(channel), []byte(message)).Scan(&id)
	return id, err
}

// PubSubChannel reads only metadata, avoiding detoasting a potentially large body.
func PubSubChannel(ctx context.Context, pool *pgxpool.Pool, id int64) (string, error) {
	var channel []byte
	err := pool.QueryRow(ctx, "SELECT channel FROM pubsub_messages WHERE id = $1", id).Scan(&channel)
	return string(channel), err
}

// PubSubMessage fetches a body only after transport interest has been established.
func PubSubMessage(ctx context.Context, pool *pgxpool.Pool, id int64) (string, error) {
	var message []byte
	err := pool.QueryRow(ctx, "SELECT message FROM pubsub_messages WHERE id = $1", id).Scan(&message)
	return string(message), err
}
