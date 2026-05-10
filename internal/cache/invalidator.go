// Package cache provides an in-memory TTL cache with distributed invalidation.
package cache

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL channel for cache invalidation
const cacheInvalidateChannel = "postkeys_cache_invalidate"

// invalidationPayload represents the JSON payload for cache invalidation
type invalidationPayload struct {
	Keys    []string `json:"keys,omitempty"`
	Flush   bool     `json:"flush,omitempty"`
	Encoded bool     `json:"enc,omitempty"` // when true, keys are base64-encoded (binary-safe)
}

// Invalidator broadcasts cache invalidations across instances using PostgreSQL LISTEN/NOTIFY
type Invalidator struct {
	pool         *pgxpool.Pool
	connStr      string
	listenerConn *pgx.Conn

	cache  *Cache
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	debug  bool
}

// NewInvalidator creates a new cache invalidator
func NewInvalidator(pool *pgxpool.Pool, connStr string, cache *Cache) *Invalidator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Invalidator{
		pool:    pool,
		connStr: connStr,
		cache:   cache,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// SetDebug enables debug logging
func (inv *Invalidator) SetDebug(debug bool) {
	inv.debug = debug
}

// Start initializes the invalidator and starts listening for notifications
func (inv *Invalidator) Start(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, inv.connStr)
	if err != nil {
		return fmt.Errorf("failed to create listener connection: %w", err)
	}
	inv.listenerConn = conn

	// Start listening on the cache invalidation channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", cacheInvalidateChannel))
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("failed to LISTEN: %w", err)
	}

	inv.wg.Add(1)
	go inv.listenLoop()

	if inv.debug {
		log.Printf("[DEBUG] Cache invalidator started, listening on %s", cacheInvalidateChannel)
	}

	return nil
}

// Stop gracefully stops the invalidator
func (inv *Invalidator) Stop() {
	inv.cancel()
	inv.wg.Wait()

	if inv.listenerConn != nil {
		inv.listenerConn.Close(context.Background())
	}
}

// InvalidateKey broadcasts a key invalidation to all instances
func (inv *Invalidator) InvalidateKey(ctx context.Context, key string) error {
	return inv.InvalidateKeys(ctx, key)
}

// InvalidateKeys broadcasts multiple key invalidations to all instances
func (inv *Invalidator) InvalidateKeys(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	// Base64-encode keys because Redis keys can contain arbitrary binary data
	// and json.Marshal replaces invalid UTF-8 with U+FFFD.
	encodedKeys := make([]string, len(keys))
	for i, k := range keys {
		encodedKeys[i] = base64.StdEncoding.EncodeToString([]byte(k))
	}
	payload := invalidationPayload{Keys: encodedKeys, Encoded: true}
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal invalidation payload: %w", err)
	}

	_, err = inv.pool.Exec(ctx, "SELECT pg_notify($1, $2)", cacheInvalidateChannel, string(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to notify cache invalidation: %w", err)
	}

	if inv.debug {
		log.Printf("[DEBUG] Broadcast cache invalidation for keys: %v", keys)
	}

	return nil
}

// InvalidateFlush broadcasts a flush (clear all) to all instances
func (inv *Invalidator) InvalidateFlush(ctx context.Context) error {
	payload := invalidationPayload{Flush: true}
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal flush payload: %w", err)
	}

	_, err = inv.pool.Exec(ctx, "SELECT pg_notify($1, $2)", cacheInvalidateChannel, string(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to notify cache flush: %w", err)
	}

	if inv.debug {
		log.Printf("[DEBUG] Broadcast cache flush")
	}

	return nil
}

// listenLoop continuously listens for PostgreSQL notifications
func (inv *Invalidator) listenLoop() {
	defer inv.wg.Done()

	// Backoff for reconnection attempts
	const (
		minBackoff = 100 * time.Millisecond
		maxBackoff = 30 * time.Second
	)
	backoff := minBackoff

	for {
		select {
		case <-inv.ctx.Done():
			return
		default:
		}

		// If the listener has no connection (initial start failure or a prior
		// failed reconnect), establish one before waiting. WaitForNotification
		// would NPE on a nil receiver.
		if inv.listenerConn == nil {
			if inv.reconnect() {
				backoff = minBackoff
			} else {
				select {
				case <-time.After(backoff):
				case <-inv.ctx.Done():
					return
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		// Wait for notification with a timeout for graceful shutdown
		ctx, cancel := context.WithTimeout(inv.ctx, 5*time.Second)
		notification, err := inv.listenerConn.WaitForNotification(ctx)
		cancel()

		if err != nil {
			if inv.ctx.Err() != nil {
				return
			}
			// Check if this is a connection error (not just timeout)
			if !isTimeoutError(err) {
				log.Printf("Cache invalidator listener error (will reconnect): %v", err)
				// Drop the connection; the top of the loop will reconnect.
				inv.listenerConn.Close(context.Background())
				inv.listenerConn = nil
			}
			continue
		}

		// Got a notification - reset backoff
		backoff = minBackoff

		// Process the invalidation
		inv.processNotification(notification.Payload)
	}
}

// reconnect attempts to re-establish the listener connection
func (inv *Invalidator) reconnect() bool {
	// Close old connection if it exists
	if inv.listenerConn != nil {
		inv.listenerConn.Close(context.Background())
		inv.listenerConn = nil
	}

	ctx, cancel := context.WithTimeout(inv.ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, inv.connStr)
	if err != nil {
		log.Printf("Cache invalidator reconnect failed: %v", err)
		return false
	}

	// Re-subscribe to the channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", cacheInvalidateChannel))
	if err != nil {
		conn.Close(ctx)
		log.Printf("Cache invalidator LISTEN failed after reconnect: %v", err)
		return false
	}

	inv.listenerConn = conn
	if inv.debug {
		log.Printf("[DEBUG] Cache invalidator reconnected successfully")
	}
	return true
}

// isTimeoutError checks if an error is a context deadline exceeded (timeout)
func isTimeoutError(err error) bool {
	return err == context.DeadlineExceeded || (err != nil && err.Error() == "context deadline exceeded")
}

// processNotification handles an incoming invalidation notification
func (inv *Invalidator) processNotification(payload string) {
	var msg invalidationPayload
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		log.Printf("Warning: failed to parse cache invalidation payload: %v", err)
		return
	}

	if msg.Flush {
		inv.cache.Flush()
		if inv.debug {
			log.Printf("[DEBUG] Cache flushed via distributed invalidation")
		}
		return
	}

	for _, key := range msg.Keys {
		if msg.Encoded {
			if keyBytes, err := base64.StdEncoding.DecodeString(key); err == nil {
				key = string(keyBytes)
			}
		}
		inv.cache.Delete(key)
	}

	if inv.debug && len(msg.Keys) > 0 {
		log.Printf("[DEBUG] Cache invalidated %d keys via distributed invalidation", len(msg.Keys))
	}
}
