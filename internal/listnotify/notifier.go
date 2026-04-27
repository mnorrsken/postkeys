// Package listnotify provides LISTEN/NOTIFY based notifications for blocking list operations.
// This allows BRPOP/BLPOP to wait efficiently without polling.
package listnotify

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channel name for list push notifications
const listPushChannel = "postkeys_list_push"

// Notifier manages notifications for blocking list operations
type Notifier struct {
	pool    *pgxpool.Pool
	connStr string

	// Listener connection
	listenerConn *pgx.Conn

	// Subscribers waiting for specific keys
	mu          sync.RWMutex
	subscribers map[string][]chan string // key -> list of waiting channels

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	debug  bool
}

// New creates a new list notifier
func New(pool *pgxpool.Pool, connStr string) *Notifier {
	ctx, cancel := context.WithCancel(context.Background())
	return &Notifier{
		pool:        pool,
		connStr:     connStr,
		subscribers: make(map[string][]chan string),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// SetDebug enables debug logging
func (n *Notifier) SetDebug(debug bool) {
	n.debug = debug
}

// Start initializes the notifier and starts listening
func (n *Notifier) Start(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, n.connStr)
	if err != nil {
		return fmt.Errorf("failed to create listener connection: %w", err)
	}
	n.listenerConn = conn

	// Start listening on the list push channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", listPushChannel))
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("failed to LISTEN: %w", err)
	}

	n.wg.Add(1)
	go n.listenLoop()

	return nil
}

// Stop gracefully stops the notifier
func (n *Notifier) Stop() {
	n.cancel()
	n.wg.Wait()

	if n.listenerConn != nil {
		n.listenerConn.Close(context.Background())
	}
}

// NotifyPush sends a notification that items were pushed to a list key.
// The key is base64-encoded because pg_notify requires valid UTF-8 payloads
// and Redis keys can contain arbitrary binary data.
func (n *Notifier) NotifyPush(ctx context.Context, key string) error {
	encoded := base64.StdEncoding.EncodeToString([]byte(key))
	_, err := n.pool.Exec(ctx, "SELECT pg_notify($1, $2)", listPushChannel, encoded)
	return err
}

// WaitForKey waits for a notification on the given key or until timeout/cancellation
// Returns true if notified, false if timeout or cancelled
func (n *Notifier) WaitForKey(ctx context.Context, key string, timeout time.Duration) bool {
	return n.WaitForKeys(ctx, []string{key}, timeout) != ""
}

// WaitForKeys waits for a notification on any of the given keys or until timeout/cancellation
// Returns the key that was notified, or empty string if timeout or cancelled
func (n *Notifier) WaitForKeys(ctx context.Context, keys []string, timeout time.Duration) string {
	if len(keys) == 0 {
		return ""
	}

	// Single channel to receive notifications from any key.
	// Intentionally never closed: listenLoop may hold a snapshotted reference
	// after we deregister, and sending on a closed channel would panic even
	// with select+default. GC reclaims it once listenLoop drops the reference.
	ch := make(chan string, 1)

	// Register the subscriber for all keys
	n.mu.Lock()
	for _, key := range keys {
		n.subscribers[key] = append(n.subscribers[key], ch)
	}
	n.mu.Unlock()

	// Ensure cleanup for all keys
	defer func() {
		n.mu.Lock()
		for _, key := range keys {
			channels := n.subscribers[key]
			for i, c := range channels {
				if c == ch {
					n.subscribers[key] = append(channels[:i], channels[i+1:]...)
					break
				}
			}
			if len(n.subscribers[key]) == 0 {
				delete(n.subscribers, key)
			}
		}
		n.mu.Unlock()
	}()

	// Wait for notification or timeout
	var timer <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timer = t.C
	}

	select {
	case key := <-ch:
		return key
	case <-timer:
		return ""
	case <-ctx.Done():
		return ""
	case <-n.ctx.Done():
		return ""
	}
}

// listenLoop continuously listens for PostgreSQL notifications
func (n *Notifier) listenLoop() {
	defer n.wg.Done()

	// Exponential backoff for idle periods
	// Start at 50ms, double on each timeout up to 2s max
	const (
		minTimeout = 50 * time.Millisecond
		maxTimeout = 2 * time.Second
	)
	currentTimeout := minTimeout

	// Backoff for reconnection attempts
	const (
		minReconnectBackoff = 100 * time.Millisecond
		maxReconnectBackoff = 30 * time.Second
	)
	reconnectBackoff := minReconnectBackoff

	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		// Wait for notification with exponential backoff
		ctx, cancel := context.WithTimeout(n.ctx, currentTimeout)
		notification, err := n.listenerConn.WaitForNotification(ctx)
		cancel()

		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			// Check if this is a connection error (not just timeout)
			if !isTimeoutError(err) {
				log.Printf("List notifier listener error (will reconnect): %v", err)
				if n.reconnect() {
					reconnectBackoff = minReconnectBackoff // Reset backoff on successful reconnect
					currentTimeout = minTimeout
				} else {
					// Exponential backoff on failed reconnect
					select {
					case <-time.After(reconnectBackoff):
					case <-n.ctx.Done():
						return
					}
					reconnectBackoff *= 2
					if reconnectBackoff > maxReconnectBackoff {
						reconnectBackoff = maxReconnectBackoff
					}
				}
				continue
			}
			// Timeout - increase backoff (up to max)
			if currentTimeout < maxTimeout {
				currentTimeout *= 2
				if currentTimeout > maxTimeout {
					currentTimeout = maxTimeout
				}
			}
			continue
		}

		// Got a notification - reset backoffs to minimum for responsiveness
		currentTimeout = minTimeout
		reconnectBackoff = minReconnectBackoff

		// Decode base64-encoded key (binary-safe via pg_notify)
		var key string
		if keyBytes, err := base64.StdEncoding.DecodeString(notification.Payload); err == nil {
			key = string(keyBytes)
		} else {
			// Fallback: treat as raw key (backward compat during rolling update)
			key = notification.Payload
		}
		if n.debug {
			log.Printf("[DEBUG] List push notification for key: %s", key)
		}

		n.notifySubscribers(key)
	}
}

// notifySubscribers delivers a key notification to every registered subscriber.
// Sends are non-blocking: a full buffer is treated as "subscriber already woken."
// The subscriber slice is copied under the lock because WaitForKeys cleanup
// mutates the backing array via append-shift on the same slice.
func (n *Notifier) notifySubscribers(key string) {
	n.mu.RLock()
	channels := append([]chan string(nil), n.subscribers[key]...)
	n.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- key:
		default:
		}
	}
}

// reconnect attempts to re-establish the listener connection
func (n *Notifier) reconnect() bool {
	// Close old connection if it exists
	if n.listenerConn != nil {
		n.listenerConn.Close(context.Background())
		n.listenerConn = nil
	}

	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, n.connStr)
	if err != nil {
		log.Printf("List notifier reconnect failed: %v", err)
		return false
	}

	// Re-subscribe to the channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", listPushChannel))
	if err != nil {
		conn.Close(ctx)
		log.Printf("List notifier LISTEN failed after reconnect: %v", err)
		return false
	}

	n.listenerConn = conn
	if n.debug {
		log.Printf("[DEBUG] List notifier reconnected successfully")
	}
	return true
}

// isTimeoutError checks if an error is a context deadline exceeded (timeout)
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	// Use errors.Is to handle wrapped errors (pgx wraps timeouts)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Also check error string for wrapped timeout messages from pgx
	errStr := err.Error()
	return strings.Contains(errStr, "context deadline exceeded")
}
