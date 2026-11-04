// Package pubsub provides Redis-compatible pub/sub backed by PostgreSQL LISTEN/NOTIFY.
package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mnorrsken/postkeys/internal/resp"
)

// broadcastChannel is the single PostgreSQL LISTEN/NOTIFY channel used for all
// pub/sub messages. Using a single channel ensures all instances receive all
// messages, which is required for PSUBSCRIBE pattern matching to work correctly
// across multiple postkeys instances sharing one PostgreSQL database.
const broadcastChannel = "postkeys_pubsub"

// broadcastPayload is the JSON envelope sent via pg_notify on the broadcast channel.
type broadcastPayload struct {
	Channel string `json:"c"`
	Message string `json:"m"`
	Binary  bool   `json:"b,omitempty"` // when true, c and m are base64-encoded (binary-safe)
}

// inlinePayloadFits computes the full outer base64 size without encoding either input.
func inlinePayloadFits(channelLen, messageLen int) bool {
	if channelLen >= 8000 || messageLen >= 8000 {
		return false
	}
	jsonLen := len(`{"c":"","m":"","b":true}`) + base64.StdEncoding.EncodedLen(channelLen) + base64.StdEncoding.EncodedLen(messageLen)
	return base64.StdEncoding.EncodedLen(jsonLen) < 8000
}

func encodeInline(channel, message string) (string, error) {
	bp := broadcastPayload{Channel: base64.StdEncoding.EncodeToString([]byte(channel)), Message: base64.StdEncoding.EncodeToString([]byte(message)), Binary: true}
	data, err := json.Marshal(bp)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// decodeBroadcast preserves legacy inline decoding while requiring references to be unambiguous.
func decodeBroadcast(payload string) (channel, message string, id int64, err error) {
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", "", 0, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil || fields == nil {
		if err == nil {
			err = errors.New("invalid envelope")
		}
		return "", "", 0, err
	}
	if raw, ok := fields["r"]; ok {
		decoder := json.NewDecoder(bytes.NewReader(data))
		_, _ = decoder.Token() // opening object, already validated by Unmarshal
		keys := 0
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return "", "", 0, err
			}
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil {
				return "", "", 0, err
			}
			keys++
		}
		if keys != 1 {
			return "", "", 0, errors.New("mixed reference envelope")
		}
		if len(fields) != 1 {
			return "", "", 0, errors.New("mixed reference envelope")
		}
		var ref string
		if err = json.Unmarshal(raw, &ref); err != nil || len(ref) == 0 {
			return "", "", 0, errors.New("invalid reference")
		}
		for _, c := range ref {
			if c < '0' || c > '9' {
				return "", "", 0, errors.New("invalid reference")
			}
		}
		id, err = strconv.ParseInt(ref, 10, 64)
		if err != nil || id <= 0 {
			return "", "", 0, errors.New("invalid reference")
		}
		return "", "", id, nil
	}
	var bp broadcastPayload
	if err = json.Unmarshal(data, &bp); err != nil {
		return "", "", 0, err
	}
	channel, message = bp.Channel, bp.Message
	if bp.Binary {
		if decoded, e := base64.StdEncoding.DecodeString(bp.Channel); e == nil {
			channel = string(decoded)
		}
		if decoded, e := base64.StdEncoding.DecodeString(bp.Message); e == nil {
			message = string(decoded)
		}
	}
	return channel, message, 0, nil
}

// Subscriber represents a client that can receive pub/sub messages
type Subscriber interface {
	// SendPubSubMessage sends a pub/sub message to the client
	SendPubSubMessage(msgType, channel, payload string) error
	// GetID returns the subscriber's unique identifier
	GetID() uint64
}

// Hub manages pub/sub subscriptions and message routing
type Hub struct {
	pool    *pgxpool.Pool
	connStr string

	mu            sync.RWMutex
	subscriptions map[string]map[uint64]Subscriber // channel -> subscriberID -> subscriber
	subscribers   map[uint64]map[string]bool       // subscriberID -> channels

	// Pattern subscriptions (for PSUBSCRIBE)
	patternMu   sync.RWMutex
	patterns    map[string]map[uint64]Subscriber // pattern -> subscriberID -> subscriber
	subPatterns map[uint64]map[string]bool       // subscriberID -> patterns

	// Listener connection (dedicated for LISTEN/NOTIFY)
	listenerConn *pgx.Conn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	debug  bool
}

// NewHub creates a new pub/sub hub
func NewHub(pool *pgxpool.Pool, connStr string) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		pool:          pool,
		connStr:       connStr,
		subscriptions: make(map[string]map[uint64]Subscriber),
		subscribers:   make(map[uint64]map[string]bool),
		patterns:      make(map[string]map[uint64]Subscriber),
		subPatterns:   make(map[uint64]map[string]bool),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// SetDebug enables or disables debug logging
func (h *Hub) SetDebug(debug bool) {
	h.debug = debug
}

// Start initializes the hub and starts the notification listener
func (h *Hub) Start(ctx context.Context) error {
	// Create a dedicated connection for LISTEN/NOTIFY
	listenerConn, err := pgx.Connect(ctx, h.connStr)
	if err != nil {
		return fmt.Errorf("failed to create listener connection: %w", err)
	}
	h.listenerConn = listenerConn

	// Listen on the single broadcast channel so we receive ALL pub/sub messages.
	// This is critical for multi-instance correctness: every instance must see
	// every published message so that both SUBSCRIBE and PSUBSCRIBE work.
	_, err = listenerConn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgxIdentifier(broadcastChannel)))
	if err != nil {
		_ = listenerConn.Close(ctx)
		return fmt.Errorf("failed to LISTEN on broadcast channel: %w", err)
	}

	// Start the notification listener goroutine
	h.wg.Add(1)
	go h.listenLoop()

	return nil
}

// Stop gracefully stops the hub
func (h *Hub) Stop() {
	h.cancel()
	h.wg.Wait()

	if h.listenerConn != nil {
		_ = h.listenerConn.Close(context.Background())
	}
}

// Subscribe adds a subscriber to channels
func (h *Hub) Subscribe(sub Subscriber, channels ...string) []int {
	h.mu.Lock()
	defer h.mu.Unlock()

	subID := sub.GetID()
	counts := make([]int, len(channels))

	// Initialize subscriber's channel set if needed
	if _, exists := h.subscribers[subID]; !exists {
		h.subscribers[subID] = make(map[string]bool)
	}

	for i, channel := range channels {
		// Initialize channel's subscriber set if needed
		if _, exists := h.subscriptions[channel]; !exists {
			h.subscriptions[channel] = make(map[uint64]Subscriber)
		}

		// Add subscriber to channel
		h.subscriptions[channel][subID] = sub
		h.subscribers[subID][channel] = true

		// Count total subscriptions for this subscriber
		counts[i] = len(h.subscribers[subID])
	}

	return counts
}

// Unsubscribe removes a subscriber from channels
// If no channels specified, unsubscribes from all
func (h *Hub) Unsubscribe(sub Subscriber, channels ...string) []int {
	h.mu.Lock()
	defer h.mu.Unlock()

	subID := sub.GetID()

	// If no channels specified, unsubscribe from all
	if len(channels) == 0 {
		if subChannels, exists := h.subscribers[subID]; exists {
			channels = make([]string, 0, len(subChannels))
			for ch := range subChannels {
				channels = append(channels, ch)
			}
		}
	}

	counts := make([]int, len(channels))

	for i, channel := range channels {
		// Remove from channel's subscriber set
		if subs, exists := h.subscriptions[channel]; exists {
			delete(subs, subID)
			if len(subs) == 0 {
				delete(h.subscriptions, channel)
			}
		}

		// Remove from subscriber's channel set
		if subChannels, exists := h.subscribers[subID]; exists {
			delete(subChannels, channel)
			counts[i] = len(subChannels)
			if len(subChannels) == 0 {
				delete(h.subscribers, subID)
			}
		}
	}

	return counts
}

// PSubscribe adds a subscriber to pattern subscriptions
func (h *Hub) PSubscribe(sub Subscriber, patterns ...string) []int {
	h.patternMu.Lock()
	defer h.patternMu.Unlock()

	subID := sub.GetID()
	counts := make([]int, len(patterns))

	// Initialize subscriber's pattern set if needed
	if _, exists := h.subPatterns[subID]; !exists {
		h.subPatterns[subID] = make(map[string]bool)
	}

	for i, pattern := range patterns {
		// Initialize pattern's subscriber set if needed
		if _, exists := h.patterns[pattern]; !exists {
			h.patterns[pattern] = make(map[uint64]Subscriber)
		}

		// Add subscriber to pattern
		h.patterns[pattern][subID] = sub
		h.subPatterns[subID][pattern] = true

		// Count total pattern subscriptions for this subscriber
		counts[i] = len(h.subPatterns[subID])
	}

	return counts
}

// PUnsubscribe removes a subscriber from pattern subscriptions
func (h *Hub) PUnsubscribe(sub Subscriber, patterns ...string) []int {
	h.patternMu.Lock()
	defer h.patternMu.Unlock()

	subID := sub.GetID()

	// If no patterns specified, unsubscribe from all
	if len(patterns) == 0 {
		if subPats, exists := h.subPatterns[subID]; exists {
			patterns = make([]string, 0, len(subPats))
			for pat := range subPats {
				patterns = append(patterns, pat)
			}
		}
	}

	counts := make([]int, len(patterns))

	for i, pattern := range patterns {
		// Remove from pattern's subscriber set
		if subs, exists := h.patterns[pattern]; exists {
			delete(subs, subID)
			if len(subs) == 0 {
				delete(h.patterns, pattern)
			}
		}

		// Remove from subscriber's pattern set
		if subPats, exists := h.subPatterns[subID]; exists {
			delete(subPats, pattern)
			counts[i] = len(subPats)
			if len(subPats) == 0 {
				delete(h.subPatterns, subID)
			}
		}
	}

	return counts
}

// Publish publishes a message to a channel, returns the number of local subscribers that received it.
func (h *Hub) Publish(ctx context.Context, channel, message string) (int64, error) {
	if inlinePayloadFits(len(channel), len(message)) {
		payload, err := encodeInline(channel, message)
		if err != nil {
			return 0, fmt.Errorf("failed to encode broadcast payload: %w", err)
		}
		if _, err := h.pool.Exec(ctx, "SELECT pg_notify($1, $2)", broadcastChannel, payload); err != nil {
			return 0, fmt.Errorf("failed to publish: %w", err)
		}
	} else {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to begin publish: %w", err)
		}
		defer func() {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rollbackCtx)
		}()
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO pubsub_messages (channel, message, expires_at) VALUES ($1, $2, clock_timestamp() + interval '5 minutes') RETURNING id`, []byte(channel), []byte(message)).Scan(&id); err != nil {
			return 0, fmt.Errorf("failed to store broadcast payload: %w", err)
		}
		payload := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"%d"}`, id)))
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", broadcastChannel, payload); err != nil {
			return 0, fmt.Errorf("failed to notify: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("failed to commit publish: %w", err)
		}
	}

	// Return local subscriber count (same semantics as Redis: count of clients
	// on this server that received the message). The actual delivery happens
	// asynchronously via the listenLoop when pg_notify is received.
	count := h.countSubscribers(channel)
	return int64(count), nil
}

// countSubscribers counts how many subscribers would receive a message on this channel
func (h *Hub) countSubscribers(channel string) int {
	h.mu.RLock()
	channelCount := len(h.subscriptions[channel])
	h.mu.RUnlock()

	h.patternMu.RLock()
	patternCount := 0
	for pattern, subs := range h.patterns {
		if matchPattern(pattern, channel) {
			patternCount += len(subs)
		}
	}
	h.patternMu.RUnlock()

	return channelCount + patternCount
}

// GetSubscriptionCount returns the number of channel and pattern subscriptions for a subscriber
func (h *Hub) GetSubscriptionCount(subID uint64) (channels int, patterns int) {
	h.mu.RLock()
	if subs, exists := h.subscribers[subID]; exists {
		channels = len(subs)
	}
	h.mu.RUnlock()

	h.patternMu.RLock()
	if pats, exists := h.subPatterns[subID]; exists {
		patterns = len(pats)
	}
	h.patternMu.RUnlock()

	return
}

// GetSubscribedChannels returns the list of channels a subscriber is subscribed to
func (h *Hub) GetSubscribedChannels(subID uint64) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if subs, exists := h.subscribers[subID]; exists {
		channels := make([]string, 0, len(subs))
		for ch := range subs {
			channels = append(channels, ch)
		}
		return channels
	}
	return nil
}

// GetSubscribedPatterns returns the list of patterns a subscriber is subscribed to
func (h *Hub) GetSubscribedPatterns(subID uint64) []string {
	h.patternMu.RLock()
	defer h.patternMu.RUnlock()

	if pats, exists := h.subPatterns[subID]; exists {
		patterns := make([]string, 0, len(pats))
		for p := range pats {
			patterns = append(patterns, p)
		}
		return patterns
	}
	return nil
}

// RemoveSubscriber removes a subscriber from all subscriptions
func (h *Hub) RemoveSubscriber(subID uint64) {
	// Remove from channel subscriptions
	h.mu.Lock()
	if channels, exists := h.subscribers[subID]; exists {
		for channel := range channels {
			if subs, ok := h.subscriptions[channel]; ok {
				delete(subs, subID)
				if len(subs) == 0 {
					delete(h.subscriptions, channel)
				}
			}
		}
		delete(h.subscribers, subID)
	}
	h.mu.Unlock()

	// Remove from pattern subscriptions
	h.patternMu.Lock()
	if patterns, exists := h.subPatterns[subID]; exists {
		for pattern := range patterns {
			if subs, ok := h.patterns[pattern]; ok {
				delete(subs, subID)
				if len(subs) == 0 {
					delete(h.patterns, pattern)
				}
			}
		}
		delete(h.subPatterns, subID)
	}
	h.patternMu.Unlock()
}

// listenLoop continuously waits for PostgreSQL notifications on the broadcast channel.
func (h *Hub) listenLoop() {
	defer h.wg.Done()

	// Exponential backoff for idle periods
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
		case <-h.ctx.Done():
			return
		default:
		}

		// If the listener has no connection (initial start failure or a prior
		// failed reconnect), establish one before waiting. WaitForNotification
		// would NPE on a nil receiver.
		if h.listenerConn == nil {
			if h.reconnect() {
				reconnectBackoff = minReconnectBackoff
				currentTimeout = minTimeout
			} else {
				select {
				case <-time.After(reconnectBackoff):
				case <-h.ctx.Done():
					return
				}
				reconnectBackoff *= 2
				if reconnectBackoff > maxReconnectBackoff {
					reconnectBackoff = maxReconnectBackoff
				}
			}
			continue
		}

		// Wait for notification with exponential backoff
		ctx, cancel := context.WithTimeout(h.ctx, currentTimeout)
		notification, err := h.listenerConn.WaitForNotification(ctx)
		cancel()

		if err != nil {
			if h.ctx.Err() != nil {
				return // Context cancelled, clean shutdown
			}
			// Check if this is a connection error (not just timeout)
			if !isTimeoutError(err) {
				log.Printf("Pub/sub listener error (will reconnect): %v", err)
				// Drop the connection; the top of the loop will reconnect.
				_ = h.listenerConn.Close(context.Background())
				h.listenerConn = nil
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

		// Got a notification - reset backoff to minimum for responsiveness
		currentTimeout = minTimeout

		// Only process messages from the broadcast channel
		if notification.Channel != broadcastChannel {
			continue
		}

		channel, message, id, err := decodeBroadcast(notification.Payload)
		if err != nil {
			log.Printf("Warning: failed to decode broadcast payload: %v", err)
			continue
		}
		if id != 0 {
			ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
			var ch, msg []byte
			err := h.pool.QueryRow(ctx, "SELECT channel, message FROM pubsub_messages WHERE id = $1", id).Scan(&ch, &msg)
			cancel()
			if err != nil {
				log.Printf("Warning: failed to fetch pub/sub message %d: %v", id, err)
				continue
			}
			channel, message = string(ch), string(msg)
		}

		if h.debug {
			log.Printf("[DEBUG] Received pub/sub broadcast: channel=%s", channel)
		}

		// Deliver to channel subscribers
		h.deliverToChannel(channel, message)

		// Deliver to pattern subscribers
		h.deliverToPatterns(channel, message)
	}
}

// reconnect attempts to re-establish the listener connection and re-subscribe to the broadcast channel
func (h *Hub) reconnect() bool {
	// Close old connection if it exists
	if h.listenerConn != nil {
		_ = h.listenerConn.Close(context.Background())
		h.listenerConn = nil
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, h.connStr)
	if err != nil {
		log.Printf("Pub/sub hub reconnect failed: %v", err)
		return false
	}
	h.listenerConn = conn

	// Re-subscribe to the broadcast channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgxIdentifier(broadcastChannel)))
	if err != nil {
		log.Printf("Failed to LISTEN on broadcast channel after reconnect: %v", err)
		_ = conn.Close(ctx)
		h.listenerConn = nil
		return false
	}

	if h.debug {
		log.Printf("[DEBUG] Pub/sub hub reconnected successfully")
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

// deliverToChannel delivers a message to all subscribers of a channel
func (h *Hub) deliverToChannel(channel, payload string) {
	h.mu.RLock()
	subs := h.subscriptions[channel]
	// Make a copy to avoid holding the lock during delivery
	subscribers := make([]Subscriber, 0, len(subs))
	for _, sub := range subs {
		subscribers = append(subscribers, sub)
	}
	h.mu.RUnlock()

	for _, sub := range subscribers {
		if err := sub.SendPubSubMessage("message", channel, payload); err != nil {
			if h.debug {
				log.Printf("[DEBUG] Failed to deliver message to subscriber %d: %v", sub.GetID(), err)
			}
		}
	}
}

// deliverToPatterns delivers a message to all pattern subscribers whose pattern matches the channel
func (h *Hub) deliverToPatterns(channel, payload string) {
	h.patternMu.RLock()
	var matches []struct {
		pattern string
		sub     Subscriber
	}
	for pattern, subs := range h.patterns {
		if matchPattern(pattern, channel) {
			for _, sub := range subs {
				matches = append(matches, struct {
					pattern string
					sub     Subscriber
				}{pattern, sub})
			}
		}
	}
	h.patternMu.RUnlock()

	for _, match := range matches {
		// For pattern matches, send pmessage
		if err := match.sub.SendPubSubMessage("pmessage", match.pattern+"\x00"+channel, payload); err != nil {
			if h.debug {
				log.Printf("[DEBUG] Failed to deliver pmessage to subscriber %d: %v", match.sub.GetID(), err)
			}
		}
	}
}

// matchPattern checks if a channel matches a Redis-style glob pattern
func matchPattern(pattern, channel string) bool {
	return globMatch(pattern, channel)
}

// globMatch implements Redis-style glob matching
func globMatch(pattern, str string) bool {
	px := 0
	sx := 0
	pLen := len(pattern)
	sLen := len(str)
	starIdx := -1
	matchIdx := 0

	for sx < sLen {
		if px < pLen && (pattern[px] == '?' || pattern[px] == str[sx]) {
			px++
			sx++
		} else if px < pLen && pattern[px] == '*' {
			starIdx = px
			matchIdx = sx
			px++
		} else if starIdx != -1 {
			px = starIdx + 1
			matchIdx++
			sx = matchIdx
		} else {
			return false
		}
	}

	for px < pLen && pattern[px] == '*' {
		px++
	}

	return px == pLen
}

// pgxIdentifier quotes a PostgreSQL identifier
func pgxIdentifier(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// BuildSubscribeResponse builds the RESP response for SUBSCRIBE
func BuildSubscribeResponse(channel string, count int) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("subscribe"),
			resp.Bulk(channel),
			resp.Int(int64(count)),
		},
	}
}

// BuildUnsubscribeResponse builds the RESP response for UNSUBSCRIBE
func BuildUnsubscribeResponse(channel string, count int) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("unsubscribe"),
			resp.Bulk(channel),
			resp.Int(int64(count)),
		},
	}
}

// BuildPSubscribeResponse builds the RESP response for PSUBSCRIBE
func BuildPSubscribeResponse(pattern string, count int) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("psubscribe"),
			resp.Bulk(pattern),
			resp.Int(int64(count)),
		},
	}
}

// BuildPUnsubscribeResponse builds the RESP response for PUNSUBSCRIBE
func BuildPUnsubscribeResponse(pattern string, count int) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("punsubscribe"),
			resp.Bulk(pattern),
			resp.Int(int64(count)),
		},
	}
}

// BuildMessageResponse builds the RESP response for a pub/sub message
func BuildMessageResponse(channel, message string) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("message"),
			resp.Bulk(channel),
			resp.Bulk(message),
		},
	}
}

// BuildPMessageResponse builds the RESP response for a pattern pub/sub message
func BuildPMessageResponse(pattern, channel, message string) resp.Value {
	return resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			resp.Bulk("pmessage"),
			resp.Bulk(pattern),
			resp.Bulk(channel),
			resp.Bulk(message),
		},
	}
}
