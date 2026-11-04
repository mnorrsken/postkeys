// Package pubsub provides Redis-compatible pub/sub backed by PostgreSQL LISTEN/NOTIFY.
package pubsub

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// broadcastChannel is the single PostgreSQL LISTEN/NOTIFY channel used for all
// pub/sub messages. Using a single channel ensures all instances receive all
// messages, which is required for PSUBSCRIBE pattern matching to work correctly
// across multiple postkeys instances sharing one PostgreSQL database.
const broadcastChannel = "postkeys_pubsub"

// broadcastPayload is the JSON envelope sent via pg_notify on the broadcast channel.
type broadcastPayload struct {
	Channel   string          `json:"c"`
	Message   string          `json:"m"`
	Reference json.RawMessage `json:"r,omitempty"`
	Binary    bool            `json:"b,omitempty"` // when true, c and m are base64-encoded (binary-safe)
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

// References intentionally cannot be decoded as base64 by old instances. A
// coordinated upgrade is required for delivery, but old pods never emit fake
// empty messages to PSUBSCRIBE * clients.
const referencePrefix = "!pk1:"

type referencePayload struct {
	ID          int64   `json:"r"`
	Channel     *string `json:"c,omitempty"`
	Key         string  `json:"k"`
	Size        *int64  `json:"s"`
	ChannelSize *int64  `json:"n"`
}

func channelKey(channel string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(channel))) }
func referenceChannelFits(headerLen, channelLen int) bool {
	// Guard before EncodedLen's arithmetic, including synthetic near-MaxInt sizes.
	return channelLen < 8000 && len(referencePrefix)+headerLen+len(`,"c":""`)+base64.StdEncoding.EncodedLen(channelLen) < 8000
}
func encodeReference(id int64, channel string, size int64) string {
	channelSize := int64(len(channel))
	ref := referencePayload{ID: id, Key: channelKey(channel), Size: &size, ChannelSize: &channelSize}
	data, _ := json.Marshal(ref)
	// Check before encoding: a Redis channel can itself be hundreds of MiB.
	// Base64 never needs JSON escaping, so its encoded length is exact here.
	if referenceChannelFits(len(data), len(channel)) {
		encoded := base64.StdEncoding.EncodeToString([]byte(channel))
		ref.Channel = &encoded
		data, _ = json.Marshal(ref)
	}
	return referencePrefix + string(data)
}

func decodeReference(payload string) (referencePayload, error) {
	var ref referencePayload
	decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(payload, referencePrefix)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ref); err != nil {
		return ref, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ref, errors.New("trailing reference data")
	}
	if _, err := hex.DecodeString(ref.Key); err != nil {
		return ref, err
	}
	if ref.ID <= 0 || len(ref.Key) != 64 || ref.Size == nil || *ref.Size < 0 || *ref.Size > resp.MaxBulkLen || ref.ChannelSize == nil || *ref.ChannelSize < 0 || *ref.ChannelSize > resp.MaxBulkLen {
		return ref, errors.New("invalid reference")
	}
	if ref.Channel != nil {
		ch, err := base64.StdEncoding.DecodeString(*ref.Channel)
		if err != nil || int64(len(ch)) != *ref.ChannelSize || channelKey(string(ch)) != ref.Key {
			return ref, errors.New("invalid reference channel")
		}
		channel := string(ch)
		ref.Channel = &channel
	}
	return ref, nil
}

// decodeBroadcast decodes inline messages once and preserves the legacy wire.
func decodeBroadcast(payload string) (channel, message string, err error) {
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", "", err
	}
	var bp *broadcastPayload
	if err = json.Unmarshal(data, &bp); err != nil {
		return "", "", err
	}
	if bp == nil || bp.Reference != nil {
		return "", "", errors.New("invalid envelope")
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
	return channel, message, nil
}

// Subscriber represents a client that can receive pub/sub messages
type Subscriber interface {
	// SendPubSubMessage sends a pub/sub message to the client
	SendPubSubMessage(msgType, channel, payload string) error
	// GetID returns the subscriber's unique identifier
	GetID() uint64
}

// subscription is an identity token for one active subscription generation.
// Removing and adding the same client/channel must never revive queued routes.
type subscription struct {
	Subscriber
	mu     sync.RWMutex
	active bool
}

// invalidateSubscriptions waits for prior sends outside the hub's map locks.
// Each removed generation owns its gate, so a concurrent re-subscription's new
// token remains active and unrelated clients never wait behind this I/O.
func invalidateSubscriptions(removed []*subscription) {
	for _, sub := range removed {
		sub.mu.Lock()
		sub.active = false
		sub.mu.Unlock()
	}
}

// Hub manages pub/sub subscriptions and message routing
type Hub struct {
	pool    *pgxpool.Pool
	connStr string

	mu            sync.RWMutex
	subscriptions map[string]map[uint64]*subscription // channel -> subscriberID -> subscriber
	subscribers   map[uint64]map[string]bool          // subscriberID -> channels

	// Pattern subscriptions (for PSUBSCRIBE)
	patternMu   sync.RWMutex
	patterns    map[string]map[uint64]*subscription // pattern -> subscriberID -> subscriber
	subPatterns map[uint64]map[string]bool          // subscriberID -> patterns

	// Listener connection (dedicated for LISTEN/NOTIFY)
	listenerConn   *pgx.Conn
	fetchPool      *pgxpool.Pool
	incoming       chan delivery
	fetchJobs      chan *pendingDelivery
	fetched        chan *pendingDelivery
	released       chan *pendingDelivery
	outputMu       sync.Mutex
	outputs        map[uint64]*clientOutput
	liveOutputs    map[*clientOutput]bool
	outputsRunning bool
	outputsStopped bool
	channelHashes  map[string]string
	channelsByKey  map[string]map[string]bool

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
		outputs:       make(map[uint64]*clientOutput),
		liveOutputs:   make(map[*clientOutput]bool),
		channelHashes: make(map[string]string),
		channelsByKey: make(map[string]map[string]bool),
		connStr:       connStr,
		subscriptions: make(map[string]map[uint64]*subscription),
		subscribers:   make(map[uint64]map[string]bool),
		patterns:      make(map[string]map[uint64]*subscription),
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

	cfg, err := pgxpool.ParseConfig(h.connStr)
	if err != nil {
		_ = listenerConn.Close(ctx)
		return err
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	h.fetchPool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_ = listenerConn.Close(ctx)
		return err
	}
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	h.released = make(chan *pendingDelivery, dispatchCapacity)
	h.enableOutputs()
	h.wg.Add(5)
	go h.dispatchLoop()
	for i := 0; i < 4; i++ {
		go h.fetchLoop()
	}
	// Start the notification listener goroutine
	h.wg.Add(1)
	go h.listenLoop()

	return nil
}

// Stop gracefully stops the hub
func (h *Hub) Stop() {
	h.outputMu.Lock()
	h.outputsStopped = true
	outputs := make([]*clientOutput, 0, len(h.liveOutputs))
	for output := range h.liveOutputs {
		outputs = append(outputs, output)
	}
	h.outputMu.Unlock()
	h.cancel()
	for _, output := range outputs {
		output.close(true)
	}
	h.wg.Wait()

	if h.listenerConn != nil {
		_ = h.listenerConn.Close(context.Background())
	}
	if h.fetchPool != nil {
		h.fetchPool.Close()
	}
}

// Subscribe adds a subscriber to channels
func (h *Hub) Subscribe(sub Subscriber, channels ...string) []int {
	h.ensureOutput(sub)
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
			h.subscriptions[channel] = make(map[uint64]*subscription)
			key := channelKey(channel)
			h.channelHashes[channel] = key
			if h.channelsByKey[key] == nil {
				h.channelsByKey[key] = make(map[string]bool)
			}
			h.channelsByKey[key][channel] = true
		}

		// Add subscriber to channel
		if _, exists := h.subscriptions[channel][subID]; !exists {
			h.subscriptions[channel][subID] = &subscription{Subscriber: sub, active: true}
		}
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
	var removed []*subscription

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
			if token := subs[subID]; token != nil {
				removed = append(removed, token)
			}
			delete(subs, subID)
			if len(subs) == 0 {
				delete(h.subscriptions, channel)
				key := h.channelHashes[channel]
				delete(h.channelHashes, channel)
				delete(h.channelsByKey[key], channel)
				if len(h.channelsByKey[key]) == 0 {
					delete(h.channelsByKey, key)
				}
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

	h.mu.Unlock()
	invalidateSubscriptions(removed)
	return counts
}

// PSubscribe adds a subscriber to pattern subscriptions
func (h *Hub) PSubscribe(sub Subscriber, patterns ...string) []int {
	h.ensureOutput(sub)
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
			h.patterns[pattern] = make(map[uint64]*subscription)
		}

		// Add subscriber to pattern
		if _, exists := h.patterns[pattern][subID]; !exists {
			h.patterns[pattern][subID] = &subscription{Subscriber: sub, active: true}
		}
		h.subPatterns[subID][pattern] = true

		// Count total pattern subscriptions for this subscriber
		counts[i] = len(h.subPatterns[subID])
	}

	return counts
}

// PUnsubscribe removes a subscriber from pattern subscriptions
func (h *Hub) PUnsubscribe(sub Subscriber, patterns ...string) []int {
	h.patternMu.Lock()
	var removed []*subscription

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
			if token := subs[subID]; token != nil {
				removed = append(removed, token)
			}
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

	h.patternMu.Unlock()
	invalidateSubscriptions(removed)
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
		id, err := storage.StorePubSubMessage(ctx, tx, channel, message)
		if err != nil {
			return 0, fmt.Errorf("failed to store broadcast payload: %w", err)
		}
		payload := encodeReference(id, channel, int64(len(message)))
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
	var removed []*subscription
	// Remove from channel subscriptions
	h.mu.Lock()
	if channels, exists := h.subscribers[subID]; exists {
		for channel := range channels {
			if subs, ok := h.subscriptions[channel]; ok {
				if token := subs[subID]; token != nil {
					removed = append(removed, token)
				}
				delete(subs, subID)
				if len(subs) == 0 {
					delete(h.subscriptions, channel)
					key := h.channelHashes[channel]
					delete(h.channelHashes, channel)
					delete(h.channelsByKey[key], channel)
					if len(h.channelsByKey[key]) == 0 {
						delete(h.channelsByKey, key)
					}
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
				if token := subs[subID]; token != nil {
					removed = append(removed, token)
				}
				delete(subs, subID)
				if len(subs) == 0 {
					delete(h.patterns, pattern)
				}
			}
		}
		delete(h.subPatterns, subID)
	}
	h.patternMu.Unlock()
	h.removeOutput(subID)
	invalidateSubscriptions(removed)
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

		item, err := decodeDelivery(notification.Payload)
		if err != nil {
			log.Printf("Warning: failed to decode broadcast payload: %v", err)
			continue
		}
		// Backpressure preserves ordinary messages. PostgreSQL buffers NOTIFY
		// traffic while bounded local queues drain; only overflowing clients close.
		select {
		case h.incoming <- item:
		case <-h.ctx.Done():
			return
		}
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
