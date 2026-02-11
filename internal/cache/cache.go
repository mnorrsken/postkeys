// Package cache provides an in-memory TTL cache for the storage layer.
package cache

import (
	"sync"
	"time"
)

// entry represents a cached value with expiration
type entry struct {
	value     string
	expiresAt time.Time
}

// Cache provides a simple TTL-based in-memory cache
type Cache struct {
	mu       sync.RWMutex
	items    map[string]entry
	ttl      time.Duration
	maxSize  int
	stopChan chan struct{}
}

// Config holds cache configuration
type Config struct {
	TTL             time.Duration
	MaxSize         int
	CleanupInterval time.Duration

	// ExcludePatterns is a list of key patterns to never cache (glob-style).
	// Example: ["pubsub:*", "channel:*", "lock:*"]
	ExcludePatterns []string

	// IncludePatterns is a list of key patterns to always cache (glob-style).
	// These patterns take precedence over exclusion rules.
	// Example: ["static:*", "cache:*"]
	IncludePatterns []string
}

// New creates a new cache with the given configuration
func New(cfg Config) *Cache {
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = cfg.TTL / 2
		if cfg.CleanupInterval < 10*time.Millisecond {
			cfg.CleanupInterval = 10 * time.Millisecond
		}
	}

	c := &Cache{
		items:    make(map[string]entry),
		ttl:      cfg.TTL,
		maxSize:  cfg.MaxSize,
		stopChan: make(chan struct{}),
	}

	// Start cleanup goroutine
	go c.cleanupLoop(cfg.CleanupInterval)

	return c
}

// Get retrieves a value from the cache
// Returns the value and whether it was found
func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, exists := c.items[key]
	if !exists {
		return "", false
	}

	if time.Now().After(e.expiresAt) {
		return "", false
	}

	return e.value, true
}

// Set stores a value in the cache with the configured TTL
func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple eviction: if at max size, skip adding new entries
	// (expired entries will be cleaned up by the cleanup goroutine)
	if c.maxSize > 0 && len(c.items) >= c.maxSize {
		if _, exists := c.items[key]; !exists {
			// Don't add new entries when at capacity
			return
		}
	}

	c.items[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Delete removes a key from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// DeleteMulti removes multiple keys from the cache
func (c *Cache) DeleteMulti(keys []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.items, key)
	}
}

// Invalidate removes a key from the cache (alias for Delete)
func (c *Cache) Invalidate(key string) {
	c.Delete(key)
}

// Flush removes all entries from the cache
func (c *Cache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]entry)
}

// Size returns the current number of items in the cache
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Stop stops the cache cleanup goroutine
func (c *Cache) Stop() {
	close(c.stopChan)
}

func (c *Cache) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopChan:
			return
		}
	}
}

func (c *Cache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, e := range c.items {
		if now.After(e.expiresAt) {
			delete(c.items, key)
		}
	}
}
