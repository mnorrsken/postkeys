package cache

import (
	"context"
	"strings"
	"time"

	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// CachedStore wraps a storage.Backend with an in-memory cache
type CachedStore struct {
	backend         storage.Backend
	cache           *Cache
	invalidator     *Invalidator
	excludePatterns []string
	includePatterns []string
}

// NewCachedStore creates a new cached storage wrapper
func NewCachedStore(backend storage.Backend, cfg Config) *CachedStore {
	return &CachedStore{
		backend:         backend,
		cache:           New(cfg),
		excludePatterns: cfg.ExcludePatterns,
		includePatterns: cfg.IncludePatterns,
	}
}

// SetInvalidator sets the distributed cache invalidator
func (s *CachedStore) SetInvalidator(inv *Invalidator) {
	s.invalidator = inv
}

// GetCache returns the underlying cache (for use with Invalidator)
func (s *CachedStore) GetCache() *Cache {
	return s.cache
}

// Close closes the cached store and underlying backend
func (s *CachedStore) Close() {
	s.cache.Stop()
	s.backend.Close()
}

// shouldCache checks if a key should be cached based on include/exclude patterns.
// Include patterns take precedence over exclude patterns.
// Returns true if no patterns are configured.
func (s *CachedStore) shouldCache(key string) bool {
	// Check include patterns first (they take precedence)
	if len(s.includePatterns) > 0 && matchesAnyPattern(key, s.includePatterns) {
		return true
	}

	// Check exclude patterns
	if len(s.excludePatterns) > 0 && matchesAnyPattern(key, s.excludePatterns) {
		metrics.CacheSkips.WithLabelValues("exclude_pattern").Inc()
		return false
	}

	return true
}

// matchesAnyPattern checks if a key matches any of the glob patterns
func matchesAnyPattern(key string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchGlob(pattern, key) {
			return true
		}
	}
	return false
}

// matchGlob performs simple glob matching with * as wildcard
func matchGlob(pattern, str string) bool {
	if pattern == str {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(str, strings.TrimSuffix(pattern, "*"))
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(str, strings.TrimPrefix(pattern, "*"))
	}
	if idx := strings.Index(pattern, "*"); idx >= 0 {
		return strings.HasPrefix(str, pattern[:idx]) && strings.HasSuffix(str, pattern[idx+1:])
	}
	return false
}

// invalidate invalidates a key locally and broadcasts to other instances
func (s *CachedStore) invalidate(ctx context.Context, key string) {
	s.cache.Invalidate(key)
	if s.invalidator != nil {
		s.invalidator.InvalidateKey(ctx, key)
	}
}

// invalidateMulti invalidates multiple keys locally and broadcasts to other instances
func (s *CachedStore) invalidateMulti(ctx context.Context, keys []string) {
	s.cache.DeleteMulti(keys)
	if s.invalidator != nil {
		s.invalidator.InvalidateKeys(ctx, keys...)
	}
}

// flush clears the cache locally and broadcasts to other instances
func (s *CachedStore) flush(ctx context.Context) {
	s.cache.Flush()
	if s.invalidator != nil {
		s.invalidator.InvalidateFlush(ctx)
	}
}

// ============== String Commands (with caching) ==============

func (s *CachedStore) Get(ctx context.Context, key string) (string, bool, error) {
	// Check policy before using cache
	if s.shouldCache(key) {
		// Try cache first
		if value, found := s.cache.Get(key); found {
			metrics.CacheHits.Inc()
			return value, true, nil
		}
	}
	metrics.CacheMisses.Inc()

	// Cache miss - fetch from backend
	value, found, err := s.backend.Get(ctx, key)
	if err != nil {
		return "", false, err
	}

	// Cache the result if found and policy allows
	if found && s.shouldCache(key) {
		s.cache.Set(key, value)
	}

	return value, found, nil
}

func (s *CachedStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := s.backend.Set(ctx, key, value, ttl)
	if err != nil {
		return err
	}
	// Invalidate cache on write (distributed write-through invalidation)
	s.invalidate(ctx, key)
	return nil
}

func (s *CachedStore) SetNX(ctx context.Context, key, value string) (bool, error) {
	ok, err := s.backend.SetNX(ctx, key, value)
	if err != nil {
		return false, err
	}
	if ok {
		s.invalidate(ctx, key)
	}
	return ok, nil
}

func (s *CachedStore) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	// For simplicity, MGet doesn't use cache (could be optimized later)
	return s.backend.MGet(ctx, keys)
}

func (s *CachedStore) MSet(ctx context.Context, pairs map[string]string) error {
	err := s.backend.MSet(ctx, pairs)
	if err != nil {
		return err
	}
	// Invalidate all keys (distributed)
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	s.invalidateMulti(ctx, keys)
	return nil
}

func (s *CachedStore) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	result, err := s.backend.Incr(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) Append(ctx context.Context, key, value string) (int64, error) {
	result, err := s.backend.Append(ctx, key, value)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return s.backend.GetRange(ctx, key, start, end)
}

func (s *CachedStore) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	result, err := s.backend.SetRange(ctx, key, offset, value)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) BitField(ctx context.Context, key string, ops []storage.BitFieldOp) ([]int64, error) {
	result, err := s.backend.BitField(ctx, key, ops)
	if err != nil {
		return nil, err
	}
	if bitFieldHasWrites(ops) {
		s.invalidate(ctx, key)
	}
	return result, nil
}

func bitFieldHasWrites(ops []storage.BitFieldOp) bool {
	for _, op := range ops {
		if op.OpType != "GET" && op.OpType != "OVERFLOW" {
			return true
		}
	}
	return false
}

func (s *CachedStore) StrLen(ctx context.Context, key string) (int64, error) {
	return s.backend.StrLen(ctx, key)
}

func (s *CachedStore) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	result, found, err := s.backend.GetEx(ctx, key, ttl, persist)
	if err != nil {
		return "", false, err
	}
	s.invalidate(ctx, key) // TTL change
	return result, found, nil
}

func (s *CachedStore) GetDel(ctx context.Context, key string) (string, bool, error) {
	result, found, err := s.backend.GetDel(ctx, key)
	if err != nil {
		return "", false, err
	}
	s.invalidate(ctx, key)
	return result, found, nil
}

func (s *CachedStore) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	result, found, err := s.backend.GetSet(ctx, key, value)
	if err != nil {
		return "", false, err
	}
	s.invalidate(ctx, key)
	return result, found, nil
}

func (s *CachedStore) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	result, err := s.backend.IncrByFloat(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

// ============== Key Commands ==============

func (s *CachedStore) Del(ctx context.Context, keys []string) (int64, error) {
	count, err := s.backend.Del(ctx, keys)
	if err != nil {
		return 0, err
	}
	s.invalidateMulti(ctx, keys)
	return count, nil
}

func (s *CachedStore) Exists(ctx context.Context, keys []string) (int64, error) {
	return s.backend.Exists(ctx, keys)
}

func (s *CachedStore) Expire(ctx context.Context, key string, ttl time.Duration, opts storage.ExpireOptions) (bool, error) {
	ok, err := s.backend.Expire(ctx, key, ttl, opts)
	if err != nil {
		return false, err
	}
	// Invalidate on TTL change (distributed)
	if ok {
		s.invalidate(ctx, key)
	}
	return ok, nil
}

func (s *CachedStore) TTL(ctx context.Context, key string) (int64, error) {
	return s.backend.TTL(ctx, key)
}

func (s *CachedStore) PTTL(ctx context.Context, key string) (int64, error) {
	return s.backend.PTTL(ctx, key)
}

func (s *CachedStore) Persist(ctx context.Context, key string) (bool, error) {
	ok, err := s.backend.Persist(ctx, key)
	if err != nil {
		return false, err
	}
	s.invalidate(ctx, key)
	return ok, nil
}

func (s *CachedStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	return s.backend.Keys(ctx, pattern)
}

func (s *CachedStore) RandomKey(ctx context.Context) (string, bool, error) {
	return s.backend.RandomKey(ctx)
}

func (s *CachedStore) Type(ctx context.Context, key string) (storage.KeyType, error) {
	return s.backend.Type(ctx, key)
}

func (s *CachedStore) Rename(ctx context.Context, oldKey, newKey string) error {
	err := s.backend.Rename(ctx, oldKey, newKey)
	if err != nil {
		return err
	}
	s.invalidateMulti(ctx, []string{oldKey, newKey})
	return nil
}

// ============== Hash Commands (pass-through, no caching) ==============

func (s *CachedStore) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return s.backend.HGet(ctx, key, field)
}

func (s *CachedStore) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	return s.backend.HSet(ctx, key, fields)
}

func (s *CachedStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return s.backend.HDel(ctx, key, fields)
}

func (s *CachedStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.backend.HGetAll(ctx, key)
}

func (s *CachedStore) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return s.backend.HMGet(ctx, key, fields)
}

func (s *CachedStore) HExists(ctx context.Context, key, field string) (bool, error) {
	return s.backend.HExists(ctx, key, field)
}

func (s *CachedStore) HKeys(ctx context.Context, key string) ([]string, error) {
	return s.backend.HKeys(ctx, key)
}

func (s *CachedStore) HVals(ctx context.Context, key string) ([]string, error) {
	return s.backend.HVals(ctx, key)
}

func (s *CachedStore) HLen(ctx context.Context, key string) (int64, error) {
	return s.backend.HLen(ctx, key)
}

func (s *CachedStore) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	result, err := s.backend.HIncrBy(ctx, key, field, increment)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

// ============== List Commands (pass-through, no caching) ==============

func (s *CachedStore) LPush(ctx context.Context, key string, values []string) (int64, error) {
	return s.backend.LPush(ctx, key, values)
}

func (s *CachedStore) RPush(ctx context.Context, key string, values []string) (int64, error) {
	return s.backend.RPush(ctx, key, values)
}

func (s *CachedStore) LPop(ctx context.Context, key string) (string, bool, error) {
	return s.backend.LPop(ctx, key)
}

func (s *CachedStore) RPop(ctx context.Context, key string) (string, bool, error) {
	return s.backend.RPop(ctx, key)
}

func (s *CachedStore) LPopMulti(ctx context.Context, keys []string) (string, string, bool, error) {
	return s.backend.LPopMulti(ctx, keys)
}

func (s *CachedStore) RPopMulti(ctx context.Context, keys []string) (string, string, bool, error) {
	return s.backend.RPopMulti(ctx, keys)
}

func (s *CachedStore) LMPop(ctx context.Context, keys []string, count int64) (string, []string, bool, error) {
	return s.backend.LMPop(ctx, keys, count)
}

func (s *CachedStore) RMPop(ctx context.Context, keys []string, count int64) (string, []string, bool, error) {
	return s.backend.RMPop(ctx, keys, count)
}

func (s *CachedStore) ZMPopMin(ctx context.Context, keys []string, count int64) (string, []storage.ZMember, bool, error) {
	return s.backend.ZMPopMin(ctx, keys, count)
}

func (s *CachedStore) ZMPopMax(ctx context.Context, keys []string, count int64) (string, []storage.ZMember, bool, error) {
	return s.backend.ZMPopMax(ctx, keys, count)
}

func (s *CachedStore) LLen(ctx context.Context, key string) (int64, error) {
	return s.backend.LLen(ctx, key)
}

func (s *CachedStore) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return s.backend.LRange(ctx, key, start, stop)
}

func (s *CachedStore) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return s.backend.LIndex(ctx, key, index)
}

// ============== Set Commands (pass-through, no caching) ==============

func (s *CachedStore) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.SAdd(ctx, key, members)
}

func (s *CachedStore) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.SRem(ctx, key, members)
}

func (s *CachedStore) SMembers(ctx context.Context, key string) ([]string, error) {
	return s.backend.SMembers(ctx, key)
}

func (s *CachedStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return s.backend.SIsMember(ctx, key, member)
}

func (s *CachedStore) SCard(ctx context.Context, key string) (int64, error) {
	return s.backend.SCard(ctx, key)
}

// ============== Sorted Set Commands ==============

func (s *CachedStore) ZAdd(ctx context.Context, key string, members []storage.ZMember) (int64, error) {
	return s.backend.ZAdd(ctx, key, members)
}

func (s *CachedStore) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]storage.ZMember, error) {
	return s.backend.ZRange(ctx, key, start, stop, withScores)
}

func (s *CachedStore) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return s.backend.ZScore(ctx, key, member)
}

func (s *CachedStore) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.ZRem(ctx, key, members)
}

func (s *CachedStore) ZCard(ctx context.Context, key string) (int64, error) {
	return s.backend.ZCard(ctx, key)
}

func (s *CachedStore) ZRevRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]storage.ZMember, error) {
	return s.backend.ZRevRange(ctx, key, start, stop, withScores)
}

func (s *CachedStore) ZRevRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]storage.ZMember, error) {
	return s.backend.ZRevRangeByScore(ctx, key, min, max, withScores, offset, count)
}

func (s *CachedStore) ZRangeByLex(ctx context.Context, key string, min, max storage.LexBound, offset, count int64) ([]string, error) {
	return s.backend.ZRangeByLex(ctx, key, min, max, offset, count)
}

func (s *CachedStore) ZRevRangeByLex(ctx context.Context, key string, min, max storage.LexBound, offset, count int64) ([]string, error) {
	return s.backend.ZRevRangeByLex(ctx, key, min, max, offset, count)
}

func (s *CachedStore) ZLexCount(ctx context.Context, key string, min, max storage.LexBound) (int64, error) {
	return s.backend.ZLexCount(ctx, key, min, max)
}

func (s *CachedStore) ZRangeStore(ctx context.Context, dst, src string, spec storage.ZRangeStoreSpec) (int64, error) {
	n, err := s.backend.ZRangeStore(ctx, dst, src, spec)
	if err != nil {
		return 0, err
	}
	// dst is a zset; not subject to caching, but we still invalidate to be
	// safe if a string with the same name was cached previously.
	s.invalidate(ctx, dst)
	return n, nil
}

func (s *CachedStore) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]storage.ZMember, error) {
	return s.backend.ZRangeByScore(ctx, key, min, max, withScores, offset, count)
}

func (s *CachedStore) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	result, err := s.backend.ZRemRangeByScore(ctx, key, min, max)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	result, err := s.backend.ZRemRangeByRank(ctx, key, start, stop)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	result, err := s.backend.ZIncrBy(ctx, key, increment, member)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) ZPopMin(ctx context.Context, key string, count int64) ([]storage.ZMember, error) {
	result, err := s.backend.ZPopMin(ctx, key, count)
	if err != nil {
		return nil, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	result, err := s.backend.LRem(ctx, key, count, element)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	err := s.backend.LTrim(ctx, key, start, stop)
	if err != nil {
		return err
	}
	s.invalidate(ctx, key)
	return nil
}

func (s *CachedStore) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	result, found, err := s.backend.RPopLPush(ctx, source, destination)
	if err != nil {
		return "", false, err
	}
	s.invalidateMulti(ctx, []string{source, destination})
	return result, found, nil
}

// ============== HyperLogLog Commands ==============

func (s *CachedStore) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	result, err := s.backend.PFAdd(ctx, key, elements)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) PFCount(ctx context.Context, keys []string) (int64, error) {
	return s.backend.PFCount(ctx, keys)
}

func (s *CachedStore) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	err := s.backend.PFMerge(ctx, destKey, sourceKeys)
	if err != nil {
		return err
	}
	s.invalidate(ctx, destKey)
	return nil
}

// ============== Server Commands ==============

func (s *CachedStore) DBSize(ctx context.Context) (int64, error) {
	return s.backend.DBSize(ctx)
}

func (s *CachedStore) FlushDB(ctx context.Context) error {
	err := s.backend.FlushDB(ctx)
	if err != nil {
		return err
	}
	s.flush(ctx)
	return nil
}

// ============== Transaction Support ==============

// BeginTx starts a transaction on the underlying backend wrapped in a CachingTx.
// The CachingTx records which keys are written; on a successful Commit those keys
// are invalidated so the in-memory cache does not serve stale data after MULTI/EXEC.
// Reads inside the transaction always go to PostgreSQL (correct for write-then-read
// within the same EXEC block).
func (s *CachedStore) BeginTx(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.backend.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return newCachingTx(tx, s.invalidateMulti), nil
}

// ============== Hash Extensions ==============

func (s *CachedStore) HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	result, err := s.backend.HIncrByFloat(ctx, key, field, increment)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	result, err := s.backend.HSetNX(ctx, key, field, value)
	if err != nil {
		return false, err
	}
	if result {
		s.invalidate(ctx, key)
	}
	return result, nil
}

// ============== List Extensions ==============

func (s *CachedStore) LPos(ctx context.Context, key, element string, rank, count, maxlen int64) ([]int64, error) {
	return s.backend.LPos(ctx, key, element, rank, count, maxlen)
}

func (s *CachedStore) LSet(ctx context.Context, key string, index int64, element string) error {
	err := s.backend.LSet(ctx, key, index, element)
	if err != nil {
		return err
	}
	s.invalidate(ctx, key)
	return nil
}

func (s *CachedStore) LInsert(ctx context.Context, key, pivot, element string, before bool) (int64, error) {
	result, err := s.backend.LInsert(ctx, key, pivot, element, before)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

// ============== Set Extensions ==============

func (s *CachedStore) SMIsMember(ctx context.Context, key string, members []string) ([]bool, error) {
	return s.backend.SMIsMember(ctx, key, members)
}

func (s *CachedStore) SInter(ctx context.Context, keys []string) ([]string, error) {
	return s.backend.SInter(ctx, keys)
}

func (s *CachedStore) SInterStore(ctx context.Context, destination string, keys []string) (int64, error) {
	result, err := s.backend.SInterStore(ctx, destination, keys)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destination)
	return result, nil
}

func (s *CachedStore) SUnion(ctx context.Context, keys []string) ([]string, error) {
	return s.backend.SUnion(ctx, keys)
}

func (s *CachedStore) SUnionStore(ctx context.Context, destination string, keys []string) (int64, error) {
	result, err := s.backend.SUnionStore(ctx, destination, keys)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destination)
	return result, nil
}

func (s *CachedStore) SDiff(ctx context.Context, keys []string) ([]string, error) {
	return s.backend.SDiff(ctx, keys)
}

func (s *CachedStore) SInterCard(ctx context.Context, keys []string, limit int64) (int64, error) {
	return s.backend.SInterCard(ctx, keys, limit)
}

func (s *CachedStore) HRandField(ctx context.Context, key string, count int64, withValues bool) ([]string, error) {
	return s.backend.HRandField(ctx, key, count, withValues)
}

func (s *CachedStore) SRandMember(ctx context.Context, key string, count int64) ([]string, error) {
	return s.backend.SRandMember(ctx, key, count)
}

func (s *CachedStore) ZRandMember(ctx context.Context, key string, count int64) ([]storage.ZMember, error) {
	return s.backend.ZRandMember(ctx, key, count)
}

func (s *CachedStore) SDiffStore(ctx context.Context, destination string, keys []string) (int64, error) {
	result, err := s.backend.SDiffStore(ctx, destination, keys)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destination)
	return result, nil
}

// ============== Sorted Set Extensions ==============

func (s *CachedStore) ZPopMax(ctx context.Context, key string, count int64) ([]storage.ZMember, error) {
	result, err := s.backend.ZPopMax(ctx, key, count)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		s.invalidate(ctx, key)
	}
	return result, nil
}

func (s *CachedStore) ZRank(ctx context.Context, key, member string) (int64, bool, error) {
	return s.backend.ZRank(ctx, key, member)
}

func (s *CachedStore) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	return s.backend.ZRevRank(ctx, key, member)
}

func (s *CachedStore) ZCount(ctx context.Context, key string, min, max float64) (int64, error) {
	return s.backend.ZCount(ctx, key, min, max)
}

func (s *CachedStore) ZScan(ctx context.Context, key string, cursor int64, pattern string, count int64) (int64, []storage.ZMember, error) {
	return s.backend.ZScan(ctx, key, cursor, pattern, count)
}

func (s *CachedStore) ZUnionStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	result, err := s.backend.ZUnionStore(ctx, destination, keys, weights, aggregate)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destination)
	return result, nil
}

func (s *CachedStore) ZInterStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	result, err := s.backend.ZInterStore(ctx, destination, keys, weights, aggregate)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destination)
	return result, nil
}

// ============== Key Extensions ==============

func (s *CachedStore) ExpireAt(ctx context.Context, key string, expireTime time.Time, opts storage.ExpireOptions) (bool, error) {
	result, err := s.backend.ExpireAt(ctx, key, expireTime, opts)
	if err != nil {
		return false, err
	}
	if result {
		s.invalidate(ctx, key)
	}
	return result, nil
}

func (s *CachedStore) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	result, err := s.backend.Copy(ctx, source, destination, replace)
	if err != nil {
		return false, err
	}
	if result {
		s.invalidate(ctx, destination)
	}
	return result, nil
}

// ============== Bitmap Commands ==============

func (s *CachedStore) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	result, err := s.backend.SetBit(ctx, key, offset, value)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, key)
	return result, nil
}

func (s *CachedStore) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	return s.backend.GetBit(ctx, key, offset)
}

func (s *CachedStore) BitCount(ctx context.Context, key string, start, end int64, useBit bool) (int64, error) {
	return s.backend.BitCount(ctx, key, start, end, useBit)
}

func (s *CachedStore) BitOp(ctx context.Context, operation, destKey string, keys []string) (int64, error) {
	result, err := s.backend.BitOp(ctx, operation, destKey, keys)
	if err != nil {
		return 0, err
	}
	s.invalidate(ctx, destKey)
	return result, nil
}

func (s *CachedStore) BitPos(ctx context.Context, key string, bit int, start, end int64, useBit bool) (int64, error) {
	return s.backend.BitPos(ctx, key, bit, start, end, useBit)
}

// Ensure CachedStore implements Backend
var _ storage.Backend = (*CachedStore)(nil)
