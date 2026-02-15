// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides PostgreSQL-backed storage for Redis operations
type Store struct {
	pool          *pgxpool.Pool
	connStr       string
	ops           queryOps
	sqlTraceLevel int // 0=off, 1=important, 2=most queries, 3=everything
}

// Config holds PostgreSQL connection configuration
type Config struct {
	Host          string
	Port          int
	User          string
	Password      string
	Database      string
	SSLMode       string
	SQLTraceLevel int // 0=off, 1=important, 2=most queries, 3=everything

	// Connection pool settings
	MaxConns         int           // Maximum number of connections in the pool
	MinConns         int           // Minimum number of connections in the pool
	MaxConnLifetime  time.Duration // Maximum lifetime of a connection before it's closed
	MaxConnIdleTime  time.Duration // Maximum time a connection can be idle before it's closed
	HealthCheckPeriod time.Duration // Period between health checks on idle connections
}

// New creates a new Store with the given configuration
func New(ctx context.Context, cfg Config) (*Store, error) {
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)

	// Parse config and set pool parameters for better resilience
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Configure pool settings for handling master node switchovers
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = int32(cfg.MaxConns)
	}
	if cfg.MinConns > 0 {
		poolConfig.MinConns = int32(cfg.MinConns)
	}
	if cfg.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	store := &Store{pool: pool, connStr: connStr, sqlTraceLevel: cfg.SQLTraceLevel}
	if err := store.initSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Start background goroutine to clean expired keys
	go store.cleanupExpiredKeys(ctx)

	return store, nil
}

// Close closes the database connection pool
func (s *Store) Close() {
	s.pool.Close()
}

// Pool returns the underlying connection pool
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// ConnString returns the connection string
func (s *Store) ConnString() string {
	return s.connStr
}

func (s *Store) initSchema(ctx context.Context) error {
	schema := `
		-- Main key-value store for string types (BYTEA for binary-safe storage)
		CREATE TABLE IF NOT EXISTS kv_strings (
			key TEXT PRIMARY KEY,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_strings_expires ON kv_strings(expires_at) WHERE expires_at IS NOT NULL;

		-- Hash type storage
		CREATE TABLE IF NOT EXISTS kv_hashes (
			key TEXT NOT NULL,
			field TEXT NOT NULL,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, field)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_hashes_expires ON kv_hashes(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_hashes_key ON kv_hashes(key);

		-- List type storage
		CREATE TABLE IF NOT EXISTS kv_lists (
			key TEXT NOT NULL,
			idx BIGINT NOT NULL,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, idx)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_lists_expires ON kv_lists(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_lists_key ON kv_lists(key);

		-- Set type storage
		CREATE TABLE IF NOT EXISTS kv_sets (
			key TEXT NOT NULL,
			member BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, member)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_sets_expires ON kv_sets(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_sets_key ON kv_sets(key);

		-- Sorted set type storage
		CREATE TABLE IF NOT EXISTS kv_zsets (
			key TEXT NOT NULL,
			member BYTEA NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, member)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_expires ON kv_zsets(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_key ON kv_zsets(key);
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_score ON kv_zsets(key, score);

		-- Key metadata for tracking types and TTL
		CREATE TABLE IF NOT EXISTS kv_meta (
			key TEXT PRIMARY KEY,
			key_type TEXT NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_meta_expires ON kv_meta(expires_at) WHERE expires_at IS NOT NULL;

		-- HyperLogLog storage (stores serialized HLL registers)
		CREATE TABLE IF NOT EXISTS kv_hyperloglog (
			key TEXT PRIMARY KEY,
			registers BYTEA NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_hyperloglog_expires ON kv_hyperloglog(expires_at) WHERE expires_at IS NOT NULL;
	`
	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *Store) cleanupExpiredKeys(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.deleteExpiredKeys(context.Background())
		}
	}
}

func (s *Store) deleteExpiredKeys(ctx context.Context) {
	now := time.Now()
	queries := []string{
		"DELETE FROM kv_strings WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_hashes WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_lists WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_sets WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_meta WHERE expires_at IS NOT NULL AND expires_at <= $1",
	}
	for _, q := range queries {
		s.pool.Exec(ctx, q, now)
	}
}

// withTx wraps an operation in a transaction
func (s *Store) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// querier returns a Querier, optionally wrapped with tracing
func (s *Store) querier() Querier {
	if s.sqlTraceLevel > 0 {
		return NewTracingQuerier(s.pool, s.sqlTraceLevel)
	}
	return s.pool
}

// txQuerier returns a Querier for a transaction, optionally wrapped with tracing
func (s *Store) txQuerier(tx pgx.Tx) Querier {
	if s.sqlTraceLevel > 0 {
		return NewTracingQuerier(tx, s.sqlTraceLevel)
	}
	return tx
}

// BeginTx starts a new transaction
func (s *Store) BeginTx(ctx context.Context) (Transaction, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TxStore{tx: tx, sqlTraceLevel: s.sqlTraceLevel}, nil
}

// ============== String Commands ==============

func (s *Store) Get(ctx context.Context, key string) (string, bool, error) {
	return s.ops.get(ctx, s.querier(), key)
}

func (s *Store) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.set(ctx, s.txQuerier(tx), key, value, ttl)
	})
}

func (s *Store) SetNX(ctx context.Context, key, value string) (bool, error) {
	return s.ops.setNX(ctx, s.querier(), key, value)
}

func (s *Store) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	return s.ops.mGet(ctx, s.querier(), keys)
}

func (s *Store) MSet(ctx context.Context, pairs map[string]string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.mSet(ctx, s.txQuerier(tx), pairs)
	})
}

func (s *Store) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.incr(ctx, s.txQuerier(tx), key, delta)
		return err
	})
	return result, err
}

func (s *Store) Append(ctx context.Context, key, value string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.appendStr(ctx, s.txQuerier(tx), key, value)
		return err
	})
	return result, err
}

func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return s.ops.getRange(ctx, s.querier(), key, start, end)
}

func (s *Store) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.setRange(ctx, s.txQuerier(tx), key, offset, value)
		return err
	})
	return result, err
}

func (s *Store) BitField(ctx context.Context, key string, ops []BitFieldOp) ([]int64, error) {
	var result []int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.bitField(ctx, s.txQuerier(tx), key, ops)
		return err
	})
	return result, err
}

func (s *Store) StrLen(ctx context.Context, key string) (int64, error) {
	return s.ops.strLen(ctx, s.querier(), key)
}

func (s *Store) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	var result string
	var exists bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, exists, err = s.ops.getEx(ctx, s.txQuerier(tx), key, ttl, persist)
		return err
	})
	return result, exists, err
}

func (s *Store) GetDel(ctx context.Context, key string) (string, bool, error) {
	var result string
	var exists bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, exists, err = s.ops.getDel(ctx, s.txQuerier(tx), key)
		return err
	})
	return result, exists, err
}

func (s *Store) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	var result string
	var exists bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, exists, err = s.ops.getSet(ctx, s.txQuerier(tx), key, value)
		return err
	})
	return result, exists, err
}

func (s *Store) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	var result float64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.incrByFloat(ctx, s.txQuerier(tx), key, delta)
		return err
	})
	return result, err
}

// ============== Key Commands ==============

func (s *Store) Del(ctx context.Context, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.del(ctx, s.txQuerier(tx), keys)
		return err
	})
	return result, err
}

func (s *Store) Exists(ctx context.Context, keys []string) (int64, error) {
	return s.ops.exists(ctx, s.querier(), keys)
}

func (s *Store) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return s.ops.expire(ctx, s.querier(), key, ttl)
}

func (s *Store) TTL(ctx context.Context, key string) (int64, error) {
	return s.ops.ttl(ctx, s.querier(), key)
}

func (s *Store) PTTL(ctx context.Context, key string) (int64, error) {
	return s.ops.pttl(ctx, s.querier(), key)
}

func (s *Store) Persist(ctx context.Context, key string) (bool, error) {
	return s.ops.persist(ctx, s.querier(), key)
}

func (s *Store) Keys(ctx context.Context, pattern string) ([]string, error) {
	return s.ops.keys(ctx, s.querier(), pattern)
}

func (s *Store) Type(ctx context.Context, key string) (KeyType, error) {
	return s.ops.keyType(ctx, s.querier(), key)
}

func (s *Store) Rename(ctx context.Context, oldKey, newKey string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.rename(ctx, s.txQuerier(tx), oldKey, newKey)
	})
}

func (s *Store) ExpireAt(ctx context.Context, key string, timestamp time.Time) (bool, error) {
	return s.ops.expireAt(ctx, s.querier(), key, timestamp)
}

func (s *Store) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	var result bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.copyKey(ctx, s.txQuerier(tx), source, destination, replace)
		return err
	})
	return result, err
}

// ============== Bitmap Commands ==============

func (s *Store) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.setBit(ctx, s.txQuerier(tx), key, offset, value)
		return err
	})
	return result, err
}

func (s *Store) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	return s.ops.getBit(ctx, s.querier(), key, offset)
}

func (s *Store) BitCount(ctx context.Context, key string, start, end int64, useBit bool) (int64, error) {
	return s.ops.bitCount(ctx, s.querier(), key, start, end, useBit)
}

func (s *Store) BitOp(ctx context.Context, operation, destKey string, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.bitOp(ctx, s.txQuerier(tx), operation, destKey, keys)
		return err
	})
	return result, err
}

func (s *Store) BitPos(ctx context.Context, key string, bit int, start, end int64, useBit bool) (int64, error) {
	return s.ops.bitPos(ctx, s.querier(), key, bit, start, end, useBit)
}

// ============== Hash Commands ==============

func (s *Store) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return s.ops.hGet(ctx, s.querier(), key, field)
}

func (s *Store) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.hSet(ctx, s.txQuerier(tx), key, fields)
		return err
	})
	return result, err
}

func (s *Store) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return s.ops.hDel(ctx, s.querier(), key, fields)
}

func (s *Store) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.ops.hGetAll(ctx, s.querier(), key)
}

func (s *Store) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return s.ops.hMGet(ctx, s.querier(), key, fields)
}

func (s *Store) HExists(ctx context.Context, key, field string) (bool, error) {
	return s.ops.hExists(ctx, s.querier(), key, field)
}

func (s *Store) HKeys(ctx context.Context, key string) ([]string, error) {
	return s.ops.hKeys(ctx, s.querier(), key)
}

func (s *Store) HVals(ctx context.Context, key string) ([]string, error) {
	return s.ops.hVals(ctx, s.querier(), key)
}

func (s *Store) HLen(ctx context.Context, key string) (int64, error) {
	return s.ops.hLen(ctx, s.querier(), key)
}

func (s *Store) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.hIncrBy(ctx, s.txQuerier(tx), key, field, increment)
		return err
	})
	return result, err
}

func (s *Store) HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	var result float64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.hIncrByFloat(ctx, s.txQuerier(tx), key, field, increment)
		return err
	})
	return result, err
}

func (s *Store) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	var result bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.hSetNX(ctx, s.txQuerier(tx), key, field, value)
		return err
	})
	return result, err
}

// ============== List Commands ==============

func (s *Store) LPush(ctx context.Context, key string, values []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.lPush(ctx, s.txQuerier(tx), key, values)
		return err
	})
	return result, err
}

func (s *Store) RPush(ctx context.Context, key string, values []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.rPush(ctx, s.txQuerier(tx), key, values)
		return err
	})
	return result, err
}

func (s *Store) LPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		value, found, err = s.ops.lPop(ctx, s.txQuerier(tx), key)
		return err
	})
	return value, found, err
}

func (s *Store) RPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		value, found, err = s.ops.rPop(ctx, s.txQuerier(tx), key)
		return err
	})
	return value, found, err
}

func (s *Store) LLen(ctx context.Context, key string) (int64, error) {
	return s.ops.lLen(ctx, s.querier(), key)
}

func (s *Store) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return s.ops.lRange(ctx, s.querier(), key, start, stop)
}

func (s *Store) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return s.ops.lIndex(ctx, s.querier(), key, index)
}

// ============== Set Commands ==============

func (s *Store) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.sAdd(ctx, s.txQuerier(tx), key, members)
		return err
	})
	return result, err
}

func (s *Store) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.ops.sRem(ctx, s.querier(), key, members)
}

func (s *Store) SMembers(ctx context.Context, key string) ([]string, error) {
	return s.ops.sMembers(ctx, s.querier(), key)
}

func (s *Store) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return s.ops.sIsMember(ctx, s.querier(), key, member)
}

func (s *Store) SCard(ctx context.Context, key string) (int64, error) {
	return s.ops.sCard(ctx, s.querier(), key)
}

func (s *Store) SMIsMember(ctx context.Context, key string, members []string) ([]bool, error) {
	return s.ops.sMIsMember(ctx, s.querier(), key, members)
}

func (s *Store) SInter(ctx context.Context, keys []string) ([]string, error) {
	return s.ops.sInter(ctx, s.querier(), keys)
}

func (s *Store) SInterStore(ctx context.Context, destination string, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.sInterStore(ctx, s.txQuerier(tx), destination, keys)
		return err
	})
	return result, err
}

func (s *Store) SUnion(ctx context.Context, keys []string) ([]string, error) {
	return s.ops.sUnion(ctx, s.querier(), keys)
}

func (s *Store) SUnionStore(ctx context.Context, destination string, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.sUnionStore(ctx, s.txQuerier(tx), destination, keys)
		return err
	})
	return result, err
}

func (s *Store) SDiff(ctx context.Context, keys []string) ([]string, error) {
	return s.ops.sDiff(ctx, s.querier(), keys)
}

func (s *Store) SDiffStore(ctx context.Context, destination string, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.sDiffStore(ctx, s.txQuerier(tx), destination, keys)
		return err
	})
	return result, err
}

// ============== Sorted Set Commands ==============

func (s *Store) ZAdd(ctx context.Context, key string, members []ZMember) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zAdd(ctx, s.txQuerier(tx), key, members)
		return err
	})
	return result, err
}

func (s *Store) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return s.ops.zRange(ctx, s.querier(), key, start, stop, withScores)
}

func (s *Store) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return s.ops.zScore(ctx, s.querier(), key, member)
}

func (s *Store) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.ops.zRem(ctx, s.querier(), key, members)
}

func (s *Store) ZCard(ctx context.Context, key string) (int64, error) {
	return s.ops.zCard(ctx, s.querier(), key)
}

func (s *Store) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return s.ops.zRangeByScore(ctx, s.querier(), key, min, max, withScores, offset, count)
}

func (s *Store) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	return s.ops.zRemRangeByScore(ctx, s.querier(), key, min, max)
}

func (s *Store) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return s.ops.zRemRangeByRank(ctx, s.querier(), key, start, stop)
}

func (s *Store) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	var result float64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zIncrBy(ctx, s.txQuerier(tx), key, increment, member)
		return err
	})
	return result, err
}

func (s *Store) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	var result []ZMember
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zPopMin(ctx, s.txQuerier(tx), key, count)
		return err
	})
	return result, err
}

func (s *Store) ZPopMax(ctx context.Context, key string, count int64) ([]ZMember, error) {
	var result []ZMember
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zPopMax(ctx, s.txQuerier(tx), key, count)
		return err
	})
	return result, err
}

func (s *Store) ZRank(ctx context.Context, key, member string) (int64, bool, error) {
	return s.ops.zRank(ctx, s.querier(), key, member)
}

func (s *Store) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	return s.ops.zRevRank(ctx, s.querier(), key, member)
}

func (s *Store) ZCount(ctx context.Context, key string, min, max float64) (int64, error) {
	return s.ops.zCount(ctx, s.querier(), key, min, max)
}

func (s *Store) ZScan(ctx context.Context, key string, cursor int64, pattern string, count int64) (int64, []ZMember, error) {
	return s.ops.zScan(ctx, s.querier(), key, cursor, pattern, count)
}

func (s *Store) ZUnionStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zUnionStore(ctx, s.txQuerier(tx), destination, keys, weights, aggregate)
		return err
	})
	return result, err
}

func (s *Store) ZInterStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zInterStore(ctx, s.txQuerier(tx), destination, keys, weights, aggregate)
		return err
	})
	return result, err
}

func (s *Store) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	return s.ops.lRem(ctx, s.querier(), key, count, element)
}

func (s *Store) LTrim(ctx context.Context, key string, start, stop int64) error {
	return s.ops.lTrim(ctx, s.querier(), key, start, stop)
}

func (s *Store) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	var result string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, found, err = s.ops.rPopLPush(ctx, s.txQuerier(tx), source, destination)
		return err
	})
	return result, found, err
}
func (s *Store) LPos(ctx context.Context, key, element string, rank, count, maxlen int64) ([]int64, error) {
	return s.ops.lPos(ctx, s.querier(), key, element, rank, count, maxlen)
}

func (s *Store) LSet(ctx context.Context, key string, index int64, element string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.lSet(ctx, s.txQuerier(tx), key, index, element)
	})
}

func (s *Store) LInsert(ctx context.Context, key, pivot, element string, before bool) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.lInsert(ctx, s.txQuerier(tx), key, pivot, element, before)
		return err
	})
	return result, err
}
// ============== HyperLogLog Commands ==============

func (s *Store) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	return s.ops.pfAdd(ctx, s.querier(), key, elements)
}

func (s *Store) PFCount(ctx context.Context, keys []string) (int64, error) {
	return s.ops.pfCount(ctx, s.querier(), keys)
}

func (s *Store) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	return s.ops.pfMerge(ctx, s.querier(), destKey, sourceKeys)
}

// ============== Server Commands ==============

func (s *Store) DBSize(ctx context.Context) (int64, error) {
	return s.ops.dbSize(ctx, s.pool)
}

func (s *Store) FlushDB(ctx context.Context) error {
	queries := []string{
		"TRUNCATE kv_strings",
		"TRUNCATE kv_hashes",
		"TRUNCATE kv_lists",
		"TRUNCATE kv_sets",
		"TRUNCATE kv_zsets",
		"TRUNCATE kv_hyperloglog",
		"TRUNCATE kv_meta",
	}
	for _, q := range queries {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
