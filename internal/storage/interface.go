package storage

import (
	"context"
	"time"
)

// KeyType represents the type of a Redis key
type KeyType string

const (
	TypeString KeyType = "string"
	TypeHash   KeyType = "hash"
	TypeList   KeyType = "list"
	TypeSet    KeyType = "set"
	TypeZSet   KeyType = "zset"
	TypeNone   KeyType = "none"
)

// ZMember represents a sorted set member with its score
type ZMember struct {
	Member string
	Score  float64
}

// BitFieldOp represents a BITFIELD operation (GET, SET, INCRBY)
type BitFieldOp struct {
	OpType   string // "GET", "SET", "INCRBY"
	Encoding string // e.g., "u8", "i16", "u32"
	Offset   int64  // bit offset (can use # prefix for type-width multiplier)
	Value    int64  // for SET and INCRBY
}

// Operations defines the common storage operations available in both regular and transaction contexts
type Operations interface {
	// String commands
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key, value string) (bool, error)
	MGet(ctx context.Context, keys []string) ([]interface{}, error)
	MSet(ctx context.Context, pairs map[string]string) error
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	IncrByFloat(ctx context.Context, key string, delta float64) (float64, error)
	Append(ctx context.Context, key, value string) (int64, error)
	GetRange(ctx context.Context, key string, start, end int64) (string, error)
	SetRange(ctx context.Context, key string, offset int64, value string) (int64, error)
	StrLen(ctx context.Context, key string) (int64, error)
	GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error)
	GetDel(ctx context.Context, key string) (string, bool, error)
	GetSet(ctx context.Context, key, value string) (string, bool, error)
	BitField(ctx context.Context, key string, ops []BitFieldOp) ([]int64, error)

	// Key commands
	Del(ctx context.Context, keys []string) (int64, error)
	Exists(ctx context.Context, keys []string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ExpireAt(ctx context.Context, key string, timestamp time.Time) (bool, error)
	TTL(ctx context.Context, key string) (int64, error)
	PTTL(ctx context.Context, key string) (int64, error)
	Persist(ctx context.Context, key string) (bool, error)
	Keys(ctx context.Context, pattern string) ([]string, error)
	Type(ctx context.Context, key string) (KeyType, error)
	Rename(ctx context.Context, oldKey, newKey string) error
	Copy(ctx context.Context, source, destination string, replace bool) (bool, error)

	// Bitmap commands
	SetBit(ctx context.Context, key string, offset int64, value int) (int64, error)
	GetBit(ctx context.Context, key string, offset int64) (int64, error)
	BitCount(ctx context.Context, key string, start, end int64, useBit bool) (int64, error)
	BitOp(ctx context.Context, operation, destKey string, keys []string) (int64, error)
	BitPos(ctx context.Context, key string, bit int, start, end int64, useBit bool) (int64, error)

	// Hash commands
	HGet(ctx context.Context, key, field string) (string, bool, error)
	HSet(ctx context.Context, key string, fields map[string]string) (int64, error)
	HDel(ctx context.Context, key string, fields []string) (int64, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error)
	HExists(ctx context.Context, key, field string) (bool, error)
	HKeys(ctx context.Context, key string) ([]string, error)
	HVals(ctx context.Context, key string) ([]string, error)
	HLen(ctx context.Context, key string) (int64, error)
	HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error)
	HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error)
	HSetNX(ctx context.Context, key, field, value string) (bool, error)

	// List commands
	LPush(ctx context.Context, key string, values []string) (int64, error)
	RPush(ctx context.Context, key string, values []string) (int64, error)
	LPop(ctx context.Context, key string) (string, bool, error)
	RPop(ctx context.Context, key string) (string, bool, error)
	// LPopMulti / RPopMulti scan the supplied keys in order and pop one element
	// from the first non-empty list in a single SQL round-trip. Used to
	// implement multi-key BLPOP/BRPOP without N sequential queries.
	// Returns the key popped from, the popped value, and found=true on success.
	LPopMulti(ctx context.Context, keys []string) (key, value string, found bool, err error)
	RPopMulti(ctx context.Context, keys []string) (key, value string, found bool, err error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LIndex(ctx context.Context, key string, index int64) (string, bool, error)
	LRem(ctx context.Context, key string, count int64, element string) (int64, error)
	LTrim(ctx context.Context, key string, start, stop int64) error
	RPopLPush(ctx context.Context, source, destination string) (string, bool, error)
	LPos(ctx context.Context, key, element string, rank, count, maxlen int64) ([]int64, error)
	LSet(ctx context.Context, key string, index int64, element string) error
	LInsert(ctx context.Context, key, pivot, element string, before bool) (int64, error)

	// Set commands
	SAdd(ctx context.Context, key string, members []string) (int64, error)
	SRem(ctx context.Context, key string, members []string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SIsMember(ctx context.Context, key, member string) (bool, error)
	SCard(ctx context.Context, key string) (int64, error)
	SMIsMember(ctx context.Context, key string, members []string) ([]bool, error)
	SInter(ctx context.Context, keys []string) ([]string, error)
	SInterStore(ctx context.Context, destination string, keys []string) (int64, error)
	SUnion(ctx context.Context, keys []string) ([]string, error)
	SUnionStore(ctx context.Context, destination string, keys []string) (int64, error)
	SDiff(ctx context.Context, keys []string) ([]string, error)
	SDiffStore(ctx context.Context, destination string, keys []string) (int64, error)

	// Sorted set commands
	ZAdd(ctx context.Context, key string, members []ZMember) (int64, error)
	ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error)
	ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error)
	ZScore(ctx context.Context, key, member string) (float64, bool, error)
	ZRem(ctx context.Context, key string, members []string) (int64, error)
	ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error)
	ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error)
	ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error)
	ZPopMax(ctx context.Context, key string, count int64) ([]ZMember, error)
	ZRank(ctx context.Context, key, member string) (int64, bool, error)
	ZRevRank(ctx context.Context, key, member string) (int64, bool, error)
	ZCount(ctx context.Context, key string, min, max float64) (int64, error)
	ZScan(ctx context.Context, key string, cursor int64, pattern string, count int64) (int64, []ZMember, error)
	ZUnionStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error)
	ZInterStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error)

	// HyperLogLog commands
	PFAdd(ctx context.Context, key string, elements []string) (int64, error)
	PFCount(ctx context.Context, keys []string) (int64, error)
	PFMerge(ctx context.Context, destKey string, sourceKeys []string) error

	// Server commands
	DBSize(ctx context.Context) (int64, error)
}

// Backend extends Operations with lifecycle and transaction support
type Backend interface {
	Operations

	// Server commands (not available in transactions)
	FlushDB(ctx context.Context) error

	// Transaction support
	BeginTx(ctx context.Context) (Transaction, error)

	// Lifecycle
	Close()
}

// Transaction extends Operations with commit/rollback
type Transaction interface {
	Operations

	// Transaction control
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Ensure Store implements Backend
var _ Backend = (*Store)(nil)
