// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TxStore wraps a PostgreSQL transaction and implements the Transaction interface
type TxStore struct {
	tx            pgx.Tx
	ops           queryOps
	done          bool
	sqlTraceLevel int // 0=off, 1=important, 2=most queries, 3=everything
}

// querier returns a Querier for the transaction, optionally wrapped with tracing
func (t *TxStore) querier() Querier {
	if t.sqlTraceLevel > 0 {
		return NewTracingQuerier(t.tx, t.sqlTraceLevel)
	}
	return t.tx
}

// Commit commits the transaction
func (t *TxStore) Commit(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("transaction already completed")
	}
	t.done = true
	return t.tx.Commit(ctx)
}

// Rollback aborts the transaction
func (t *TxStore) Rollback(ctx context.Context) error {
	if t.done {
		return nil // Already done, no-op
	}
	t.done = true
	return t.tx.Rollback(ctx)
}

// ============== String Commands ==============

func (t *TxStore) Get(ctx context.Context, key string) (string, bool, error) {
	return t.ops.get(ctx, t.querier(), key)
}

func (t *TxStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return t.ops.set(ctx, t.querier(), key, value, ttl)
}

func (t *TxStore) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return t.ops.setNX(ctx, t.querier(), key, value, ttl)
}

func (t *TxStore) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	return t.ops.mGet(ctx, t.querier(), keys)
}

func (t *TxStore) MSet(ctx context.Context, pairs map[string]string) error {
	return t.ops.mSet(ctx, t.querier(), pairs)
}

func (t *TxStore) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return t.ops.incr(ctx, t.querier(), key, delta)
}

func (t *TxStore) Append(ctx context.Context, key, value string) (int64, error) {
	return t.ops.appendStr(ctx, t.querier(), key, value)
}

func (t *TxStore) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return t.ops.getRange(ctx, t.querier(), key, start, end)
}

func (t *TxStore) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return t.ops.setRange(ctx, t.querier(), key, offset, value)
}

func (t *TxStore) BitField(ctx context.Context, key string, ops []BitFieldOp) ([]*int64, error) {
	return t.ops.bitField(ctx, t.querier(), key, ops)
}

func (t *TxStore) StrLen(ctx context.Context, key string) (int64, error) {
	return t.ops.strLen(ctx, t.querier(), key)
}

func (t *TxStore) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	return t.ops.getEx(ctx, t.querier(), key, ttl, persist)
}

func (t *TxStore) GetDel(ctx context.Context, key string) (string, bool, error) {
	return t.ops.getDel(ctx, t.querier(), key)
}

func (t *TxStore) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	return t.ops.getSet(ctx, t.querier(), key, value)
}

func (t *TxStore) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	return t.ops.incrByFloat(ctx, t.querier(), key, delta)
}

// ============== Key Commands ==============

func (t *TxStore) Del(ctx context.Context, keys []string) (int64, error) {
	return t.ops.del(ctx, t.querier(), keys)
}

func (t *TxStore) Exists(ctx context.Context, keys []string) (int64, error) {
	return t.ops.exists(ctx, t.querier(), keys)
}

func (t *TxStore) Expire(ctx context.Context, key string, ttl time.Duration, opts ExpireOptions) (bool, error) {
	return t.ops.expire(ctx, t.querier(), key, ttl, opts)
}

func (t *TxStore) TTL(ctx context.Context, key string) (int64, error) {
	return t.ops.ttl(ctx, t.querier(), key)
}

func (t *TxStore) PTTL(ctx context.Context, key string) (int64, error) {
	return t.ops.pttl(ctx, t.querier(), key)
}

func (t *TxStore) Persist(ctx context.Context, key string) (bool, error) {
	return t.ops.persist(ctx, t.querier(), key)
}

func (t *TxStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	return t.ops.keys(ctx, t.querier(), pattern)
}

func (t *TxStore) RandomKey(ctx context.Context) (string, bool, error) {
	return t.ops.randomKey(ctx, t.querier())
}

func (t *TxStore) Type(ctx context.Context, key string) (KeyType, error) {
	return t.ops.keyType(ctx, t.querier(), key)
}

func (t *TxStore) Rename(ctx context.Context, oldKey, newKey string) error {
	return t.ops.rename(ctx, t.querier(), oldKey, newKey)
}

func (t *TxStore) ExpireAt(ctx context.Context, key string, timestamp time.Time, opts ExpireOptions) (bool, error) {
	return t.ops.expireAt(ctx, t.querier(), key, timestamp, opts)
}

func (t *TxStore) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	return t.ops.copyKey(ctx, t.querier(), source, destination, replace)
}

// ============== Bitmap Commands ==============

func (t *TxStore) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	return t.ops.setBit(ctx, t.querier(), key, offset, value)
}

func (t *TxStore) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	return t.ops.getBit(ctx, t.querier(), key, offset)
}

func (t *TxStore) BitCount(ctx context.Context, key string, start, end int64, useBit bool) (int64, error) {
	return t.ops.bitCount(ctx, t.querier(), key, start, end, useBit)
}

func (t *TxStore) BitOp(ctx context.Context, operation, destKey string, keys []string) (int64, error) {
	return t.ops.bitOp(ctx, t.querier(), operation, destKey, keys)
}

func (t *TxStore) BitPos(ctx context.Context, key string, bit int, start, end int64, useBit bool) (int64, error) {
	return t.ops.bitPos(ctx, t.querier(), key, bit, start, end, useBit)
}

// ============== Hash Commands ==============

func (t *TxStore) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return t.ops.hGet(ctx, t.querier(), key, field)
}

func (t *TxStore) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	return t.ops.hSet(ctx, t.querier(), key, fields)
}

func (t *TxStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return t.ops.hDel(ctx, t.querier(), key, fields)
}

func (t *TxStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return t.ops.hGetAll(ctx, t.querier(), key)
}

func (t *TxStore) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return t.ops.hMGet(ctx, t.querier(), key, fields)
}

func (t *TxStore) HExists(ctx context.Context, key, field string) (bool, error) {
	return t.ops.hExists(ctx, t.querier(), key, field)
}

func (t *TxStore) HKeys(ctx context.Context, key string) ([]string, error) {
	return t.ops.hKeys(ctx, t.querier(), key)
}

func (t *TxStore) HVals(ctx context.Context, key string) ([]string, error) {
	return t.ops.hVals(ctx, t.querier(), key)
}

func (t *TxStore) HLen(ctx context.Context, key string) (int64, error) {
	return t.ops.hLen(ctx, t.querier(), key)
}

func (t *TxStore) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	return t.ops.hIncrBy(ctx, t.querier(), key, field, increment)
}

func (t *TxStore) HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	return t.ops.hIncrByFloat(ctx, t.querier(), key, field, increment)
}

func (t *TxStore) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	return t.ops.hSetNX(ctx, t.querier(), key, field, value)
}

// ============== List Commands ==============

func (t *TxStore) LPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.ops.lPush(ctx, t.querier(), key, values)
}

func (t *TxStore) RPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.ops.rPush(ctx, t.querier(), key, values)
}

func (t *TxStore) LPop(ctx context.Context, key string) (string, bool, error) {
	return t.ops.lPop(ctx, t.querier(), key)
}

func (t *TxStore) RPop(ctx context.Context, key string) (string, bool, error) {
	return t.ops.rPop(ctx, t.querier(), key)
}

func (t *TxStore) LPopMulti(ctx context.Context, keys []string) (string, string, bool, error) {
	return t.ops.lPopMulti(ctx, t.querier(), keys)
}

func (t *TxStore) RPopMulti(ctx context.Context, keys []string) (string, string, bool, error) {
	return t.ops.rPopMulti(ctx, t.querier(), keys)
}

func (t *TxStore) LMPop(ctx context.Context, keys []string, count int64) (string, []string, bool, error) {
	return t.ops.lMPop(ctx, t.querier(), keys, count)
}

func (t *TxStore) RMPop(ctx context.Context, keys []string, count int64) (string, []string, bool, error) {
	return t.ops.rMPop(ctx, t.querier(), keys, count)
}

func (t *TxStore) ZMPopMin(ctx context.Context, keys []string, count int64) (string, []ZMember, bool, error) {
	return t.ops.zMPopMin(ctx, t.querier(), keys, count)
}

func (t *TxStore) ZMPopMax(ctx context.Context, keys []string, count int64) (string, []ZMember, bool, error) {
	return t.ops.zMPopMax(ctx, t.querier(), keys, count)
}

func (t *TxStore) LLen(ctx context.Context, key string) (int64, error) {
	return t.ops.lLen(ctx, t.querier(), key)
}

func (t *TxStore) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return t.ops.lRange(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return t.ops.lIndex(ctx, t.querier(), key, index)
}

func (t *TxStore) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	return t.ops.lRem(ctx, t.querier(), key, count, element)
}

func (t *TxStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	return t.ops.lTrim(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	return t.ops.rPopLPush(ctx, t.querier(), source, destination)
}

func (t *TxStore) LPos(ctx context.Context, key, element string, rank, count, maxlen int64) ([]int64, error) {
	return t.ops.lPos(ctx, t.querier(), key, element, rank, count, maxlen)
}

func (t *TxStore) LSet(ctx context.Context, key string, index int64, element string) error {
	return t.ops.lSet(ctx, t.querier(), key, index, element)
}

func (t *TxStore) LInsert(ctx context.Context, key, pivot, element string, before bool) (int64, error) {
	return t.ops.lInsert(ctx, t.querier(), key, pivot, element, before)
}

// ============== Set Commands ==============

func (t *TxStore) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.sAdd(ctx, t.querier(), key, members)
}

func (t *TxStore) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.sRem(ctx, t.querier(), key, members)
}

func (t *TxStore) SMembers(ctx context.Context, key string) ([]string, error) {
	return t.ops.sMembers(ctx, t.querier(), key)
}

func (t *TxStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return t.ops.sIsMember(ctx, t.querier(), key, member)
}

func (t *TxStore) SCard(ctx context.Context, key string) (int64, error) {
	return t.ops.sCard(ctx, t.querier(), key)
}

func (t *TxStore) SMIsMember(ctx context.Context, key string, members []string) ([]bool, error) {
	return t.ops.sMIsMember(ctx, t.querier(), key, members)
}

func (t *TxStore) SInter(ctx context.Context, keys []string) ([]string, error) {
	return t.ops.sInter(ctx, t.querier(), keys)
}

func (t *TxStore) SInterStore(ctx context.Context, destination string, keys []string) (int64, error) {
	return t.ops.sInterStore(ctx, t.querier(), destination, keys)
}

func (t *TxStore) SUnion(ctx context.Context, keys []string) ([]string, error) {
	return t.ops.sUnion(ctx, t.querier(), keys)
}

func (t *TxStore) SUnionStore(ctx context.Context, destination string, keys []string) (int64, error) {
	return t.ops.sUnionStore(ctx, t.querier(), destination, keys)
}

func (t *TxStore) SDiff(ctx context.Context, keys []string) ([]string, error) {
	return t.ops.sDiff(ctx, t.querier(), keys)
}

func (t *TxStore) SInterCard(ctx context.Context, keys []string, limit int64) (int64, error) {
	return t.ops.sInterCard(ctx, t.querier(), keys, limit)
}

func (t *TxStore) HRandField(ctx context.Context, key string, count int64, withValues bool) ([]string, error) {
	return t.ops.hRandField(ctx, t.querier(), key, count, withValues)
}

func (t *TxStore) SRandMember(ctx context.Context, key string, count int64) ([]string, error) {
	return t.ops.sRandMember(ctx, t.querier(), key, count)
}

func (t *TxStore) ZRandMember(ctx context.Context, key string, count int64) ([]ZMember, error) {
	return t.ops.zRandMember(ctx, t.querier(), key, count)
}

func (t *TxStore) SDiffStore(ctx context.Context, destination string, keys []string) (int64, error) {
	return t.ops.sDiffStore(ctx, t.querier(), destination, keys)
}

// ============== Sorted Set Commands ==============

func (t *TxStore) ZAdd(ctx context.Context, key string, members []ZMember, opts ZAddOptions) (int64, error) {
	return t.ops.zAdd(ctx, t.querier(), key, members, opts)
}

func (t *TxStore) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return t.ops.zRange(ctx, t.querier(), key, start, stop, withScores)
}

func (t *TxStore) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return t.ops.zScore(ctx, t.querier(), key, member)
}

func (t *TxStore) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.zRem(ctx, t.querier(), key, members)
}

func (t *TxStore) ZCard(ctx context.Context, key string) (int64, error) {
	return t.ops.zCard(ctx, t.querier(), key)
}

func (t *TxStore) ZRevRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return t.ops.zRevRange(ctx, t.querier(), key, start, stop, withScores)
}

func (t *TxStore) ZRevRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return t.ops.zRevRangeByScore(ctx, t.querier(), key, min, max, withScores, offset, count)
}

func (t *TxStore) ZRangeByLex(ctx context.Context, key string, min, max LexBound, offset, count int64) ([]string, error) {
	return t.ops.zRangeByLex(ctx, t.querier(), key, min, max, offset, count)
}

func (t *TxStore) ZRevRangeByLex(ctx context.Context, key string, min, max LexBound, offset, count int64) ([]string, error) {
	return t.ops.zRevRangeByLex(ctx, t.querier(), key, min, max, offset, count)
}

func (t *TxStore) ZLexCount(ctx context.Context, key string, min, max LexBound) (int64, error) {
	return t.ops.zLexCount(ctx, t.querier(), key, min, max)
}

func (t *TxStore) ZRangeStore(ctx context.Context, dst, src string, spec ZRangeStoreSpec) (int64, error) {
	return t.ops.zRangeStore(ctx, t.querier(), dst, src, spec)
}

func (t *TxStore) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return t.ops.zRangeByScore(ctx, t.querier(), key, min, max, withScores, offset, count)
}

func (t *TxStore) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	return t.ops.zRemRangeByScore(ctx, t.querier(), key, min, max)
}

func (t *TxStore) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return t.ops.zRemRangeByRank(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return t.ops.zIncrBy(ctx, t.querier(), key, increment, member)
}

func (t *TxStore) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	return t.ops.zPopMin(ctx, t.querier(), key, count)
}

func (t *TxStore) ZPopMax(ctx context.Context, key string, count int64) ([]ZMember, error) {
	return t.ops.zPopMax(ctx, t.querier(), key, count)
}

func (t *TxStore) ZRank(ctx context.Context, key, member string) (int64, bool, error) {
	return t.ops.zRank(ctx, t.querier(), key, member)
}

func (t *TxStore) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	return t.ops.zRevRank(ctx, t.querier(), key, member)
}

func (t *TxStore) ZCount(ctx context.Context, key string, min, max float64) (int64, error) {
	return t.ops.zCount(ctx, t.querier(), key, min, max)
}

func (t *TxStore) ZScan(ctx context.Context, key string, cursor int64, pattern string, count int64) (int64, []ZMember, error) {
	return t.ops.zScan(ctx, t.querier(), key, cursor, pattern, count)
}

func (t *TxStore) ZUnionStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	return t.ops.zUnionStore(ctx, t.querier(), destination, keys, weights, aggregate)
}

func (t *TxStore) ZInterStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	return t.ops.zInterStore(ctx, t.querier(), destination, keys, weights, aggregate)
}

// ============== HyperLogLog Commands ==============

func (t *TxStore) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	return t.ops.pfAdd(ctx, t.querier(), key, elements)
}

func (t *TxStore) PFCount(ctx context.Context, keys []string) (int64, error) {
	return t.ops.pfCount(ctx, t.querier(), keys)
}

func (t *TxStore) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	return t.ops.pfMerge(ctx, t.querier(), destKey, sourceKeys)
}

// ============== Server Commands ==============

func (t *TxStore) DBSize(ctx context.Context) (int64, error) {
	return t.ops.dbSize(ctx, t.tx)
}

// Ensure TxStore implements Transaction
var _ Transaction = (*TxStore)(nil)
