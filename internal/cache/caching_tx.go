package cache

import (
	"context"
	"time"

	"github.com/mnorrsken/postkeys/internal/storage"
)

// CachingTx wraps a storage.Transaction and records which keys are written.
// On a successful Commit the dirty keys are invalidated via invalidateFn so
// that the in-memory cache does not serve stale data after a MULTI/EXEC block.
// Reads are always passed through to PostgreSQL — reading from cache inside a
// transaction would be incorrect for write-then-read patterns within the same EXEC.
type CachingTx struct {
	storage.Transaction                                        // all read methods promoted as-is
	invalidateFn        func(ctx context.Context, keys []string) // called after successful commit
	dirty               map[string]struct{}
}

func newCachingTx(inner storage.Transaction, invalidateFn func(context.Context, []string)) *CachingTx {
	return &CachingTx{
		Transaction:  inner,
		invalidateFn: invalidateFn,
		dirty:        make(map[string]struct{}),
	}
}

func (t *CachingTx) mark(keys ...string) {
	for _, k := range keys {
		t.dirty[k] = struct{}{}
	}
}

func (t *CachingTx) dirtySlice() []string {
	if len(t.dirty) == 0 {
		return nil
	}
	keys := make([]string, 0, len(t.dirty))
	for k := range t.dirty {
		keys = append(keys, k)
	}
	return keys
}

// Commit commits the transaction and invalidates dirty keys on success.
func (t *CachingTx) Commit(ctx context.Context) error {
	err := t.Transaction.Commit(ctx)
	if err == nil {
		if keys := t.dirtySlice(); len(keys) > 0 {
			t.invalidateFn(ctx, keys)
		}
	}
	return err
}

// Rollback aborts the transaction; no cache invalidation is needed.
func (t *CachingTx) Rollback(ctx context.Context) error {
	t.dirty = nil
	return t.Transaction.Rollback(ctx)
}

// ============== String writes ==============

func (t *CachingTx) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := t.Transaction.Set(ctx, key, value, ttl)
	if err == nil {
		t.mark(key)
	}
	return err
}

func (t *CachingTx) SetNX(ctx context.Context, key, value string) (bool, error) {
	ok, err := t.Transaction.SetNX(ctx, key, value)
	if err == nil && ok {
		t.mark(key)
	}
	return ok, err
}

func (t *CachingTx) MSet(ctx context.Context, pairs map[string]string) error {
	err := t.Transaction.MSet(ctx, pairs)
	if err == nil {
		for k := range pairs {
			t.mark(k)
		}
	}
	return err
}

func (t *CachingTx) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	v, err := t.Transaction.Incr(ctx, key, delta)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	v, err := t.Transaction.IncrByFloat(ctx, key, delta)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) Append(ctx context.Context, key, value string) (int64, error) {
	v, err := t.Transaction.Append(ctx, key, value)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	v, err := t.Transaction.SetRange(ctx, key, offset, value)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) BitField(ctx context.Context, key string, ops []storage.BitFieldOp) ([]int64, error) {
	v, err := t.Transaction.BitField(ctx, key, ops)
	if err == nil && bitFieldHasWrites(ops) {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	v, found, err := t.Transaction.GetEx(ctx, key, ttl, persist)
	if err == nil {
		t.mark(key)
	}
	return v, found, err
}

func (t *CachingTx) GetDel(ctx context.Context, key string) (string, bool, error) {
	v, found, err := t.Transaction.GetDel(ctx, key)
	if err == nil {
		t.mark(key)
	}
	return v, found, err
}

func (t *CachingTx) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	v, found, err := t.Transaction.GetSet(ctx, key, value)
	if err == nil {
		t.mark(key)
	}
	return v, found, err
}

// ============== Key writes ==============

func (t *CachingTx) Del(ctx context.Context, keys []string) (int64, error) {
	v, err := t.Transaction.Del(ctx, keys)
	if err == nil {
		t.mark(keys...)
	}
	return v, err
}

func (t *CachingTx) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := t.Transaction.Expire(ctx, key, ttl)
	if err == nil {
		t.mark(key)
	}
	return ok, err
}

func (t *CachingTx) ExpireAt(ctx context.Context, key string, ts time.Time) (bool, error) {
	ok, err := t.Transaction.ExpireAt(ctx, key, ts)
	if err == nil && ok {
		t.mark(key)
	}
	return ok, err
}

func (t *CachingTx) Persist(ctx context.Context, key string) (bool, error) {
	ok, err := t.Transaction.Persist(ctx, key)
	if err == nil {
		t.mark(key)
	}
	return ok, err
}

func (t *CachingTx) Rename(ctx context.Context, oldKey, newKey string) error {
	err := t.Transaction.Rename(ctx, oldKey, newKey)
	if err == nil {
		t.mark(oldKey, newKey)
	}
	return err
}

func (t *CachingTx) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	ok, err := t.Transaction.Copy(ctx, source, destination, replace)
	if err == nil && ok {
		t.mark(destination)
	}
	return ok, err
}

// ============== Bitmap writes ==============

func (t *CachingTx) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	v, err := t.Transaction.SetBit(ctx, key, offset, value)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) BitOp(ctx context.Context, operation, destKey string, keys []string) (int64, error) {
	v, err := t.Transaction.BitOp(ctx, operation, destKey, keys)
	if err == nil {
		t.mark(destKey)
	}
	return v, err
}

// ============== Hash writes ==============

func (t *CachingTx) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	v, err := t.Transaction.HIncrBy(ctx, key, field, increment)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	v, err := t.Transaction.HIncrByFloat(ctx, key, field, increment)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	ok, err := t.Transaction.HSetNX(ctx, key, field, value)
	if err == nil && ok {
		t.mark(key)
	}
	return ok, err
}

// ============== List writes ==============

func (t *CachingTx) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	v, err := t.Transaction.LRem(ctx, key, count, element)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) LTrim(ctx context.Context, key string, start, stop int64) error {
	err := t.Transaction.LTrim(ctx, key, start, stop)
	if err == nil {
		t.mark(key)
	}
	return err
}

func (t *CachingTx) LSet(ctx context.Context, key string, index int64, element string) error {
	err := t.Transaction.LSet(ctx, key, index, element)
	if err == nil {
		t.mark(key)
	}
	return err
}

func (t *CachingTx) LInsert(ctx context.Context, key, pivot, element string, before bool) (int64, error) {
	v, err := t.Transaction.LInsert(ctx, key, pivot, element, before)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	v, found, err := t.Transaction.RPopLPush(ctx, source, destination)
	if err == nil {
		t.mark(source, destination)
	}
	return v, found, err
}

// ============== Set writes ==============

func (t *CachingTx) SInterStore(ctx context.Context, destination string, keys []string) (int64, error) {
	v, err := t.Transaction.SInterStore(ctx, destination, keys)
	if err == nil {
		t.mark(destination)
	}
	return v, err
}

func (t *CachingTx) SUnionStore(ctx context.Context, destination string, keys []string) (int64, error) {
	v, err := t.Transaction.SUnionStore(ctx, destination, keys)
	if err == nil {
		t.mark(destination)
	}
	return v, err
}

func (t *CachingTx) SDiffStore(ctx context.Context, destination string, keys []string) (int64, error) {
	v, err := t.Transaction.SDiffStore(ctx, destination, keys)
	if err == nil {
		t.mark(destination)
	}
	return v, err
}

// ============== Sorted set writes ==============

func (t *CachingTx) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	v, err := t.Transaction.ZRemRangeByScore(ctx, key, min, max)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	v, err := t.Transaction.ZRemRangeByRank(ctx, key, start, stop)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	v, err := t.Transaction.ZIncrBy(ctx, key, increment, member)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) ZPopMin(ctx context.Context, key string, count int64) ([]storage.ZMember, error) {
	v, err := t.Transaction.ZPopMin(ctx, key, count)
	if err == nil && len(v) > 0 {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) ZPopMax(ctx context.Context, key string, count int64) ([]storage.ZMember, error) {
	v, err := t.Transaction.ZPopMax(ctx, key, count)
	if err == nil && len(v) > 0 {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) ZUnionStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	v, err := t.Transaction.ZUnionStore(ctx, destination, keys, weights, aggregate)
	if err == nil {
		t.mark(destination)
	}
	return v, err
}

func (t *CachingTx) ZInterStore(ctx context.Context, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	v, err := t.Transaction.ZInterStore(ctx, destination, keys, weights, aggregate)
	if err == nil {
		t.mark(destination)
	}
	return v, err
}

// ============== HyperLogLog writes ==============

func (t *CachingTx) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	v, err := t.Transaction.PFAdd(ctx, key, elements)
	if err == nil {
		t.mark(key)
	}
	return v, err
}

func (t *CachingTx) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	err := t.Transaction.PFMerge(ctx, destKey, sourceKeys)
	if err == nil {
		t.mark(destKey)
	}
	return err
}

// Ensure CachingTx implements Transaction.
var _ storage.Transaction = (*CachingTx)(nil)
