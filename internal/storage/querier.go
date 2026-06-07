// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the common interface implemented by both pgxpool.Pool and pgx.Tx
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// binaryPrefix is used to identify base64-encoded binary keys and field names.
// The \x1F (Unit Separator) byte cannot appear in valid UTF-8 text, so this
// prefix is unambiguous: any string starting with it was encoded by us.
const binaryPrefix = "\x1Fb64:"

// encodeKey encodes a key for PostgreSQL TEXT storage.
// Keys with null bytes or invalid UTF-8 are base64-encoded with a marker prefix.
// Valid UTF-8 keys are returned unchanged (zero overhead for normal keys).
func encodeKey(key string) string {
	if strings.ContainsRune(key, 0) || !utf8.ValidString(key) {
		return binaryPrefix + base64.StdEncoding.EncodeToString([]byte(key))
	}
	return key
}

// decodeKey decodes a key from PostgreSQL TEXT storage.
func decodeKey(key string) string {
	if strings.HasPrefix(key, binaryPrefix) {
		encoded := key[len(binaryPrefix):]
		if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			return string(decoded)
		}
	}
	return key
}

// encodeKeys encodes a slice of keys for PostgreSQL TEXT storage.
func encodeKeys(keys []string) []string {
	for i, k := range keys {
		keys[i] = encodeKey(k)
	}
	return keys
}

// encodeField encodes a field name for PostgreSQL storage.
// Field names with null bytes or invalid UTF-8 are base64-encoded.
func encodeField(field string) string {
	if strings.ContainsRune(field, 0) || !utf8.ValidString(field) {
		return binaryPrefix + base64.StdEncoding.EncodeToString([]byte(field))
	}
	return field
}

// decodeField decodes a field name from PostgreSQL storage.
func decodeField(field string) string {
	if strings.HasPrefix(field, binaryPrefix) {
		encoded := field[len(binaryPrefix):]
		if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			return string(decoded)
		}
	}
	return field
}

// queryOps provides the actual implementation of storage operations using a Querier.
// This is shared between Store (using pool) and TxStore (using tx).
type queryOps struct{}

// ============== Helper Methods ==============

// dataTableForType returns the data table name for a given key type.
// Returns empty string for TypeNone and unknown types.
func dataTableForType(kt KeyType) string {
	switch kt {
	case TypeString:
		return "kv_strings"
	case TypeHash:
		return "kv_hashes"
	case TypeList:
		return "kv_lists"
	case TypeSet:
		return "kv_sets"
	case TypeZSet:
		return "kv_zsets"
	default:
		return ""
	}
}

func (queryOps) getKeyType(ctx context.Context, q Querier, key string) (KeyType, error) {
	var keyType string
	err := q.QueryRow(ctx,
		"SELECT key_type FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&keyType)

	if err == pgx.ErrNoRows {
		return TypeNone, nil
	}
	if err != nil {
		return TypeNone, err
	}
	return KeyType(keyType), nil
}

// errWrongType is returned by typed read helpers when a key exists with a
// non-matching type. Matches the error string used elsewhere in this file.
var errWrongType = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")

// queueTypeCheck queues a kv_meta lookup into batch. Use readTypeCheck to read
// the result and verify the key is missing or of the expected type. This is
// the read-side companion to the data query queued alongside it.
func (queryOps) queueTypeCheck(batch *pgx.Batch, key string) {
	batch.Queue(
		"SELECT key_type FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
}

// readTypeCheck reads the result queued by queueTypeCheck and returns
// errWrongType if the stored type doesn't match expected (missing key is OK).
func (queryOps) readTypeCheck(br pgx.BatchResults, expected KeyType) error {
	var keyType string
	err := br.QueryRow().Scan(&keyType)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if KeyType(keyType) != expected {
		return errWrongType
	}
	return nil
}

// lockKey acquires a transaction-scoped advisory lock for the given key.
// This serializes all concurrent write operations on the same key, preventing
// deadlocks between transactions that need to lock both kv_meta and data tables.
// The lock is automatically released when the transaction commits or rolls back.
// Reentrant within the same transaction (safe to call multiple times for the same key).
func (queryOps) lockKey(ctx context.Context, q Querier, key string) error {
	_, err := q.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", key)
	return err
}

// lockKeys acquires advisory locks for multiple keys in sorted order.
func (o queryOps) lockKeys(ctx context.Context, q Querier, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	// Deduplicate and lock in order
	prev := ""
	for _, key := range sorted {
		if key == prev {
			continue
		}
		if err := o.lockKey(ctx, q, key); err != nil {
			return err
		}
		prev = key
	}
	return nil
}

func (o queryOps) setMeta(ctx context.Context, q Querier, key string, keyType KeyType, expiresAt *time.Time) error {
	if err := o.lockKey(ctx, q, key); err != nil {
		return err
	}
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type, expires_at) VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET key_type = $2, expires_at = $3`,
		key, string(keyType), expiresAt,
	)
	return err
}

// setMetaBatch sets metadata for multiple keys at once
func (o queryOps) setMetaBatch(ctx context.Context, q Querier, keys []string, keyType KeyType) error {
	if len(keys) == 0 {
		return nil
	}
	if err := o.lockKeys(ctx, q, keys); err != nil {
		return err
	}
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type)
		 SELECT unnest($1::text[]), $2
		 ON CONFLICT (key) DO UPDATE SET key_type = EXCLUDED.key_type`,
		keys, string(keyType),
	)
	return err
}

func (o queryOps) deleteKeyFromAllTables(ctx context.Context, q Querier, key string) error {
	if err := o.lockKey(ctx, q, key); err != nil {
		return err
	}
	queries := []string{
		"DELETE FROM kv_meta WHERE key = $1",
		"DELETE FROM kv_strings WHERE key = $1",
		"DELETE FROM kv_hashes WHERE key = $1",
		"DELETE FROM kv_lists WHERE key = $1",
		"DELETE FROM kv_sets WHERE key = $1",
		"DELETE FROM kv_zsets WHERE key = $1",
		"DELETE FROM kv_hyperloglog WHERE key = $1",
	}
	for _, query := range queries {
		if _, err := q.Exec(ctx, query, key); err != nil {
			return err
		}
	}
	return nil
}

// deleteKeysFromAllTables deletes multiple keys from all tables in batch
func (o queryOps) deleteKeysFromAllTables(ctx context.Context, q Querier, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := o.lockKeys(ctx, q, keys); err != nil {
		return err
	}
	queries := []string{
		"DELETE FROM kv_meta WHERE key = ANY($1)",
		"DELETE FROM kv_strings WHERE key = ANY($1)",
		"DELETE FROM kv_hashes WHERE key = ANY($1)",
		"DELETE FROM kv_lists WHERE key = ANY($1)",
		"DELETE FROM kv_sets WHERE key = ANY($1)",
		"DELETE FROM kv_zsets WHERE key = ANY($1)",
		"DELETE FROM kv_hyperloglog WHERE key = ANY($1)",
	}
	for _, query := range queries {
		if _, err := q.Exec(ctx, query, keys); err != nil {
			return err
		}
	}
	return nil
}

// ============== String Commands ==============

func (o queryOps) get(ctx context.Context, q Querier, key string) (string, bool, error) {
	key = encodeKey(key)
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(value), true, nil
}

func (o queryOps) set(ctx context.Context, q Querier, key, value string, ttl time.Duration) error {
	key = encodeKey(key)
	if err := o.deleteKeyFromAllTables(ctx, q, key); err != nil {
		return err
	}

	var expiresAt *time.Time
	if ttl > 0 {
		t := time.Now().Add(ttl)
		expiresAt = &t
	}

	// Set meta before data table to maintain consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, key, TypeString, expiresAt); err != nil {
		return err
	}

	_, err := q.Exec(ctx,
		`INSERT INTO kv_strings (key, value, expires_at) VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3`,
		key, []byte(value), expiresAt,
	)
	return err
}

func (o queryOps) setNX(ctx context.Context, q Querier, key, value string, ttl time.Duration) (bool, error) {
	enc := encodeKey(key)
	// Advisory lock serializes concurrent SETNX on the same key for the duration
	// of the surrounding transaction, making the check-then-set below atomic.
	if err := o.lockKey(ctx, q, enc); err != nil {
		return false, err
	}

	// Lazy expiry: a key whose TTL has already passed but has not yet been
	// removed by the background sweeper must be treated as absent, so an
	// expiring lock/lease can be re-acquired the instant it expires. kv_meta
	// tracks every key regardless of value type.
	var exists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW()))`,
		enc,
	).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	// Key is absent or expired: write the value (clearing any stale data of a
	// different type) together with its TTL. set() encodes the key and persists
	// expires_at to both kv_meta and kv_strings.
	if err := o.set(ctx, q, key, value, ttl); err != nil {
		return false, err
	}
	return true, nil
}

func (o queryOps) mGet(ctx context.Context, q Querier, keys []string) ([]interface{}, error) {
	keys = encodeKeys(keys)
	results := make([]interface{}, len(keys))

	rows, err := q.Query(ctx,
		`SELECT key, value FROM kv_strings 
		 WHERE key = ANY($1) AND (expires_at IS NULL OR expires_at > NOW())`,
		keys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keyValues := make(map[string]string)
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		keyValues[key] = string(value)
	}

	for i, key := range keys {
		if val, ok := keyValues[key]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}

	return results, nil
}

func (o queryOps) mSet(ctx context.Context, q Querier, pairs map[string]string) error {
	encoded := make(map[string]string, len(pairs))
	for k, v := range pairs {
		encoded[encodeKey(k)] = v
	}
	pairs = encoded
	if len(pairs) == 0 {
		return nil
	}

	// Collect and sort keys for consistent lock ordering to prevent deadlocks
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	values := make([][]byte, 0, len(pairs))
	for _, key := range keys {
		values = append(values, []byte(pairs[key]))
	}

	// Batch delete from all tables
	if err := o.deleteKeysFromAllTables(ctx, q, keys); err != nil {
		return err
	}

	// Batch set metadata first for consistent lock ordering (kv_meta before kv_strings)
	if err := o.setMetaBatch(ctx, q, keys, TypeString); err != nil {
		return err
	}

	// Batch insert using UNNEST
	_, err := q.Exec(ctx,
		`INSERT INTO kv_strings (key, value)
		 SELECT unnest($1::text[]), unnest($2::bytea[])
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		keys, values,
	)
	return err
}

func (o queryOps) incr(ctx context.Context, q Querier, key string, delta int64) (int64, error) {
	key = encodeKey(key)
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)

	var current int64
	if err == pgx.ErrNoRows {
		current = 0
	} else if err != nil {
		return 0, err
	} else {
		current, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	result := current + delta

	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return 0, err
	}

	_, err = q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, []byte(strconv.FormatInt(result, 10)),
	)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (o queryOps) appendStr(ctx context.Context, q Querier, key, value string) (int64, error) {
	key = encodeKey(key)
	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return 0, err
	}

	_, err := q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = kv_strings.value || $2`,
		key, []byte(value),
	)
	if err != nil {
		return 0, err
	}

	var newValue []byte
	err = q.QueryRow(ctx, "SELECT value FROM kv_strings WHERE key = $1", key).Scan(&newValue)
	if err != nil {
		return 0, err
	}

	return int64(len(newValue)), nil
}

func (o queryOps) getRange(ctx context.Context, q Querier, key string, start, end int64) (string, error) {
	key = encodeKey(key)
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	length := int64(len(value))
	if length == 0 {
		return "", nil
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// Clamp to valid range
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return "", nil
	}

	return string(value[start : end+1]), nil
}

func (o queryOps) setRange(ctx context.Context, q Querier, key string, offset int64, value string) (int64, error) {
	key = encodeKey(key)
	// Get existing value or create empty
	var existing []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&existing)
	if err == pgx.ErrNoRows {
		existing = []byte{}
	} else if err != nil {
		return 0, err
	}

	// Extend buffer if needed
	endPos := offset + int64(len(value))
	if int64(len(existing)) < endPos {
		newBuf := make([]byte, endPos)
		copy(newBuf, existing)
		existing = newBuf
	}

	// Copy value at offset
	copy(existing[offset:], value)

	// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return 0, err
	}

	// Save back
	_, err = q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, existing,
	)
	if err != nil {
		return 0, err
	}

	return int64(len(existing)), nil
}

func (o queryOps) bitField(ctx context.Context, q Querier, key string, ops []BitFieldOp) ([]*int64, error) {
	key = encodeKey(key)
	// Get existing value or create empty
	var value []byte
	isNewKey := false
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		value = []byte{}
		isNewKey = true
	} else if err != nil {
		return nil, err
	}

	results := make([]*int64, 0, len(ops))
	modified := false
	appendResult := func(v int64) {
		r := v
		results = append(results, &r)
	}

	for _, op := range ops {
		// Parse encoding (e.g., "u8", "i16", "u32")
		signed := false
		if len(op.Encoding) > 0 && op.Encoding[0] == 'i' {
			signed = true
		}
		bitWidth := int64(0)
		if len(op.Encoding) > 1 {
			bitWidth, _ = strconv.ParseInt(op.Encoding[1:], 10, 64)
		}
		if bitWidth <= 0 || bitWidth > 64 {
			bitWidth = 8 // default to 8 bits
		}

		// Calculate byte positions
		bitOffset := op.Offset
		byteOffset := bitOffset / 8
		bitInByte := bitOffset % 8

		// Ensure buffer is large enough
		neededBytes := byteOffset + (bitWidth+bitInByte+7)/8
		if int64(len(value)) < neededBytes {
			newValue := make([]byte, neededBytes)
			copy(newValue, value)
			value = newValue
		}

		min, max := bitfieldBounds(bitWidth, signed)

		switch op.OpType {
		case "GET":
			appendResult(getBitField(value, bitOffset, bitWidth, signed))

		case "SET":
			oldValue := getBitField(value, bitOffset, bitWidth, signed)
			// WRAP stores the low bitWidth bits (setBitField truncates); SAT
			// clamps; FAIL rejects an out-of-range value.
			stored, ok := bitfieldResolveOverflow(op.Overflow, op.Value > max, op.Value < min, op.Value, min, max)
			if !ok {
				results = append(results, nil) // FAIL: report nil, leave value unchanged
				continue
			}
			appendResult(oldValue)
			setBitField(value, bitOffset, bitWidth, stored)
			modified = true

		case "INCRBY":
			oldValue := getBitField(value, bitOffset, bitWidth, signed)
			sum := oldValue + op.Value
			// Detect overflow of the int64 addition itself, so SAT/FAIL still
			// react correctly when the true result exceeds int64. WRAP is
			// unaffected: the wrapped value is correct modulo 2^bitWidth.
			addOverflow := (op.Value > 0 && sum < oldValue) || (op.Value < 0 && sum > oldValue)
			high := (addOverflow && op.Value > 0) || (!addOverflow && sum > max)
			low := (addOverflow && op.Value < 0) || (!addOverflow && sum < min)
			newValue, ok := bitfieldResolveOverflow(op.Overflow, high, low, wrapBitfield(sum, bitWidth, signed), min, max)
			if !ok {
				results = append(results, nil) // FAIL
				continue
			}
			setBitField(value, bitOffset, bitWidth, newValue)
			appendResult(newValue)
			modified = true
		}
	}

	if modified {
		if isNewKey {
			// Only upsert meta for new keys; existing keys already have the correct meta row.
			if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
				return nil, err
			}
		} else {
			// Still need the advisory lock for write serialization on existing keys.
			if err := o.lockKey(ctx, q, key); err != nil {
				return nil, err
			}
		}

		_, err = q.Exec(ctx,
			`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET value = $2`,
			key, value,
		)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// bitfieldBounds returns the min and max representable values for a BITFIELD
// encoding. For the full-width signed case (i64) this is the entire int64 range.
func bitfieldBounds(bitWidth int64, signed bool) (min, max int64) {
	if signed {
		max = (int64(1) << (bitWidth - 1)) - 1
		min = -(int64(1) << (bitWidth - 1))
	} else {
		max = (int64(1) << bitWidth) - 1
		min = 0
	}
	return min, max
}

// wrapBitfield reduces v into the range of a BITFIELD encoding (the WRAP mode),
// matching Redis' two's-complement wrap-around for signed encodings and a bit
// mask for unsigned ones.
func wrapBitfield(v, bitWidth int64, signed bool) int64 {
	if bitWidth >= 64 {
		return v // full int64 range; wrapping is the identity
	}
	if signed {
		max := int64(1) << (bitWidth - 1)
		min := -max
		for v >= max {
			v -= max * 2
		}
		for v < min {
			v += max * 2
		}
		return v
	}
	mask := int64((1 << bitWidth) - 1)
	return v & mask
}

// bitfieldResolveOverflow applies the active OVERFLOW mode to a SET/INCRBY whose
// desired value is out of range when high or low is set. wrapped is the value to
// use in WRAP mode (and whenever the value is already in range). ok=false means
// FAIL mode rejected the op, so the caller must store nothing and reply nil.
func bitfieldResolveOverflow(mode string, high, low bool, wrapped, min, max int64) (int64, bool) {
	if !high && !low {
		return wrapped, true
	}
	switch mode {
	case "SAT":
		if high {
			return max, true
		}
		return min, true
	case "FAIL":
		return 0, false
	default: // WRAP
		return wrapped, true
	}
}

// getBitField extracts a bit field value from a byte slice
func getBitField(data []byte, bitOffset, bitWidth int64, signed bool) int64 {
	var result int64
	for i := int64(0); i < bitWidth; i++ {
		byteIdx := (bitOffset + i) / 8
		bitIdx := 7 - ((bitOffset + i) % 8) // MSB first
		if byteIdx < int64(len(data)) {
			if data[byteIdx]&(1<<bitIdx) != 0 {
				result |= 1 << (bitWidth - 1 - i)
			}
		}
	}
	// Sign extend if signed
	if signed && bitWidth > 0 && (result&(1<<(bitWidth-1))) != 0 {
		// Set all bits above bitWidth to 1
		result |= ^((1 << bitWidth) - 1)
	}
	return result
}

// setBitField sets a bit field value in a byte slice
func setBitField(data []byte, bitOffset, bitWidth, value int64) {
	for i := int64(0); i < bitWidth; i++ {
		byteIdx := (bitOffset + i) / 8
		bitIdx := 7 - ((bitOffset + i) % 8) // MSB first
		if byteIdx < int64(len(data)) {
			bitValue := (value >> (bitWidth - 1 - i)) & 1
			if bitValue != 0 {
				data[byteIdx] |= 1 << bitIdx
			} else {
				data[byteIdx] &^= 1 << bitIdx
			}
		}
	}
}

func (o queryOps) strLen(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int64(len(value)), nil
}

func (o queryOps) getEx(ctx context.Context, q Querier, key string, ttl time.Duration, persist bool) (string, bool, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return "", false, err
	}
	var value []byte
	var expiresAt *time.Time

	err := q.QueryRow(ctx,
		"SELECT value, expires_at FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value, &expiresAt)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	// Update expiration based on options - update kv_meta for TTL tracking
	if persist {
		// Remove expiration from both tables
		_, err = q.Exec(ctx,
			"UPDATE kv_meta SET expires_at = NULL WHERE key = $1",
			key,
		)
		if err == nil {
			_, err = q.Exec(ctx,
				"UPDATE kv_strings SET expires_at = NULL WHERE key = $1",
				key,
			)
		}
	} else if ttl > 0 {
		// Set new expiration on both tables
		newExpiry := time.Now().Add(ttl)
		_, err = q.Exec(ctx,
			"UPDATE kv_meta SET expires_at = $2 WHERE key = $1",
			key, newExpiry,
		)
		if err == nil {
			_, err = q.Exec(ctx,
				"UPDATE kv_strings SET expires_at = $2 WHERE key = $1",
				key, newExpiry,
			)
		}
	}
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

func (o queryOps) getDel(ctx context.Context, q Querier, key string) (string, bool, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return "", false, err
	}
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	// Delete kv_meta before data table for consistent lock ordering (kv_meta before kv_strings).
	_, _ = q.Exec(ctx, "DELETE FROM kv_meta WHERE key = $1", key)
	_, err = q.Exec(ctx, "DELETE FROM kv_strings WHERE key = $1", key)
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

func (o queryOps) getSet(ctx context.Context, q Querier, key, value string) (string, bool, error) {
	key = encodeKey(key)
	// Get old value
	var oldValue []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&oldValue)
	exists := err == nil
	if err != nil && err != pgx.ErrNoRows {
		return "", false, err
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return "", false, err
	}

	// Set new value (upsert)
	_, err = q.Exec(ctx,
		`INSERT INTO kv_strings (key, value, expires_at) VALUES ($1, $2, NULL)
		 ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = NULL`,
		key, []byte(value),
	)
	if err != nil {
		return "", false, err
	}

	if exists {
		return string(oldValue), true, nil
	}
	return "", false, nil
}

func (o queryOps) incrByFloat(ctx context.Context, q Querier, key string, delta float64) (float64, error) {
	key = encodeKey(key)
	// Check key type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeString {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var currentValue float64 = 0
	var valueBytes []byte

	err = q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&valueBytes)
	if err == nil {
		currentValue, err = strconv.ParseFloat(string(valueBytes), 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not a valid float")
		}
	} else if err != pgx.ErrNoRows {
		return 0, err
	}

	newValue := currentValue + delta

	// Format without trailing zeros, but preserve precision
	valueStr := strconv.FormatFloat(newValue, 'f', -1, 64)

	// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return 0, err
	}

	_, err = q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, []byte(valueStr),
	)
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

// ============== Key Commands ==============

func (o queryOps) del(ctx context.Context, q Querier, keys []string) (int64, error) {
	keys = encodeKeys(keys)
	// Sort keys for consistent lock ordering to prevent deadlocks
	sort.Strings(keys)
	var deleted int64
	for _, key := range keys {
		keyType, err := o.getKeyType(ctx, q, key)
		if err != nil {
			return deleted, err
		}
		if keyType == TypeNone {
			continue
		}

		if err := o.deleteKeyFromAllTables(ctx, q, key); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (o queryOps) exists(ctx context.Context, q Querier, keys []string) (int64, error) {
	keys = encodeKeys(keys)
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_meta 
		 WHERE key = ANY($1) AND (expires_at IS NULL OR expires_at > NOW())`,
		keys,
	).Scan(&count)
	return count, err
}

// expireWhereClause builds the WHERE filter that determines whether the
// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT update is applied, based on the Redis
// 7.0 NX/XX/GT/LT flags. The clause always references $1 (key) and $2 (new
// expires_at), and inherits the base existence check so already-expired keys
// are not resurrected.
func expireWhereClause(opts ExpireOptions) string {
	switch {
	case opts.NX:
		// Only when no TTL is set. The base existence check is implied because
		// expires_at IS NULL also means "no TTL", and we don't need the
		// "or expires_at > NOW()" branch.
		return "key = $1 AND expires_at IS NULL"
	case opts.XX:
		return "key = $1 AND expires_at IS NOT NULL AND expires_at > NOW()"
	case opts.GT:
		// A key with no TTL is conceptually infinite; nothing can be greater.
		return "key = $1 AND expires_at IS NOT NULL AND expires_at > NOW() AND $2 > expires_at"
	case opts.LT:
		// A key with no TTL is conceptually infinite; anything is less.
		return "key = $1 AND (expires_at IS NULL OR (expires_at > NOW() AND $2 < expires_at))"
	default:
		return "key = $1 AND (expires_at IS NULL OR expires_at > NOW())"
	}
}

func (o queryOps) expire(ctx context.Context, q Querier, key string, ttl time.Duration, opts ExpireOptions) (bool, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return false, err
	}
	expiresAt := time.Now().Add(ttl)

	result, err := q.Exec(ctx,
		fmt.Sprintf("UPDATE kv_meta SET expires_at = $2 WHERE %s", expireWhereClause(opts)),
		key, expiresAt,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	// Update expires_at in the data table
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return false, err
	}

	table := dataTableForType(keyType)
	if table == "" {
		return true, nil
	}

	_, err = q.Exec(ctx, fmt.Sprintf("UPDATE %s SET expires_at = $2 WHERE key = $1", table), key, expiresAt)
	return err == nil, err
}

func (o queryOps) ttl(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)
	var expiresAt *time.Time
	err := q.QueryRow(ctx,
		"SELECT expires_at FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&expiresAt)

	if err == pgx.ErrNoRows {
		return -2, nil // Key does not exist
	}
	if err != nil {
		return 0, err
	}
	if expiresAt == nil {
		return -1, nil // Key exists but no TTL
	}

	ttl := time.Until(*expiresAt).Seconds()
	if ttl < 0 {
		return -2, nil
	}
	return int64(ttl), nil
}

func (o queryOps) pttl(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)
	var expiresAt *time.Time
	err := q.QueryRow(ctx,
		"SELECT expires_at FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&expiresAt)

	if err == pgx.ErrNoRows {
		return -2, nil
	}
	if err != nil {
		return 0, err
	}
	if expiresAt == nil {
		return -1, nil
	}

	pttl := time.Until(*expiresAt).Milliseconds()
	if pttl < 0 {
		return -2, nil
	}
	return pttl, nil
}

func (o queryOps) persist(ctx context.Context, q Querier, key string) (bool, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return false, err
	}
	result, err := q.Exec(ctx,
		`UPDATE kv_meta SET expires_at = NULL 
		 WHERE key = $1 AND expires_at IS NOT NULL AND expires_at > NOW()`,
		key,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	// Also clear expires_at in data tables
	tables := []string{"kv_strings", "kv_hashes", "kv_lists", "kv_sets", "kv_zsets", "kv_hyperloglog"}
	for _, table := range tables {
		_, _ = q.Exec(ctx, fmt.Sprintf("UPDATE %s SET expires_at = NULL WHERE key = $1", table), key)
	}

	return true, nil
}

func (o queryOps) randomKey(ctx context.Context, q Querier) (string, bool, error) {
	var key string
	err := q.QueryRow(ctx,
		`SELECT key FROM kv_meta
		 WHERE expires_at IS NULL OR expires_at > NOW()
		 ORDER BY random() LIMIT 1`,
	).Scan(&key)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return decodeKey(key), true, nil
}

func (o queryOps) keys(ctx context.Context, q Querier, pattern string) ([]string, error) {
	// Convert Redis glob pattern to SQL LIKE pattern
	likePattern := strings.ReplaceAll(pattern, "*", "%")
	likePattern = strings.ReplaceAll(likePattern, "?", "_")

	rows, err := q.Query(ctx,
		`SELECT key FROM kv_meta 
		 WHERE key LIKE $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		likePattern,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, decodeKey(key))
	}
	return keys, nil
}

func (o queryOps) keyType(ctx context.Context, q Querier, key string) (KeyType, error) {
	key = encodeKey(key)
	return o.getKeyType(ctx, q, key)
}

func (o queryOps) rename(ctx context.Context, q Querier, oldKey, newKey string) error {
	oldKey, newKey = encodeKey(oldKey), encodeKey(newKey)
	// Lock both keys in sorted order to prevent deadlocks
	if err := o.lockKeys(ctx, q, []string{oldKey, newKey}); err != nil {
		return err
	}
	keyType, err := o.getKeyType(ctx, q, oldKey)
	if err != nil {
		return err
	}
	if keyType == TypeNone {
		return fmt.Errorf("no such key")
	}

	// Delete new key if it exists
	if err := o.deleteKeyFromAllTables(ctx, q, newKey); err != nil {
		return err
	}

	// Rename in data table
	table := dataTableForType(keyType)
	if table == "" {
		return fmt.Errorf("unsupported key type for rename: %s", keyType)
	}

	// Update meta before data table for consistent lock ordering (kv_meta before data tables).
	_, err = q.Exec(ctx, "UPDATE kv_meta SET key = $2 WHERE key = $1", oldKey, newKey)
	if err != nil {
		return err
	}

	_, err = q.Exec(ctx, fmt.Sprintf("UPDATE %s SET key = $2 WHERE key = $1", table), oldKey, newKey)
	return err
}

// ============== Hash Commands ==============

func (o queryOps) hGet(ctx context.Context, q Querier, key, field string) (string, bool, error) {
	key = encodeKey(key)
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_hashes WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())",
		key, encodeField(field),
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(value), true, nil
}

func (o queryOps) hSet(ctx context.Context, q Querier, key string, fields map[string]string) (int64, error) {
	key = encodeKey(key)
	if len(fields) == 0 {
		return 0, nil
	}

	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Collect fields and values for batch insert
	fieldNames := make([]string, 0, len(fields))
	fieldValues := make([][]byte, 0, len(fields))
	for field, value := range fields {
		fieldNames = append(fieldNames, encodeField(field))
		fieldValues = append(fieldValues, []byte(value))
	}

	// Count existing fields before insert (to calculate newly added)
	var existingCount int64
	err = q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_hashes WHERE key = $1 AND field = ANY($2)",
		key, fieldNames,
	).Scan(&existingCount)
	if err != nil {
		return 0, err
	}

	// Set metadata before data table for consistent lock ordering (kv_meta before kv_hashes).
	if err := o.setMeta(ctx, q, key, TypeHash, nil); err != nil {
		return 0, err
	}

	// Batch upsert all fields at once
	_, err = q.Exec(ctx,
		`INSERT INTO kv_hashes (key, field, value)
		 SELECT $1, unnest($2::text[]), unnest($3::bytea[])
		 ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value`,
		key, fieldNames, fieldValues,
	)
	if err != nil {
		return 0, err
	}

	// Return number of newly added fields
	return int64(len(fields)) - existingCount, nil
}

func (o queryOps) hDel(ctx context.Context, q Querier, key string, fields []string) (int64, error) {
	key = encodeKey(key)
	// Encode field names for PostgreSQL
	encFields := make([]string, len(fields))
	for i, f := range fields {
		encFields[i] = encodeField(f)
	}
	result, err := q.Exec(ctx,
		"DELETE FROM kv_hashes WHERE key = $1 AND field = ANY($2)",
		key, encFields,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (o queryOps) hGetAll(ctx context.Context, q Querier, key string) (map[string]string, error) {
	key = encodeKey(key)

	// Batch the WRONGTYPE check and the data fetch into a single round-trip.
	batch := &pgx.Batch{}
	o.queueTypeCheck(batch, key)
	batch.Queue(
		"SELECT field, value FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	br := q.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	if err := o.readTypeCheck(br, TypeHash); err != nil {
		return nil, err
	}

	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var field string
		var value []byte
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		result[decodeField(field)] = string(value)
	}
	return result, rows.Err()
}

func (o queryOps) hMGet(ctx context.Context, q Querier, key string, fields []string) ([]interface{}, error) {
	key = encodeKey(key)
	results := make([]interface{}, len(fields))

	// Encode field names for query
	encFields := make([]string, len(fields))
	for i, f := range fields {
		encFields[i] = encodeField(f)
	}

	rows, err := q.Query(ctx,
		`SELECT field, value FROM kv_hashes 
		 WHERE key = $1 AND field = ANY($2) AND (expires_at IS NULL OR expires_at > NOW())`,
		key, encFields,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fieldValues := make(map[string]string)
	for rows.Next() {
		var field string
		var value []byte
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		fieldValues[field] = string(value)
	}

	for i, field := range fields {
		encField := encodeField(field)
		if val, ok := fieldValues[encField]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}
	return results, nil
}

func (o queryOps) hExists(ctx context.Context, q Querier, key, field string) (bool, error) {
	key = encodeKey(key)
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_hashes 
		 WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, encodeField(field),
	).Scan(&count)
	return count > 0, err
}

func (o queryOps) hKeys(ctx context.Context, q Querier, key string) ([]string, error) {
	key = encodeKey(key)
	rows, err := q.Query(ctx,
		"SELECT field FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			return nil, err
		}
		keys = append(keys, decodeField(field))
	}
	return keys, nil
}

func (o queryOps) hVals(ctx context.Context, q Querier, key string) ([]string, error) {
	key = encodeKey(key)
	rows, err := q.Query(ctx,
		"SELECT value FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		vals = append(vals, string(value))
	}
	return vals, nil
}

func (o queryOps) hLen(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

func (o queryOps) hIncrBy(ctx context.Context, q Querier, key, field string, increment int64) (int64, error) {
	key = encodeKey(key)
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Encode field name for PostgreSQL
	encField := encodeField(field)

	// Get current value or default to 0
	var currentValue int64 = 0
	var valueBytes []byte
	err = q.QueryRow(ctx,
		"SELECT value FROM kv_hashes WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())",
		key, encField,
	).Scan(&valueBytes)
	if err == nil {
		// Parse existing value as integer
		currentValue, err = strconv.ParseInt(string(valueBytes), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}
	} else if err != pgx.ErrNoRows {
		return 0, err
	}

	// Calculate new value
	newValue := currentValue + increment

	// Set metadata before data table for consistent lock ordering (kv_meta before kv_hashes).
	if err := o.setMeta(ctx, q, key, TypeHash, nil); err != nil {
		return 0, err
	}

	// Upsert the new value
	_, err = q.Exec(ctx,
		`INSERT INTO kv_hashes (key, field, value) VALUES ($1, $2, $3)
		 ON CONFLICT (key, field) DO UPDATE SET value = $3`,
		key, encField, []byte(strconv.FormatInt(newValue, 10)),
	)
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

func (o queryOps) hIncrByFloat(ctx context.Context, q Querier, key, field string, increment float64) (float64, error) {
	key = encodeKey(key)
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Encode field name for PostgreSQL
	encField := encodeField(field)

	// Get current value or default to 0
	var currentValue float64 = 0
	var valueBytes []byte
	err = q.QueryRow(ctx,
		"SELECT value FROM kv_hashes WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())",
		key, encField,
	).Scan(&valueBytes)
	if err == nil {
		// Parse existing value as float
		currentValue, err = strconv.ParseFloat(string(valueBytes), 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not a valid float")
		}
	} else if err != pgx.ErrNoRows {
		return 0, err
	}

	// Calculate new value
	newValue := currentValue + increment

	// Format without trailing zeros, but preserve precision
	valueStr := strconv.FormatFloat(newValue, 'f', -1, 64)

	// Set metadata before data table for consistent lock ordering (kv_meta before kv_hashes).
	if err := o.setMeta(ctx, q, key, TypeHash, nil); err != nil {
		return 0, err
	}

	// Upsert the new value
	_, err = q.Exec(ctx,
		`INSERT INTO kv_hashes (key, field, value) VALUES ($1, $2, $3)
		 ON CONFLICT (key, field) DO UPDATE SET value = $3`,
		key, encField, []byte(valueStr),
	)
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

func (o queryOps) hSetNX(ctx context.Context, q Querier, key, field, value string) (bool, error) {
	key = encodeKey(key)
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return false, err
	}
	if keyType != TypeNone && keyType != TypeHash {
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Encode field name for PostgreSQL
	encField := encodeField(field)

	// Set metadata before data table for consistent lock ordering (kv_meta before kv_hashes).
	// Use DO UPDATE to ensure meta exists (harmless if key already exists with correct type).
	if err := o.setMeta(ctx, q, key, TypeHash, nil); err != nil {
		return false, err
	}

	// Try to insert only if not exists
	result, err := q.Exec(ctx,
		`INSERT INTO kv_hashes (key, field, value) VALUES ($1, $2, $3)
		 ON CONFLICT (key, field) DO NOTHING`,
		key, encField, []byte(value),
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() > 0 {
		return true, nil
	}
	return false, nil
}

// ============== List Commands ==============

func (o queryOps) lPush(ctx context.Context, q Querier, key string, values []string) (int64, error) {
	key = encodeKey(key)
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if len(values) == 0 {
		// Just return current length
		var length int64
		if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length); err != nil {
			return 0, fmt.Errorf("failed to get list length: %w", err)
		}
		return length, nil
	}

	// Use advisory lock to serialize list operations on this key
	// hashtext returns int4, we need int8 for pg_advisory_xact_lock
	_, err = q.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", key)
	if err != nil {
		return 0, err
	}

	// Get current min index
	var minIdx int64 = 0
	if err := q.QueryRow(ctx, "SELECT COALESCE(MIN(idx), 0) FROM kv_lists WHERE key = $1", key).Scan(&minIdx); err != nil {
		return 0, fmt.Errorf("failed to get min index: %w", err)
	}

	// Prepare indices and values for batch insert
	// For LPUSH, first value ends up at head, so we insert in reverse order
	indices := make([]int64, len(values))
	valueBytes := make([][]byte, len(values))
	for i, value := range values {
		indices[i] = minIdx - int64(i+1)
		valueBytes[i] = []byte(value)
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_lists).
	if err := o.setMeta(ctx, q, key, TypeList, nil); err != nil {
		return 0, err
	}

	// Batch insert all values at once
	_, err = q.Exec(ctx,
		`INSERT INTO kv_lists (key, idx, value)
		 SELECT $1, unnest($2::bigint[]), unnest($3::bytea[])`,
		key, indices, valueBytes,
	)
	if err != nil {
		return 0, err
	}

	// Return new length
	var length int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length); err != nil {
		return 0, fmt.Errorf("failed to get list length: %w", err)
	}

	return length, nil
}

func (o queryOps) rPush(ctx context.Context, q Querier, key string, values []string) (int64, error) {
	key = encodeKey(key)
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if len(values) == 0 {
		// Just return current length
		var length int64
		if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length); err != nil {
			return 0, fmt.Errorf("failed to get list length: %w", err)
		}
		return length, nil
	}

	// Use advisory lock to serialize list operations on this key
	_, err = q.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", key)
	if err != nil {
		return 0, err
	}

	// Get current max index
	var maxIdx int64 = -1
	if err := q.QueryRow(ctx, "SELECT COALESCE(MAX(idx), -1) FROM kv_lists WHERE key = $1", key).Scan(&maxIdx); err != nil {
		return 0, fmt.Errorf("failed to get max index: %w", err)
	}

	// Prepare indices and values for batch insert
	indices := make([]int64, len(values))
	valueBytes := make([][]byte, len(values))
	for i, value := range values {
		indices[i] = maxIdx + int64(i+1)
		valueBytes[i] = []byte(value)
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_lists).
	if err := o.setMeta(ctx, q, key, TypeList, nil); err != nil {
		return 0, err
	}

	// Batch insert all values at once
	_, err = q.Exec(ctx,
		`INSERT INTO kv_lists (key, idx, value)
		 SELECT $1, unnest($2::bigint[]), unnest($3::bytea[])`,
		key, indices, valueBytes,
	)
	if err != nil {
		return 0, err
	}

	// Return new length
	var length int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length); err != nil {
		return 0, fmt.Errorf("failed to get list length: %w", err)
	}

	return length, nil
}

func (o queryOps) lPop(ctx context.Context, q Querier, key string) (string, bool, error) {
	key = encodeKey(key)
	// Find and delete the leftmost element in a single query using CTE
	// Use FOR UPDATE SKIP LOCKED to prevent deadlocks when multiple clients pop concurrently
	var value []byte
	err := q.QueryRow(ctx,
		`WITH deleted AS (
			DELETE FROM kv_lists
			WHERE key = $1 AND idx = (
				SELECT idx FROM kv_lists WHERE key = $1 ORDER BY idx ASC LIMIT 1 FOR UPDATE SKIP LOCKED
			)
			RETURNING value
		)
		SELECT value FROM deleted`,
		key,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

func (o queryOps) rPop(ctx context.Context, q Querier, key string) (string, bool, error) {
	key = encodeKey(key)
	// Find and delete the rightmost element in a single query using CTE
	// Use FOR UPDATE SKIP LOCKED to prevent deadlocks when multiple clients pop concurrently
	var value []byte
	err := q.QueryRow(ctx,
		`WITH deleted AS (
			DELETE FROM kv_lists
			WHERE key = $1 AND idx = (
				SELECT idx FROM kv_lists WHERE key = $1 ORDER BY idx DESC LIMIT 1 FOR UPDATE SKIP LOCKED
			)
			RETURNING value
		)
		SELECT value FROM deleted`,
		key,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

// popMulti pops a single element from the first non-empty list among keys
// (in caller order) using one SQL round-trip. order is "ASC" for LPop
// semantics (leftmost element of the first non-empty key) or "DESC" for RPop.
func (o queryOps) popMulti(ctx context.Context, q Querier, keys []string, order string) (string, string, bool, error) {
	if len(keys) == 0 {
		return "", "", false, nil
	}
	// Copy before encoding — encodeKeys mutates its input.
	encoded := make([]string, len(keys))
	copy(encoded, keys)
	encoded = encodeKeys(encoded)

	// array_position orders candidate rows by the caller's key order so we
	// always pop from the earliest non-empty list. Within that list, idx ASC
	// gives the leftmost element (LPOP) and idx DESC gives the rightmost (RPOP).
	// SKIP LOCKED keeps concurrent poppers from deadlocking on the same row.
	sql := `WITH candidate AS (
		SELECT key, idx FROM kv_lists
		WHERE key = ANY($1::text[]) AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY array_position($1::text[], key), idx ` + order + `
		LIMIT 1 FOR UPDATE SKIP LOCKED
	),
	deleted AS (
		DELETE FROM kv_lists
		WHERE (key, idx) = (SELECT key, idx FROM candidate)
		RETURNING key, value
	)
	SELECT key, value FROM deleted`

	var poppedKey string
	var value []byte
	err := q.QueryRow(ctx, sql, encoded).Scan(&poppedKey, &value)
	if err == pgx.ErrNoRows {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return decodeKey(poppedKey), string(value), true, nil
}

func (o queryOps) lPopMulti(ctx context.Context, q Querier, keys []string) (string, string, bool, error) {
	return o.popMulti(ctx, q, keys, "ASC")
}

func (o queryOps) rPopMulti(ctx context.Context, q Querier, keys []string) (string, string, bool, error) {
	return o.popMulti(ctx, q, keys, "DESC")
}

// popMultiN is the LMPOP/RMPOP analogue of popMulti: it identifies the first
// non-empty key among keys (in caller order) and pops up to count elements
// from that one key in one SQL round-trip. order is "ASC" for LMPOP (left)
// or "DESC" for RMPOP (right).
func (o queryOps) popMultiN(ctx context.Context, q Querier, keys []string, order string, count int64) (string, []string, bool, error) {
	if len(keys) == 0 || count <= 0 {
		return "", nil, false, nil
	}
	encoded := make([]string, len(keys))
	copy(encoded, keys)
	encoded = encodeKeys(encoded)

	// winner: the first non-empty key (precedence by array_position).
	// target: rows from that key in pop order, capped at count.
	sql := `WITH winner AS (
		SELECT key FROM kv_lists
		WHERE key = ANY($1::text[]) AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY array_position($1::text[], key), idx ` + order + `
		LIMIT 1
	),
	target AS (
		SELECT key, idx FROM kv_lists
		WHERE key = (SELECT key FROM winner)
		  AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY idx ` + order + `
		LIMIT $2 FOR UPDATE SKIP LOCKED
	),
	deleted AS (
		DELETE FROM kv_lists
		WHERE (key, idx) IN (SELECT key, idx FROM target)
		RETURNING key, idx, value
	)
	SELECT key, value FROM deleted ORDER BY idx ` + order

	rows, err := q.Query(ctx, sql, encoded, count)
	if err != nil {
		return "", nil, false, err
	}
	defer rows.Close()

	var poppedKey string
	var values []string
	for rows.Next() {
		var k string
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return "", nil, false, err
		}
		poppedKey = k
		values = append(values, string(v))
	}
	if err := rows.Err(); err != nil {
		return "", nil, false, err
	}
	if len(values) == 0 {
		return "", nil, false, nil
	}
	return decodeKey(poppedKey), values, true, nil
}

func (o queryOps) lMPop(ctx context.Context, q Querier, keys []string, count int64) (string, []string, bool, error) {
	return o.popMultiN(ctx, q, keys, "ASC", count)
}

func (o queryOps) rMPop(ctx context.Context, q Querier, keys []string, count int64) (string, []string, bool, error) {
	return o.popMultiN(ctx, q, keys, "DESC", count)
}

// zMPop is the ZMPOP analogue: identify the first non-empty zset, then pop
// up to count members by score (DESC for MAX, ASC for MIN) in one round-trip.
func (o queryOps) zMPop(ctx context.Context, q Querier, keys []string, order string, count int64) (string, []ZMember, bool, error) {
	if len(keys) == 0 || count <= 0 {
		return "", nil, false, nil
	}
	encoded := make([]string, len(keys))
	copy(encoded, keys)
	encoded = encodeKeys(encoded)

	sql := `WITH winner AS (
		SELECT key FROM kv_zsets
		WHERE key = ANY($1::text[]) AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY array_position($1::text[], key), score ` + order + `, member ` + order + `
		LIMIT 1
	),
	target AS (
		SELECT key, member FROM kv_zsets
		WHERE key = (SELECT key FROM winner)
		  AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY score ` + order + `, member ` + order + `
		LIMIT $2 FOR UPDATE SKIP LOCKED
	),
	deleted AS (
		DELETE FROM kv_zsets
		WHERE (key, member) IN (SELECT key, member FROM target)
		RETURNING key, member, score
	)
	SELECT key, member, score FROM deleted ORDER BY score ` + order + `, member ` + order

	rows, err := q.Query(ctx, sql, encoded, count)
	if err != nil {
		return "", nil, false, err
	}
	defer rows.Close()

	var poppedKey string
	var members []ZMember
	for rows.Next() {
		var k string
		var m []byte
		var s float64
		if err := rows.Scan(&k, &m, &s); err != nil {
			return "", nil, false, err
		}
		poppedKey = k
		members = append(members, ZMember{Member: string(m), Score: s})
	}
	if err := rows.Err(); err != nil {
		return "", nil, false, err
	}
	if len(members) == 0 {
		return "", nil, false, nil
	}
	return decodeKey(poppedKey), members, true, nil
}

func (o queryOps) zMPopMin(ctx context.Context, q Querier, keys []string, count int64) (string, []ZMember, bool, error) {
	return o.zMPop(ctx, q, keys, "ASC", count)
}

func (o queryOps) zMPopMax(ctx context.Context, q Querier, keys []string, count int64) (string, []ZMember, bool, error) {
	return o.zMPop(ctx, q, keys, "DESC", count)
}

func (o queryOps) lLen(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)

	batch := &pgx.Batch{}
	o.queueTypeCheck(batch, key)
	batch.Queue(
		"SELECT COUNT(*) FROM kv_lists WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	br := q.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	if err := o.readTypeCheck(br, TypeList); err != nil {
		return 0, err
	}

	var count int64
	err := br.QueryRow().Scan(&count)
	return count, err
}

func (o queryOps) lRange(ctx context.Context, q Querier, key string, start, stop int64) ([]string, error) {
	key = encodeKey(key)

	// Batch the WRONGTYPE check and the count query into a single round-trip.
	batch := &pgx.Batch{}
	o.queueTypeCheck(batch, key)
	batch.Queue("SELECT COUNT(*) FROM kv_lists WHERE key = $1", key)
	br := q.SendBatch(ctx, batch)

	if err := o.readTypeCheck(br, TypeList); err != nil {
		_ = br.Close()
		return nil, err
	}

	var total int64
	if err := br.QueryRow().Scan(&total); err != nil {
		_ = br.Close()
		return nil, fmt.Errorf("failed to get list count: %w", err)
	}
	_ = br.Close()

	// Convert negative indices
	if start < 0 {
		start = total + start
	}
	if stop < 0 {
		stop = total + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= total {
		stop = total - 1
	}
	if start > stop {
		return []string{}, nil
	}

	rows, err := q.Query(ctx,
		`SELECT value FROM kv_lists WHERE key = $1 
		 ORDER BY idx ASC LIMIT $2 OFFSET $3`,
		key, stop-start+1, start,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, string(value))
	}
	return result, nil
}

func (o queryOps) lIndex(ctx context.Context, q Querier, key string, index int64) (string, bool, error) {
	key = encodeKey(key)

	if index >= 0 {
		// Positive index: no count needed. OFFSET past end just returns no rows.
		var value []byte
		err := q.QueryRow(ctx,
			`SELECT value FROM kv_lists
			 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY idx ASC LIMIT 1 OFFSET $2`,
			key, index,
		).Scan(&value)
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		return string(value), true, nil
	}

	// Negative index: fold the count + select into one round-trip via CTE.
	// The WHERE filter on `total + idx >= 0` suppresses rows when |index|
	// exceeds the list length; otherwise GREATEST would clamp the OFFSET to
	// 0 and we'd incorrectly return row 0.
	var value []byte
	err := q.QueryRow(ctx,
		`WITH cnt AS (
			SELECT COUNT(*)::bigint AS total FROM kv_lists
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		)
		SELECT value FROM kv_lists, cnt
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		  AND cnt.total + $2::bigint >= 0
		ORDER BY idx ASC
		LIMIT 1 OFFSET GREATEST((SELECT total FROM cnt) + $2::bigint, 0)`,
		key, index,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(value), true, nil
}

// ============== Set Commands ==============

func (o queryOps) sAdd(ctx context.Context, q Querier, key string, members []string) (int64, error) {
	key = encodeKey(key)
	if len(members) == 0 {
		return 0, nil
	}

	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Convert members to bytes for batch insert
	memberBytes := make([][]byte, len(members))
	for i, m := range members {
		memberBytes[i] = []byte(m)
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_sets).
	if err := o.setMeta(ctx, q, key, TypeSet, nil); err != nil {
		return 0, err
	}

	// Batch insert with ON CONFLICT DO NOTHING, returning count of inserted rows
	var added int64
	err = q.QueryRow(ctx,
		`WITH inserted AS (
			INSERT INTO kv_sets (key, member)
			SELECT $1, unnest($2::bytea[])
			ON CONFLICT (key, member) DO NOTHING
			RETURNING 1
		)
		SELECT COUNT(*) FROM inserted`,
		key, memberBytes,
	).Scan(&added)
	if err != nil {
		return 0, err
	}

	return added, nil
}

func (o queryOps) sRem(ctx context.Context, q Querier, key string, members []string) (int64, error) {
	key = encodeKey(key)
	memberBytes := make([][]byte, len(members))
	for i, m := range members {
		memberBytes[i] = []byte(m)
	}

	result, err := q.Exec(ctx,
		"DELETE FROM kv_sets WHERE key = $1 AND member = ANY($2)",
		key, memberBytes,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (o queryOps) sMembers(ctx context.Context, q Querier, key string) ([]string, error) {
	key = encodeKey(key)

	batch := &pgx.Batch{}
	o.queueTypeCheck(batch, key)
	batch.Queue(
		"SELECT member FROM kv_sets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	br := q.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	if err := o.readTypeCheck(br, TypeSet); err != nil {
		return nil, err
	}

	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}
	return members, rows.Err()
}

func (o queryOps) sIsMember(ctx context.Context, q Querier, key, member string) (bool, error) {
	key = encodeKey(key)
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_sets 
		 WHERE key = $1 AND member = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, []byte(member),
	).Scan(&count)
	return count > 0, err
}

func (o queryOps) sCard(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)

	batch := &pgx.Batch{}
	o.queueTypeCheck(batch, key)
	batch.Queue(
		"SELECT COUNT(*) FROM kv_sets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	br := q.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	if err := o.readTypeCheck(br, TypeSet); err != nil {
		return 0, err
	}

	var count int64
	err := br.QueryRow().Scan(&count)
	return count, err
}

// ============== Sorted Set Commands ==============

func (o queryOps) zAdd(ctx context.Context, q Querier, key string, members []ZMember, opts ZAddOptions) (int64, error) {
	key = encodeKey(key)
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// XX on a non-existent key adds nothing and must not create the key.
	if keyType == TypeNone && opts.XX {
		return 0, nil
	}

	// setMeta takes the key advisory lock, serializing the per-member
	// read-modify-write below for the duration of the transaction (so the
	// existence check and conditional write are atomic against other writers).
	// Set meta before the data table for consistent lock ordering. When the key
	// is new this is only reached without XX, where at least one member is always
	// added, so no phantom empty key is created.
	if err := o.setMeta(ctx, q, key, TypeZSet, nil); err != nil {
		return 0, err
	}

	var added, changed int64
	for _, m := range members {
		var oldScore float64
		exists := true
		err := q.QueryRow(ctx,
			`SELECT score FROM kv_zsets WHERE key = $1 AND member = $2`,
			key, []byte(m.Member),
		).Scan(&oldScore)
		if err == pgx.ErrNoRows {
			exists = false
		} else if err != nil {
			return 0, err
		}

		if exists {
			// NX never updates existing members. GT/LT gate the update by the
			// direction of the score change.
			if opts.NX {
				continue
			}
			if opts.GT && m.Score <= oldScore {
				continue
			}
			if opts.LT && m.Score >= oldScore {
				continue
			}
			if m.Score == oldScore {
				continue // no change, not counted by CH
			}
			if _, err := q.Exec(ctx,
				`UPDATE kv_zsets SET score = $3 WHERE key = $1 AND member = $2`,
				key, []byte(m.Member), m.Score,
			); err != nil {
				return 0, err
			}
			changed++
		} else {
			// XX never adds new members; GT/LT do not block adds of new members.
			if opts.XX {
				continue
			}
			if _, err := q.Exec(ctx,
				`INSERT INTO kv_zsets (key, member, score) VALUES ($1, $2, $3)`,
				key, []byte(m.Member), m.Score,
			); err != nil {
				return 0, err
			}
			added++
		}
	}

	if opts.CH {
		return added + changed, nil
	}
	return added, nil
}

func (o queryOps) zRange(ctx context.Context, q Querier, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	key = encodeKey(key)

	// Fold the count, negative-index normalization, and the SELECT into one
	// round-trip via a CTE. `bounds` performs the same clamping that the Go
	// code used to do; the join with kv_zsets is a 1×N cross product (bounds
	// has exactly one row). The WHERE guard rejects all rows when the range
	// is empty (total=0 or start_pos > stop_pos after clamping).
	rows, err := q.Query(ctx,
		`WITH cnt AS (
			SELECT COUNT(*)::bigint AS total FROM kv_zsets
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		),
		bounds AS (
			SELECT
				GREATEST(CASE WHEN $2::bigint < 0 THEN total + $2::bigint ELSE $2::bigint END, 0) AS start_pos,
				LEAST(CASE WHEN $3::bigint < 0 THEN total + $3::bigint ELSE $3::bigint END, total - 1) AS stop_pos,
				total
			FROM cnt
		)
		SELECT member, score FROM kv_zsets, bounds
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		  AND bounds.total > 0
		  AND bounds.start_pos <= bounds.stop_pos
		ORDER BY score ASC, member ASC
		LIMIT (SELECT GREATEST(stop_pos - start_pos + 1, 0) FROM bounds)
		OFFSET (SELECT start_pos FROM bounds)`,
		key, start, stop,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}
	return members, rows.Err()
}

// zRevRange mirrors zRange with ORDER BY DESC: positions are still 0-based but
// counted from the highest score downward.
func (o queryOps) zRevRange(ctx context.Context, q Querier, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	key = encodeKey(key)
	rows, err := q.Query(ctx,
		`WITH cnt AS (
			SELECT COUNT(*)::bigint AS total FROM kv_zsets
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		),
		bounds AS (
			SELECT
				GREATEST(CASE WHEN $2::bigint < 0 THEN total + $2::bigint ELSE $2::bigint END, 0) AS start_pos,
				LEAST(CASE WHEN $3::bigint < 0 THEN total + $3::bigint ELSE $3::bigint END, total - 1) AS stop_pos,
				total
			FROM cnt
		)
		SELECT member, score FROM kv_zsets, bounds
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		  AND bounds.total > 0
		  AND bounds.start_pos <= bounds.stop_pos
		ORDER BY score DESC, member DESC
		LIMIT (SELECT GREATEST(stop_pos - start_pos + 1, 0) FROM bounds)
		OFFSET (SELECT start_pos FROM bounds)`,
		key, start, stop,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}
	return members, rows.Err()
}

func (o queryOps) zScore(ctx context.Context, q Querier, key, member string) (float64, bool, error) {
	key = encodeKey(key)
	var score float64
	err := q.QueryRow(ctx,
		`SELECT score FROM kv_zsets 
		 WHERE key = $1 AND member = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, []byte(member),
	).Scan(&score)

	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

func (o queryOps) zRem(ctx context.Context, q Querier, key string, members []string) (int64, error) {
	key = encodeKey(key)
	memberBytes := make([][]byte, len(members))
	for i, m := range members {
		memberBytes[i] = []byte(m)
	}

	result, err := q.Exec(ctx,
		"DELETE FROM kv_zsets WHERE key = $1 AND member = ANY($2)",
		key, memberBytes,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (o queryOps) zCard(ctx context.Context, q Querier, key string) (int64, error) {
	key = encodeKey(key)
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

func (o queryOps) zRangeByScore(ctx context.Context, q Querier, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	key = encodeKey(key)
	var query string
	var args []interface{}

	if count > 0 {
		query = `SELECT member, score FROM kv_zsets 
			 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY score ASC, member ASC
			 LIMIT $4 OFFSET $5`
		args = []interface{}{key, min, max, count, offset}
	} else {
		query = `SELECT member, score FROM kv_zsets 
			 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY score ASC, member ASC`
		args = []interface{}{key, min, max}
	}

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}
	return members, nil
}

// lexBoundClause turns one LexBound side into a SQL fragment + positional
// argument (or no argument if Infinity). The clause is empty when the bound
// imposes no restriction (matching ±infinity on the corresponding side).
func lexBoundClause(b LexBound, isLower bool, paramIdx int) (clause string, arg interface{}) {
	// Lower bound: -inf matches everything → no clause; +inf rejects everything
	// (we represent that by a falsy 1=0). Mirror for upper bound.
	if isLower {
		switch b.Infinity {
		case -1:
			return "", nil
		case +1:
			return "1=0", nil
		}
	} else {
		switch b.Infinity {
		case +1:
			return "", nil
		case -1:
			return "1=0", nil
		}
	}
	op := "<"
	if isLower {
		op = ">"
	}
	if b.Inclusive {
		op += "="
	}
	return fmt.Sprintf("member %s $%d", op, paramIdx), []byte(b.Value)
}

// buildLexWhere produces the WHERE-clause tail and ordered args for
// zRangeByLex/zLexCount given parsed min/max bounds.
func buildLexWhere(min, max LexBound, baseArgs []interface{}) (where string, args []interface{}) {
	args = baseArgs
	clauses := []string{}
	if c, a := lexBoundClause(min, true, len(args)+1); c != "" {
		clauses = append(clauses, c)
		if a != nil {
			args = append(args, a)
		}
	}
	if c, a := lexBoundClause(max, false, len(args)+1); c != "" {
		clauses = append(clauses, c)
		if a != nil {
			args = append(args, a)
		}
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " AND " + strings.Join(clauses, " AND "), args
}

func (o queryOps) zRangeByLex(ctx context.Context, q Querier, key string, min, max LexBound, offset, count int64) ([]string, error) {
	key = encodeKey(key)
	where, args := buildLexWhere(min, max, []interface{}{key})

	sql := "SELECT member FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())" + where + " ORDER BY member ASC"
	if count > 0 {
		sql += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
		args = append(args, count, offset)
	} else if offset > 0 {
		sql += fmt.Sprintf(" OFFSET $%d", len(args)+1)
		args = append(args, offset)
	}

	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}
	return members, rows.Err()
}

// zRevRangeByLex is zRangeByLex with ORDER BY DESC. The Redis wire protocol
// passes max before min for ZREVRANGEBYLEX; the caller normalizes that.
func (o queryOps) zRevRangeByLex(ctx context.Context, q Querier, key string, min, max LexBound, offset, count int64) ([]string, error) {
	key = encodeKey(key)
	where, args := buildLexWhere(min, max, []interface{}{key})

	sql := "SELECT member FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())" + where + " ORDER BY member DESC"
	if count > 0 {
		sql += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
		args = append(args, count, offset)
	} else if offset > 0 {
		sql += fmt.Sprintf(" OFFSET $%d", len(args)+1)
		args = append(args, offset)
	}

	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}
	return members, rows.Err()
}

// zRangeStore evaluates the ZRANGE-style query described by spec against src
// and copies the resulting (member, score) pairs into dst. dst is fully
// replaced (any prior content is dropped). The caller is responsible for
// running this inside a transaction so the replace is atomic.
func (o queryOps) zRangeStore(ctx context.Context, q Querier, dst, src string, spec ZRangeStoreSpec) (int64, error) {
	srcEnc := encodeKey(src)
	dstEnc := encodeKey(dst)

	// Build the source SELECT (member, score) + ordered args based on spec.
	// $1 is always the source key; downstream args follow.
	args := []interface{}{srcEnc}
	var srcSQL string

	orderAsc := "ORDER BY score ASC, member ASC"
	orderDesc := "ORDER BY score DESC, member DESC"
	order := orderAsc
	if spec.Rev {
		order = orderDesc
	}

	switch spec.By {
	case ZRangeByIndex:
		// Use the same bounds CTE as zRange/zRevRange to handle negative
		// indices server-side.
		args = append(args, spec.Start, spec.Stop)
		srcSQL = fmt.Sprintf(`WITH cnt AS (
			SELECT COUNT(*)::bigint AS total FROM kv_zsets
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		),
		bounds AS (
			SELECT
				GREATEST(CASE WHEN $2::bigint < 0 THEN total + $2::bigint ELSE $2::bigint END, 0) AS start_pos,
				LEAST(CASE WHEN $3::bigint < 0 THEN total + $3::bigint ELSE $3::bigint END, total - 1) AS stop_pos,
				total
			FROM cnt
		)
		SELECT member, score FROM kv_zsets, bounds
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		  AND bounds.total > 0
		  AND bounds.start_pos <= bounds.stop_pos
		%s
		LIMIT (SELECT GREATEST(stop_pos - start_pos + 1, 0) FROM bounds)
		OFFSET (SELECT start_pos FROM bounds)`, order)
	case ZRangeByScore:
		args = append(args, spec.MinScore, spec.MaxScore)
		srcSQL = fmt.Sprintf(`SELECT member, score FROM kv_zsets
			WHERE key = $1 AND score >= $2 AND score <= $3
			  AND (expires_at IS NULL OR expires_at > NOW())
			%s`, order)
		if spec.Count > 0 {
			srcSQL += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
			args = append(args, spec.Count, spec.Offset)
		}
	case ZRangeByLex:
		where, withArgs := buildLexWhere(spec.MinLex, spec.MaxLex, args)
		args = withArgs
		// Lex ordering: by member only (scores tie because lex queries assume
		// equal scores; we still preserve the actual score from the source).
		lexOrder := "ORDER BY member ASC"
		if spec.Rev {
			lexOrder = "ORDER BY member DESC"
		}
		srcSQL = "SELECT member, score FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())" + where + " " + lexOrder
		if spec.Count > 0 {
			srcSQL += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
			args = append(args, spec.Count, spec.Offset)
		}
	default:
		return 0, fmt.Errorf("unknown ZRangeStoreBy: %d", spec.By)
	}

	// Replace dst: drop any prior data and meta entries, then insert.
	if err := o.deleteKeyFromAllTables(ctx, q, dstEnc); err != nil {
		return 0, err
	}

	// Insert: prepend $dstPlaceholder via a CTE so we only need one statement.
	dstArgIdx := len(args) + 1
	args = append(args, dstEnc)
	insertSQL := fmt.Sprintf(`WITH src AS (%s)
		INSERT INTO kv_zsets (key, member, score)
		SELECT $%d, member, score FROM src
		RETURNING 1`, srcSQL, dstArgIdx)

	rows, err := q.Query(ctx, insertSQL, args...)
	if err != nil {
		return 0, err
	}
	var n int64
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// Only register the key in kv_meta if we stored something. ZRangeStore on
	// an empty result leaves dst missing, matching Redis.
	if n > 0 {
		if err := o.setMeta(ctx, q, dstEnc, TypeZSet, nil); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (o queryOps) zLexCount(ctx context.Context, q Querier, key string, min, max LexBound) (int64, error) {
	key = encodeKey(key)
	where, args := buildLexWhere(min, max, []interface{}{key})

	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())"+where,
		args...,
	).Scan(&count)
	return count, err
}

// zRevRangeByScore mirrors zRangeByScore with ORDER BY DESC. The caller is
// responsible for passing min/max in the right slots (Redis ZREVRANGEBYSCORE
// flips them in the wire protocol; this helper takes them already normalized).
func (o queryOps) zRevRangeByScore(ctx context.Context, q Querier, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	key = encodeKey(key)
	var query string
	var args []interface{}

	if count > 0 {
		query = `SELECT member, score FROM kv_zsets
			 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY score DESC, member DESC
			 LIMIT $4 OFFSET $5`
		args = []interface{}{key, min, max, count, offset}
	} else {
		query = `SELECT member, score FROM kv_zsets
			 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY score DESC, member DESC`
		args = []interface{}{key, min, max}
	}

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}
	return members, nil
}

func (o queryOps) zRemRangeByScore(ctx context.Context, q Querier, key string, min, max float64) (int64, error) {
	key = encodeKey(key)
	result, err := q.Exec(ctx,
		"DELETE FROM kv_zsets WHERE key = $1 AND score >= $2 AND score <= $3",
		key, min, max,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (o queryOps) zRemRangeByRank(ctx context.Context, q Querier, key string, start, stop int64) (int64, error) {
	key = encodeKey(key)
	// Get total count first to handle negative indices
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	if err != nil {
		return 0, err
	}

	// Convert negative indices
	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop || start >= count {
		return 0, nil
	}

	// Delete members within the rank range
	result, err := q.Exec(ctx,
		`DELETE FROM kv_zsets WHERE key = $1 AND member IN (
			SELECT member FROM kv_zsets 
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			ORDER BY score ASC, member ASC
			LIMIT $3 OFFSET $2
		)`,
		key, start, stop-start+1,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (o queryOps) zIncrBy(ctx context.Context, q Querier, key string, increment float64, member string) (float64, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return 0, err
	}
	// Ensure meta entry exists
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type) VALUES ($1, 'zset') ON CONFLICT (key) DO NOTHING`,
		key,
	)
	if err != nil {
		return 0, err
	}

	var newScore float64
	err = q.QueryRow(ctx,
		`INSERT INTO kv_zsets (key, member, score) VALUES ($1, $2, $3)
		 ON CONFLICT (key, member) DO UPDATE SET score = kv_zsets.score + EXCLUDED.score
		 RETURNING score`,
		key, []byte(member), increment,
	).Scan(&newScore)
	return newScore, err
}

func (o queryOps) zPopMin(ctx context.Context, q Querier, key string, count int64) ([]ZMember, error) {
	key = encodeKey(key)
	// Get the lowest-scored members
	rows, err := q.Query(ctx,
		`SELECT member, score FROM kv_zsets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY score ASC, member ASC
		 LIMIT $2`,
		key, count,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}

	// Delete the popped members
	if len(members) > 0 {
		memberBytes := make([][]byte, len(members))
		for i, m := range members {
			memberBytes[i] = []byte(m.Member)
		}
		_, err = q.Exec(ctx,
			"DELETE FROM kv_zsets WHERE key = $1 AND member = ANY($2)",
			key, memberBytes,
		)
		if err != nil {
			return nil, err
		}
	}

	return members, nil
}

func (o queryOps) lRem(ctx context.Context, q Querier, key string, count int64, element string) (int64, error) {
	key = encodeKey(key)
	// count > 0: Remove count elements from head
	// count < 0: Remove -count elements from tail
	// count = 0: Remove all elements

	var result int64
	if count == 0 {
		// Remove all matching elements
		res, err := q.Exec(ctx,
			"DELETE FROM kv_lists WHERE key = $1 AND value = $2",
			key, []byte(element),
		)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected(), nil
	}

	absCount := count
	if count < 0 {
		absCount = -count
	}

	var order string
	if count > 0 {
		order = "ASC"
	} else {
		order = "DESC"
	}

	// Delete specific number of elements from head or tail
	res, err := q.Exec(ctx,
		fmt.Sprintf(`DELETE FROM kv_lists WHERE ctid IN (
			SELECT ctid FROM kv_lists 
			WHERE key = $1 AND value = $2
			ORDER BY idx %s
			LIMIT $3
		)`, order),
		key, []byte(element), absCount,
	)
	if err != nil {
		return 0, err
	}
	result = res.RowsAffected()

	return result, nil
}

func (o queryOps) rPopLPush(ctx context.Context, q Querier, source, destination string) (string, bool, error) {
	source, destination = encodeKey(source), encodeKey(destination)
	if err := o.lockKeys(ctx, q, []string{source, destination}); err != nil {
		return "", false, err
	}
	// Pop from source (right)
	// Use FOR UPDATE SKIP LOCKED to prevent deadlocks when multiple clients pop concurrently
	var value []byte
	var idx int64

	err := q.QueryRow(ctx,
		`SELECT value, idx FROM kv_lists 
		 WHERE key = $1 
		 ORDER BY idx DESC 
		 LIMIT 1 FOR UPDATE SKIP LOCKED`,
		source,
	).Scan(&value, &idx)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	// Delete from source
	_, err = q.Exec(ctx,
		"DELETE FROM kv_lists WHERE key = $1 AND idx = $2",
		source, idx,
	)
	if err != nil {
		return "", false, err
	}

	// Ensure meta entry exists for destination
	_, err = q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type) VALUES ($1, 'list') ON CONFLICT (key) DO NOTHING`,
		destination,
	)
	if err != nil {
		return "", false, err
	}

	// Push to destination (left) using atomic subquery
	_, err = q.Exec(ctx,
		`INSERT INTO kv_lists (key, idx, value) 
		 VALUES ($1, COALESCE((SELECT MIN(idx) FROM kv_lists WHERE key = $1), 0) - 1, $2)`,
		destination, value,
	)
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

func (o queryOps) lTrim(ctx context.Context, q Querier, key string, start, stop int64) error {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return err
	}
	// Get total length
	var length int64
	err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length)
	if err != nil {
		return err
	}

	if length == 0 {
		return nil
	}

	// Normalize negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Bound to valid range
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	// If start > stop, delete entire list
	if start > stop {
		// Delete kv_meta before data table for consistent lock ordering (kv_meta before kv_lists).
		_, err := q.Exec(ctx, "DELETE FROM kv_meta WHERE key = $1", key)
		if err != nil {
			return err
		}
		_, err = q.Exec(ctx, "DELETE FROM kv_lists WHERE key = $1", key)
		return err
	}

	// Delete elements outside the range using ROW_NUMBER
	_, err = q.Exec(ctx,
		`DELETE FROM kv_lists WHERE ctid IN (
			SELECT ctid FROM (
				SELECT ctid, ROW_NUMBER() OVER (ORDER BY idx) - 1 AS pos
				FROM kv_lists WHERE key = $1
			) sub
			WHERE pos < $2 OR pos > $3
		)`,
		key, start, stop,
	)

	return err
}

// LPos finds the position of an element in a list
func (o queryOps) lPos(ctx context.Context, q Querier, key, element string, rank, count, maxlen int64) ([]int64, error) {
	key = encodeKey(key)
	// Get all elements in order
	rows, err := q.Query(ctx,
		`SELECT ROW_NUMBER() OVER (ORDER BY idx) - 1 AS pos, value 
		 FROM kv_lists WHERE key = $1 
		 ORDER BY idx`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []int64
	elemBytes := []byte(element)
	matches := int64(0)
	skipped := int64(0)
	scanned := int64(0)

	for rows.Next() {
		var pos int64
		var value []byte
		if err := rows.Scan(&pos, &value); err != nil {
			return nil, err
		}

		scanned++
		if maxlen > 0 && scanned > maxlen {
			break
		}

		if string(value) == string(elemBytes) {
			matches++
			// Skip matches based on rank (1-indexed)
			if rank > 0 && skipped < rank-1 {
				skipped++
				continue
			}
			positions = append(positions, pos)
			if count > 0 && int64(len(positions)) >= count {
				break
			}
		}
	}

	return positions, nil
}

// LSet sets an element at a specific index
func (o queryOps) lSet(ctx context.Context, q Querier, key string, index int64, element string) error {
	key = encodeKey(key)
	// Get total count
	var total int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&total); err != nil {
		return err
	}

	if total == 0 {
		return fmt.Errorf("ERR no such key")
	}

	// Convert negative index
	if index < 0 {
		index = total + index
	}
	if index < 0 || index >= total {
		return fmt.Errorf("ERR index out of range")
	}

	// Get the idx value at the position
	var idx int64
	err := q.QueryRow(ctx,
		`SELECT idx FROM kv_lists WHERE key = $1 ORDER BY idx LIMIT 1 OFFSET $2`,
		key, index,
	).Scan(&idx)
	if err != nil {
		return err
	}

	// Update the value
	_, err = q.Exec(ctx,
		"UPDATE kv_lists SET value = $3 WHERE key = $1 AND idx = $2",
		key, idx, []byte(element),
	)
	return err
}

// LInsert inserts an element before or after a pivot element
func (o queryOps) lInsert(ctx context.Context, q Querier, key, pivot, element string, before bool) (int64, error) {
	key = encodeKey(key)
	// Find the pivot element
	var pivotIdx int64
	err := q.QueryRow(ctx,
		`SELECT idx FROM kv_lists WHERE key = $1 AND value = $2 ORDER BY idx LIMIT 1`,
		key, []byte(pivot),
	).Scan(&pivotIdx)
	if err == pgx.ErrNoRows {
		return -1, nil // Pivot not found
	}
	if err != nil {
		return 0, err
	}

	// Use advisory lock to serialize list operations
	_, err = q.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", key)
	if err != nil {
		return 0, err
	}

	// Calculate insertion index - shift elements to make room
	if before {
		// BEFORE: Insert before the pivot
		// Shift pivot and all elements after it UP by 1 to make room
		_, err = q.Exec(ctx,
			"UPDATE kv_lists SET idx = idx + 1 WHERE key = $1 AND idx >= $2",
			key, pivotIdx,
		)
		if err != nil {
			return 0, err
		}
		// Insert at the original pivot position (pivot has moved up)
		_, err = q.Exec(ctx,
			"INSERT INTO kv_lists (key, idx, value) VALUES ($1, $2, $3)",
			key, pivotIdx, []byte(element),
		)
	} else {
		// AFTER: Insert after the pivot
		// Shift all elements after pivot UP by 1 to make room
		_, err = q.Exec(ctx,
			"UPDATE kv_lists SET idx = idx + 1 WHERE key = $1 AND idx > $2",
			key, pivotIdx,
		)
		if err != nil {
			return 0, err
		}
		// Insert right after the pivot
		_, err = q.Exec(ctx,
			"INSERT INTO kv_lists (key, idx, value) VALUES ($1, $2, $3)",
			key, pivotIdx+1, []byte(element),
		)
	}
	if err != nil {
		return 0, err
	}

	// Return new length
	var length int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&length); err != nil {
		return 0, err
	}
	return length, nil
}

// ============== Set Operation Extensions ==============

func (o queryOps) sMIsMember(ctx context.Context, q Querier, key string, members []string) ([]bool, error) {
	key = encodeKey(key)
	result := make([]bool, len(members))

	// Build a set of existing members for O(1) lookup
	existing := make(map[string]bool)
	rows, err := q.Query(ctx,
		"SELECT member FROM kv_sets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		existing[string(member)] = true
	}

	for i, member := range members {
		result[i] = existing[member]
	}
	return result, nil
}

func (o queryOps) sInter(ctx context.Context, q Querier, keys []string) ([]string, error) {
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Get members of first set
	first, err := o.sMembers(ctx, q, keys[0])
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return []string{}, nil
	}

	// For each subsequent set, keep only common members
	result := make(map[string]bool)
	for _, m := range first {
		result[m] = true
	}

	for i := 1; i < len(keys); i++ {
		members, err := o.sMembers(ctx, q, keys[i])
		if err != nil {
			return nil, err
		}
		nextResult := make(map[string]bool)
		for _, m := range members {
			if result[m] {
				nextResult[m] = true
			}
		}
		result = nextResult
		if len(result) == 0 {
			return []string{}, nil
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out, nil
}

func (o queryOps) sInterStore(ctx context.Context, q Querier, destination string, keys []string) (int64, error) {
	destination = encodeKey(destination)
	keys = encodeKeys(keys)
	members, err := o.sInter(ctx, q, keys)
	if err != nil {
		return 0, err
	}

	// Delete destination key
	if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return 0, nil
	}

	// Add members to destination
	return o.sAdd(ctx, q, destination, members)
}

func (o queryOps) sUnion(ctx context.Context, q Querier, keys []string) ([]string, error) {
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return []string{}, nil
	}

	result := make(map[string]bool)
	for _, key := range keys {
		members, err := o.sMembers(ctx, q, key)
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			result[m] = true
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out, nil
}

func (o queryOps) sUnionStore(ctx context.Context, q Querier, destination string, keys []string) (int64, error) {
	destination = encodeKey(destination)
	keys = encodeKeys(keys)
	members, err := o.sUnion(ctx, q, keys)
	if err != nil {
		return 0, err
	}

	// Delete destination key
	if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return 0, nil
	}

	// Add members to destination
	return o.sAdd(ctx, q, destination, members)
}

func (o queryOps) sDiff(ctx context.Context, q Querier, keys []string) ([]string, error) {
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Get members of first set
	first, err := o.sMembers(ctx, q, keys[0])
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return []string{}, nil
	}

	result := make(map[string]bool)
	for _, m := range first {
		result[m] = true
	}

	// Remove members that exist in any other set
	for i := 1; i < len(keys); i++ {
		members, err := o.sMembers(ctx, q, keys[i])
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			delete(result, m)
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out, nil
}

func (o queryOps) sDiffStore(ctx context.Context, q Querier, destination string, keys []string) (int64, error) {
	destination = encodeKey(destination)
	keys = encodeKeys(keys)
	members, err := o.sDiff(ctx, q, keys)
	if err != nil {
		return 0, err
	}

	// Delete destination key
	if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return 0, nil
	}

	// Add members to destination
	return o.sAdd(ctx, q, destination, members)
}

// randInt returns a uniform int in [0, n). Used by the sample-with-replacement
// paths of HRandField/SRandMember/ZRandMember. Crypto-strength randomness is
// not required — these are non-security samples.
func randInt(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.Intn(n)
}

// hRandField samples random fields (and optionally values) from a hash.
func (o queryOps) hRandField(ctx context.Context, q Querier, key string, count int64, withValues bool) ([]string, error) {
	if count == 0 {
		return nil, nil
	}
	key = encodeKey(key)

	if count > 0 {
		// Distinct sample via ORDER BY random() LIMIT count. Postgres caps the
		// result at the row count automatically — matches Redis (no padding).
		var sql string
		if withValues {
			sql = `SELECT field, value FROM kv_hashes
				   WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
				   ORDER BY random() LIMIT $2`
		} else {
			sql = `SELECT field FROM kv_hashes
				   WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
				   ORDER BY random() LIMIT $2`
		}
		rows, err := q.Query(ctx, sql, key, count)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result []string
		for rows.Next() {
			var f string
			if withValues {
				var v []byte
				if err := rows.Scan(&f, &v); err != nil {
					return nil, err
				}
				result = append(result, decodeField(f), string(v))
			} else {
				if err := rows.Scan(&f); err != nil {
					return nil, err
				}
				result = append(result, decodeField(f))
			}
		}
		return result, rows.Err()
	}

	// count < 0: sample with replacement. Fetch all then bootstrap.
	rows, err := q.Query(ctx,
		`SELECT field, value FROM kv_hashes
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type fv struct{ field, value string }
	var pool []fv
	for rows.Next() {
		var f string
		var v []byte
		if err := rows.Scan(&f, &v); err != nil {
			return nil, err
		}
		pool = append(pool, fv{decodeField(f), string(v)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pool) == 0 {
		return nil, nil
	}
	n := -count
	out := make([]string, 0, n)
	if withValues {
		out = make([]string, 0, n*2)
	}
	for i := int64(0); i < n; i++ {
		p := pool[randInt(len(pool))]
		out = append(out, p.field)
		if withValues {
			out = append(out, p.value)
		}
	}
	return out, nil
}

func (o queryOps) sRandMember(ctx context.Context, q Querier, key string, count int64) ([]string, error) {
	if count == 0 {
		return nil, nil
	}
	key = encodeKey(key)
	if count > 0 {
		rows, err := q.Query(ctx,
			`SELECT member FROM kv_sets
			 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY random() LIMIT $2`,
			key, count,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var members []string
		for rows.Next() {
			var m []byte
			if err := rows.Scan(&m); err != nil {
				return nil, err
			}
			members = append(members, string(m))
		}
		return members, rows.Err()
	}
	rows, err := q.Query(ctx,
		`SELECT member FROM kv_sets
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pool []string
	for rows.Next() {
		var m []byte
		if err := rows.Scan(&m); err != nil {
			return nil, err
		}
		pool = append(pool, string(m))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pool) == 0 {
		return nil, nil
	}
	n := -count
	out := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		out = append(out, pool[randInt(len(pool))])
	}
	return out, nil
}

func (o queryOps) zRandMember(ctx context.Context, q Querier, key string, count int64) ([]ZMember, error) {
	if count == 0 {
		return nil, nil
	}
	key = encodeKey(key)
	if count > 0 {
		rows, err := q.Query(ctx,
			`SELECT member, score FROM kv_zsets
			 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY random() LIMIT $2`,
			key, count,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var members []ZMember
		for rows.Next() {
			var m []byte
			var s float64
			if err := rows.Scan(&m, &s); err != nil {
				return nil, err
			}
			members = append(members, ZMember{Member: string(m), Score: s})
		}
		return members, rows.Err()
	}
	rows, err := q.Query(ctx,
		`SELECT member, score FROM kv_zsets
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pool []ZMember
	for rows.Next() {
		var m []byte
		var s float64
		if err := rows.Scan(&m, &s); err != nil {
			return nil, err
		}
		pool = append(pool, ZMember{Member: string(m), Score: s})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pool) == 0 {
		return nil, nil
	}
	n := -count
	out := make([]ZMember, 0, n)
	for i := int64(0); i < n; i++ {
		out = append(out, pool[randInt(len(pool))])
	}
	return out, nil
}

// sInterCard returns |INTER(keys)|, optionally capped at limit. We reuse the
// Go-side intersection from sInter and apply the cap on the result — for the
// command's typical use (gating a "is the overlap >= N" check) the size of
// the intersection itself is the limiting factor, not the per-set fetch cost.
func (o queryOps) sInterCard(ctx context.Context, q Querier, keys []string, limit int64) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	members, err := o.sInter(ctx, q, keys)
	if err != nil {
		return 0, err
	}
	count := int64(len(members))
	if limit > 0 && count > limit {
		return limit, nil
	}
	return count, nil
}

// ============== Sorted Set Extensions ==============

func (o queryOps) zPopMax(ctx context.Context, q Querier, key string, count int64) ([]ZMember, error) {
	key = encodeKey(key)
	// Get the highest-scored members
	rows, err := q.Query(ctx,
		`SELECT member, score FROM kv_zsets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY score DESC, member DESC
		 LIMIT $2`,
		key, count,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{Member: string(member), Score: score})
	}

	// Delete the popped members
	if len(members) > 0 {
		memberBytes := make([][]byte, len(members))
		for i, m := range members {
			memberBytes[i] = []byte(m.Member)
		}
		_, err = q.Exec(ctx,
			"DELETE FROM kv_zsets WHERE key = $1 AND member = ANY($2)",
			key, memberBytes,
		)
		if err != nil {
			return nil, err
		}
	}

	return members, nil
}

func (o queryOps) zRank(ctx context.Context, q Querier, key, member string) (int64, bool, error) {
	key = encodeKey(key)
	var rank int64
	err := q.QueryRow(ctx,
		`SELECT rank FROM (
			SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank
			FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		) sub WHERE member = $2`,
		key, []byte(member),
	).Scan(&rank)

	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return rank, true, nil
}

func (o queryOps) zRevRank(ctx context.Context, q Querier, key, member string) (int64, bool, error) {
	key = encodeKey(key)
	var rank int64
	err := q.QueryRow(ctx,
		`SELECT rank FROM (
			SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank
			FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		) sub WHERE member = $2`,
		key, []byte(member),
	).Scan(&rank)

	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return rank, true, nil
}

func (o queryOps) zCount(ctx context.Context, q Querier, key string, min, max float64) (int64, error) {
	key = encodeKey(key)
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_zsets 
		 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, min, max,
	).Scan(&count)
	return count, err
}

func (o queryOps) zScan(ctx context.Context, q Querier, key string, cursor int64, pattern string, count int64) (int64, []ZMember, error) {
	key = encodeKey(key)
	// Get all members
	rows, err := q.Query(ctx,
		`SELECT member, score FROM kv_zsets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY score ASC, member ASC`,
		key,
	)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	var allMembers []ZMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return 0, nil, err
		}
		// Apply pattern matching
		if pattern != "" && pattern != "*" {
			matched, _ := matchGlob(pattern, string(member))
			if !matched {
				continue
			}
		}
		allMembers = append(allMembers, ZMember{Member: string(member), Score: score})
	}

	// Simulate cursor-based pagination
	start := int(cursor)
	if start >= len(allMembers) {
		return 0, []ZMember{}, nil
	}

	end := start + int(count)
	if end > len(allMembers) {
		end = len(allMembers)
	}

	result := allMembers[start:end]

	var nextCursor int64
	if end >= len(allMembers) {
		nextCursor = 0
	} else {
		nextCursor = int64(end)
	}

	return nextCursor, result, nil
}

func matchGlob(pattern, s string) (bool, error) {
	pi, si := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false, nil
		}
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern), nil
}

func (o queryOps) zUnionStore(ctx context.Context, q Querier, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	destination = encodeKey(destination)
	keys = encodeKeys(keys)
	if len(weights) == 0 {
		weights = make([]float64, len(keys))
		for i := range weights {
			weights[i] = 1.0
		}
	}

	// Collect all members with aggregated scores
	memberScores := make(map[string][]float64)
	for i, key := range keys {
		weight := weights[i]
		members, err := o.zRange(ctx, q, key, 0, -1, true)
		if err != nil {
			return 0, err
		}
		for _, m := range members {
			memberScores[m.Member] = append(memberScores[m.Member], m.Score*weight)
		}
	}

	// Delete destination
	if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
		return 0, err
	}

	if len(memberScores) == 0 {
		return 0, nil
	}

	// Calculate final scores and add to destination
	var members []ZMember
	for member, scores := range memberScores {
		var finalScore float64
		switch strings.ToUpper(aggregate) {
		case "MIN":
			finalScore = scores[0]
			for _, s := range scores[1:] {
				if s < finalScore {
					finalScore = s
				}
			}
		case "MAX":
			finalScore = scores[0]
			for _, s := range scores[1:] {
				if s > finalScore {
					finalScore = s
				}
			}
		default: // SUM
			for _, s := range scores {
				finalScore += s
			}
		}
		members = append(members, ZMember{Member: member, Score: finalScore})
	}

	return o.zAdd(ctx, q, destination, members, ZAddOptions{})
}

func (o queryOps) zInterStore(ctx context.Context, q Querier, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	destination = encodeKey(destination)
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return 0, nil
	}

	if len(weights) == 0 {
		weights = make([]float64, len(keys))
		for i := range weights {
			weights[i] = 1.0
		}
	}

	// Get members from first set
	firstMembers, err := o.zRange(ctx, q, keys[0], 0, -1, true)
	if err != nil {
		return 0, err
	}

	// Build map of member -> scores from all sets
	memberScores := make(map[string][]float64)
	for _, m := range firstMembers {
		memberScores[m.Member] = []float64{m.Score * weights[0]}
	}

	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		members, err := o.zRange(ctx, q, keys[i], 0, -1, true)
		if err != nil {
			return 0, err
		}
		setMembers := make(map[string]float64)
		for _, m := range members {
			setMembers[m.Member] = m.Score
		}

		// Keep only members that exist in all sets
		for member := range memberScores {
			if score, ok := setMembers[member]; ok {
				memberScores[member] = append(memberScores[member], score*weights[i])
			} else {
				delete(memberScores, member)
			}
		}
	}

	// Delete destination
	if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
		return 0, err
	}

	if len(memberScores) == 0 {
		return 0, nil
	}

	// Calculate final scores and add to destination
	var members []ZMember
	for member, scores := range memberScores {
		var finalScore float64
		switch strings.ToUpper(aggregate) {
		case "MIN":
			finalScore = scores[0]
			for _, s := range scores[1:] {
				if s < finalScore {
					finalScore = s
				}
			}
		case "MAX":
			finalScore = scores[0]
			for _, s := range scores[1:] {
				if s > finalScore {
					finalScore = s
				}
			}
		default: // SUM
			for _, s := range scores {
				finalScore += s
			}
		}
		members = append(members, ZMember{Member: member, Score: finalScore})
	}

	return o.zAdd(ctx, q, destination, members, ZAddOptions{})
}

// ============== Key Extensions ==============

func (o queryOps) expireAt(ctx context.Context, q Querier, key string, timestamp time.Time, opts ExpireOptions) (bool, error) {
	key = encodeKey(key)
	if err := o.lockKey(ctx, q, key); err != nil {
		return false, err
	}
	result, err := q.Exec(ctx,
		fmt.Sprintf("UPDATE kv_meta SET expires_at = $2 WHERE %s", expireWhereClause(opts)),
		key, timestamp,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	// Update expires_at in the data table
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return false, err
	}

	table := dataTableForType(keyType)
	if table == "" {
		return true, nil
	}

	_, err = q.Exec(ctx, fmt.Sprintf("UPDATE %s SET expires_at = $2 WHERE key = $1", table), key, timestamp)
	return err == nil, err
}

func (o queryOps) copyKey(ctx context.Context, q Querier, source, destination string, replace bool) (bool, error) {
	source, destination = encodeKey(source), encodeKey(destination)
	if err := o.lockKeys(ctx, q, []string{source, destination}); err != nil {
		return false, err
	}
	// Get source key type
	keyType, err := o.getKeyType(ctx, q, source)
	if err != nil {
		return false, err
	}
	if keyType == TypeNone {
		return false, nil // Source doesn't exist
	}

	// Check if destination exists
	destType, err := o.getKeyType(ctx, q, destination)
	if err != nil {
		return false, err
	}
	if destType != TypeNone && !replace {
		return false, nil // Destination exists and replace not set
	}

	// Delete destination if it exists
	if destType != TypeNone {
		if err := o.deleteKeyFromAllTables(ctx, q, destination); err != nil {
			return false, err
		}
	}

	// Copy based on type
	switch keyType {
	case TypeString:
		var value []byte
		var expiresAt *time.Time
		err := q.QueryRow(ctx,
			"SELECT value, expires_at FROM kv_strings WHERE key = $1",
			source,
		).Scan(&value, &expiresAt)
		if err != nil {
			return false, err
		}
		// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
		if err := o.setMeta(ctx, q, destination, TypeString, expiresAt); err != nil {
			return false, err
		}
		_, err = q.Exec(ctx,
			"INSERT INTO kv_strings (key, value, expires_at) VALUES ($1, $2, $3)",
			destination, value, expiresAt,
		)
		if err != nil {
			return false, err
		}

	case TypeHash:
		// Set meta before data table for consistent lock ordering (kv_meta before kv_hashes).
		if err := o.setMeta(ctx, q, destination, TypeHash, nil); err != nil {
			return false, err
		}
		rows, err := q.Query(ctx,
			"SELECT field, value, expires_at FROM kv_hashes WHERE key = $1",
			source,
		)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var field string
			var value []byte
			var expiresAt *time.Time
			if err := rows.Scan(&field, &value, &expiresAt); err != nil {
				return false, err
			}
			_, err = q.Exec(ctx,
				"INSERT INTO kv_hashes (key, field, value, expires_at) VALUES ($1, $2, $3, $4)",
				destination, field, value, expiresAt,
			)
			if err != nil {
				return false, err
			}
		}

	case TypeList:
		// Set meta before data table for consistent lock ordering (kv_meta before kv_lists).
		if err := o.setMeta(ctx, q, destination, TypeList, nil); err != nil {
			return false, err
		}
		rows, err := q.Query(ctx,
			"SELECT idx, value, expires_at FROM kv_lists WHERE key = $1",
			source,
		)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var idx int64
			var value []byte
			var expiresAt *time.Time
			if err := rows.Scan(&idx, &value, &expiresAt); err != nil {
				return false, err
			}
			_, err = q.Exec(ctx,
				"INSERT INTO kv_lists (key, idx, value, expires_at) VALUES ($1, $2, $3, $4)",
				destination, idx, value, expiresAt,
			)
			if err != nil {
				return false, err
			}
		}

	case TypeSet:
		// Set meta before data table for consistent lock ordering (kv_meta before kv_sets).
		if err := o.setMeta(ctx, q, destination, TypeSet, nil); err != nil {
			return false, err
		}
		rows, err := q.Query(ctx,
			"SELECT member, expires_at FROM kv_sets WHERE key = $1",
			source,
		)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var member []byte
			var expiresAt *time.Time
			if err := rows.Scan(&member, &expiresAt); err != nil {
				return false, err
			}
			_, err = q.Exec(ctx,
				"INSERT INTO kv_sets (key, member, expires_at) VALUES ($1, $2, $3)",
				destination, member, expiresAt,
			)
			if err != nil {
				return false, err
			}
		}

	case TypeZSet:
		// Set meta before data table for consistent lock ordering (kv_meta before kv_zsets).
		if err := o.setMeta(ctx, q, destination, TypeZSet, nil); err != nil {
			return false, err
		}
		rows, err := q.Query(ctx,
			"SELECT member, score, expires_at FROM kv_zsets WHERE key = $1",
			source,
		)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var member []byte
			var score float64
			var expiresAt *time.Time
			if err := rows.Scan(&member, &score, &expiresAt); err != nil {
				return false, err
			}
			_, err = q.Exec(ctx,
				"INSERT INTO kv_zsets (key, member, score, expires_at) VALUES ($1, $2, $3, $4)",
				destination, member, score, expiresAt,
			)
			if err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

// ============== Bitmap Commands ==============

func (o queryOps) setBit(ctx context.Context, q Querier, key string, offset int64, value int) (int64, error) {
	key = encodeKey(key)
	// Get existing value or create empty
	var data []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&data)
	if err == pgx.ErrNoRows {
		data = []byte{}
	} else if err != nil {
		return 0, err
	}

	// Extend buffer if needed
	byteOffset := offset / 8
	if int64(len(data)) <= byteOffset {
		newData := make([]byte, byteOffset+1)
		copy(newData, data)
		data = newData
	}

	// Get old bit value
	bitOffset := 7 - (offset % 8)
	oldBit := int64((data[byteOffset] >> bitOffset) & 1)

	// Set new bit value
	if value == 1 {
		data[byteOffset] |= (1 << bitOffset)
	} else {
		data[byteOffset] &^= (1 << bitOffset)
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
		return 0, err
	}

	// Save back
	_, err = q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, data,
	)
	if err != nil {
		return 0, err
	}

	return oldBit, nil
}

func (o queryOps) getBit(ctx context.Context, q Querier, key string, offset int64) (int64, error) {
	key = encodeKey(key)
	var data []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&data)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	byteOffset := offset / 8
	if int64(len(data)) <= byteOffset {
		return 0, nil
	}

	bitOffset := 7 - (offset % 8)
	return int64((data[byteOffset] >> bitOffset) & 1), nil
}

func (o queryOps) bitCount(ctx context.Context, q Querier, key string, start, end int64, useBit bool) (int64, error) {
	key = encodeKey(key)
	var data []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&data)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return 0, nil
	}

	length := int64(len(data))

	if useBit {
		// Bit mode
		totalBits := length * 8
		if start < 0 {
			start = totalBits + start
		}
		if end < 0 {
			end = totalBits + end
		}
		if start < 0 {
			start = 0
		}
		if end >= totalBits {
			end = totalBits - 1
		}
		if start > end {
			return 0, nil
		}

		var count int64
		for i := start; i <= end; i++ {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			if data[byteIdx]&(1<<bitIdx) != 0 {
				count++
			}
		}
		return count, nil
	}

	// Byte mode
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return 0, nil
	}

	var count int64
	for i := start; i <= end; i++ {
		// Count bits in this byte (popcount)
		b := data[i]
		for b != 0 {
			count += int64(b & 1)
			b >>= 1
		}
	}
	return count, nil
}

func (o queryOps) bitOp(ctx context.Context, q Querier, operation, destKey string, keys []string) (int64, error) {
	destKey = encodeKey(destKey)
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return 0, nil
	}

	// Get all values
	values := make([][]byte, len(keys))
	maxLen := 0
	for i, key := range keys {
		var data []byte
		err := q.QueryRow(ctx,
			"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
			key,
		).Scan(&data)
		if err == pgx.ErrNoRows {
			values[i] = []byte{}
		} else if err != nil {
			return 0, err
		} else {
			values[i] = data
		}
		if len(values[i]) > maxLen {
			maxLen = len(values[i])
		}
	}

	// Pad all values to maxLen
	for i := range values {
		if len(values[i]) < maxLen {
			newVal := make([]byte, maxLen)
			copy(newVal, values[i])
			values[i] = newVal
		}
	}

	result := make([]byte, maxLen)
	op := strings.ToUpper(operation)

	switch op {
	case "AND":
		if len(values) > 0 {
			copy(result, values[0])
			for i := 1; i < len(values); i++ {
				for j := 0; j < maxLen; j++ {
					result[j] &= values[i][j]
				}
			}
		}
	case "OR":
		for i := 0; i < len(values); i++ {
			for j := 0; j < maxLen; j++ {
				result[j] |= values[i][j]
			}
		}
	case "XOR":
		for i := 0; i < len(values); i++ {
			for j := 0; j < maxLen; j++ {
				result[j] ^= values[i][j]
			}
		}
	case "NOT":
		if len(values) > 0 {
			for j := 0; j < len(values[0]); j++ {
				result[j] = ^values[0][j]
			}
		}
	default:
		return 0, fmt.Errorf("ERR BITOP: unsupported operation '%s'", operation)
	}

	// Delete destination and save result
	if err := o.deleteKeyFromAllTables(ctx, q, destKey); err != nil {
		return 0, err
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
	if err := o.setMeta(ctx, q, destKey, TypeString, nil); err != nil {
		return 0, err
	}

	_, err := q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2`,
		destKey, result,
	)
	if err != nil {
		return 0, err
	}

	return int64(maxLen), nil
}

func (o queryOps) bitPos(ctx context.Context, q Querier, key string, bit int, start, end int64, useBit bool) (int64, error) {
	key = encodeKey(key)
	var data []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&data)
	if err == pgx.ErrNoRows {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}

	length := int64(len(data))

	if useBit {
		// Bit mode
		totalBits := length * 8
		if start < 0 {
			start = totalBits + start
		}
		if end < 0 {
			end = totalBits + end
		}
		if start < 0 {
			start = 0
		}
		if end >= totalBits {
			end = totalBits - 1
		}

		for i := start; i <= end; i++ {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			bitVal := int((data[byteIdx] >> bitIdx) & 1)
			if bitVal == bit {
				return i, nil
			}
		}
		return -1, nil
	}

	// Byte mode - search within byte range
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}

	for i := start; i <= end; i++ {
		for j := 7; j >= 0; j-- {
			bitVal := int((data[i] >> j) & 1)
			if bitVal == bit {
				return i*8 + (7 - int64(j)), nil
			}
		}
	}

	// If looking for 0 and not found in range, return first bit after range
	if bit == 0 && end < length-1 {
		return (end + 1) * 8, nil
	}

	return -1, nil
}

// ============== HyperLogLog Commands ==============

func (o queryOps) pfAdd(ctx context.Context, q Querier, key string, elements []string) (int64, error) {
	key = encodeKey(key)
	// Check key type if exists
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != "hyperloglog" {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Get existing HLL or create new one
	var hll *HyperLogLog
	var registers []byte
	err = q.QueryRow(ctx,
		"SELECT registers FROM kv_hyperloglog WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&registers)
	if err == pgx.ErrNoRows {
		hll = NewHyperLogLog()
	} else if err != nil {
		return 0, err
	} else {
		hll = HyperLogLogFromBytes(registers)
	}

	// Add elements and track if anything changed
	changed := false
	for _, elem := range elements {
		if hll.Add(elem) {
			changed = true
		}
	}

	// Update metadata before data table for consistent lock ordering (kv_meta before kv_hyperloglog).
	err = o.setMeta(ctx, q, key, "hyperloglog", nil)
	if err != nil {
		return 0, err
	}

	// Save updated HLL
	_, err = q.Exec(ctx,
		`INSERT INTO kv_hyperloglog (key, registers) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET registers = $2`,
		key, hll.ToBytes(),
	)
	if err != nil {
		return 0, err
	}

	if changed {
		return 1, nil
	}
	return 0, nil
}

func (o queryOps) pfCount(ctx context.Context, q Querier, keys []string) (int64, error) {
	keys = encodeKeys(keys)
	if len(keys) == 0 {
		return 0, nil
	}

	if len(keys) == 1 {
		// Single key - just count
		var registers []byte
		err := q.QueryRow(ctx,
			"SELECT registers FROM kv_hyperloglog WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
			keys[0],
		).Scan(&registers)
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		hll := HyperLogLogFromBytes(registers)
		return hll.Count(), nil
	}

	// Multiple keys - merge then count
	merged := NewHyperLogLog()
	for _, key := range keys {
		var registers []byte
		err := q.QueryRow(ctx,
			"SELECT registers FROM kv_hyperloglog WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
			key,
		).Scan(&registers)
		if err == pgx.ErrNoRows {
			continue // Skip non-existent keys
		}
		if err != nil {
			return 0, err
		}
		hll := HyperLogLogFromBytes(registers)
		merged.Merge(hll)
	}

	return merged.Count(), nil
}

func (o queryOps) pfMerge(ctx context.Context, q Querier, destKey string, sourceKeys []string) error {
	destKey = encodeKey(destKey)
	sourceKeys = encodeKeys(sourceKeys)
	// Check dest key type if exists
	keyType, err := o.getKeyType(ctx, q, destKey)
	if err != nil {
		return err
	}
	if keyType != TypeNone && keyType != "hyperloglog" {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Start with dest key's existing HLL (if any)
	merged := NewHyperLogLog()
	var registers []byte
	err = q.QueryRow(ctx,
		"SELECT registers FROM kv_hyperloglog WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		destKey,
	).Scan(&registers)
	if err == nil {
		merged = HyperLogLogFromBytes(registers)
	} else if err != pgx.ErrNoRows {
		return err
	}

	// Merge all source keys
	for _, key := range sourceKeys {
		err := q.QueryRow(ctx,
			"SELECT registers FROM kv_hyperloglog WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
			key,
		).Scan(&registers)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}
		hll := HyperLogLogFromBytes(registers)
		merged.Merge(hll)
	}

	// Update metadata before data table for consistent lock ordering (kv_meta before kv_hyperloglog).
	if err := o.setMeta(ctx, q, destKey, "hyperloglog", nil); err != nil {
		return err
	}

	// Save merged HLL to dest
	_, err = q.Exec(ctx,
		`INSERT INTO kv_hyperloglog (key, registers) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET registers = $2`,
		destKey, merged.ToBytes(),
	)
	return err
}

// ============== Server Commands ==============

func (o queryOps) dbSize(ctx context.Context, q Querier) (int64, error) {
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_meta WHERE expires_at IS NULL OR expires_at > NOW()",
	).Scan(&count)
	return count, err
}
