// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
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
}

// binaryFieldPrefix is used to identify base64-encoded binary field names
const binaryFieldPrefix = "\x1Fb64:"

// encodeField encodes a field name for PostgreSQL storage.
// Field names with null bytes or invalid UTF-8 are base64-encoded.
func encodeField(field string) string {
	if strings.ContainsRune(field, 0) || !utf8.ValidString(field) {
		return binaryFieldPrefix + base64.StdEncoding.EncodeToString([]byte(field))
	}
	return field
}

// decodeField decodes a field name from PostgreSQL storage.
func decodeField(field string) string {
	if strings.HasPrefix(field, binaryFieldPrefix) {
		encoded := field[len(binaryFieldPrefix):]
		if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			return string(decoded)
		}
	}
	return field
}

// keyspaceChannelPrefix is the prefix for keyspace notification channels
const keyspaceChannelPrefix = "__keyspace@0__:"

// maxPgChannelLen is the maximum length for PostgreSQL NOTIFY channel names (NAMEDATALEN - 1)
const maxPgChannelLen = 63

// keyspaceChannel creates a safe channel name for keyspace notifications.
// PostgreSQL channel names are limited to 63 bytes. For long keys, we use
// a hash suffix to ensure uniqueness while staying within the limit.
func keyspaceChannel(key string) string {
	channel := keyspaceChannelPrefix + key
	if len(channel) <= maxPgChannelLen {
		return channel
	}

	// For long keys, use: prefix + truncated key + hash suffix
	// Hash suffix is 8 chars (short hex hash) + 1 for separator = 9 chars
	// Available for key: 63 - 14 (prefix) - 9 (hash suffix) = 40 chars
	hash := sha256.Sum256([]byte(key))
	hashSuffix := ":" + hex.EncodeToString(hash[:4]) // 8 hex chars
	maxKeyLen := maxPgChannelLen - len(keyspaceChannelPrefix) - len(hashSuffix)

	return keyspaceChannelPrefix + key[:maxKeyLen] + hashSuffix
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

func (queryOps) setMeta(ctx context.Context, q Querier, key string, keyType KeyType, expiresAt *time.Time) error {
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type, expires_at) VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET key_type = $2, expires_at = $3`,
		key, string(keyType), expiresAt,
	)
	return err
}

// setMetaBatch sets metadata for multiple keys at once
func (queryOps) setMetaBatch(ctx context.Context, q Querier, keys []string, keyType KeyType) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type)
		 SELECT unnest($1::text[]), $2
		 ON CONFLICT (key) DO UPDATE SET key_type = EXCLUDED.key_type`,
		keys, string(keyType),
	)
	return err
}

func (queryOps) deleteKeyFromAllTables(ctx context.Context, q Querier, key string) error {
	// Delete kv_meta first to ensure consistent lock ordering (kv_meta before data tables)
	// and prevent deadlocks with concurrent operations that also lock kv_meta then data tables.
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
func (queryOps) deleteKeysFromAllTables(ctx context.Context, q Querier, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Delete kv_meta first for consistent lock ordering (kv_meta before data tables).
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

func (o queryOps) setNX(ctx context.Context, q Querier, key, value string) (bool, error) {
	// Set meta first with DO NOTHING for consistent lock ordering (kv_meta before kv_strings).
	// If the key already exists, this is a no-op; if the key is new, meta is created first.
	_, err := q.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type) VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING`,
		key, TypeString,
	)
	if err != nil {
		return false, err
	}

	result, err := q.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING`,
		key, []byte(value),
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() > 0 {
		return true, nil
	}
	return false, nil
}

func (o queryOps) mGet(ctx context.Context, q Querier, keys []string) ([]interface{}, error) {
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

func (o queryOps) bitField(ctx context.Context, q Querier, key string, ops []BitFieldOp) ([]int64, error) {
	// Get existing value or create empty
	var value []byte
	err := q.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		value = []byte{}
	} else if err != nil {
		return nil, err
	}

	results := make([]int64, 0, len(ops))
	modified := false

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

		switch op.OpType {
		case "GET":
			result := getBitField(value, bitOffset, bitWidth, signed)
			results = append(results, result)

		case "SET":
			oldValue := getBitField(value, bitOffset, bitWidth, signed)
			results = append(results, oldValue)
			setBitField(value, bitOffset, bitWidth, op.Value)
			modified = true

		case "INCRBY":
			oldValue := getBitField(value, bitOffset, bitWidth, signed)
			newValue := oldValue + op.Value
			// Handle overflow based on encoding
			if signed {
				// Signed overflow wraps around
				max := int64(1) << (bitWidth - 1)
				min := -max
				for newValue >= max {
					newValue -= max * 2
				}
				for newValue < min {
					newValue += max * 2
				}
			} else {
				// Unsigned overflow wraps around
				mask := int64((1 << bitWidth) - 1)
				newValue = newValue & mask
			}
			setBitField(value, bitOffset, bitWidth, newValue)
			results = append(results, newValue)
			modified = true

		case "OVERFLOW":
			// OVERFLOW just sets mode for subsequent ops, we ignore it for now (default WRAP)
			continue
		}
	}

	if modified {
		// Set meta before data table for consistent lock ordering (kv_meta before kv_strings).
		if err := o.setMeta(ctx, q, key, TypeString, nil); err != nil {
			return nil, err
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
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_meta 
		 WHERE key = ANY($1) AND (expires_at IS NULL OR expires_at > NOW())`,
		keys,
	).Scan(&count)
	return count, err
}

func (o queryOps) expire(ctx context.Context, q Querier, key string, ttl time.Duration) (bool, error) {
	expiresAt := time.Now().Add(ttl)

	result, err := q.Exec(ctx,
		`UPDATE kv_meta SET expires_at = $2 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
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
		q.Exec(ctx, fmt.Sprintf("UPDATE %s SET expires_at = NULL WHERE key = $1", table), key)
	}

	return true, nil
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
		keys = append(keys, key)
	}
	return keys, nil
}

func (o queryOps) keyType(ctx context.Context, q Querier, key string) (KeyType, error) {
	return o.getKeyType(ctx, q, key)
}

func (o queryOps) rename(ctx context.Context, q Querier, oldKey, newKey string) error {
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
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return nil, err
	}
	if keyType != TypeNone && keyType != TypeHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	rows, err := q.Query(ctx,
		"SELECT field, value FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
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
	return result, nil
}

func (o queryOps) hMGet(ctx context.Context, q Querier, key string, fields []string) ([]interface{}, error) {
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
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_hashes 
		 WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, encodeField(field),
	).Scan(&count)
	return count > 0, err
}

func (o queryOps) hKeys(ctx context.Context, q Querier, key string) ([]string, error) {
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
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_hashes WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

func (o queryOps) hIncrBy(ctx context.Context, q Querier, key, field string, increment int64) (int64, error) {
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

func (o queryOps) lLen(ctx context.Context, q Querier, key string) (int64, error) {
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var count int64
	err = q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_lists WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

func (o queryOps) lRange(ctx context.Context, q Querier, key string, start, stop int64) ([]string, error) {
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return nil, err
	}
	if keyType != TypeNone && keyType != TypeList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Get total count
	var total int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to get list count: %w", err)
	}

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
	// Get total count
	var total int64
	if err := q.QueryRow(ctx, "SELECT COUNT(*) FROM kv_lists WHERE key = $1", key).Scan(&total); err != nil {
		return "", false, fmt.Errorf("failed to get list count: %w", err)
	}

	// Convert negative index
	if index < 0 {
		index = total + index
	}
	if index < 0 || index >= total {
		return "", false, nil
	}

	var value []byte
	err := q.QueryRow(ctx,
		`SELECT value FROM kv_lists WHERE key = $1 
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

// ============== Set Commands ==============

func (o queryOps) sAdd(ctx context.Context, q Querier, key string, members []string) (int64, error) {
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
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return nil, err
	}
	if keyType != TypeNone && keyType != TypeSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	rows, err := q.Query(ctx,
		"SELECT member FROM kv_sets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	)
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
	return members, nil
}

func (o queryOps) sIsMember(ctx context.Context, q Querier, key, member string) (bool, error) {
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_sets 
		 WHERE key = $1 AND member = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, []byte(member),
	).Scan(&count)
	return count > 0, err
}

func (o queryOps) sCard(ctx context.Context, q Querier, key string) (int64, error) {
	// Check if key exists but is wrong type
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var count int64
	err = q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_sets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

// ============== Sorted Set Commands ==============

func (o queryOps) zAdd(ctx context.Context, q Querier, key string, members []ZMember) (int64, error) {
	keyType, err := o.getKeyType(ctx, q, key)
	if err != nil {
		return 0, err
	}
	if keyType != TypeNone && keyType != TypeZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Set meta before data table for consistent lock ordering (kv_meta before kv_zsets).
	if err := o.setMeta(ctx, q, key, TypeZSet, nil); err != nil {
		return 0, err
	}

	var added int64
	for _, m := range members {
		result, err := q.Exec(ctx,
			`INSERT INTO kv_zsets (key, member, score) VALUES ($1, $2, $3)
			 ON CONFLICT (key, member) DO UPDATE SET score = $3`,
			key, []byte(m.Member), m.Score,
		)
		if err != nil {
			return 0, err
		}
		added += result.RowsAffected()
	}

	return added, nil
}

func (o queryOps) zRange(ctx context.Context, q Querier, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	// Get total count first to handle negative indices
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return []ZMember{}, nil
	}

	// Convert negative indices
	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}

	// Clamp to valid range
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop {
		return []ZMember{}, nil
	}

	limit := stop - start + 1
	rows, err := q.Query(ctx,
		`SELECT member, score FROM kv_zsets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY score ASC, member ASC
		 LIMIT $2 OFFSET $3`,
		key, limit, start,
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
	return members, nil
}

func (o queryOps) zScore(ctx context.Context, q Querier, key, member string) (float64, bool, error) {
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
	var count int64
	err := q.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_zsets WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&count)
	return count, err
}

func (o queryOps) zRangeByScore(ctx context.Context, q Querier, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
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

func (o queryOps) zRemRangeByScore(ctx context.Context, q Querier, key string, min, max float64) (int64, error) {
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

// ============== Sorted Set Extensions ==============

func (o queryOps) zPopMax(ctx context.Context, q Querier, key string, count int64) ([]ZMember, error) {
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
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_zsets 
		 WHERE key = $1 AND score >= $2 AND score <= $3 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, min, max,
	).Scan(&count)
	return count, err
}

func (o queryOps) zScan(ctx context.Context, q Querier, key string, cursor int64, pattern string, count int64) (int64, []ZMember, error) {
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

	return o.zAdd(ctx, q, destination, members)
}

func (o queryOps) zInterStore(ctx context.Context, q Querier, destination string, keys []string, weights []float64, aggregate string) (int64, error) {
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

	return o.zAdd(ctx, q, destination, members)
}

// ============== Key Extensions ==============

func (o queryOps) expireAt(ctx context.Context, q Querier, key string, timestamp time.Time) (bool, error) {
	result, err := q.Exec(ctx,
		`UPDATE kv_meta SET expires_at = $2 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
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
