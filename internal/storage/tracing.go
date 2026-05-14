// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQL trace levels:
// 0 = off
// 1 = important only (DDL, TRUNCATE, errors)
// 2 = most queries except high-frequency reads (excludes simple SELECTs)
// 3 = everything

// TracingQuerier wraps a Querier and logs SQL queries based on trace level
type TracingQuerier struct {
	q     Querier
	level int
}

// NewTracingQuerier creates a new TracingQuerier that wraps the given Querier
func NewTracingQuerier(q Querier, level int) *TracingQuerier {
	return &TracingQuerier{q: q, level: level}
}

// formatArg formats an argument for logging, detecting binary data
func formatArg(arg any) string {
	switch v := arg.(type) {
	case string:
		if isBinary(v) {
			return "<binary:" + formatSize(len(v)) + ">"
		}
		if len(v) > 100 {
			return "\"" + v[:100] + "...\"" + " (" + formatSize(len(v)) + ")"
		}
		return "\"" + v + "\""
	case []byte:
		if isBinaryBytes(v) {
			return "<binary:" + formatSize(len(v)) + ">"
		}
		s := string(v)
		if len(s) > 100 {
			return "\"" + s[:100] + "...\"" + " (" + formatSize(len(v)) + ")"
		}
		return "\"" + s + "\""
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		return v.Format(time.RFC3339)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatSize(size int) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	}
	if size < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(size)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(size)/(1024*1024))
}

// isBinary checks if a string contains binary data
func isBinary(s string) bool {
	if !utf8.ValidString(s) {
		return true
	}
	for _, r := range s {
		if r == utf8.RuneError {
			return true
		}
		// Check for control characters except common whitespace
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return true
		}
	}
	return false
}

// isBinaryBytes checks if a byte slice contains binary data
func isBinaryBytes(b []byte) bool {
	return isBinary(string(b))
}

// formatArgs formats all arguments for logging
func formatArgs(args []any) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = fmt.Sprintf("$%d=%s", i+1, formatArg(arg))
	}
	return " [" + strings.Join(parts, ", ") + "]"
}

// getSQLLevel returns the minimum trace level required to log a SQL query
// Level 1: DDL, TRUNCATE (important/destructive operations)
// Level 2: INSERT, UPDATE, DELETE, CREATE TABLE (write operations)
// Level 3: SELECT (high-frequency reads)
func getSQLLevel(sql string) int {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Level 1: Important/destructive operations
	if strings.HasPrefix(upper, "TRUNCATE") ||
		strings.HasPrefix(upper, "DROP") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.Contains(upper, "pg_notify") {
		return 1
	}

	// Level 2: Write operations
	if strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "UPSERT") ||
		strings.Contains(upper, "ON CONFLICT") {
		return 2
	}

	// Level 3: Read operations (SELECT, etc.)
	return 3
}

// shouldLog returns true if the SQL should be logged at the current trace level
func (t *TracingQuerier) shouldLog(sql string, isError bool) bool {
	if t.level <= 0 {
		return false
	}
	// Always log errors at any trace level > 0
	if isError {
		return true
	}
	return t.level >= getSQLLevel(sql)
}

// Exec executes a query and logs it based on trace level
func (t *TracingQuerier) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	start := time.Now()
	result, err := t.q.Exec(ctx, sql, arguments...)
	duration := time.Since(start)

	if t.shouldLog(sql, err != nil) {
		if err != nil {
			log.Printf("[SQLTRACE] %s%s -> ERROR: %v (%v)", sql, formatArgs(arguments), err, duration)
		} else {
			log.Printf("[SQLTRACE] %s%s -> %s (%v)", sql, formatArgs(arguments), result.String(), duration)
		}
	}

	return result, err
}

// Query executes a query and logs it based on trace level
func (t *TracingQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	start := time.Now()
	rows, err := t.q.Query(ctx, sql, args...)
	duration := time.Since(start)

	if t.shouldLog(sql, err != nil) {
		if err != nil {
			log.Printf("[SQLTRACE] %s%s -> ERROR: %v (%v)", sql, formatArgs(args), err, duration)
		} else {
			log.Printf("[SQLTRACE] %s%s -> rows (%v)", sql, formatArgs(args), duration)
		}
	}

	return rows, err
}

// QueryRow executes a query that returns a single row and logs it based on trace level
func (t *TracingQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	start := time.Now()
	row := t.q.QueryRow(ctx, sql, args...)
	duration := time.Since(start)

	if t.shouldLog(sql, false) {
		log.Printf("[SQLTRACE] %s%s (%v)", sql, formatArgs(args), duration)
	}

	return row
}

// SendBatch forwards the batch to the underlying Querier. Tracing of individual
// queries inside the batch is left to the caller (the batch wrapper is a thin
// passthrough; per-query timing is not meaningful here).
func (t *TracingQuerier) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if t.level >= 2 {
		log.Printf("[SQLTRACE] SendBatch (%d queries)", b.Len())
	}
	return t.q.SendBatch(ctx, b)
}
