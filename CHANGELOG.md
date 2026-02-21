# Changelog

All notable changes to this project will be documented in this file.

## [0.20.3] - 2026-02-21

### Fixed
- **Orphaned `kv_meta` entries for empty data structures**: When all elements were removed from lists/sets/hashes/sorted sets via operations like `LPOP`, `SREM`, `HDEL`, `ZREM`, the `kv_meta` entry was not deleted. These accumulated indefinitely for keys without TTL. The background cleanup now includes a Phase 3 that removes orphaned `kv_meta` entries where the corresponding data tables have no rows.

## [0.20.2] - 2026-02-19

### Added
- **PostgreSQL advisory lock for expired key cleanup**: When running multiple replicas, only one instance performs the background cleanup per cycle using `pg_try_advisory_xact_lock`. This eliminates redundant DELETE queries from all other pods.
- **Integration tests for expiration and cleanup fixes**: Added 8 tests covering EXPIRE/EXPIREAT/PERSIST/RENAME/DEL on sorted sets and HyperLogLog keys, verifying no orphaned data rows remain at the PostgreSQL level.

## [0.20.1] - 2026-02-19

### Fixed
- **Critical: Expired key data not cleaned from `kv_zsets` and `kv_hyperloglog` tables**: The background cleanup goroutine only deleted expired rows from `kv_strings`, `kv_hashes`, `kv_lists`, and `kv_sets`, completely missing sorted sets and HyperLogLog data. This caused unbounded disk growth in PostgreSQL.
- **EXPIRE/EXPIREAT not propagated to sorted set data rows**: When `EXPIRE` was called on a sorted set key, `expires_at` was updated in `kv_meta` but never in `kv_zsets`, so the background cleaner could not match those rows. The meta row was eventually deleted, leaving permanently orphaned data.
- **Orphaned data row cleanup**: `deleteExpiredKeys` now uses `DELETE ... RETURNING key` on `kv_meta` and batch-deletes any remaining data rows for those keys, preventing orphaned rows from accumulating.
- **PERSIST missing `kv_zsets` and `kv_hyperloglog`**: Clearing a TTL via `PERSIST` did not update these tables.
- **RENAME missing sorted set and HyperLogLog types**: Renaming a key of these types silently left data behind under the old key name.
- **DEL/overwrite missing `kv_hyperloglog`**: `deleteKeyFromAllTables` and `deleteKeysFromAllTables` did not include the HyperLogLog table, leaving orphaned rows on key deletion or type change.

## [0.20.0] - 2026-02-15

### Added
- **Database connection pool configuration for improved resilience**: Added configurable connection pool settings to handle PostgreSQL master node switchovers and connection failures more gracefully
  - `PG_MAX_CONNS` (default: 10): Maximum number of connections in the pool
  - `PG_MIN_CONNS` (default: 2): Minimum number of connections in the pool
  - `PG_MAX_CONN_LIFETIME` (default: 30m): Maximum lifetime of a connection before it's closed and recreated
  - `PG_MAX_CONN_IDLE_TIME` (default: 5m): Maximum time a connection can be idle before it's closed
  - `PG_HEALTH_CHECK_PERIOD` (default: 1m): Period between health checks on idle connections
  - These settings ensure connections are regularly refreshed and health-checked, providing better resilience during database failovers

### Changed
- Connection pool now proactively manages connection lifecycle with configurable health checks and connection lifetimes
- Log messages now include pool configuration details on startup

## [0.19.0] - 2026-02-11

### Changed
- **Cache pattern filtering moved to normal cache settings**: `excludePatterns` and `includePatterns` are now part of the standard cache configuration instead of the smart policy
- Helm chart: `cache.excludePatterns` and `cache.includePatterns` replace `cache.smartPolicy.excludePatterns` and `cache.smartPolicy.includePatterns`

### Removed
- **Smart cache policy**: Removed the `cache.smartPolicy` feature including TTL-based filtering, write frequency tracking, and all related configuration (`CACHE_SMART_POLICY`, `CACHE_MIN_TTL`, `CACHE_MAX_WRITE_FREQ`, `CACHE_WRITE_TRACKING_WINDOW`)
- Removed `NewCachedStoreWithPolicy` constructor and `Policy` struct
- Helm chart: Removed `cache.smartPolicy.*` configuration section

## [0.18.1] - 2026-02-04

### Fixed
- **Pub/sub and list notifier timeout detection**: Fixed false reconnection attempts caused by wrapped timeout errors from pgx. The `isTimeoutError` function now properly handles wrapped errors using `errors.Is()` and string matching, preventing unnecessary "will reconnect" log spam during normal operation.

## [0.18.0] - 2026-02-04

### Added
- **Smart cache policy**: Intelligent caching that avoids caching "hot" keys for mixed workloads (caching + messaging/pubsub)
  - **TTL-based filtering** (`CACHE_MIN_TTL`): Keys with short TTL (e.g., < 1 second) are considered transient and skip the cache
  - **Write frequency tracking** (`CACHE_MAX_WRITE_FREQ`): Keys written frequently (> N writes/sec) are detected as "hot" and excluded from cache
  - **Pattern matching** (`CACHE_EXCLUDE_PATTERNS`, `CACHE_INCLUDE_PATTERNS`): Explicit include/exclude patterns for known key prefixes
  - New Prometheus metrics: `postkeys_cache_skips_total{reason}` for monitoring policy decisions
  - Helm chart: `cache.smartPolicy.*` configuration options
  - Ideal for applications using Redis as both a cache AND a message bus

## [0.17.3] - 2026-02-04

### Fixed
- **Critical: PostgreSQL listener reconnection on connection loss**: Fixed high CPU usage (spinning tight loop) when PostgreSQL LISTEN connections are lost unexpectedly (e.g., during CNI/network disruptions). All three listener loops (cache invalidator, pub/sub hub, list notifier) now properly detect connection errors vs timeouts, automatically reconnect with exponential backoff (100ms to 30s), and re-subscribe to all channels. Previously, a lost connection would cause the listener loops to spin at 100% CPU without any backoff or reconnection attempt.

## [0.17.2] - 2026-02-03

### Fixed
- **PostgreSQL deadlock in BRPOP/BLPOP/RPOPLPUSH**: Fixed deadlock errors (SQLSTATE 40P01) when multiple clients concurrently pop from the same list. Now uses `FOR UPDATE SKIP LOCKED` to prevent concurrent transactions from contending for the same row.

## [0.17] - 2026-02-02

### Added
- **Redis 7 benchmark suite**: New `make bench-redis` and `make bench-compare` targets for comparing PostgreSQL vs Redis performance

### Changed
- **Batch write optimizations**: Major performance improvements for bulk operations
  - MSET: ~7x faster (uses UNNEST-based batch insert instead of per-key queries)
  - HSET: Batch insert for multiple fields
  - SADD: Batch insert for multiple members using CTE
  - LPUSH/RPUSH: Batch insert for multiple values
  - New `deleteKeysFromAllTables` for batch key deletion

### Fixed
- **BRPOP/BLPOP multi-key support**: Now correctly waits on all keys, not just the first one
- **Duplicate pg_notify removed**: LPUSH/RPUSH no longer send redundant keyspace notifications (listNotifier handles this)

### Improved
- **Exponential backoff for LISTEN loops**: Pub/sub and list notifier now use exponential backoff (50ms-2s) instead of fixed 100ms polling, reducing CPU usage when idle
- **Test coverage**: Added integration tests for previously untested commands:
  - String commands: INCRBYFLOAT, GETRANGE, SETRANGE, STRLEN, GETEX, GETDEL, BITFIELD
  - Key commands: PEXPIRE, PTTL
  - Connection commands: ECHO
  - Scripting commands: SCRIPT FLUSH

### Fixed
- **GETEX expiration**: Fixed bug where GETEX was not updating TTL correctly (was updating wrong table)

## [0.16] - 2026-02-02

### Changed
- **Cache distributed invalidation is now optional** (off by default)
  - New env var: `CACHE_DISTRIBUTED_INVALIDATION=true` enables NOTIFY-based invalidation
  - Default: pure TTL cache (no NOTIFY overhead, ~15-25% faster writes)
  - Recommended: Enable for multi-pod deployments requiring cache coherency
  - Helm: `cache.distributedInvalidation: true` to enable
  - Default cache TTL changed from 5s to 250ms (appropriate for non-distributed mode)

## [0.15] - 2026-02-02

### Changed
- **Test infrastructure overhaul**: All integration tests now run against real PostgreSQL
  - Removed in-memory mock storage (~2,000 lines of code removed)
  - Tests now validate actual PostgreSQL behavior and SQL queries
  - Single `make test` command starts PostgreSQL and runs all tests
- Simplified Makefile with consolidated test/bench targets

### Fixed
- **ZINCRBY**: Fixed incorrect column name in kv_meta insert
- **LINSERT**: Fixed element ordering for BEFORE/AFTER insertion
- **WRONGTYPE errors**: Added proper type checking to read operations
  - HGETALL now returns WRONGTYPE when key is not a hash
  - LLEN/LRANGE now return WRONGTYPE when key is not a list
  - SMEMBERS/SCARD now return WRONGTYPE when key is not a set

### Removed
- Mock storage implementation (mock.go, mock_transaction.go)
- Separate mock vs PostgreSQL test targets

## [0.14] - 2026-02-02

### Added
- **Bitmap commands**: SETBIT, GETBIT, BITCOUNT (with BYTE/BIT mode), BITOP (AND/OR/XOR/NOT), BITPOS
- **Hash commands**: HINCRBYFLOAT, HSETNX
- **List commands**: LPOS (with RANK/COUNT/MAXLEN options), LSET, LINSERT (BEFORE/AFTER)
- **Set commands**: SMISMEMBER, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE
- **Sorted set commands**: ZPOPMAX, ZRANK, ZREVRANK, ZCOUNT, ZSCAN, ZUNIONSTORE (with WEIGHTS/AGGREGATE), ZINTERSTORE (with WEIGHTS/AGGREGATE)
- **Key commands**: EXPIREAT, PEXPIREAT, COPY (with REPLACE option)

## [0.13] - 2026-02-01

### Added
- **Distributed cache invalidation** via PostgreSQL LISTEN/NOTIFY
  - All cache writes broadcast invalidations to all postkeys instances
  - Enables safe multi-pod deployments with caching enabled
  - Near-instant cache coherency across instances (millisecond latency)
- Cache invalidator listens on `postkeys_cache_invalidate` channel

### Changed
- Default cache TTL increased from 250ms to 5s (safe with distributed invalidation)
- Helm chart cache documentation updated to reflect distributed invalidation support

## [0.11] - 2026-02-01

### Added
- Production profiling support via pprof endpoints on metrics server
  - CPU profile: `/debug/pprof/profile?seconds=30`
  - Heap profile: `/debug/pprof/heap`
  - Goroutine dump: `/debug/pprof/goroutine`
  - Mutex/block profiling available
- **LISTEN/NOTIFY for BRPOP/BLPOP** - Eliminates polling when waiting for list items
  - LPUSH/RPUSH now send PostgreSQL notifications
  - BRPOP/BLPOP wait for notifications instead of polling every 100ms
  - Dramatically reduces CPU and database load for blocking list operations

### Fixed
- **High CPU usage** caused by aggressive 10ms polling in BRPOP/BLPOP
  - Reduced poll interval as fallback, but now uses LISTEN/NOTIFY for near-instant wakeup

## [0.10] - 2026-02-01

### Added
- More complete RESP3 support, including Lua and queue support.
- Configurable trace levels (0-3) for SQL and RESP command logging
- Graceful shutdown with 30-second timeout and ordered component shutdown
- Force-exit on second signal during shutdown

### Fixed
- Transaction rollback errors from ignored QueryRow errors in list/hash operations
- pg_notify "channel name too long" errors for keys exceeding 63 bytes

### Changed
- SQLTRACE and TRACE now accept levels 0-3 instead of boolean values

## [0.9] - 2026-01-31

### Added
- HELLO command support for Redis protocol negotiation (RESP2/RESP3)
- HELLO can run without authentication (like PING, QUIT, COMMAND)
- HELLO AUTH inline authentication support

## [0.8] - 2026-01-31

### Added
- Debug logging support via `DEBUG=1` environment variable
- Enhanced error logging with remote address details when debug enabled
- RESP parser logs full buffer content on unknown type errors in debug mode
- Helm chart `debug` option to enable debug logging

## [0.7] - 2026-01-31

### Added
- Redis password management with secret generation job
- Example configuration with CloudNativePG for full HA setup
- Helm chart installation and configuration details to README

### Changed
- Renamed project from `pg-kv-backend` to `postkeys`
- Updated database key references in configuration files
- Refactored Grafana dashboard configuration and panel settings
- Updated test command to include internal tests

## [0.6] - 2026-01-29

### Changed
- Updated Docker publish workflow to include GitHub release creation
- Streamlined tagging process in CI/CD

### Added
- Grafana dashboard for monitoring
- Unit tests for cache and RESP protocol
- Additional PostgreSQL integration tests

## [0.5] - 2026-01-28

### Added
- CLIENT commands handling with ClientState management
- In-memory cache support with configurable TTL and max size

## [0.4] - 2026-01-27

### Added
- Prometheus metrics support with `/metrics` endpoint
- ServiceMonitor for Prometheus Operator integration
- Configurable metrics server address

### Changed
- Updated PostgreSQL secret handling and configuration options

## [0.3] - 2026-01-26

### Added
- Initial release with Redis-compatible protocol
- PostgreSQL backend for persistent storage
- Helm chart for Kubernetes deployment
- Docker image published to GHCR

