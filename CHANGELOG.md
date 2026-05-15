# Changelog

All notable changes to this project will be documented in this file.

## [0.27.0] - 2026-05-15

### Added
Workstream B from the 1.0 plan — testing-infrastructure pieces that don't add commands but raise confidence in the ones we ship:

- **`golangci-lint` in CI** — new `.golangci.yml` (v1 schema, build-tagged for `postgres,redis`) enabling `errcheck`, `govet`, `staticcheck`, `unused`, `gosimple`, `gosec`, `revive`, and `gofmt`. Exclusions cover the intentional cases: SHA1 (Redis `SCRIPT LOAD` specifies SHA1 digests), `math/rand` (`RANDOMKEY` / `HRANDFIELD` / `SRANDMEMBER` / `ZRANDMEMBER` are deliberately non-cryptographic), and `gosec G115` integer-conversion false positives. Test files relax `errcheck` / `gosec` / `revive` and silence `staticcheck SA1019` for deprecated-but-still-supported go-redis APIs we exercise on purpose (`ZRangeByLex`, `RPopLPush`, etc.) for wire compatibility. New `lint` job in `.github/workflows/ci.yml` runs `golangci-lint v1.64.8` on every push and PR.
- **`FuzzReader` and `FuzzRoundtrip` for the RESP parser** — new `internal/resp/fuzz_test.go` seeded from the existing test corpus (every shape `TestReader_*` and `TestReader_Errors` cover, including the negative cases). `FuzzReader` reads up to 32 framed values per input so back-to-back parses are also exercised; `FuzzRoundtrip` writes a value and asserts the reader accepts the bytes back. CI runs `FuzzReader` for 30s on every push (`go test -run=^$ -fuzz=FuzzReader -fuzztime=30s ./internal/resp/`).
- **`goleak.VerifyTestMain` in the integration suite** — new `tests/main_test.go` (postgres-tagged, `package integration`) wraps the whole binary so any leaked goroutine after the last test fails the run. Ignores cover known-benign background loops in `pgxpool` (`backgroundHealthCheck`, `triggerHealthCheck`) and `go-redis` (`internal/pool.(*ConnPool).reaper`) that stay running for the lifetime of the test binary.
- **Benchmarks for the v0.24+/v0.26 hot-path additions** — seven new entries in `tests/postgres_benchmark_test.go`: `BenchmarkPgHGetAll` and `BenchmarkPgHMGet` for the batched type-check path on hashes, `BenchmarkPgBLPOPMultiKey` and `BenchmarkPgLMPOP` for the single-RTT multi-key list ops, and `BenchmarkPgZRange` / `BenchmarkPgZRangeByScore` / `BenchmarkPgZRangeStore` for the CTE-based ZRANGE family.

### Fixed
Bugs the new tooling surfaced:

- **RESP DoS via huge declared length** — a single command like `*9223372036854775807\r\n` made `readArray` call `make([]Value, count)` before any element bytes were read, OOM-ing the process. `internal/resp/resp.go` now caps bulk-string length at `MaxBulkLen` (512 MiB, matching Redis's `proto-max-bulk-len` default) and array length at `MaxArrayLen` (1 MiB elements).
- **`internal/resp` parser panic on negative lengths** — `$-2\r\n` and `*-2\r\n` (and similar) caused a `runtime error: slice bounds out of range [:-2]` panic in `readBulkString` / `readArray` because `make([]T, length+2)` underflowed. The known-issue test (`TestReader_NegativeLengthPanic`) was renamed to `TestReader_NegativeLength` and now asserts a clean error return.
- **Background `cleanupExpiredKeys` goroutine leaked across `Store.Close()`** — `storage.New` started the cleanup loop watching the caller's context, but `Close()` only closed the pool, so callers passing `context.Background()` (every test, plus the production server until `cancel()` runs) left the goroutine spinning. `Store` now holds its own `context.CancelFunc` + `sync.WaitGroup`; `Close()` cancels and waits before pool teardown. Surfaced by `goleak` on the very first integration run.
- **Slowloris on the metrics HTTP server** — `ReadHeaderTimeout` was unset on the `http.Server` in `internal/metrics/metrics.go`, so a malicious client could hold the `/metrics`/`/health`/`/ready` connection open with a partial header forever. Now defaults to 10s.

### Changed
- Removed dead code: `keyspaceChannel` / `keyspaceChannelPrefix` / `maxPgChannelLen` in `internal/storage/querier.go` and `hashToBytes` in `internal/storage/hyperloglog.go` (with their now-unused `crypto/sha256`, `encoding/hex`, `encoding/binary` imports). `unused` flagged them; greps confirmed no callers.
- Tightened the genuine `errcheck` violations the linter found into explicit `_ =` assignments to mark intent: `Store.deleteExpiredKeys` rollback/commit/exec, `Store.execTx` rollback, `Handler.EXEC` rollback on commit failure, `Handler.l/rpushOp` `listNotifier.NotifyPush`, `CachedStore.invalidate` / `invalidateMulti`, the metrics handler `w.Write` calls, and `metricsSrv.Stop` in `cmd/server/main.go`.

### Note
All `internal/...` unit tests pass with `-race`; the full postgres-tagged integration suite (~292 tests) passes with `goleak` active; the new benchmarks all run under `-benchtime=2x`. `golangci-lint run --build-tags=postgres,redis ./...` is clean.

## [0.26.0] - 2026-05-14

### Added
Curated set of Redis 7.x commands that drop in cleanly on top of existing primitives (per the 1.0 plan, Workstream A — subset+quick-wins scope):

- **`EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` NX/XX/GT/LT flags** (Redis 7.0) — `NX` only when no TTL is set, `XX` only when one is, `GT`/`LT` only when the new expiration is greater/less than the current (a key with no TTL is treated as infinity, so `GT` never wins against it and `LT` always does). Mutually exclusive combinations are rejected at parse time.
- **`RANDOMKEY`** — returns a random non-expired key, or nil when the keyspace is empty.
- **`SINTERCARD numkeys key [key ...] [LIMIT n]`** (Redis 7.0) — cardinality of the intersection, with optional cap.
- **`ZREVRANGE` / `ZREVRANGEBYSCORE`** — descending variants mirroring `ZRANGE` / `ZRANGEBYSCORE` with `ORDER BY ... DESC` in the storage layer (the ZRANGE `REV` flag was previously emulated by reversing the result in the handler — these are first-class).
- **`ZRANGEBYLEX` / `ZREVRANGEBYLEX` / `ZLEXCOUNT`** — bytea-lex comparisons via a `LexBound` type (`-`, `+`, `[member`, `(member` parse paths supported).
- **`ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]`** (Redis 6.2) — copies the result of a ZRANGE-style query into dst in a single transaction. INDEX, BYSCORE, and BYLEX modes all use one SQL with a CTE wrapping the source SELECT, an INSERT … RETURNING for the count, and `setMeta` for the destination type.
- **`HRANDFIELD` / `SRANDMEMBER` (with COUNT) / `ZRANDMEMBER`** — distinct sampling via `ORDER BY random() LIMIT n` on the positive-count path; negative count fetches the full collection and bootstraps with replacement in Go.
- **`OBJECT ENCODING`** — returns synthetic encoding names per type (`raw` / `listpack` / `quicklist` / `skiplist`). Cosmetic — we only store one canonical representation per type — but keeps Redis admin tooling happy. `OBJECT FREQ` / `IDLETIME` / `REFCOUNT` return the canonical "policy not selected" error.
- **`RESET`** (Redis 6.2) — discards a pending `MULTI`, drops all pub/sub subscriptions for the connection, reverts to RESP2, clears the client name, and (if auth is required) de-authenticates the connection. Replies `+RESET`.
- **`LMPOP` / `BLMPOP` / `ZMPOP` / `BZMPOP`** (Redis 7.0) — pop up to `count` elements from the first non-empty key (in caller order) in a single SQL round-trip. Generalizes the `popMulti` CTE (`array_position` for precedence + `FOR UPDATE SKIP LOCKED`) to N-at-a-time. The blocking variants share a `blockingMPop` helper that mirrors the existing `blockingPop` loop and respects `BLOCKING_POLL_INTERVAL`. `BLMPOP` benefits from the list LISTEN/NOTIFY path; `BZMPOP` always uses the poll fallback since there's no zset notifier.

### Changed
- **`storage.Operations` interface** gained: `RandomKey`, `SInterCard`, `ZRevRange`, `ZRevRangeByScore`, `ZRangeByLex`, `ZRevRangeByLex`, `ZLexCount`, `ZRangeStore`, `HRandField`, `SRandMember`, `ZRandMember`, `LMPop`, `RMPop`, `ZMPopMin`, `ZMPopMax`. **`Expire` / `ExpireAt` signatures changed** to take an `ExpireOptions` value (NX/XX/GT/LT). Downstream stores (`Store`, `TxStore`, `CachedStore`, `CachingTx`) were updated.
- **`Querier` is unchanged**; the new commands all use the existing `Exec`/`Query`/`QueryRow`/`SendBatch` surface.

### Note
12 integration tests added in `tests/integration_test.go` covering the happy paths and edge cases for every new command (NX/XX/GT/LT semantics, lex bound parsing, negative-count duplicates, empty-keyspace returns, BLMPOP timeout behaviour, etc.). Full `-race` suite passes.

## [0.25.0] - 2026-05-14

### Added
- **Grafana dashboard provisioning via Helm** — the dashboard JSON moved from `grafana/dashboard.json` to `charts/postkeys/dashboards/postkeys.json` (single source of truth, embedded into the chart via `.Files.Get`). A new ConfigMap template renders when `grafana.dashboard.enabled=true` (off by default) and ships labels matching the Grafana sidecar's defaults (`grafana_dashboard: "1"`). Folder, label key/value, and extra labels/annotations are all configurable. Verified the embedded JSON round-trips through Helm templating intact (28 panels, uid preserved) — `{{command}}`/`{{pod}}` legend formats are not consumed by Helm because `.Files.Get` returns the raw file content as a string.
- **Prometheus alerting rules via `PrometheusRule` CRD** — new template gated on `prometheusRule.enabled` (off by default). Five built-in alerts, each individually toggleable with tunable thresholds and `for` durations: `PostkeysPodDown` (scrape stopped 5m), `PostkeysHighErrorRate` (>5% errors 10m), `PostkeysHighP99Latency` (>100ms p99 10m, excludes `BRPOP|BLPOP`), `PostkeysBlockingCallsAccumulating` (>50 concurrent blocking calls 15m — catches LISTEN/NOTIFY breakage where clients hit the poll fallback), and `PostkeysLowCacheHitRate` (off by default, only meaningful with `cache.enabled=true`). `prometheusRule.alertLabels` applies labels to every alert in the group (default `severity: warning`); per-rule `labels:` overrides for individual promotion (e.g. `severity: critical` on `PostkeysPodDown`). `prometheusRule.extraRules` is a free-form list appended to the group for additional alerts without forking the chart.

### Changed
- **Dashboard JSON location** — was `grafana/dashboard.json`, now `charts/postkeys/dashboards/postkeys.json`. The `grafana/` directory has been removed. Standalone consumers (e.g. importing the JSON manually into Grafana without the chart) should update their path; the file content is unchanged.

## [0.24.0] - 2026-05-14

### Added
- **Single-query multi-key BLPOP/BRPOP** — new `LPopMulti`/`RPopMulti` on `storage.Operations`, implemented as one CTE that picks the leftmost (LPOP) or rightmost (RPOP) element from the first non-empty key (ordered by the caller's key sequence via `array_position`) under `FOR UPDATE SKIP LOCKED`. Multi-key blocking-pop wakeups now do one DB round-trip regardless of how many keys are watched, instead of N sequential round-trips. Single-key calls hit the same path with negligible overhead. The BLPOP and BRPOP handler loops collapsed into one shared `blockingPop` helper.
- **`BLOCKING_POLL_INTERVAL` env var (default 100ms)** — configurable fallback poll interval for BLPOP/BRPOP when no LISTEN/NOTIFY notifier is wired up. Exposed as `blocking.pollInterval` in the Helm chart; emitted only when set.

### Changed
- **One-round-trip type check for hot reads** — `Querier` now exposes `SendBatch`, and `hGetAll`, `lLen`, `sMembers`, `sCard` now batch the `kv_meta` WRONGTYPE check and the data query into a single round-trip instead of two sequential queries. `lRange` batches the type check with its count query (2 round-trips instead of 3 — the offset/limit fetch still depends on the count).
- **`lIndex` is now one round-trip on the hot path** — positive indices skip the count query entirely and rely on `OFFSET ... LIMIT 1`; negative indices fold the count and select into a single CTE with a `total + idx >= 0` guard so an over-negative index correctly returns nothing instead of clamping to row 0.
- **`zRange` is now one round-trip** — the count, negative-index normalization, clamping, and SELECT collapsed into one CTE. The `bounds` CTE produces a single row with `start_pos`/`stop_pos`/`total`, joined with `kv_zsets`; the WHERE guard rejects everything when the range is empty (total=0 or start>stop). Was previously two round-trips (count, then SELECT).

### Fixed
- **`net.ErrClosed` no longer logs at info level** — when `Stop()` force-closes connections, the in-flight `reader.Read()` returns `net.ErrClosed` ("use of closed network connection"), which the handler only special-cased for `io.EOF` and timeouts. Tests like `TestRenameZSet` emitted a "Read error" line per closed connection even though they passed. Now treated as a clean exit and logged only when `s.debug` is set.

## [0.23.4] - 2026-05-11

### Fixed
- **`Server.Stop()` hangs when a client leaks a connection** — `Stop` closed the listener and then called `wg.Wait()` on the handler-goroutine WaitGroup, but did not close the already-accepted TCP connections. Any client that held a connection open (or any client-library bug that leaked one) made `Stop` block forever. This surfaced after the dependabot bump to `go-redis/v9 v9.19.0`, which leaks the underlying TCP connection on the failed-AUTH path even when `client.Close()` is called: the integration test `TestAuthWrongPassword` then hung indefinitely on its `defer ts.Close()`. `Stop` now iterates the connection map already maintained for graceful drain and force-closes every entry, which causes the blocked `Read` in each handler goroutine to return so `wg.Wait()` can complete. Graceful shutdown (`CloseListener` + `DrainConnections`) is unchanged and still the production path; this only hardens the "immediately stops" path that `Stop` already promised in its comment.
- **Pub/sub writer data race** — `handleConnection` wrote command responses directly to the per-connection `bufio.Writer` without holding `ClientState.writerMu`, while `ClientState.SendPubSubMessage` (invoked from the pub/sub hub's listener goroutine on every broadcast) wrote pub/sub frames to the same writer while holding that mutex. With one goroutine writing under the lock and the other ignoring it, `bufio.(*Writer).WriteString` and `Flush` could race, producing the `WARNING: DATA RACE` reports that started failing CI once `-race` was added. Worse than the race report itself: nothing prevented a command response and a pub/sub message from interleaving on the wire, so a subscribed client could see corrupted RESP frames. Introduced `ClientState.WriteResponses(values ...resp.Value)` which acquires `writerMu`, writes every value, and flushes as one atomic batch under the lock. `handleConnection` now routes all response writes through it (both the single-response and the multi-response pub/sub paths), and the local `writer` variable is gone because the writer is owned by the client state.

## [0.23.3] - 2026-05-11

### Fixed
- **Nil pointer panic in listener reconnect path** — `reconnect()` in the list notifier, pub/sub hub, and cache invalidator each set `listenerConn = nil` before calling `pgx.Connect`. If the database was still unreachable (e.g. mid-CNPG failover where the connection had just been reset and the new primary was not yet accepting connections), `Connect` returned an error, the function returned `false`, and `listenerConn` stayed nil. The loop slept for backoff and then `continue`d straight back into `listenerConn.WaitForNotification(...)`, dereferencing the nil receiver and crashing the pod with SIGSEGV. The reconnect step now lives at the top of each `listenLoop` and is gated on `listenerConn == nil`; the error path just closes the conn, nils it, and continues, so failed reconnects retry with backoff instead of crashing.

## [0.23.2] - 2026-04-30

### Fixed
- **Crash-loop on PostgreSQL unavailability at startup** — All four PG-dependent startup steps (`storage.New`, cache invalidator `Start`, list notifier `Start`, pub/sub hub `Start`) previously called `log.Fatalf` on connection failure, so a pod that booted while the database was briefly unavailable (e.g. during a CNPG primary switchover, where the `-rw` service has no endpoints for ~15-45s) would crash and be restarted by the kubelet, then fail again, looping until the new primary was reachable. Each call site is now wrapped in a `retryStartup` helper that retries forever with exponential backoff (1s → 15s capped). Signal handling is installed at the top of `main` so a SIGTERM during the retry loop cancels the context and exits cleanly. The post-startup wait now selects on either the signal channel or `ctx.Done()` to handle a narrow race where the startup-watcher goroutine consumes the signal at the moment startup completes. The `metrics.NewServer` `log.Fatalf` is unchanged — it only binds a TCP port and retrying does not help.
- **Accept-loop log spam during graceful shutdown** — `CloseListener` (called as step 2 of the graceful shutdown sequence) closes the TCP listener but does not close the server's `quit` channel, so `Accept` returned with `use of closed network connection`, the `select` in `acceptLoop` fell through to the `default` branch, and the loop spun in a tight loop logging the same error every iteration until the process exited. Between `CloseListener` and the final `cancel()` this could produce over a million log lines per shutdown. The accept loop now detects `net.ErrClosed` directly and exits cleanly — the canonical Go signal that we closed the listener ourselves, avoiding any juggling of `s.quit` (which `Stop` and `CloseListener` treat differently — tests use `Stop`, main uses `CloseListener`).

## [0.23.1] - 2026-04-27

### Fixed
- **List notifier panic on subscriber cleanup race** — `WaitForKeys` closed its receive channel in a defer when a waiter timed out or was cancelled, but the listener goroutine could already be holding a snapshot of that channel taken under read-lock and about to send on it, producing `panic: send on closed channel` and crashing the process. The receive channel is now never closed (it is only ever read by the registering goroutine and is reclaimed by GC once unreferenced), and the listener now copies the subscriber slice under the read-lock to avoid an additional latent data race with cleanup mutating the backing array via append-shift. Added unit tests with `-race` coverage for the dispatch race, happy-path delivery, and timeout cleanup.

## [0.23.0] - 2026-04-12

### Changed
- **Leader election backed by Kubernetes Lease** — replaces the PostgreSQL session-scoped advisory lock. Leadership is now unaffected by CNPG failovers, eliminating the Service-endpoint gap that could briefly drop traffic when the database primary switched over while leader-label routing was in use. The leader maintains a `coordination.k8s.io/v1` Lease named `postkeys-leader` (15s duration, 10s renew grace window, 2s retry period); standbys take over within a few seconds if the leader fails to renew. The Helm chart automatically adds the required RBAC on `coordination.k8s.io/leases` when `leaderElection.enabled=true`. No configuration changes required — existing deployments pick up the new mechanism on upgrade.

## [0.22.1] - 2026-03-30

### Fixed
- **Graceful shutdown during rolling restarts** — Restructured the shutdown sequence to prevent connection errors when pods are restarted. Previously, all client connections were terminated immediately on SIGTERM via context cancellation; now connections are drained with a 10-second read deadline. The shutdown order is: mark not-ready (`/ready` returns 503), close TCP listener, wait for endpoint propagation, release leader lock, drain connections, then clean up. Added a configurable preStop hook (`gracefulShutdown.preStopSleepSeconds`, default 5s) to allow kube-proxy time to remove endpoints before the pod starts refusing connections. Switched the readiness probe from TCP to HTTP `/ready` on the metrics port for faster shutdown detection.

## [0.22.0] - 2026-03-29

### Changed
- **Leader election uses pod labels instead of readiness probes**: The leader now patches its own pod with `postkeys/role=leader` via the Kubernetes API, and the Service selector routes traffic only to the labeled pod. All pods report ready via TCP probe, so Deployments and ArgoCD show healthy status. Replaces the previous approach of returning 503 on `/ready` for standby pods, which caused stuck rollouts and permanent "Progressing" state. Requires `serviceAccount.create=true` (RBAC for pod label patching is created automatically).

## [0.21.2] - 2026-03-29

### Fixed
- **Leader election deadlock during rolling updates**: Helm chart rolling updates would deadlock because the new pod could never acquire the leader lock while the old pod was still running. Added `maxUnavailable: 1` to the deployment strategy when leader election is enabled, and reordered graceful shutdown to release the leader lock before draining connections.

## [0.21.1] - 2026-03-29

### Changed
- **BITFIELD performance optimization**: Skip redundant `kv_meta` upsert for existing keys, reducing SQL round trips from 4 to 2 on the hot path. Read-only BITFIELD calls (GET-only) no longer trigger cache invalidation.

## [0.21.0] - 2026-03-29

### Added
- **Leader election for cache coherency**: When running multiple instances, set `LEADER_ELECTION_ENABLED=true` to elect a single leader via a PostgreSQL session advisory lock. Only the leader returns HTTP 200 on the new `/ready` endpoint; standbys return 503. Kubernetes readiness probes use `/ready` to ensure all traffic is routed to the leader, giving full in-memory cache coherency without distributed invalidation overhead. The standby automatically takes over within ~2 seconds if the leader disconnects. Helm chart: `leaderElection.enabled: true`.
- **`/ready` HTTP endpoint**: New readiness endpoint on the metrics server (`:9090/ready`). Always returns 200 when leader election is disabled; returns 200/503 based on leader status when enabled. The existing `/health` endpoint is unchanged (always 200, for liveness probes).

## [0.20.9] - 2026-03-25

### Fixed
- **`pg_notify` crash with binary Redis keys**: `SELECT pg_notify($1, $2)` failed with `invalid byte sequence for encoding "UTF8"` when Redis keys or pub/sub messages contained non-UTF-8 binary data. All three notification paths (list push, pub/sub broadcast, cache invalidation) now base64-encode their payloads, with backward-compatible decoding for rolling updates.
- **Binary-safe key storage**: Redis keys containing null bytes or invalid UTF-8 are now transparently encoded before storage in PostgreSQL TEXT columns. Normal UTF-8 keys (the vast majority) pass through unchanged with zero overhead. Uses the same `\x1Fb64:` prefix encoding already proven for hash field names.

## [0.20.8] - 2026-03-24

### Fixed
- **Lua scripting sandbox**: EVAL/EVALSHA no longer expose `os`, `io`, `debug`, or `package` libraries. Only safe libraries (base, table, string, math, coroutine) are loaded, matching Redis sandbox behavior. Prevents arbitrary command execution via scripts.

### Changed
- **pprof endpoints gated behind config flag**: `/debug/pprof/*` endpoints on the metrics server are now disabled by default. Set `ENABLE_PPROF=true` to enable them.

## [0.20.7] - 2026-03-24

### Fixed
- **Multi-instance pub/sub desync**: The pub/sub system used per-channel `LISTEN/NOTIFY`, so `PSUBSCRIBE` pattern subscriptions never received cross-instance messages. Switched to a single broadcast channel (`postkeys_pubsub`) that all instances always listen on, enabling correct pattern matching across the cluster. Fixes session state desync in multi-replica deployments (e.g. Authelia).
- **CLUSTER INFO returned `cluster_state:fail`**: Now correctly returns `cluster_state:ok`.

### Added
- **CONFIG command handler**: go-redis sends `CONFIG GET` on init; postkeys now handles it gracefully.
- **SELECT command handler**: Returns OK for db 0, error otherwise.
- **WAIT command handler**: Returns immediately (PostgreSQL handles durability).

## [0.20.6] - 2026-03-09

### Fixed
- **Deadlock elimination via per-key advisory locks**: Added `pg_advisory_xact_lock(hashtext(key))` to all write operations, serializing concurrent transactions on the same key. The previous lock-ordering fix (0.20.5) was insufficient because 3-way deadlock cycles could still form when transactions were at different stages (DELETE vs INSERT on the same tables). Advisory locks are acquired before any row locks, completely preventing deadlock cycles. Locks are transaction-scoped and automatically released on commit/rollback.

## [0.20.5] - 2026-03-09

### Fixed
- **Deadlock prevention via consistent lock ordering**: Concurrent write operations (SET, MSET, INCR, HSET, LPUSH, SADD, ZADD, etc.) could deadlock when multiple transactions acquired locks on `kv_meta` and data tables (`kv_strings`, `kv_hashes`, etc.) in inconsistent orders. All operations now lock `kv_meta` before data tables, eliminating circular wait conditions. This resolves cascading deadlock errors under high concurrency (e.g. GitLab Redis workloads).

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

