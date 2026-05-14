# postkeys

A Redis 7 API-compatible server that uses PostgreSQL as the backend storage.

## Features

- Redis protocol compatible (RESP2 and RESP3)
- PostgreSQL persistent storage
- Full pub/sub support with RESP3 Push messages
- Lua scripting support (EVAL/EVALSHA/SCRIPT)
- Transaction support (MULTI/EXEC/DISCARD)
- Supports most common Redis commands for strings, hashes, lists, sets, sorted sets, HyperLogLog, pub/sub, and more

### Unsupported Commands

The following Redis command groups and features are **not supported**:

| Category | Unsupported |
|----------|-------------|
| **Streams** | XADD, XREAD, XRANGE, XGROUP, etc. (entire stream API) |
| **Cluster** | Cluster mode (CLUSTER commands return standalone mode) |
| **Replication** | REPLICAOF, SLAVEOF, WAIT, PSYNC |
| **Geospatial** | GEOADD, GEODIST, GEOSEARCH, etc. |
| **Time Series** | RedisTimeSeries module commands |
| **JSON** | RedisJSON module commands |
| **Search** | RediSearch module commands |
| **ACL** | ACL commands (use `REDIS_PASSWORD` for simple auth) |
| **Blocking Streams** | XREADGROUP, XAUTOCLAIM with blocking |
| **Memory Management** | MEMORY, OBJECT FREQ/IDLETIME, DEBUG |
| **Slow Log** | SLOWLOG commands |
| **Modules** | MODULE LOAD and custom modules |

## Protocol Support

postkeys supports both RESP2 and RESP3 protocols:

- **RESP2**: Default protocol for backwards compatibility
- **RESP3**: Modern protocol with native types (Maps, Sets, Booleans, etc.)

Clients can negotiate the protocol version using the `HELLO` command:

```bash
# Upgrade to RESP3
HELLO 3

# RESP3 benefits:
# - HGETALL returns native Map type instead of flat array
# - Pub/sub messages use Push type (out-of-band), allowing commands while subscribed
# - Better type information for clients
```

## Lua Scripting

postkeys supports Lua scripting with `EVAL`, `EVALSHA`, and `SCRIPT` commands, enabling atomic operations and complex logic:

```bash
# Execute a script directly
EVAL "return redis.call('GET', KEYS[1])" 1 mykey

# Load and cache a script
SCRIPT LOAD "return redis.call('INCR', KEYS[1])"
# Returns: "sha1hash..."

# Execute cached script
EVALSHA sha1hash 1 counter

# Check if scripts exist
SCRIPT EXISTS sha1hash1 sha1hash2

# Clear script cache
SCRIPT FLUSH
```

Scripts have access to:
- `KEYS` table - keys passed to the script
- `ARGV` table - additional arguments
- `redis.call(cmd, ...)` - execute Redis command (raises error on failure)
- `redis.pcall(cmd, ...)` - execute Redis command (returns error as table)
- `redis.sha1hex(str)` - compute SHA1 hash

**Note**: Scripts execute atomically. Certain commands are blocked from scripts: `SUBSCRIBE`, `PUBLISH`, `MULTI`, `EXEC`, `WATCH`, nested `EVAL`/`EVALSHA`.

## Requirements

- Go 1.24+
- PostgreSQL 16+

## Installation

```bash
go build -o postkeys ./cmd/server
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_ADDR` | Address to listen on | `:6379` |
| `REDIS_PASSWORD` | Authentication password (optional) | `` |
| `METRICS_ADDR` | Prometheus metrics server address | `:9090` |
| `PG_HOST` | PostgreSQL host | `localhost` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_USER` | PostgreSQL user | `postgres` |
| `PG_PASSWORD` | PostgreSQL password | `postgres` |
| `PG_DATABASE` | PostgreSQL database | `postkeys` |
| `PG_SSLMODE` | PostgreSQL SSL mode | `disable` |
| `PG_MAX_CONNS` | Maximum number of connections in pool | `10` |
| `PG_MIN_CONNS` | Minimum number of connections in pool | `2` |
| `PG_MAX_CONN_LIFETIME` | Maximum lifetime of a connection | `30m` |
| `PG_MAX_CONN_IDLE_TIME` | Maximum idle time before closing connection | `5m` |
| `PG_HEALTH_CHECK_PERIOD` | Period between health checks on idle connections | `1m` |
| `CACHE_ENABLED` | Enable in-memory cache (opt-in) | `false` |
| `CACHE_TTL` | Cache TTL duration | `250ms` |
| `CACHE_MAX_SIZE` | Maximum cached entries | `10000` |
| `CACHE_DISTRIBUTED_INVALIDATION` | Enable distributed cache invalidation via PostgreSQL LISTEN/NOTIFY | `false` |
| `CACHE_EXCLUDE_PATTERNS` | Comma-separated key patterns to never cache (e.g., `pubsub:*,lock:*`) | `` |
| `CACHE_INCLUDE_PATTERNS` | Comma-separated key patterns to always cache (overrides exclusions) | `` |
| `DEBUG` | Enable debug logging (set to `1` to enable) | `` |
| `SQLTRACE` | SQL query tracing level (0-3, see Tracing section) | `0` |
| `TRACE` | RESP command tracing level (0-3, see Tracing section) | `0` |
| `ENABLE_PPROF` | Enable `/debug/pprof/*` endpoints on metrics server | `false` |
| `LEADER_ELECTION_ENABLED` | Enable Kubernetes Lease based leader election. The leader labels its own pod with `postkeys/role=leader` so the Service selector routes traffic exclusively to it, giving full cache coherency without distributed invalidation. Requires pod name/namespace env vars and RBAC on `coordination.k8s.io/leases` (added automatically by the Helm chart). | `false` |
| `BLOCKING_POLL_INTERVAL` | Fallback poll interval for BLPOP/BRPOP when no LISTEN/NOTIFY notifier is wired up. Ignored on the notify path (the production default). Lower = faster wakeup, higher = less DB load. | `100ms` |

### Database Connection Pool

postkeys uses a configurable connection pool to manage PostgreSQL connections efficiently and handle database failovers gracefully:

- **Health checks** (`PG_HEALTH_CHECK_PERIOD`): Idle connections are periodically checked to ensure they're still alive
- **Connection lifetime** (`PG_MAX_CONN_LIFETIME`): Connections are automatically closed and recreated after a maximum lifetime, ensuring fresh connections during database switchovers
- **Idle timeout** (`PG_MAX_CONN_IDLE_TIME`): Idle connections are closed to free resources
- **Pool sizing** (`PG_MIN_CONNS`, `PG_MAX_CONNS`): Controls the minimum and maximum number of connections maintained

These settings work together with the existing reconnection logic in LISTEN/NOTIFY components (pub/sub, cache invalidation, list blocking operations) to provide resilience during PostgreSQL master node switchovers or network disruptions.

### In-Memory Cache

The optional in-memory cache reduces PostgreSQL load for read-heavy workloads by caching `GET` results:

```bash
export CACHE_ENABLED=true
export CACHE_TTL=5s
export CACHE_MAX_SIZE=10000
```

**Features:**
- Cache is **opt-in** and disabled by default
- Only caches string `GET` operations (hashes, lists, sets are not cached)
- **Distributed invalidation** via PostgreSQL LISTEN/NOTIFY provides better cache consistency across pods
- Writes (`SET`, `DEL`, etc.) broadcast invalidations to all instances
- Monitor cache effectiveness with `postkeys_cache_hits_total` and `postkeys_cache_misses_total` metrics

**Multi-pod deployments:**
With distributed cache invalidation, all postkeys instances share cache coherency. When any instance writes a key, all instances invalidate that key from their local caches within milliseconds. This allows using longer cache TTLs (e.g., 5-30 seconds) while maintaining consistency.

### Cache Pattern Filtering

Use include/exclude patterns (glob-style) to control which keys are cached:

```bash
export CACHE_ENABLED=true
export CACHE_EXCLUDE_PATTERNS="pubsub:*,channel:*,lock:*,queue:*"
export CACHE_INCLUDE_PATTERNS="static:*,cache:*"
```

- **Include patterns** take precedence over exclude patterns
- Patterns use simple glob matching with `*` as wildcard
- Monitor excluded keys with `postkeys_cache_skips_total{reason="exclude_pattern"}`

### Tracing

postkeys provides configurable tracing with three levels for both SQL and RESP commands:

| Level | Description |
|-------|-------------|
| 0 | Off (default) |
| 1 | Important only - administrative commands, DDL, errors |
| 2 | Most operations - write operations, moderate frequency commands |
| 3 | Everything - including high-frequency reads (GET, SET, etc.) |

**SQL Tracing** (`SQLTRACE=1-3`) logs PostgreSQL queries based on level:
- Level 1: DDL (TRUNCATE, DROP, ALTER), pg_notify, errors
- Level 2: All writes (INSERT, UPDATE, DELETE, CREATE)
- Level 3: Everything including SELECTs

```
[SQLTRACE] SELECT value FROM kv_strings WHERE key = $1 [$1="mykey"] -> rows (1.234ms)
```

**RESP Command Tracing** (`TRACE=1-3`) logs Redis commands based on level:
- Level 1: AUTH, FLUSHDB, FLUSHALL, CONFIG, CLUSTER, DEBUG
- Level 2: PUBLISH, SUBSCRIBE, DEL, EXPIRE, RENAME, etc.
- Level 3: GET, SET, HGET, HSET, LPUSH, RPUSH, and all other commands

```
[TRACE] 10.0.0.1:54321 <- ["SET", "mykey", "myvalue"]
[TRACE] 10.0.0.1:54321 -> +OK
```

Binary data is automatically detected and replaced with `<binary:SIZE>` to keep logs readable.
Errors are always logged at any trace level > 0.

> **Warning:** Higher trace levels generate significant log volume. Use level 3 only for debugging, not in production.

### Graceful Shutdown

postkeys handles shutdown signals gracefully:

- **SIGINT** (Ctrl+C) and **SIGTERM**: Initiate graceful shutdown
- **SIGHUP**: Also triggers graceful shutdown (can be used for restarts)
- A second signal during shutdown forces immediate exit

During graceful shutdown:
1. Mark as not ready (`/ready` returns 503)
2. Close TCP listener (stops accepting new connections)
3. Wait for endpoint removal to propagate (2 seconds)
4. Release leader lock if leader election is enabled (standby takes over)
5. Drain existing connections with a 10-second read deadline
6. Stop background goroutines and close database connections
7. Exit cleanly

In Kubernetes, a preStop hook (`sleep 5` by default) runs before SIGTERM, giving kube-proxy time to remove the pod from Service endpoints before shutdown begins. This prevents traffic from being routed to a pod that is already shutting down.

## Running

### With Docker Compose

```bash
docker-compose up -d
```

### Manually

1. Start PostgreSQL and create a database
2. Set environment variables
3. Run the server:

```bash
./postkeys
```

## Usage

Connect using any Redis client:

```bash
redis-cli -p 6379

> SET mykey "Hello"
OK
> GET mykey
"Hello"
> HSET user:1 name "John" age "30"
(integer) 2
> HGETALL user:1
1) "name"
2) "John"
3) "age"
4) "30"
```

## Metrics

Prometheus metrics are exposed on a separate HTTP server (default port `:9090`).

### Available Endpoints

- `GET /metrics` - Prometheus metrics
- `GET /health` - Liveness check (always 200)
- `GET /ready` - Readiness check (returns 200; returns 503 only during graceful shutdown)

### Metrics Exposed

| Metric | Type | Description |
|--------|------|-------------|
| `postkeys_commands_total` | Counter | Total number of Redis commands processed (labeled by command) |
| `postkeys_command_duration_seconds` | Histogram | Duration of Redis command execution in seconds (labeled by command) |
| `postkeys_command_errors_total` | Counter | Total number of Redis command errors (labeled by command) |
| `postkeys_active_connections` | Gauge | Number of active client connections |
| `postkeys_connections_total` | Counter | Total number of connections accepted |

### Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'postkeys'
    static_configs:
      - targets: ['localhost:9090']
```

## Helm Chart

The Helm chart is available for deploying postkeys to Kubernetes.

### Installation

```bash
# Add the repository (if hosted) or install from local chart
helm install postkeys ./charts/postkeys

# Install with custom values
helm install postkeys ./charts/postkeys -f my-values.yaml

# Install in a specific namespace
helm install postkeys ./charts/postkeys -n my-namespace --create-namespace
```

### Configuration

The following table lists the configurable parameters of the postkeys chart and their default values.

#### General

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/mnorrsken/postkeys` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `imagePullSecrets` | Image pull secrets | `[]` |
| `nameOverride` | Override the chart name | `""` |
| `fullnameOverride` | Override the full release name | `""` |

#### Service Account

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |

#### Pod Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podAnnotations` | Pod annotations | `{}` |
| `podSecurityContext` | Pod security context | `{}` |
| `securityContext.readOnlyRootFilesystem` | Read-only root filesystem | `true` |
| `securityContext.runAsNonRoot` | Run as non-root user | `true` |
| `securityContext.runAsUser` | User ID to run as | `1000` |
| `resources` | CPU/Memory resource requests/limits | `{}` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |

#### Service

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Service type | `ClusterIP` |
| `service.port` | Service port | `6379` |

#### Ingress

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable ingress | `false` |
| `ingress.className` | Ingress class name | `""` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | Ingress hosts configuration | `[]` |
| `ingress.tls` | Ingress TLS configuration | `[]` |

#### Autoscaling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `autoscaling.enabled` | Enable horizontal pod autoscaling | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `1` |
| `autoscaling.maxReplicas` | Maximum replicas | `100` |
| `autoscaling.targetCPUUtilizationPercentage` | Target CPU utilization | `80` |

#### Redis Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `redis.addr` | Address to listen on inside the container | `:6379` |
| `redis.password.create` | Enable auto-generation of a Redis password secret via a Helm hook Job | `false` |
| `redis.password.secretName` | Name of the secret to create (if `create` is true) | `postkeys-secret` |
| `redis.password.value` | Redis password (ignored if `create` is true or existingSecret is set) | `""` |
| `redis.password.secretGenerator.image.repository` | Image repository for the secret generator Job | `rancher/kubectl` |
| `redis.password.secretGenerator.image.tag` | Image tag for the secret generator Job | `v1.35.0` |
| `redis.password.secretGenerator.image.pullPolicy` | Image pull policy for the secret generator Job | `IfNotPresent` |
| `redis.password.existingSecret.name` | Name of existing secret for Redis password | `""` |
| `redis.password.existingSecret.key` | Key in secret containing the password | `redis-password` |

> **Note:** When `redis.password.create` is `true`, a random 32-character password is automatically generated using a Kubernetes Job that runs as a Helm pre-install/pre-upgrade hook. The `password.value` field is ignored in this case. If the secret already exists, it will not be overwritten. The Job inherits `nodeSelector` and `tolerations` from the main deployment configuration.

#### PostgreSQL Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `postgresql.host` | PostgreSQL host | `postgresql` |
| `postgresql.port` | PostgreSQL port | `5432` |
| `postgresql.database` | PostgreSQL database name | `postkeys` |
| `postgresql.sslmode` | PostgreSQL SSL mode | `disable` |
| `postgresql.auth.username` | PostgreSQL username | `postgres` |
| `postgresql.auth.password` | PostgreSQL password (ignored if existingSecret is set) | `""` |
| `postgresql.existingSecret.name` | Name of existing secret for PostgreSQL credentials | `""` |
| `postgresql.existingSecret.usernameKey` | Key in secret containing the username | `""` |
| `postgresql.existingSecret.passwordKey` | Key in secret containing the password | `password` |
| `postgresql.existingSecret.hostKey` | Key in secret containing the host | `""` |
| `postgresql.existingSecret.portKey` | Key in secret containing the port | `""` |
| `postgresql.existingSecret.databaseKey` | Key in secret containing the database name | `""` |

#### Cache Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `cache.enabled` | Enable in-memory cache (opt-in) | `false` |
| `cache.ttl` | Cache TTL duration | `250ms` |
| `cache.maxSize` | Maximum number of cached entries | `10000` |
| `cache.distributedInvalidation` | Enable distributed cache invalidation via PostgreSQL LISTEN/NOTIFY | `false` |
| `cache.excludePatterns` | Comma-separated key patterns to never cache (e.g., `pubsub:*,lock:*`) | `""` |
| `cache.includePatterns` | Comma-separated key patterns to always cache (overrides exclusions) | `""` |

> **Note:** When `cache.distributedInvalidation` is enabled, cache invalidations are broadcast across all pods via PostgreSQL LISTEN/NOTIFY, ensuring cache coherency in multi-pod deployments. This adds ~0.3ms overhead per write operation. For single-pod deployments, leave disabled and use a short TTL.

#### Debug Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `debug` | Enable debug logging (sets DEBUG=1) | `false` |
| `sqlTraceLevel` | SQL query tracing level 0-3 (0=off, 1=important, 2=writes, 3=all) | `0` |
| `traceLevel` | RESP command tracing level 0-3 (0=off, 1=important, 2=most, 3=all) | `0` |

#### Metrics Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.enabled` | Enable metrics endpoint | `true` |
| `metrics.addr` | Metrics server address inside the container | `:9090` |
| `metrics.service.port` | Metrics service port | `9090` |
| `metrics.service.annotations` | Metrics service annotations | `{}` |
| `metrics.serviceMonitor.enabled` | Enable ServiceMonitor (requires Prometheus Operator) | `false` |
| `metrics.serviceMonitor.namespace` | ServiceMonitor namespace | `""` |
| `metrics.serviceMonitor.labels` | ServiceMonitor labels | `{}` |
| `metrics.serviceMonitor.interval` | Scrape interval | `30s` |
| `metrics.serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `metrics.serviceMonitor.metricRelabelings` | Metric relabel configs | `[]` |
| `metrics.serviceMonitor.relabelings` | Relabel configs | `[]` |
| `metrics.serviceMonitor.honorLabels` | Honor labels | `false` |

#### Leader Election

| Parameter | Description | Default |
|-----------|-------------|---------|
| `leaderElection.enabled` | Enable Kubernetes Lease based leader election. The leader patches its own pod with `postkeys/role=leader`; the Service selector routes traffic exclusively to that pod, guaranteeing cache coherency with multiple replicas. Automatically creates the required RBAC on pods and `coordination.k8s.io/leases`. | `false` |

#### Graceful Shutdown

| Parameter | Description | Default |
|-----------|-------------|---------|
| `gracefulShutdown.preStopSleepSeconds` | Seconds to sleep in preStop hook before SIGTERM is processed. Allows kube-proxy time to remove endpoints before the pod starts refusing connections. | `5` |

#### Additional Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `extraEnv` | Additional environment variables | `[]` |
| `extraEnvFrom` | Additional environment variables from secrets/configmaps | `[]` |

### Examples

For deployment examples, including usage with CloudNativePG, see the [examples/](examples/) folder.

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              postkeys Server                               │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │Redis Clients│───▶│ RESP Parser │───▶│   Handler   │───▶│  Storage    │  │
│  │ (RESP2/3)   │    │             │    │             │    │  Backend    │  │
│  └─────────────┘    └─────────────┘    └──────┬──────┘    └──────┬──────┘  │
│                                               │                  │         │
│                           ┌───────────────────┼──────────────────┤         │
│                           │                   │                  │         │
│                           ▼                   ▼                  ▼         │
│                    ┌─────────────┐     ┌─────────────┐     ┌───────────┐   │
│                    │  Pub/Sub    │     │    Cache    │     │    Lua    │   │
│                    │    Hub      │     │   (opt-in)  │     │  Scripts  │   │
│                    └──────┬──────┘     └──────┬──────┘     └───────────┘   │
│                           │                   │                            │
│                           │     ┌─────────────┤                            │
│                           │     │             │                            │
│                           ▼     ▼             ▼                            │
│                    ┌─────────────────────────────────────┐                 │
│                    │       PostgreSQL LISTEN/NOTIFY      │                 │
│                    │  (pub/sub, cache invalidation,      │                 │
│                    │   BRPOP/BLPOP notifications)        │                 │
│                    └──────────────────┬──────────────────┘                 │
│                                       │                                    │
└───────────────────────────────────────┼────────────────────────────────────┘
                                        │
                                        ▼
                               ┌─────────────────┐
                               │  PostgreSQL DB  │
                               │                 │
                               │  ┌───────────┐  │
                               │  │ kv_strings│  │
                               │  │ kv_hashes │  │
                               │  │ kv_lists  │  │
                               │  │ kv_sets   │  │
                               │  │ kv_zsets  │  │
                               │  │ kv_hll    │  │
                               │  └───────────┘  │
                               └─────────────────┘
```

**Key Components:**

- **RESP Parser**: Handles Redis protocol (RESP2 and RESP3) encoding/decoding
- **Handler**: Routes commands to appropriate storage operations, manages transactions
- **Storage Backend**: PostgreSQL-backed storage with optional in-memory cache layer
- **Pub/Sub Hub**: Implements Redis pub/sub using PostgreSQL LISTEN/NOTIFY
- **Cache**: Optional in-memory cache with distributed invalidation for multi-pod deployments
- **Lua Scripts**: EVAL/EVALSHA scripting engine with script caching

