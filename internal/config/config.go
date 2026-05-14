// Package config provides configuration for the server.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the server configuration
type Config struct {
	// Redis server address
	RedisAddr string

	// Redis authentication password (optional)
	RedisPassword string

	// Metrics server address
	MetricsAddr string

	// PostgreSQL configuration
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string
	PGSSLMode  string

	// PostgreSQL connection pool configuration
	PGMaxConns          int           // Maximum number of connections in the pool
	PGMinConns          int           // Minimum number of connections in the pool
	PGMaxConnLifetime   time.Duration // Maximum lifetime of a connection before it's closed
	PGMaxConnIdleTime   time.Duration // Maximum time a connection can be idle before it's closed
	PGHealthCheckPeriod time.Duration // Period between health checks on idle connections

	// Cache configuration
	CacheEnabled                 bool
	CacheTTL                     time.Duration
	CacheMaxSize                 int
	CacheDistributedInvalidation bool

	// Cache key patterns
	CacheExcludePatterns string // Comma-separated patterns to never cache (e.g., "pubsub:*,lock:*")
	CacheIncludePatterns string // Comma-separated patterns to always cache (e.g., "static:*")

	// Debug mode
	Debug bool

	// SQLTraceLevel controls SQL query logging verbosity
	// 0 = off, 1 = important only (DDL, errors), 2 = most commands, 3 = everything
	SQLTraceLevel int

	// TraceLevel controls RESP command logging verbosity
	// 0 = off, 1 = important only (AUTH, FLUSHDB), 2 = most commands, 3 = everything (including GET/SET)
	TraceLevel int

	// EnablePprof enables /debug/pprof/* endpoints on the metrics server
	EnablePprof bool

	// LeaderElectionEnabled enables PostgreSQL advisory lock based leader election.
	// The leader labels its own pod via the Kubernetes API so a Service selector
	// routes traffic exclusively to it, ensuring full cache coherency.
	LeaderElectionEnabled bool

	// PodName and PodNamespace identify this pod for Kubernetes label patching.
	// Set via the downward API (fieldRef: metadata.name / metadata.namespace).
	PodName      string
	PodNamespace string

	// BlockingPollInterval is the fallback poll interval for BLPOP/BRPOP when
	// no LISTEN/NOTIFY notifier is available. Ignored on the notify path.
	BlockingPollInterval time.Duration
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		RedisAddr:                    getEnv("REDIS_ADDR", ":6379"),
		RedisPassword:                getEnv("REDIS_PASSWORD", ""),
		MetricsAddr:                  getEnv("METRICS_ADDR", ":9090"),
		PGHost:                       getEnv("PG_HOST", "localhost"),
		PGPort:                       getEnvInt("PG_PORT", 5432),
		PGUser:                       getEnv("PG_USER", "postgres"),
		PGPassword:                   getEnv("PG_PASSWORD", "postgres"),
		PGDatabase:                   getEnv("PG_DATABASE", "postkeys"),
		PGSSLMode:                    getEnv("PG_SSLMODE", "disable"),
		PGMaxConns:                   getEnvInt("PG_MAX_CONNS", 10),
		PGMinConns:                   getEnvInt("PG_MIN_CONNS", 2),
		PGMaxConnLifetime:            getEnvDuration("PG_MAX_CONN_LIFETIME", 30*time.Minute),
		PGMaxConnIdleTime:            getEnvDuration("PG_MAX_CONN_IDLE_TIME", 5*time.Minute),
		PGHealthCheckPeriod:          getEnvDuration("PG_HEALTH_CHECK_PERIOD", 1*time.Minute),
		CacheEnabled:                 getEnvBool("CACHE_ENABLED", false),
		CacheTTL:                     getEnvDuration("CACHE_TTL", 250*time.Millisecond),
		CacheMaxSize:                 getEnvInt("CACHE_MAX_SIZE", 10000),
		CacheDistributedInvalidation: getEnvBool("CACHE_DISTRIBUTED_INVALIDATION", false),
		CacheExcludePatterns:         getEnv("CACHE_EXCLUDE_PATTERNS", ""),
		CacheIncludePatterns:         getEnv("CACHE_INCLUDE_PATTERNS", ""),
		Debug:                        getEnv("DEBUG", "") == "1",
		SQLTraceLevel:                getEnvInt("SQLTRACE", 0),
		TraceLevel:                   getEnvInt("TRACE", 0),
		EnablePprof:                  getEnvBool("ENABLE_PPROF", false),
		LeaderElectionEnabled:        getEnvBool("LEADER_ELECTION_ENABLED", false),
		PodName:                      getEnv("POD_NAME", ""),
		PodNamespace:                 getEnv("POD_NAMESPACE", ""),
		BlockingPollInterval:         getEnvDuration("BLOCKING_POLL_INTERVAL", 100*time.Millisecond),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
