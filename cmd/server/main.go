package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mnorrsken/postkeys/internal/cache"
	"github.com/mnorrsken/postkeys/internal/config"
	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/leader"
	"github.com/mnorrsken/postkeys/internal/listnotify"
	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/pubsub"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// shutdownTimeout is the maximum time to wait for graceful shutdown
const shutdownTimeout = 30 * time.Second

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to PostgreSQL
	log.Printf("Connecting to PostgreSQL at %s:%d...", cfg.PGHost, cfg.PGPort)
	store, err := storage.New(ctx, storage.Config{
		Host:              cfg.PGHost,
		Port:              cfg.PGPort,
		User:              cfg.PGUser,
		Password:          cfg.PGPassword,
		Database:          cfg.PGDatabase,
		SSLMode:           cfg.PGSSLMode,
		SQLTraceLevel:     cfg.SQLTraceLevel,
		MaxConns:          cfg.PGMaxConns,
		MinConns:          cfg.PGMinConns,
		MaxConnLifetime:   cfg.PGMaxConnLifetime,
		MaxConnIdleTime:   cfg.PGMaxConnIdleTime,
		HealthCheckPeriod: cfg.PGHealthCheckPeriod,
	})
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Printf("Connected to PostgreSQL (pool: min=%d, max=%d, lifetime=%v, health_check=%v)",
		cfg.PGMinConns, cfg.PGMaxConns, cfg.PGMaxConnLifetime, cfg.PGHealthCheckPeriod)

	// Wrap with cache if enabled
	var backend storage.Backend = store
	var cachedStore *cache.CachedStore
	var cacheInvalidator *cache.Invalidator
	if cfg.CacheEnabled {
		cacheCfg := cache.Config{
			TTL:             cfg.CacheTTL,
			MaxSize:         cfg.CacheMaxSize,
			ExcludePatterns: parsePatterns(cfg.CacheExcludePatterns),
			IncludePatterns: parsePatterns(cfg.CacheIncludePatterns),
		}

		cachedStore = cache.NewCachedStore(store, cacheCfg)
		backend = cachedStore

		// Set up distributed cache invalidation (optional, for multi-pod deployments)
		if cfg.CacheDistributedInvalidation {
			cacheInvalidator = cache.NewInvalidator(store.Pool(), store.ConnString(), cachedStore.GetCache())
			cacheInvalidator.SetDebug(cfg.Debug)
			if err := cacheInvalidator.Start(ctx); err != nil {
				log.Fatalf("Failed to start cache invalidator: %v", err)
			}
			cachedStore.SetInvalidator(cacheInvalidator)
			log.Printf("In-memory cache enabled with distributed invalidation (TTL: %v, MaxSize: %d)", cfg.CacheTTL, cfg.CacheMaxSize)
		} else {
			log.Printf("In-memory cache enabled (TTL: %v, MaxSize: %d)", cfg.CacheTTL, cfg.CacheMaxSize)
		}
	}

	// Set up leader election if enabled
	var election *leader.Election
	readyFn := func() bool { return true }
	if cfg.LeaderElectionEnabled {
		election = leader.New(store.Pool())
		election.Start(ctx)
		readyFn = election.IsLeader
		log.Printf("Leader election enabled (LEADER_ELECTION_ENABLED=true)")
	}

	// Start metrics server
	metricsSrv := metrics.NewServer(cfg.MetricsAddr, cfg.EnablePprof, readyFn)
	if err := metricsSrv.Start(); err != nil {
		log.Fatalf("Failed to start metrics server: %v", err)
	}
	log.Printf("Metrics server listening on %s", cfg.MetricsAddr)

	// Create handler
	h := handler.New(backend, cfg.RedisPassword)

	// Initialize list notifier for BRPOP/BLPOP
	listNotifier := listnotify.New(store.Pool(), store.ConnString())
	listNotifier.SetDebug(cfg.Debug)
	if err := listNotifier.Start(ctx); err != nil {
		log.Fatalf("Failed to start list notifier: %v", err)
	}
	h.SetListNotifier(listNotifier)
	log.Println("List notification support enabled (BRPOP/BLPOP)")

	// Create and start server
	srv := server.NewWithOptions(cfg.RedisAddr, h, cfg.Debug, cfg.TraceLevel)

	// Initialize pub/sub hub
	hub := pubsub.NewHub(store.Pool(), store.ConnString())
	if err := hub.Start(ctx); err != nil {
		log.Fatalf("Failed to start pub/sub hub: %v", err)
	}
	srv.SetPubSubHub(hub)
	log.Println("Pub/sub support enabled")

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	if cfg.Debug {
		log.Println("Debug logging is enabled (DEBUG=1)")
	}
	if cfg.SQLTraceLevel > 0 {
		log.Printf("SQL query tracing is enabled at level %d (SQLTRACE=%d)", cfg.SQLTraceLevel, cfg.SQLTraceLevel)
	}
	if cfg.TraceLevel > 0 {
		log.Printf("RESP command tracing is enabled at level %d (TRACE=%d)", cfg.TraceLevel, cfg.TraceLevel)
	}
	if cfg.RedisPassword != "" {
		log.Println("Authentication is enabled")
	}
	log.Printf("postkeys is ready to accept connections on %s", cfg.RedisAddr)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for first shutdown signal
	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Start a goroutine to handle forced shutdown on second signal
	go func() {
		sig := <-sigChan
		log.Printf("Received second signal %v, forcing immediate shutdown", sig)
		os.Exit(1)
	}()

	// Graceful shutdown sequence
	log.Println("Stopping accepting new connections...")
	cancel() // Cancel the main context to signal all goroutines

	// Stop components in order: release leadership first so a standby can
	// take over immediately, then drain connections and stop the rest.
	done := make(chan struct{})
	go func() {
		// Release leader lock first — allows the new pod to become ready
		// before we finish draining.
		if election != nil {
			log.Println("Releasing leader lock...")
			election.Stop()
		}

		log.Println("Stopping Redis server...")
		srv.Stop()

		log.Println("Stopping metrics server...")
		metricsSrv.Stop()

		// Stop cache invalidator if running
		if cacheInvalidator != nil {
			log.Println("Stopping cache invalidator...")
			cacheInvalidator.Stop()
		}

		log.Println("Closing database connections...")
		backend.Close()

		close(done)
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-done:
		log.Println("Graceful shutdown completed successfully")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timed out, forcing exit")
		os.Exit(1)
	}
}

// parsePatterns parses a comma-separated list of patterns
func parsePatterns(s string) []string {
	if s == "" {
		return nil
	}
	patterns := strings.Split(s, ",")
	result := make([]string, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
