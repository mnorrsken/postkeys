// Package metrics provides Prometheus metrics for the Redis-compatible server.
package metrics

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// CommandsTotal counts the total number of commands processed
	CommandsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "postkeys_commands_total",
			Help: "Total number of Redis commands processed",
		},
		[]string{"command"},
	)

	// CommandDuration measures the duration of command execution
	CommandDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "postkeys_command_duration_seconds",
			Help:    "Duration of Redis command execution in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms to ~6.5s
		},
		[]string{"command"},
	)

	// CommandErrors counts the number of command errors
	CommandErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "postkeys_command_errors_total",
			Help: "Total number of Redis command errors",
		},
		[]string{"command"},
	)

	// ActiveConnections tracks the number of active client connections
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "postkeys_active_connections",
			Help: "Number of active client connections",
		},
	)

	// ConnectionsTotal counts the total number of connections accepted
	ConnectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "postkeys_connections_total",
			Help: "Total number of connections accepted",
		},
	)

	// CacheHits counts the number of cache hits
	CacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "postkeys_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	// CacheMisses counts the number of cache misses
	CacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "postkeys_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	// CacheSkips counts cache skips by reason (pattern-based exclusion)
	CacheSkips = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "postkeys_cache_skips_total",
			Help: "Total number of cache skips by reason",
		},
		[]string{"reason"},
	)
)

// RecordCommand records metrics for a command execution
func RecordCommand(command string, duration time.Duration, isError bool) {
	CommandsTotal.WithLabelValues(command).Inc()
	CommandDuration.WithLabelValues(command).Observe(duration.Seconds())
	if isError {
		CommandErrors.WithLabelValues(command).Inc()
	}
}

// Server represents a metrics HTTP server
type Server struct {
	server *http.Server
}

// NewServer creates a new metrics server.
// If enablePprof is true, /debug/pprof/* endpoints are registered.
// readyFn is called on each /ready request; return true to indicate readiness (HTTP 200),
// false for not-ready (HTTP 503). Pass nil to always report ready.
func NewServer(addr string, enablePprof bool, readyFn func() bool) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if readyFn != nil && !readyFn() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("standby"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	if enablePprof {
		// Register pprof handlers for profiling (opt-in via ENABLE_PPROF=true)
		// CPU profile: curl http://host:port/debug/pprof/profile?seconds=30 > cpu.prof
		// Heap profile: curl http://host:port/debug/pprof/heap > heap.prof
		// Goroutine: curl http://host:port/debug/pprof/goroutine?debug=1
		// Block: curl http://host:port/debug/pprof/block > block.prof
		// Mutex: curl http://host:port/debug/pprof/mutex > mutex.prof
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

// Start starts the metrics server
func (s *Server) Start() error {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash - metrics server is optional
			println("Metrics server error:", err.Error())
		}
	}()
	return nil
}

// Stop gracefully stops the metrics server
func (s *Server) Stop() error {
	return s.server.Close()
}
