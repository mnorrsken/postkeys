//go:build postgres
// +build postgres

package integration

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wraps the test binary with goleak to surface goroutine leaks at
// teardown. Each test's testServer.Close() is expected to fully release the
// PostgreSQL pool, listener, and pub/sub goroutines; if any of them leak, the
// final goroutine snapshot will fail the run.
//
// Ignores cover background goroutines we know stay running for the lifetime of
// the test binary and don't represent a real leak:
//   - pgxpool's background health check loop, which we never explicitly stop
//     (it terminates when the context the pool was created with is cancelled,
//     and we use context.Background() in tests for simplicity).
//   - go-redis's connection-pool reaper, similarly tied to client lifetime.
//   - the runtime goroutine that backs `go test` itself.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}
