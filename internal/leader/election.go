// Package leader provides PostgreSQL-based leader election for multi-instance deployments.
// Only one instance holds the leader role at a time; all others are standbys.
// When running in Kubernetes with pod name/namespace configured, the leader labels
// its own pod with postkeys/role=leader so a Service selector can route traffic
// exclusively to the leader.
package leader

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// leaderLockID is the PostgreSQL session advisory lock ID used for leader election.
// Distinct from the cleanup lock (0x706B5F636C65616E).
const leaderLockID int64 = 0x706B5F6C656164 // "pk_lead"

// retryInterval is how often standbys attempt to acquire the leader lock.
const retryInterval = 2 * time.Second

// heartbeatInterval is how often the leader pings its held connection to keep it alive.
const heartbeatInterval = 30 * time.Second

// Election manages PostgreSQL session-level advisory lock based leader election.
type Election struct {
	pool     *pgxpool.Pool
	isLeader atomic.Bool
	labeler  *k8sLabeler

	mu   sync.Mutex
	conn *pgxpool.Conn // held by the leader to keep the session lock alive

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a new Election using the given connection pool.
// If podName and namespace are non-empty, the election will label the pod
// via the Kubernetes API when leadership changes.
func New(pool *pgxpool.Pool, podName, namespace string) *Election {
	e := &Election{
		pool:   pool,
		stopCh: make(chan struct{}),
	}

	if podName != "" && namespace != "" {
		labeler, err := newK8sLabeler(podName, namespace)
		if err != nil {
			log.Printf("Leader election: pod labeling disabled (%v)", err)
		} else {
			e.labeler = labeler
			log.Printf("Leader election: will label pod %s/%s on leadership changes", namespace, podName)
		}
	}

	return e
}

// Start attempts to acquire the leader lock immediately, then launches a background
// goroutine that retries acquisition (if standby) or sends heartbeats (if leader).
func (e *Election) Start(ctx context.Context) {
	e.tryAcquire(ctx)

	e.wg.Add(1)
	go e.run(ctx)
}

// Stop releases the leader lock (if held) and stops the background goroutine.
func (e *Election) Stop() {
	close(e.stopCh)
	e.wg.Wait()
	e.release()
}

// IsLeader reports whether this instance currently holds the leader lock.
func (e *Election) IsLeader() bool {
	return e.isLeader.Load()
}

// run is the background goroutine. Standbys retry every retryInterval;
// the leader sends a heartbeat every heartbeatInterval.
func (e *Election) run(ctx context.Context) {
	defer e.wg.Done()

	retryTicker := time.NewTicker(retryInterval)
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer retryTicker.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ctx.Done():
			return
		case <-retryTicker.C:
			if !e.isLeader.Load() {
				e.tryAcquire(ctx)
			}
		case <-heartbeatTicker.C:
			if e.isLeader.Load() {
				e.heartbeat(ctx)
			}
		}
	}
}

// tryAcquire attempts a non-blocking acquisition of the session advisory lock.
// If successful, this instance becomes the leader and holds the connection.
func (e *Election) tryAcquire(ctx context.Context) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return
	}

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", leaderLockID).Scan(&acquired); err != nil {
		conn.Release()
		return
	}

	if !acquired {
		conn.Release()
		return
	}

	e.mu.Lock()
	e.conn = conn
	e.mu.Unlock()

	e.isLeader.Store(true)
	log.Println("Leader election: became leader")

	if e.labeler != nil {
		if err := e.labeler.setLeaderLabel(true); err != nil {
			log.Printf("Leader election: failed to set leader label: %v", err)
		}
	}
}

// heartbeat pings the held connection to keep it alive. If it fails, leadership is lost.
func (e *Election) heartbeat(ctx context.Context) {
	e.mu.Lock()
	conn := e.conn
	e.mu.Unlock()

	if conn == nil {
		e.isLeader.Store(false)
		return
	}

	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		log.Printf("Leader election: lost leadership (heartbeat failed: %v)", err)
		e.release()
	}
}

// release gives up the leader lock and releases the held connection.
func (e *Election) release() {
	e.mu.Lock()
	conn := e.conn
	e.conn = nil
	e.mu.Unlock()

	if conn == nil {
		return
	}

	wasLeader := e.isLeader.Swap(false)

	if wasLeader && e.labeler != nil {
		if err := e.labeler.setLeaderLabel(false); err != nil {
			log.Printf("Leader election: failed to remove leader label: %v", err)
		}
	}

	// Best-effort explicit unlock before releasing the connection back to the pool.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", leaderLockID) //nolint:errcheck
	conn.Release()
}
