// Package leader provides Kubernetes-Lease-based leader election for multi-instance
// deployments. Only one instance holds the leader role at a time; all others are
// standbys. The leader labels its own pod with postkeys/role=leader so a Service
// selector can route traffic exclusively to the leader.
//
// Leadership is tracked via a coordination.k8s.io/v1 Lease object, so it survives
// PostgreSQL failovers (CNPG primary switchovers) completely — the Lease lives in
// the Kubernetes API server and is unaffected by database outages.
package leader

import (
	"context"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// leaseName is the name of the coordination.k8s.io/v1 Lease used for election.
	leaseName = "postkeys-leader"

	// leaseDuration is how long a Lease remains valid without a renewal.
	// Standbys consider the lease takeable once (now - renewTime) > leaseDuration.
	leaseDuration = 15 * time.Second

	// renewDeadline is the grace window during which a leader retries renewals
	// after a transient API-server failure before giving up the leadership.
	// Must be < leaseDuration so standbys don't take over before we give up.
	renewDeadline = 10 * time.Second

	// retryPeriod is how often the election loop ticks (both for renewal by the
	// leader and takeover attempts by standbys).
	retryPeriod = 2 * time.Second

	// stopTimeout bounds how long Stop() waits when surrendering the lease.
	stopTimeout = 2 * time.Second
)

// k8sOps is the minimal set of Kubernetes operations the election needs.
// Satisfied by *k8sClient; abstracted to keep unit tests fast.
type k8sOps interface {
	getLease(ctx context.Context, name string) (*lease, int, error)
	createLease(ctx context.Context, name string, durationSeconds int32, now time.Time) (*lease, int, error)
	updateLease(ctx context.Context, l *lease) (*lease, int, error)
	setLeaderLabel(ctx context.Context, leader bool) error
}

// Election manages Kubernetes-Lease-based leader election.
type Election struct {
	k8s     k8sOps
	podName string

	isLeader  atomic.Bool
	lastRenew time.Time // wall-clock of the most recent successful renewal

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates an Election. If the Kubernetes in-cluster service account token
// cannot be read (e.g. running locally), returns an error and the caller should
// skip leader election.
func New(podName, namespace string) (*Election, error) {
	if podName == "" || namespace == "" {
		return nil, &configError{msg: "podName and namespace are required"}
	}
	client, err := newK8sClient(podName, namespace)
	if err != nil {
		return nil, err
	}
	return &Election{
		k8s:     client,
		podName: podName,
	}, nil
}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

// Start launches the election loop in a background goroutine. The loop ticks
// every retryPeriod, attempting to acquire or renew the Lease.
func (e *Election) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.wg.Add(1)
	go e.run(runCtx)
}

// Stop cancels the election loop and, if this instance is currently the leader,
// surrenders the Lease so a standby can take over immediately. Best-effort,
// bounded by stopTimeout.
func (e *Election) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()

	if e.isLeader.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		e.surrender(ctx)
	}
}

// IsLeader reports whether this instance currently holds the Lease.
func (e *Election) IsLeader() bool {
	return e.isLeader.Load()
}

func (e *Election) run(ctx context.Context) {
	defer e.wg.Done()

	// Immediate first attempt so we don't wait a full retryPeriod on startup.
	e.tick(ctx)

	ticker := time.NewTicker(retryPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

// tick runs one iteration of the election loop.
func (e *Election) tick(ctx context.Context) {
	opCtx, cancel := context.WithTimeout(ctx, retryPeriod)
	defer cancel()

	wasLeader := e.isLeader.Load()
	acquired, err := e.attempt(opCtx)
	now := time.Now()

	if acquired {
		e.lastRenew = now
		if !wasLeader {
			e.isLeader.Store(true)
			if labelErr := e.k8s.setLeaderLabel(opCtx, true); labelErr != nil {
				log.Printf("Leader election: failed to set leader label: %v", labelErr)
			}
			log.Println("Leader election: became leader")
		}
		return
	}

	if !wasLeader {
		return
	}

	// We thought we were leader but attempt didn't acquire. If it was a transient
	// failure and we're still within the renewDeadline grace window, hold on and
	// retry next tick — don't flap the label during API-server blips.
	if err != nil && now.Sub(e.lastRenew) < renewDeadline {
		log.Printf("Leader election: renew failed (within grace window): %v", err)
		return
	}

	// Lost leadership: either someone else holds the lease now, or we ran out
	// of grace time.
	e.isLeader.Store(false)
	if labelErr := e.k8s.setLeaderLabel(opCtx, false); labelErr != nil {
		log.Printf("Leader election: failed to remove leader label: %v", labelErr)
	}
	if err != nil {
		log.Printf("Leader election: lost leadership (renew exceeded grace window): %v", err)
	} else {
		log.Println("Leader election: lost leadership (taken by another holder)")
	}
}

// attempt tries to acquire, renew, or take over the Lease.
// Returns (true, nil) on success; (false, nil) when another valid holder exists;
// (false, err) on transient failure.
func (e *Election) attempt(ctx context.Context) (bool, error) {
	l, status, err := e.k8s.getLease(ctx, leaseName)
	if err != nil {
		return false, err
	}

	if status == http.StatusNotFound || l == nil {
		// First-ever acquisition: create it.
		_, createStatus, createErr := e.k8s.createLease(ctx, leaseName, int32(leaseDuration.Seconds()), time.Now())
		if createErr == nil {
			return true, nil
		}
		if createStatus == http.StatusConflict {
			// Another pod created it at the same time. Try again next tick.
			return false, nil
		}
		return false, createErr
	}

	now := time.Now()
	holder := ""
	if l.Spec.HolderIdentity != nil {
		holder = *l.Spec.HolderIdentity
	}

	if holder == e.podName {
		// We already hold it — renew by updating renewTime.
		l.Spec.RenewTime = strPtr(microTime(now))
		if l.Spec.LeaseDurationSeconds == nil {
			l.Spec.LeaseDurationSeconds = int32Ptr(int32(leaseDuration.Seconds()))
		}
		_, updateStatus, updateErr := e.k8s.updateLease(ctx, l)
		if updateErr == nil {
			return true, nil
		}
		if updateStatus == http.StatusConflict {
			// Another writer interleaved — treat as "no longer leader".
			return false, nil
		}
		return false, updateErr
	}

	// Someone else holds the lease. Check if it's expired.
	if !leaseExpired(l, now) {
		return false, nil
	}

	// Expired — attempt takeover with optimistic concurrency.
	transitions := int32(0)
	if l.Spec.LeaseTransitions != nil {
		transitions = *l.Spec.LeaseTransitions
	}
	l.Spec.HolderIdentity = strPtr(e.podName)
	l.Spec.LeaseDurationSeconds = int32Ptr(int32(leaseDuration.Seconds()))
	l.Spec.AcquireTime = strPtr(microTime(now))
	l.Spec.RenewTime = strPtr(microTime(now))
	l.Spec.LeaseTransitions = int32Ptr(transitions + 1)
	_, updateStatus, updateErr := e.k8s.updateLease(ctx, l)
	if updateErr == nil {
		return true, nil
	}
	if updateStatus == http.StatusConflict {
		// Another standby beat us to the takeover.
		return false, nil
	}
	return false, updateErr
}

// surrender is a best-effort release of the Lease on shutdown. It zeroes the
// holder and backdates renewTime so the next standby's tick takes over
// immediately. Also removes the pod label.
func (e *Election) surrender(ctx context.Context) {
	l, _, err := e.k8s.getLease(ctx, leaseName)
	if err == nil && l != nil && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity == e.podName {
		l.Spec.HolderIdentity = strPtr("")
		l.Spec.RenewTime = strPtr(microTime(time.Unix(0, 0)))
		if _, _, updateErr := e.k8s.updateLease(ctx, l); updateErr != nil {
			log.Printf("Leader election: failed to surrender lease: %v", updateErr)
		}
	}
	if labelErr := e.k8s.setLeaderLabel(ctx, false); labelErr != nil {
		log.Printf("Leader election: failed to remove leader label on shutdown: %v", labelErr)
	}
	e.isLeader.Store(false)
}

// leaseExpired reports whether the given lease has expired at time now.
// A lease with a missing or unparseable renewTime is considered expired.
func leaseExpired(l *lease, now time.Time) bool {
	if l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return true
	}
	rt, err := time.Parse("2006-01-02T15:04:05.000000Z", *l.Spec.RenewTime)
	if err != nil {
		return true
	}
	expires := rt.Add(time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second)
	return now.After(expires)
}
