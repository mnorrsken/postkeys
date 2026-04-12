package leader

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// fakeK8s is a thread-safe in-memory stand-in for the Kubernetes API.
type fakeK8s struct {
	mu      sync.Mutex
	lease   *lease
	labels  map[string]bool // podName → leader label currently set
	podName string

	// forceGetErr, if non-nil, makes getLease return that error.
	forceGetErr error
	// forceUpdateErr, if non-nil, makes updateLease return that error.
	forceUpdateErr error
}

func newFakeK8s(podName string) *fakeK8s {
	return &fakeK8s{
		labels:  map[string]bool{},
		podName: podName,
	}
}

func (f *fakeK8s) getLease(_ context.Context, name string) (*lease, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceGetErr != nil {
		return nil, 0, f.forceGetErr
	}
	if f.lease == nil || f.lease.Metadata.Name != name {
		return nil, http.StatusNotFound, nil
	}
	// Return a deep-ish copy so the caller's mutations don't silently mutate state.
	cp := *f.lease
	specCopy := *f.lease
	cp.Spec = specCopy.Spec
	return &cp, http.StatusOK, nil
}

func (f *fakeK8s) createLease(_ context.Context, name string, durationSeconds int32, now time.Time) (*lease, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lease != nil {
		return nil, http.StatusConflict, errors.New("already exists")
	}
	f.lease = &lease{
		Metadata: leaseMetadata{Name: name, ResourceVersion: "1"},
		Spec: leaseSpec{
			HolderIdentity:       strPtr(f.podName),
			LeaseDurationSeconds: int32Ptr(durationSeconds),
			AcquireTime:          strPtr(microTime(now)),
			RenewTime:            strPtr(microTime(now)),
			LeaseTransitions:     int32Ptr(0),
		},
	}
	cp := *f.lease
	return &cp, http.StatusCreated, nil
}

func (f *fakeK8s) updateLease(_ context.Context, l *lease) (*lease, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceUpdateErr != nil {
		return nil, 0, f.forceUpdateErr
	}
	if f.lease == nil {
		return nil, http.StatusNotFound, errors.New("not found")
	}
	if l.Metadata.ResourceVersion != f.lease.Metadata.ResourceVersion {
		return nil, http.StatusConflict, errors.New("conflict")
	}
	updated := *l
	// Bump resourceVersion so subsequent stale writes conflict.
	updated.Metadata.ResourceVersion = f.lease.Metadata.ResourceVersion + "x"
	f.lease = &updated
	cp := *f.lease
	return &cp, http.StatusOK, nil
}

func (f *fakeK8s) setLeaderLabel(_ context.Context, leader bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.labels[f.podName] = leader
	return nil
}

func (f *fakeK8s) hasLabel() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.labels[f.podName]
}

func newTestElection(podName string, k8s k8sOps) *Election {
	return &Election{k8s: k8s, podName: podName}
}

func TestAttempt_CreateLease(t *testing.T) {
	f := newFakeK8s("pod-a")
	e := newTestElection("pod-a", f)

	acquired, err := e.attempt(context.Background())
	if err != nil {
		t.Fatalf("attempt: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire on create")
	}
}

func TestAttempt_RenewOwnLease(t *testing.T) {
	f := newFakeK8s("pod-a")
	e := newTestElection("pod-a", f)

	if ok, err := e.attempt(context.Background()); !ok || err != nil {
		t.Fatalf("initial create: ok=%v err=%v", ok, err)
	}

	if ok, err := e.attempt(context.Background()); !ok || err != nil {
		t.Fatalf("renew: ok=%v err=%v", ok, err)
	}
}

func TestAttempt_StandbyWhenOtherHoldsValidLease(t *testing.T) {
	f := newFakeK8s("pod-a")
	// pod-a creates first.
	eA := newTestElection("pod-a", f)
	if ok, err := eA.attempt(context.Background()); !ok || err != nil {
		t.Fatalf("pod-a acquire: ok=%v err=%v", ok, err)
	}
	// pod-b tries: should fail to acquire, no error.
	fB := &fakeK8s{labels: map[string]bool{}, podName: "pod-b"}
	// Share the lease state between the two pods — just point at the same underlying
	// lease via a shared fake. Easiest: give fB a reference to f's lease.
	fB.lease = f.lease
	eB := newTestElection("pod-b", fB)
	ok, err := eB.attempt(context.Background())
	if err != nil {
		t.Fatalf("pod-b attempt: %v", err)
	}
	if ok {
		t.Fatal("pod-b should not have acquired a valid lease held by pod-a")
	}
}

func TestAttempt_TakeoverExpiredLease(t *testing.T) {
	f := newFakeK8s("pod-b")
	// Install an expired lease owned by pod-a.
	expired := time.Now().Add(-1 * time.Hour)
	f.lease = &lease{
		Metadata: leaseMetadata{Name: leaseName, ResourceVersion: "1"},
		Spec: leaseSpec{
			HolderIdentity:       strPtr("pod-a"),
			LeaseDurationSeconds: int32Ptr(15),
			AcquireTime:          strPtr(microTime(expired)),
			RenewTime:            strPtr(microTime(expired)),
			LeaseTransitions:     int32Ptr(3),
		},
	}
	eB := newTestElection("pod-b", f)
	ok, err := eB.attempt(context.Background())
	if err != nil {
		t.Fatalf("pod-b attempt: %v", err)
	}
	if !ok {
		t.Fatal("pod-b should have taken over the expired lease")
	}
	if got := *f.lease.Spec.HolderIdentity; got != "pod-b" {
		t.Errorf("holder = %q, want pod-b", got)
	}
	if got := *f.lease.Spec.LeaseTransitions; got != 4 {
		t.Errorf("leaseTransitions = %d, want 4", got)
	}
}

func TestTick_LabelTransitions(t *testing.T) {
	f := newFakeK8s("pod-a")
	e := newTestElection("pod-a", f)

	// Tick 1: should acquire + set label.
	e.tick(context.Background())
	if !e.IsLeader() {
		t.Fatal("after tick 1: expected to be leader")
	}
	if !f.hasLabel() {
		t.Fatal("after tick 1: expected label to be set")
	}

	// Simulate another pod stealing the lease (expired + takeover in-place).
	f.mu.Lock()
	f.lease.Spec.HolderIdentity = strPtr("pod-b")
	f.lease.Metadata.ResourceVersion = "99"
	f.mu.Unlock()

	// Pretend we last renewed long ago so we're outside the grace window.
	e.lastRenew = time.Now().Add(-1 * time.Hour)

	e.tick(context.Background())
	if e.IsLeader() {
		t.Fatal("after takeover: expected to not be leader")
	}
	if f.hasLabel() {
		t.Fatal("after takeover: expected label to be removed")
	}
}

func TestTick_GraceWindowAbsorbsTransientFailure(t *testing.T) {
	f := newFakeK8s("pod-a")
	e := newTestElection("pod-a", f)

	// Become leader.
	e.tick(context.Background())
	if !e.IsLeader() {
		t.Fatal("expected leader after first tick")
	}

	// Simulate a transient API-server failure on the next renewal.
	f.forceGetErr = errors.New("api server unreachable")

	// Inside the grace window we keep leadership.
	e.lastRenew = time.Now() // just renewed
	e.tick(context.Background())
	if !e.IsLeader() {
		t.Fatal("grace window should have preserved leadership")
	}
	if !f.hasLabel() {
		t.Fatal("label should still be set during grace window")
	}

	// Outside the grace window we give up.
	e.lastRenew = time.Now().Add(-2 * renewDeadline)
	e.tick(context.Background())
	if e.IsLeader() {
		t.Fatal("grace window expired: should have demoted")
	}
	if f.hasLabel() {
		t.Fatal("label should be removed after grace window expiry")
	}
}

func TestLeaseExpired(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		l    *lease
		want bool
	}{
		{
			name: "fresh",
			l: &lease{Spec: leaseSpec{
				RenewTime:            strPtr(microTime(now)),
				LeaseDurationSeconds: int32Ptr(15),
			}},
			want: false,
		},
		{
			name: "expired",
			l: &lease{Spec: leaseSpec{
				RenewTime:            strPtr(microTime(now.Add(-1 * time.Hour))),
				LeaseDurationSeconds: int32Ptr(15),
			}},
			want: true,
		},
		{
			name: "missing renewTime",
			l:    &lease{Spec: leaseSpec{LeaseDurationSeconds: int32Ptr(15)}},
			want: true,
		},
		{
			name: "missing duration",
			l:    &lease{Spec: leaseSpec{RenewTime: strPtr(microTime(now))}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := leaseExpired(tt.l, now); got != tt.want {
				t.Errorf("leaseExpired = %v, want %v", got, tt.want)
			}
		})
	}
}
