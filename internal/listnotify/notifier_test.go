package listnotify

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestNotifySubscribersRaceWithWaitCleanup is a regression test for a panic
// where listenLoop sent on a channel that WaitForKeys had just closed during
// its defer, after timing out. The channel must not be closed by WaitForKeys.
//
// Run with -race for best signal; the panic is non-deterministic but the
// stress loop reliably reproduces it within a few hundred iterations when
// the close is reintroduced.
func TestNotifySubscribersRaceWithWaitCleanup(t *testing.T) {
	n := &Notifier{
		subscribers: make(map[string][]chan string),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()

	const (
		waiters    = 50
		notifyLoop = 200
	)
	key := "test-key"

	stop := make(chan struct{})
	var notifierWG sync.WaitGroup
	notifierWG.Add(1)
	go func() {
		defer notifierWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < notifyLoop; i++ {
				n.notifySubscribers(key)
			}
		}
	}()

	var waitWG sync.WaitGroup
	for i := 0; i < waiters; i++ {
		waitWG.Add(1)
		go func() {
			defer waitWG.Done()
			// Short timeout so cleanup races with notifySubscribers.
			n.WaitForKeys(context.Background(), []string{key}, 1*time.Millisecond)
		}()
	}
	waitWG.Wait()

	close(stop)
	notifierWG.Wait()
}

// TestWaitForKeysDeliversNotification verifies the happy path still works:
// a notification posted while a waiter is registered wakes the waiter and
// returns the key.
func TestWaitForKeysDeliversNotification(t *testing.T) {
	n := &Notifier{
		subscribers: make(map[string][]chan string),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()

	key := "mykey"
	got := make(chan string, 1)
	go func() {
		got <- n.WaitForKeys(context.Background(), []string{key}, time.Second)
	}()

	// Give the waiter a moment to register.
	time.Sleep(10 * time.Millisecond)
	n.notifySubscribers(key)

	select {
	case k := <-got:
		if k != key {
			t.Fatalf("expected %q, got %q", key, k)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not receive notification")
	}
}

// TestWaitForKeysTimeoutCleansUp verifies that a timed-out waiter removes
// itself from the subscribers map.
func TestWaitForKeysTimeoutCleansUp(t *testing.T) {
	n := &Notifier{
		subscribers: make(map[string][]chan string),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()

	key := "ephemeral"
	result := n.WaitForKeys(context.Background(), []string{key}, 5*time.Millisecond)
	if result != "" {
		t.Fatalf("expected empty result on timeout, got %q", result)
	}

	n.mu.RLock()
	defer n.mu.RUnlock()
	if _, present := n.subscribers[key]; present {
		t.Fatalf("subscribers map should not retain entry for %q", key)
	}
}
