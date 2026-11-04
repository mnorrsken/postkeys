package pubsub

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type receivedMessage struct{ channel, message string }
type recordingSubscriber struct {
	messages chan receivedMessage
	id       uint64
}

func (s *recordingSubscriber) GetID() uint64 {
	if s.id != 0 {
		return s.id
	}
	return 1
}
func (s *recordingSubscriber) SendPubSubMessage(_, channel, message string) error {
	s.messages <- receivedMessage{channel, message}
	return nil
}
func awaitMessage(t *testing.T, s *recordingSubscriber, channel, message string) {
	t.Helper()
	select {
	case got := <-s.messages:
		if got != (receivedMessage{channel, message}) {
			t.Fatalf("got %+v, want %q %q", got, channel, message)
		}
	case <-time.After(time.Second):
		t.Fatalf("missing %q %q", channel, message)
	}
}
func TestDispatchReferencePreservesChannelOrderWithoutBlockingInline(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	h.released = make(chan *pendingDelivery, dispatchCapacity)
	h.enableOutputs()
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10)}
	h.Subscribe(sub, "slow")
	independent := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 2}
	h.Subscribe(independent, "fast")
	pattern := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 3}
	h.PSubscribe(pattern, "*")
	h.wg.Add(1)
	go h.dispatchLoop()
	defer h.Stop()
	h.incoming <- delivery{key: channelKey("slow"), channel: "slow", id: 1, known: true}
	var pending *pendingDelivery
	select {
	case pending = <-h.fetchJobs:
	case <-time.After(time.Second):
		t.Fatal("reference not scheduled")
	}
	h.incoming <- delivery{key: channelKey("slow"), channel: "slow", message: "second", known: true}
	h.incoming <- delivery{key: channelKey("fast"), channel: "fast", message: "independent", known: true}
	awaitMessage(t, independent, "fast", "independent")
	select {
	case msg := <-pattern.messages:
		t.Fatalf("pattern client observed later channel first: %+v", msg)
	default:
	}
	select {
	case msg := <-sub.messages:
		t.Fatalf("same-channel message overtook reference: %+v", msg)
	default:
	}
	pending.message = "first"
	h.fetched <- pending
	awaitMessage(t, sub, "slow", "first")
	awaitMessage(t, sub, "slow", "second")
	awaitMessage(t, pattern, "*\x00slow", "first")
	awaitMessage(t, pattern, "*\x00slow", "second")
	awaitMessage(t, pattern, "*\x00fast", "independent")
}
func TestDelayedReferenceInvalidatedByUnsubscribe(t *testing.T) {
	for _, mode := range []string{"exact", "pattern", "remove-all"} {
		for _, resubscribe := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resubscribe=%t", mode, resubscribe), func(t *testing.T) {
				h := NewHub(nil, "")
				h.incoming = make(chan delivery, 256)
				h.fetchJobs = make(chan *pendingDelivery)
				h.fetched = make(chan *pendingDelivery)
				h.released = make(chan *pendingDelivery, dispatchCapacity)
				h.enableOutputs()
				sub := &recordingSubscriber{id: 1, messages: make(chan receivedMessage, 10)}
				observer := &recordingSubscriber{id: 2, messages: make(chan receivedMessage, 10)}
				h.Subscribe(observer, "marker")
				subscribe := func() {
					if mode != "pattern" {
						h.Subscribe(sub, "route")
					}
					if mode != "exact" {
						h.PSubscribe(sub, "r*")
					}
				}
				subscribe()
				h.wg.Add(1)
				go h.dispatchLoop()
				defer h.Stop()
				h.incoming <- delivery{channel: "route", known: true, id: 1}
				var pending *pendingDelivery
				select {
				case pending = <-h.fetchJobs:
				case <-time.After(time.Second):
					t.Fatal("reference not pending")
				}
				switch mode {
				case "exact":
					h.Unsubscribe(sub, "route")
				case "pattern":
					h.PUnsubscribe(sub, "r*")
				case "remove-all":
					h.RemoveSubscriber(sub.GetID())
				}
				// Unsubscribe has returned: a real handler may now acknowledge removal and
				// reuse RESP2 for ordinary commands. Reusing the same client and route must
				// create a fresh generation instead of reviving the pending reference.
				if resubscribe {
					subscribe()
				}
				pending.message = "stale"
				h.fetched <- pending
				h.incoming <- delivery{channel: "route", known: true, message: "new generation"}
				h.incoming <- delivery{channel: "marker", known: true, message: "processed"}
				awaitMessage(t, observer, "marker", "processed")
				waitConsumed(t, pending)
				if resubscribe {
					if mode != "pattern" {
						awaitMessage(t, sub, "route", "new generation")
					}
					if mode != "exact" {
						awaitMessage(t, sub, "r*\x00route", "new generation")
					}
				}
				select {
				case got := <-sub.messages:
					t.Fatalf("delivery survived subscription removal: %+v", got)
				default:
				}
			})
		}
	}
}

func TestDuplicateSubscribePreservesPendingGeneration(t *testing.T) {
	for _, pattern := range []bool{false, true} {
		t.Run(fmt.Sprintf("pattern=%t", pattern), func(t *testing.T) {
			h := NewHub(nil, "")
			sub := &recordingSubscriber{messages: make(chan receivedMessage, 2)}
			subscribe := func() {
				if pattern {
					h.PSubscribe(sub, "r*")
				} else {
					h.Subscribe(sub, "route")
				}
			}
			subscribe()
			targets := h.snapshotTargets(delivery{channel: "route", known: true})
			if len(targets) != 1 {
				t.Fatal("missing route")
			}
			subscribe()
			h.sendTarget(targets[0], delivery{channel: "route", message: "retained"})
			channel := "route"
			if pattern {
				channel = "r*\x00route"
			}
			awaitMessage(t, sub, channel, "retained")
		})
	}
}

type blockingSubscriber struct{ entered, release, completed chan struct{} }

func (*blockingSubscriber) GetID() uint64 { return 1 }
func (s *blockingSubscriber) SendPubSubMessage(_, _, _ string) error {
	close(s.entered)
	<-s.release
	close(s.completed)
	return nil
}
func TestUnsubscribeSerializesWithInProgressDelivery(t *testing.T) {
	for _, pattern := range []bool{false, true} {
		t.Run(fmt.Sprintf("pattern=%t", pattern), func(t *testing.T) {
			h := NewHub(nil, "")
			sub := &blockingSubscriber{entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
			if pattern {
				h.PSubscribe(sub, "r*")
			} else {
				h.Subscribe(sub, "route")
			}
			targets := h.snapshotTargets(delivery{channel: "route", known: true})
			go h.sendTarget(targets[0], delivery{channel: "route", message: "before unsubscribe"})
			<-sub.entered
			released := false
			defer func() {
				if !released {
					close(sub.release)
				}
			}()
			// Removal cannot be acknowledged while an earlier send can still write.
			removed := make(chan struct{})
			go func() {
				if pattern {
					h.PUnsubscribe(sub, "r*")
				} else {
					h.Unsubscribe(sub, "route")
				}
				close(removed)
			}()
			select {
			case <-removed:
				t.Error("unsubscribe returned while a delivery could still write")
			case <-time.After(20 * time.Millisecond):
			}
			close(sub.release)
			released = true
			select {
			case <-removed:
			case <-time.After(time.Second):
				t.Fatal("unsubscribe blocked after send completed")
			}
			select {
			case <-sub.completed:
			default:
				t.Fatal("removal acknowledged before send completed")
			}
		})
	}
}

// Observe detached memberships while removal is still waiting for its send;
// this verifies other operations run with both send and unsubscribe pending.
func awaitDetachedSubscriptions(t *testing.T, h *Hub, id uint64) {
	t.Helper()
	detached := make(chan struct{})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			channels, patterns := h.GetSubscriptionCount(id)
			if channels == 0 && patterns == 0 {
				close(detached)
				return
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	select {
	case <-detached:
	case <-time.After(time.Second):
		t.Fatal("subscription removal held a global lock while waiting for client I/O")
	}
}

func TestStalledSubscriberDoesNotBlockOtherSubscriptions(t *testing.T) {
	for _, mode := range []string{"exact", "pattern", "remove-all"} {
		t.Run(mode, func(t *testing.T) {
			h := NewHub(nil, "")
			stalled := &blockingSubscriber{entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
			if mode != "pattern" {
				h.Subscribe(stalled, "route")
			}
			if mode != "exact" {
				h.PSubscribe(stalled, "r*")
			}
			targets := h.snapshotTargets(delivery{channel: "route", known: true})
			go h.sendTarget(targets[0], delivery{channel: "route", message: "stalled"})
			<-stalled.entered
			released := false
			defer func() {
				if !released {
					close(stalled.release)
				}
			}()
			removed := make(chan struct{})
			go func() {
				switch mode {
				case "exact":
					h.Unsubscribe(stalled, "route")
				case "pattern":
					h.PUnsubscribe(stalled, "r*")
				case "remove-all":
					h.RemoveSubscriber(stalled.GetID())
				}
				close(removed)
			}()
			awaitDetachedSubscriptions(t, h, stalled.GetID())
			select {
			case <-removed:
				t.Fatal("removal returned while send still pending")
			default:
			}
			independent := &recordingSubscriber{id: 2, messages: make(chan receivedMessage, 4)}
			progressed := make(chan error, 1)
			go func() {
				h.Subscribe(independent, "fast")
				h.PSubscribe(independent, "f*")
				channels, patterns := h.GetSubscriptionCount(independent.GetID())
				if channels != 1 || patterns != 1 {
					progressed <- fmt.Errorf("subscription counts %d,%d", channels, patterns)
					return
				}
				if count := h.countSubscribers("fast"); count != 2 {
					progressed <- fmt.Errorf("publish subscriber count %d", count)
					return
				}
				routes := h.snapshotTargets(delivery{channel: "fast", known: true})
				if len(routes) != 2 {
					progressed <- fmt.Errorf("publish routing has %d targets", len(routes))
					return
				}
				for _, route := range routes {
					h.sendTarget(route, delivery{channel: "fast", message: "independent"})
				}
				h.Unsubscribe(independent, "fast")
				h.PUnsubscribe(independent, "f*")
				progressed <- nil
			}()
			select {
			case err := <-progressed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("stalled subscriber blocked unrelated subscription or publish routing")
			}
			awaitMessage(t, independent, "fast", "independent")
			awaitMessage(t, independent, "f*\x00fast", "independent")
			select {
			case <-removed:
				t.Fatal("removal returned before its in-flight send completed")
			default:
			}
			close(stalled.release)
			released = true
			select {
			case <-removed:
			case <-time.After(time.Second):
				t.Fatal("removal did not finish after send released")
			}
		})
	}
}

func waitConsumed(t *testing.T, pending *pendingDelivery) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for pending.remaining.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("client output did not release message")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestReferenceBudgetAdmitsInNotificationOrder(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	h.released = make(chan *pendingDelivery, dispatchCapacity)
	h.enableOutputs()
	blocked := &blockingSubscriber{entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}
	h.Subscribe(blocked, "a")
	b := &recordingSubscriber{id: 2, messages: make(chan receivedMessage, 2)}
	h.Subscribe(b, "b")
	c := &recordingSubscriber{id: 3, messages: make(chan receivedMessage, 2)}
	h.Subscribe(c, "c")
	observer := &recordingSubscriber{id: 4, messages: make(chan receivedMessage, 2)}
	h.Subscribe(observer, "marker")
	h.wg.Add(1)
	go h.dispatchLoop()
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
		h.Stop()
	}()
	take := func() *pendingDelivery {
		t.Helper()
		select {
		case p := <-h.fetchJobs:
			return p
		case <-time.After(time.Second):
			t.Fatal("reference not admitted")
			return nil
		}
	}
	h.incoming <- delivery{channel: "a", known: true, id: 1, size: 32 << 20}
	a := take()
	a.message = "a"
	h.fetched <- a
	<-blocked.entered
	h.incoming <- delivery{channel: "b", known: true, id: 2, size: 64 << 20}
	h.incoming <- delivery{channel: "c", known: true, id: 3, size: 1}
	h.incoming <- delivery{channel: "marker", known: true, message: "inline proceeds"}
	awaitMessage(t, observer, "marker", "inline proceeds")
	select {
	case p := <-h.fetchJobs:
		t.Fatalf("reference %d bypassed earlier byte reservation", p.id)
	case <-time.After(20 * time.Millisecond):
	}
	close(blocked.release)
	released = true
	next := take()
	if next.id != 2 {
		t.Fatalf("admitted %d before2", next.id)
	}
	next.skip = true
	h.fetched <- next
	last := take()
	if last.id != 3 {
		t.Fatalf("admitted %d before3", last.id)
	}
	last.message = "c"
	h.fetched <- last
	awaitMessage(t, c, "c", "c")
	waitConsumed(t, a)
	waitConsumed(t, next)
	waitConsumed(t, last)
	// The same budget handles a huge channel with zero body without allocating
	// that channel in the test; after a skipped fetch its permit is reusable.
	h.incoming <- delivery{channel: "c", known: true, id: 4, channelSize: 512 << 20}
	huge := take()
	huge.skip = true
	h.fetched <- huge
	waitConsumed(t, huge)
	h.incoming <- delivery{channel: "c", known: true, id: 5, size: 1}
	after := take()
	after.message = "after"
	h.fetched <- after
	awaitMessage(t, c, "c", "after")
}

type disconnectRecordingSubscriber struct {
	recordingSubscriber
	disconnected chan struct{}
}

func (sub *disconnectRecordingSubscriber) DisconnectPubSub() {
	select {
	case <-sub.disconnected:
	default:
		close(sub.disconnected)
	}
}
func TestOutputOverflowDisconnectsOnlyItsClient(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	h.released = make(chan *pendingDelivery, dispatchCapacity)
	h.enableOutputs()
	slow := &disconnectRecordingSubscriber{recordingSubscriber: recordingSubscriber{id: 1, messages: make(chan receivedMessage, 1)}, disconnected: make(chan struct{})}
	h.Subscribe(slow, "slow")
	healthy := &recordingSubscriber{id: 2, messages: make(chan receivedMessage, 2)}
	h.Subscribe(healthy, "healthy")
	h.wg.Add(1)
	go h.dispatchLoop()
	defer h.Stop()
	h.incoming <- delivery{channel: "slow", known: true, id: 1, channelSize: 512 << 20}
	var pending *pendingDelivery
	select {
	case pending = <-h.fetchJobs:
	case <-time.After(time.Second):
		t.Fatal("oversized entry not admitted")
	}
	h.incoming <- delivery{channel: "slow", known: true, message: "exceeds sole oversized exception"}
	h.incoming <- delivery{channel: "healthy", known: true, message: "still delivered"}
	awaitMessage(t, healthy, "healthy", "still delivered")
	select {
	case <-slow.disconnected:
	case <-time.After(time.Second):
		t.Fatal("overflowing client was not disconnected")
	}
	pending.skip = true
	h.fetched <- pending
	waitConsumed(t, pending)
}

type disconnectBlockingSubscriber struct {
	blockingSubscriber
	unblock     sync.Once
	disconnects int
}

func (sub *disconnectBlockingSubscriber) DisconnectPubSub() {
	sub.disconnects++
	sub.unblock.Do(func() { close(sub.release) })
}
func TestStopInterruptsRetiredOutputWriter(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	h.released = make(chan *pendingDelivery, dispatchCapacity)
	h.enableOutputs()
	sub := &disconnectBlockingSubscriber{blockingSubscriber: blockingSubscriber{entered: make(chan struct{}), release: make(chan struct{}), completed: make(chan struct{})}}
	h.Subscribe(sub, "retired")
	output := h.output(sub.GetID())
	h.wg.Add(1)
	go h.dispatchLoop()
	defer func() { sub.unblock.Do(func() { close(sub.release) }); h.Stop() }()
	h.incoming <- delivery{channel: "retired", known: true, message: "blocked writer"}
	select {
	case <-sub.entered:
	case <-time.After(time.Second):
		t.Fatal("mailbox writer did not start")
	}
	removed := make(chan struct{})
	go func() { h.RemoveSubscriber(sub.GetID()); close(removed) }()
	// Synchronize the precise lifecycle race: removal has closed/detached its
	// mailbox, but the in-flight write still prevents removal from returning.
	select {
	case <-output.stop:
	case <-time.After(time.Second):
		t.Fatal("mailbox was not retired")
	}
	select {
	case <-removed:
		t.Fatal("removal returned while its writer was blocked")
	default:
	}
	stopped := make(chan struct{})
	go func() { h.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop lost the retired writer's connection interrupt")
	}
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("removal stayed blocked after Stop")
	}
	if sub.disconnects != 1 {
		t.Fatalf("connection interrupt called %d times", sub.disconnects)
	}
}
