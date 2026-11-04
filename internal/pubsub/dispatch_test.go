package pubsub

import (
	"fmt"
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
func TestDispatchStopWithSaturatedWorkQueue(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10)}
	h.Subscribe(sub, "blocked")
	h.wg.Add(1)
	go h.dispatchLoop()
	for i := 0; i < dispatchCapacity+8; i++ {
		select {
		case h.incoming <- delivery{key: channelKey("blocked"), channel: "blocked", known: true, id: 1}:
		case <-time.After(time.Second):
			t.Fatal("dispatcher blocked on fetch queue")
		}
	}
	done := make(chan struct{})
	go func() { h.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked with saturated queues")
	}
}

func TestDispatchBoundsSubscriberReservations(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	first := &recordingSubscriber{id: 1, messages: make(chan receivedMessage, 2)}
	h.Subscribe(first, "broadcast", "marker")
	for i := 1; i <= dispatchRouteCapacity; i++ {
		h.Subscribe(&recordingSubscriber{id: uint64(i + 1)}, "broadcast")
	}
	h.wg.Add(1)
	go h.dispatchLoop()
	defer h.Stop()
	h.incoming <- delivery{channel: "broadcast", known: true, id: 1}
	h.incoming <- delivery{channel: "marker", known: true, message: "still responsive"}
	awaitMessage(t, first, "marker", "still responsive")
	select {
	case <-h.fetchJobs:
		t.Fatal("over-capacity notification was retained")
	default:
	}
}

func TestDispatchBodyBudgetAndOversizedAdmission(t *testing.T) {
	h := NewHub(nil, "")
	h.incoming = make(chan delivery, 256)
	h.fetchJobs = make(chan *pendingDelivery)
	h.fetched = make(chan *pendingDelivery)
	sub := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 1}
	h.Subscribe(sub, "body")
	observer := &recordingSubscriber{messages: make(chan receivedMessage, 10), id: 2}
	h.Subscribe(observer, "marker")
	h.wg.Add(1)
	go h.dispatchLoop()
	defer h.Stop()
	reference := func(channel string, size int64) {
		h.incoming <- delivery{channel: channel, known: true, id: 1, size: size}
	}
	marker := func() {
		h.incoming <- delivery{channel: "marker", known: true, message: "processed"}
		awaitMessage(t, observer, "marker", "processed")
	}
	take := func() *pendingDelivery {
		t.Helper()
		select {
		case pending := <-h.fetchJobs:
			return pending
		case <-time.After(time.Second):
			t.Fatal("expected admitted reference")
			return nil
		}
	}
	none := func() {
		t.Helper()
		select {
		case <-h.fetchJobs:
			t.Fatal("over-budget or uninterested reference was retained")
		case <-time.After(20 * time.Millisecond):
		}
	}
	complete := func(pending *pendingDelivery) { pending.skip = true; h.fetched <- pending }
	// No allocation scales with these declared body sizes. An uninterested
	// maximum-size reference must not consume the byte reservation budget.
	reference("uninterested", 512<<20)
	marker()
	none()
	reference("body", 512<<20)
	oversized := take()
	reference("body", 1)
	marker()
	none()
	complete(oversized)
	// A huge channel with an empty body consumes the same byte budget; no
	// 512 MiB string is allocated to exercise this admission rule.
	h.incoming <- delivery{channel: "body", known: true, id: 1, channelSize: 512 << 20}
	hugeChannel := take()
	reference("body", 1)
	marker()
	none()
	complete(hugeChannel)
	reference("body", dispatchBodyCapacity)
	normal := take()
	// Aggregate normal references obey the cap; a reference declaring zero
	// channel and body bytes consumes no additional byte reservation.
	reference("body", 1)
	reference("body", 0)
	zero := take()
	marker()
	none()
	complete(zero)
	complete(normal)
	reference("body", 1)
	complete(take())
	h.incoming <- delivery{channel: "body", known: true, message: "released"}
	awaitMessage(t, sub, "body", "released")
}

func TestDelayedReferenceInvalidatedByUnsubscribe(t *testing.T) {
	for _, mode := range []string{"exact", "pattern", "remove-all"} {
		for _, resubscribe := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resubscribe=%t", mode, resubscribe), func(t *testing.T) {
				h := NewHub(nil, "")
				h.incoming = make(chan delivery, 256)
				h.fetchJobs = make(chan *pendingDelivery)
				h.fetched = make(chan *pendingDelivery)
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
			targets, ok := h.snapshotTargets(delivery{channel: "route", known: true}, 10)
			if !ok || len(targets) != 1 {
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
			targets, _ := h.snapshotTargets(delivery{channel: "route", known: true}, 10)
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
