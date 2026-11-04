package pubsub

import (
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
