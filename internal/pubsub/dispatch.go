package pubsub

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mnorrsken/postkeys/internal/storage"
)

// Bound unresolved references and shared fetched bytes. Inline notifications
// backpressure at ingress; ordinary fanout is bounded per connection, never by
// a global route limit. References acquire bytes strictly in notification FIFO.
const dispatchCapacity = 256
const dispatchBodyCapacity int64 = 64 << 20

type delivery struct {
	key, channel, message string
	id                    int64
	size                  int64
	channelSize           int64
	known, skip           bool
}
type deliveryTarget struct {
	sub       *subscription
	pattern   string
	isPattern bool
}

func (target deliveryTarget) matches(channel string) bool {
	return !target.isPattern || matchPattern(target.pattern, channel)
}

type pendingDelivery struct {
	delivery
	targets                      []deliveryTarget
	ready                        chan struct{}
	remaining                    atomic.Int64
	reserved, complete, finished bool // dispatcher-owned state
}

func (pending *pendingDelivery) release(h *Hub) {
	if pending.remaining.Add(-1) == 0 && pending.id != 0 {
		// At most dispatchCapacity references exist; one completion per reference
		// fits even during shutdown. Never wait on a stopped dispatcher.
		select {
		case h.released <- pending:
		case <-h.ctx.Done():
		}
	}
}

func decodeDelivery(payload string) (delivery, error) {
	if strings.HasPrefix(payload, referencePrefix) {
		ref, err := decodeReference(payload)
		item := delivery{key: ref.Key, id: ref.ID, known: ref.Channel != nil}
		if ref.Size != nil {
			item.size = *ref.Size
		}
		if ref.ChannelSize != nil {
			item.channelSize = *ref.ChannelSize
		}
		if ref.Channel != nil {
			item.channel = *ref.Channel
		}
		return item, err
	}
	channel, message, err := decodeBroadcast(payload)
	return delivery{channel: channel, message: message, known: true}, err
}

// Snapshot routes at notification intake. An oversized channel's hash identifies
// its exact subscribers; reserve all pattern clients until metadata is fetched.
// This conservative reservation preserves each client's observable Redis order.
func (h *Hub) snapshotTargets(item delivery) []deliveryTarget {
	targets := make([]deliveryTarget, 0)
	h.mu.RLock()
	if item.known {
		for _, sub := range h.subscriptions[item.channel] {
			targets = append(targets, deliveryTarget{sub: sub})
		}
	} else {
		for channel := range h.channelsByKey[item.key] {
			for _, sub := range h.subscriptions[channel] {
				targets = append(targets, deliveryTarget{sub: sub})
			}
		}
	}
	h.mu.RUnlock()
	h.patternMu.RLock()
	defer h.patternMu.RUnlock()
	for pattern, subs := range h.patterns {
		if item.known && !matchPattern(pattern, item.channel) {
			continue
		}
		for _, sub := range subs {
			targets = append(targets, deliveryTarget{sub: sub, pattern: pattern, isPattern: true})
		}
	}

	return targets
}
func (h *Hub) sendTarget(target deliveryTarget, item delivery) {
	if item.skip || !target.matches(item.channel) {
		return
	}
	// Serialize validity and the send only on this subscription generation.
	// Removal waits on the same token after releasing all global map locks,
	// preserving the acknowledgement boundary without blocking unrelated clients.
	target.sub.mu.RLock()
	defer target.sub.mu.RUnlock()
	if !target.sub.active {
		return
	}
	kind, channel := "message", item.channel
	if target.isPattern {
		kind = "pmessage"
		channel = target.pattern + "\x00" + item.channel
	}
	if err := target.sub.SendPubSubMessage(kind, channel, item.message); err != nil && h.debug {
		log.Printf("[DEBUG] Failed to deliver pub/sub message to subscriber %d: %v", target.sub.GetID(), err)
	}
}
func (h *Hub) dispatchLoop() {
	defer h.wg.Done()
	waiting := make([]*pendingDelivery, 0)
	references := 0
	var reservedBytes int64
	finish := func(pending *pendingDelivery) {
		if pending.finished {
			return
		}
		pending.finished = true
		references--
		if pending.reserved {
			reservedBytes -= pending.size + pending.channelSize
		}
		pending.message = ""
		pending.channel = ""
	}
	for {
		for len(waiting) > 0 && waiting[0].finished {
			waiting[0] = nil
			waiting = waiting[1:]
		}
		var work chan *pendingDelivery
		var next *pendingDelivery
		if len(waiting) > 0 {
			candidate := waiting[0]
			bytes := candidate.size + candidate.channelSize
			if candidate.remaining.Load() > 0 && (reservedBytes == 0 || bytes <= dispatchBodyCapacity-reservedBytes) {
				work = h.fetchJobs
				next = candidate
			}
		}
		var incoming <-chan delivery = h.incoming
		if references >= dispatchCapacity {
			incoming = nil
		}
		select {
		case <-h.ctx.Done():
			return
		case work <- next:
			next.reserved = true
			reservedBytes += next.size + next.channelSize
			waiting[0] = nil
			waiting = waiting[1:]
		case item := <-incoming:
			targets := h.snapshotTargets(item)
			if len(targets) == 0 {
				continue
			}
			grouped := make(map[uint64][]deliveryTarget)
			for _, target := range targets {
				id := target.sub.GetID()
				grouped[id] = append(grouped[id], target)
			}
			pending := &pendingDelivery{delivery: item, targets: targets, ready: make(chan struct{})}
			pending.remaining.Store(int64(len(grouped)))
			if item.id != 0 {
				references++
				waiting = append(waiting, pending)
			} else {
				close(pending.ready)
			}
			for id, routes := range grouped {
				bytes := int64(len(item.channel) + len(item.message))
				if item.id != 0 {
					bytes = item.size + item.channelSize
				}
				bytes += 64 + int64(len(routes))*40
				output := h.output(id)
				if output == nil || !output.enqueue(outputEntry{message: pending, targets: routes, bytes: bytes}) {
					pending.release(h)
				}
			}
		case pending := <-h.released:
			if !pending.reserved || pending.complete {
				finish(pending)
			}
		case pending := <-h.fetched:
			pending.complete = true
			close(pending.ready)
			if pending.remaining.Load() == 0 {
				finish(pending)
			}
		}
	}
}
func (h *Hub) fetchLoop() {
	defer h.wg.Done()
	for {
		select {
		case <-h.ctx.Done():
			return
		case pending := <-h.fetchJobs:
			ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
			var err error
			if !pending.known {
				pending.channel, err = storage.PubSubChannel(ctx, h.fetchPool, pending.id)
			}
			if err == nil && int64(len(pending.channel)) != pending.channelSize {
				err = fmt.Errorf("reference channel length mismatch: got %d, want %d", len(pending.channel), pending.channelSize)
			}
			interested := false
			if err == nil && channelKey(pending.channel) == pending.key {
				for _, target := range pending.targets {
					if target.matches(pending.channel) {
						interested = true
						break
					}
				}
			}
			if interested {
				pending.message, err = storage.PubSubMessage(ctx, h.fetchPool, pending.id)
				if err == nil && int64(len(pending.message)) != pending.size {
					err = fmt.Errorf("reference body length mismatch: got %d, want %d", len(pending.message), pending.size)
				}
			} else {
				pending.skip = true
			}
			cancel()
			if err != nil {
				pending.skip = true
				log.Printf("Warning: failed to fetch pub/sub message %d: %v", pending.id, err)
			}
			select {
			case h.fetched <- pending:
			case <-h.ctx.Done():
				return
			}
		}
	}
}
