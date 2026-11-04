package pubsub

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mnorrsken/postkeys/internal/storage"
)

// Bound both messages and subscriber routes: one pattern broadcast can target
// many clients. No subscriber gets a goroutine or an unbounded pending queue.
const dispatchCapacity = 256
const dispatchRouteCapacity = 4096
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
	targets   []deliveryTarget
	ready     bool
	remaining int
}
type reservation struct {
	message *pendingDelivery
	targets []deliveryTarget
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
	channel, message, _, err := decodeBroadcast(payload)
	return delivery{channel: channel, message: message, known: true}, err
}

// Snapshot routes at notification intake. An oversized channel's hash identifies
// its exact subscribers; reserve all pattern clients until metadata is fetched.
// This conservative reservation preserves each client's observable Redis order.
func (h *Hub) snapshotTargets(item delivery, limit int) ([]deliveryTarget, bool) {
	targets := make([]deliveryTarget, 0)
	appendTarget := func(target deliveryTarget) bool {
		if len(targets) >= limit {
			return false
		}
		targets = append(targets, target)
		return true
	}
	h.mu.RLock()
	if item.known {
		for _, sub := range h.subscriptions[item.channel] {
			if !appendTarget(deliveryTarget{sub: sub}) {
				h.mu.RUnlock()
				return nil, false
			}
		}
	} else {
		for channel, subs := range h.subscriptions {
			if channelKey(channel) != item.key {
				continue
			}
			for _, sub := range subs {
				if !appendTarget(deliveryTarget{sub: sub}) {
					h.mu.RUnlock()
					return nil, false
				}
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
			if !appendTarget(deliveryTarget{sub: sub, pattern: pattern, isPattern: true}) {
				return nil, false
			}
		}
	}
	return targets, true
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
	queues := make(map[uint64][]reservation)
	waiting := make([]*pendingDelivery, 0, dispatchCapacity)
	messages, routes := 0, 0
	var bodyBytes int64
	flush := func(id uint64) {
		queue := queues[id]
		for len(queue) > 0 && queue[0].message.ready {
			head := queue[0]
			for _, target := range head.targets {
				h.sendTarget(target, head.message.delivery)
			}
			routes -= len(head.targets)
			head.message.remaining--
			if head.message.remaining == 0 {
				messages--
				bodyBytes -= head.message.size + head.message.channelSize
			}
			queue[0] = reservation{}
			queue = queue[1:]
		}
		if len(queue) == 0 {
			delete(queues, id)
		} else {
			queues[id] = queue
		}
	}
	for {
		var work chan *pendingDelivery
		var next *pendingDelivery
		if len(waiting) > 0 {
			work = h.fetchJobs
			next = waiting[0]
		}
		select {
		case <-h.ctx.Done():
			return
		case work <- next:
			waiting[0] = nil
			waiting = waiting[1:]
		case item := <-h.incoming:
			targets, ok := h.snapshotTargets(item, dispatchRouteCapacity-routes)
			if !ok {
				log.Printf("Warning: pub/sub subscriber reservation capacity reached; dropping notification")
				continue
			}
			if len(targets) == 0 {
				continue
			}
			if messages >= dispatchCapacity {
				log.Printf("Warning: pub/sub pending capacity reached; dropping notification")
				continue
			}
			// Reserve channel and body bytes before any SELECT. One oversized
			// reference is allowed only with no other reserved bytes.
			referenceBytes := item.size + item.channelSize
			if referenceBytes > 0 && bodyBytes > 0 && referenceBytes > dispatchBodyCapacity-bodyBytes {
				log.Printf("Warning: pub/sub referenced byte capacity reached; dropping notification")
				continue
			}
			bodyBytes += referenceBytes
			pending := &pendingDelivery{delivery: item, targets: targets, ready: item.id == 0}
			grouped := make(map[uint64][]deliveryTarget)
			for _, target := range targets {
				id := target.sub.GetID()
				grouped[id] = append(grouped[id], target)
			}
			pending.remaining = len(grouped)
			messages++
			routes += len(targets)
			for id, clientTargets := range grouped {
				queues[id] = append(queues[id], reservation{message: pending, targets: clientTargets})
			}
			if pending.ready {
				for id := range grouped {
					flush(id)
				}
			} else {
				waiting = append(waiting, pending)
			}
		case pending := <-h.fetched:
			pending.ready = true
			for _, target := range pending.targets {
				flush(target.sub.GetID())
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
