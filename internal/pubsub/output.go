package pubsub

import (
	"log"
	"sync"
)

// Match Redis's policy: bounded client output buffers disconnect only the
// overflowing connection. Storage grows lazily, rather than allocating a full
// buffer for every subscribed client. In-flight entries count toward both caps.
const outputEntries = 8192
const outputBytes int64 = 32 << 20

type outputEntry struct {
	message *pendingDelivery
	targets []deliveryTarget
	bytes   int64
}
type clientOutput struct {
	hub            *Hub
	sub            Subscriber
	mu             sync.Mutex
	queue          []outputEntry
	head           int
	bytes          int64
	entries        int
	closed         bool
	disconnectOnce sync.Once
	wake           chan struct{}
	stop           chan struct{}
}

func (h *Hub) enableOutputs() {
	h.outputMu.Lock()
	h.outputsRunning = true
	h.outputMu.Unlock()
	// Also support subscriptions registered before Start.
	h.mu.RLock()
	subs := make(map[uint64]Subscriber)
	for _, clients := range h.subscriptions {
		for id, sub := range clients {
			subs[id] = sub.Subscriber
		}
	}
	h.mu.RUnlock()
	h.patternMu.RLock()
	for _, clients := range h.patterns {
		for id, sub := range clients {
			subs[id] = sub.Subscriber
		}
	}
	h.patternMu.RUnlock()
	for _, sub := range subs {
		h.ensureOutput(sub)
	}
}
func (h *Hub) ensureOutput(sub Subscriber) {
	h.outputMu.Lock()
	defer h.outputMu.Unlock()
	if !h.outputsRunning || h.outputsStopped || h.outputs[sub.GetID()] != nil {
		return
	}
	output := &clientOutput{hub: h, sub: sub, wake: make(chan struct{}, 1), stop: make(chan struct{})}
	h.outputs[sub.GetID()] = output
	h.liveOutputs[output] = true
	h.wg.Add(1)
	go output.run()
}
func (h *Hub) output(subID uint64) *clientOutput {
	h.outputMu.Lock()
	defer h.outputMu.Unlock()
	return h.outputs[subID]
}
func disconnectSubscriber(sub Subscriber) {
	// Production clients close only the connection here. Their connection handler
	// owns subscription cleanup; never re-enter it while a token send gate is held.
	if closer, ok := sub.(interface{ DisconnectPubSub() }); ok {
		closer.DisconnectPubSub()
	}
}
func (h *Hub) removeOutput(id uint64) {
	h.outputMu.Lock()
	output := h.outputs[id]
	delete(h.outputs, id)
	h.outputMu.Unlock()
	if output != nil {
		output.close(false)
	}
}
func (output *clientOutput) enqueue(entry outputEntry) bool {
	output.mu.Lock()
	overflow := output.entries >= outputEntries || (output.entries > 0 && entry.bytes > outputBytes-output.bytes)
	if output.closed || overflow {
		output.mu.Unlock()
		if overflow {
			log.Printf("Pub/sub client %d exceeded its output buffer; disconnecting", output.sub.GetID())
			output.close(true)
		}
		return false
	}
	if output.head > 0 && len(output.queue) == cap(output.queue) {
		remaining := copy(output.queue, output.queue[output.head:])
		clear(output.queue[remaining:])
		output.queue = output.queue[:remaining]
		output.head = 0
	}
	if output.queue == nil {
		output.queue = make([]outputEntry, 0, 8)
	}
	output.queue = append(output.queue, entry)
	output.bytes += entry.bytes
	output.entries++
	output.mu.Unlock()
	select {
	case output.wake <- struct{}{}:
	default:
	}
	return true
}
func (output *clientOutput) close(disconnect bool) {
	output.mu.Lock()
	if output.closed {
		output.mu.Unlock()
		if disconnect {
			output.disconnectOnce.Do(func() { disconnectSubscriber(output.sub) })
		}
		return
	}
	output.closed = true
	close(output.stop)
	queued := output.queue[output.head:]
	output.head = 0
	output.queue = nil
	for _, entry := range queued {
		output.bytes -= entry.bytes
		output.entries--
	}
	output.mu.Unlock()
	if disconnect {
		output.disconnectOnce.Do(func() { disconnectSubscriber(output.sub) })
	}
	for _, entry := range queued {
		entry.message.release(output.hub)
	}
}
func (output *clientOutput) run() {
	defer output.hub.wg.Done()
	defer func() {
		output.hub.outputMu.Lock()
		delete(output.hub.liveOutputs, output)
		output.hub.outputMu.Unlock()
	}()
	for {
		output.mu.Lock()
		if output.head == len(output.queue) {
			output.queue = output.queue[:0]
			output.head = 0
			closed := output.closed
			output.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-output.wake:
				continue
			case <-output.stop:
				return
			case <-output.hub.ctx.Done():
				output.close(true)
				return
			}
		}
		entry := output.queue[output.head]
		output.queue[output.head] = outputEntry{}
		output.head++
		output.mu.Unlock()
		select {
		case <-entry.message.ready:
			for _, target := range entry.targets {
				output.hub.sendTarget(target, entry.message.delivery)
			}
		case <-output.stop:
		case <-output.hub.ctx.Done():
		}
		output.mu.Lock()
		output.bytes -= entry.bytes
		output.entries--
		output.mu.Unlock()
		entry.message.release(output.hub)
		select {
		case <-output.stop:
			return
		default:
		}
	}
}
