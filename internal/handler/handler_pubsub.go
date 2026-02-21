package handler

import (
	"context"
	"time"

	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/pubsub"
	"github.com/mnorrsken/postkeys/internal/resp"
)

// PubSubClientState interface for pub/sub-capable client state
type PubSubClientState interface {
	ClientState
	GetID() uint64
	EnterPubSubMode()
	ExitPubSubMode()
	InPubSubMode() bool
	SendPubSubMessage(msgType, channel, payload string) error
}

// HandleSubscribe handles SUBSCRIBE command
func (h *Handler) HandleSubscribe(hub *pubsub.Hub, client PubSubClientState, channels []string) []resp.Value {
	start := time.Now()

	if len(channels) == 0 {
		return []resp.Value{resp.ErrWrongArgs("subscribe")}
	}

	// Enter pub/sub mode
	client.EnterPubSubMode()

	// Subscribe to channels
	counts := hub.Subscribe(client, channels...)

	// Build responses for each channel
	responses := make([]resp.Value, len(channels))
	for i, channel := range channels {
		responses[i] = pubsub.BuildSubscribeResponse(channel, counts[i])
	}

	duration := time.Since(start)
	metrics.RecordCommand("SUBSCRIBE", duration, false)

	return responses
}

// HandleUnsubscribe handles UNSUBSCRIBE command
func (h *Handler) HandleUnsubscribe(hub *pubsub.Hub, client PubSubClientState, channels []string) []resp.Value {
	start := time.Now()

	// Get the list of channels to unsubscribe from
	// If no channels specified, get current subscriptions
	unsubChannels := channels
	if len(unsubChannels) == 0 {
		// Get the channels the client is currently subscribed to
		unsubChannels = hub.GetSubscribedChannels(client.GetID())
	}

	// Unsubscribe from channels
	counts := hub.Unsubscribe(client, channels...)

	// If no channels to unsubscribe from, return single response
	if len(unsubChannels) == 0 {
		responses := []resp.Value{pubsub.BuildUnsubscribeResponse("", 0)}
		client.ExitPubSubMode()
		duration := time.Since(start)
		metrics.RecordCommand("UNSUBSCRIBE", duration, false)
		return responses
	}

	// Build responses for each channel
	responses := make([]resp.Value, len(unsubChannels))
	for i, channel := range unsubChannels {
		count := 0
		if i < len(counts) {
			count = counts[i]
		}
		responses[i] = pubsub.BuildUnsubscribeResponse(channel, count)
	}

	// Check if we should exit pub/sub mode
	channelCount, patternCount := hub.GetSubscriptionCount(client.GetID())
	if channelCount == 0 && patternCount == 0 {
		client.ExitPubSubMode()
	}

	duration := time.Since(start)
	metrics.RecordCommand("UNSUBSCRIBE", duration, false)

	return responses
}

// HandlePSubscribe handles PSUBSCRIBE command
func (h *Handler) HandlePSubscribe(hub *pubsub.Hub, client PubSubClientState, patterns []string) []resp.Value {
	start := time.Now()

	if len(patterns) == 0 {
		return []resp.Value{resp.ErrWrongArgs("psubscribe")}
	}

	// Enter pub/sub mode
	client.EnterPubSubMode()

	// Subscribe to patterns
	counts := hub.PSubscribe(client, patterns...)

	// Build responses for each pattern
	responses := make([]resp.Value, len(patterns))
	for i, pattern := range patterns {
		responses[i] = pubsub.BuildPSubscribeResponse(pattern, counts[i])
	}

	duration := time.Since(start)
	metrics.RecordCommand("PSUBSCRIBE", duration, false)

	return responses
}

// HandlePUnsubscribe handles PUNSUBSCRIBE command
func (h *Handler) HandlePUnsubscribe(hub *pubsub.Hub, client PubSubClientState, patterns []string) []resp.Value {
	start := time.Now()

	// Unsubscribe from patterns
	counts := hub.PUnsubscribe(client, patterns...)

	if len(patterns) == 0 {
		// No patterns were subscribed
		responses := []resp.Value{pubsub.BuildPUnsubscribeResponse("", 0)}
		// Check if we should exit pub/sub mode
		channelCount, patternCount := hub.GetSubscriptionCount(client.GetID())
		if channelCount == 0 && patternCount == 0 {
			client.ExitPubSubMode()
		}
		return responses
	}

	// Build responses for each pattern
	responses := make([]resp.Value, len(patterns))
	for i, pattern := range patterns {
		responses[i] = pubsub.BuildPUnsubscribeResponse(pattern, counts[i])
	}

	// Check if we should exit pub/sub mode
	channelCount, patternCount := hub.GetSubscriptionCount(client.GetID())
	if channelCount == 0 && patternCount == 0 {
		client.ExitPubSubMode()
	}

	duration := time.Since(start)
	metrics.RecordCommand("PUNSUBSCRIBE", duration, false)

	return responses
}

// HandlePublish handles PUBLISH command
func (h *Handler) HandlePublish(ctx context.Context, hub *pubsub.Hub, channel, message string) resp.Value {
	start := time.Now()

	count, err := hub.Publish(ctx, channel, message)
	if err != nil {
		duration := time.Since(start)
		metrics.RecordCommand("PUBLISH", duration, true)
		return resp.Err("ERR " + err.Error())
	}

	duration := time.Since(start)
	metrics.RecordCommand("PUBLISH", duration, false)

	return resp.Int(count)
}
