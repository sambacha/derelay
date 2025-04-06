package actor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/ports"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RedisActor handles all Redis operations
type RedisActor struct {
	*BaseActor
	client          ports.RedisClient
	config          *config.RedisConfig
	pubSubActor     Actor
	streamReader    Actor
	streamWriter    Actor
	subscriptions   map[string][]Actor // channel -> interested actors
	instanceID      string
	subscriptionsMu sync.RWMutex
}

// NewRedisActor creates a new Redis actor
func NewRedisActor(ctx ActorContext, client ports.RedisClient, config *config.RedisConfig, instanceID string) *RedisActor {
	actor := &RedisActor{
		BaseActor:     NewBaseActor(ctx, WithMailboxSize(512)),
		client:        client,
		config:        config,
		subscriptions: make(map[string][]Actor),
		instanceID:    instanceID,
	}

	actor.initializeBehaviors()
	return actor
}

// initializeBehaviors sets up message handlers
func (a *RedisActor) initializeBehaviors() {
	// Handle Redis publish
	a.Become(RedisPublishType, func(msg Message) {
		pubMsg := msg.(RedisPublish)
		a.handlePublish(pubMsg)
	})

	// Handle Redis subscribe
	a.Become(RedisSubscribeType, func(msg Message) {
		subMsg := msg.(RedisSubscribe)
		a.handleSubscribe(subMsg.Channels)
	})

	// Handle Redis unsubscribe
	a.Become(RedisUnsubscribeType, func(msg Message) {
		unsubMsg := msg.(RedisUnsubscribe)
		a.handleUnsubscribe(unsubMsg.Channels)
	})

	// Handle Redis XAdd (stream message)
	a.Become(RedisXAddType, func(msg Message) {
		xaddMsg := msg.(RedisXAdd)
		a.handleXAdd(xaddMsg)
	})

	// Handle Redis XReadGroup (stream read)
	a.Become(RedisXReadGroupType, func(msg Message) {
		xreadMsg := msg.(RedisXReadGroup)
		a.handleXReadGroup(xreadMsg)
	})

	// Handle Redis XAck (stream ack)
	a.Become(RedisXAckType, func(msg Message) {
		xackMsg := msg.(RedisXAck)
		a.handleXAck(xackMsg)
	})

	// Handle Redis client state update
	a.Become(RedisClientStateUpdateType, func(msg Message) {
		updateMsg := msg.(RedisClientStateUpdate)
		a.handleClientStateUpdate(updateMsg)
	})

	// Handle Redis client state delete
	a.Become(RedisClientStateDeleteType, func(msg Message) {
		deleteMsg := msg.(RedisClientStateDelete)
		a.handleClientStateDelete(deleteMsg)
	})

	// System messages
	a.Become(StartMessage{}.MessageType(), func(msg Message) {
		a.startChildActors()
	})

	a.Become(StopMessage{}.MessageType(), func(msg Message) {
		a.stopChildActors()
	})

	a.Become(TerminatedMessage{}.MessageType(), func(msg Message) {
		terminated := msg.(TerminatedMessage)
		a.handleTerminated(terminated)
	})
}

// Start initializes the actor
func (a *RedisActor) Start() error {
	log.Info("Starting RedisActor", zap.String("instanceID", a.instanceID))
	return a.BaseActor.Start()
}

// Stop terminates the actor and all resources
func (a *RedisActor) Stop() error {
	log.Info("Stopping RedisActor", zap.String("instanceID", a.instanceID))
	if a.client != nil {
		if err := a.client.Close(); err != nil {
			log.Error("Error closing Redis client", err)
		}
	}
	return a.BaseActor.Stop()
}

// startChildActors creates and starts child actors
func (a *RedisActor) startChildActors() {
	log.Debug("Starting RedisActor child actors")

	// Start PubSub actor
	pubSubProps := ActorPropsFunc(func(ctx ActorContext) Actor {
		return NewRedisPubSubActor(ctx, a.client, a.config, a.context.Self())
	})

	// Start stream reader actor
	streamReaderProps := ActorPropsFunc(func(ctx ActorContext) Actor {
		return NewRedisStreamReaderActor(ctx, a.client, a.config, a.context.Self())
	})

	// Start stream writer actor
	streamWriterProps := ActorPropsFunc(func(ctx ActorContext) Actor {
		return NewRedisStreamWriterActor(ctx, a.client, a.config, a.context.Self())
	})

	// Start the child actors
	var err error
	a.pubSubActor, err = a.context.SpawnChild(pubSubProps)
	if err != nil {
		log.Error("Failed to start PubSub actor", err)
		// Continue even if PubSub actor fails to start
	}

	a.streamReader, err = a.context.SpawnChild(streamReaderProps)
	if err != nil {
		log.Error("Failed to start stream reader actor", err)
		// Continue even if stream reader actor fails to start
	}

	a.streamWriter, err = a.context.SpawnChild(streamWriterProps)
	if err != nil {
		log.Error("Failed to start stream writer actor", err)
		// Continue even if stream writer actor fails to start
	}

	// Watch child actors for termination
	if a.pubSubActor != nil {
		a.context.Watch(a.pubSubActor)
	}
	if a.streamReader != nil {
		a.context.Watch(a.streamReader)
	}
	if a.streamWriter != nil {
		a.context.Watch(a.streamWriter)
	}
}

// stopChildActors stops all child actors
func (a *RedisActor) stopChildActors() {
	if a.pubSubActor != nil {
		if err := a.pubSubActor.Stop(); err != nil {
			log.Warn("Error stopping PubSub actor", zap.Error(err))
		}
	}

	if a.streamReader != nil {
		if err := a.streamReader.Stop(); err != nil {
			log.Warn("Error stopping stream reader actor", zap.Error(err))
		}
	}

	if a.streamWriter != nil {
		if err := a.streamWriter.Stop(); err != nil {
			log.Warn("Error stopping stream writer actor", zap.Error(err))
		}
	}
}

// handleTerminated processes termination of child actors
func (a *RedisActor) handleTerminated(msg TerminatedMessage) {
	terminatedActor := msg.Actor

	switch {
	case a.pubSubActor != nil && terminatedActor.ID() == a.pubSubActor.ID():
		log.Warn("PubSub actor terminated unexpectedly", zap.Error(msg.Err))
		a.pubSubActor = nil
		// TODO: Implement retry logic

	case a.streamReader != nil && terminatedActor.ID() == a.streamReader.ID():
		log.Warn("Stream reader actor terminated unexpectedly", zap.Error(msg.Err))
		a.streamReader = nil
		// TODO: Implement retry logic

	case a.streamWriter != nil && terminatedActor.ID() == a.streamWriter.ID():
		log.Warn("Stream writer actor terminated unexpectedly", zap.Error(msg.Err))
		a.streamWriter = nil
		// TODO: Implement retry logic
	}
}

// handlePublish processes a publish message
func (a *RedisActor) handlePublish(msg RedisPublish) {
	// If PubSub actor is available, delegate to it
	if a.pubSubActor != nil {
		a.context.Send(a.pubSubActor, msg)
		return
	}

	// Otherwise, handle directly
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.PublishTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute publish operation
	count, err := a.client.Publish(ctx, msg.Channel, msg.Message).Result()
	if err != nil {
		log.Error("Redis publish error", err, zap.String("channel", msg.Channel))
		return
	}

	log.Debug("Redis publish success",
		zap.String("channel", msg.Channel),
		zap.Int64("subscribers", count))
}

// handleSubscribe processes a subscribe request
func (a *RedisActor) handleSubscribe(channels []string) {
	// If PubSub actor is available, delegate to it
	if a.pubSubActor != nil {
		a.context.Send(a.pubSubActor, RedisSubscribe{Channels: channels})
		return
	}

	log.Warn("Cannot subscribe, PubSub actor not available", zap.Strings("channels", channels))
}

// handleUnsubscribe processes an unsubscribe request
func (a *RedisActor) handleUnsubscribe(channels []string) {
	// If PubSub actor is available, delegate to it
	if a.pubSubActor != nil {
		a.context.Send(a.pubSubActor, RedisUnsubscribe{Channels: channels})
		return
	}

	log.Warn("Cannot unsubscribe, PubSub actor not available", zap.Strings("channels", channels))
}

// handleXAdd processes an XAdd request (add message to stream)
func (a *RedisActor) handleXAdd(msg RedisXAdd) {
	// If stream writer actor is available, delegate to it
	if a.streamWriter != nil {
		a.context.Send(a.streamWriter, msg)
		return
	}

	// Otherwise, handle directly
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.CacheWriteTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XAdd operation
	streamID, err := a.client.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Stream,
		MaxLen: msg.MaxLen,
		Approx: msg.Approx,
		Values: msg.Values,
	}).Result()

	if err != nil {
		log.Error("Redis XAdd error", err, zap.String("stream", msg.Stream))
		return
	}

	log.Debug("Redis XAdd success",
		zap.String("stream", msg.Stream),
		zap.String("id", streamID))

	// Set TTL on the stream if configured
	if a.config.StreamTTLSeconds > 0 {
		ttlDuration := time.Duration(a.config.StreamTTLSeconds) * time.Second
		_, err := a.client.Expire(ctx, msg.Stream, ttlDuration).Result()
		if err != nil {
			log.Warn("Failed to set TTL on stream after XAdd",
				zap.Error(err),
				zap.String("stream", msg.Stream))
		}
	}
}

// handleXReadGroup processes an XReadGroup request (read messages from stream)
func (a *RedisActor) handleXReadGroup(msg RedisXReadGroup) {
	// If stream reader actor is available, delegate to it
	if a.streamReader != nil {
		a.context.Send(a.streamReader, msg)
		return
	}

	// Otherwise, handle directly
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StreamReadTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XReadGroup operation
	streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    msg.Group,
		Consumer: msg.Consumer,
		Streams:  msg.Streams,
		Count:    msg.Count,
		Block:    time.Duration(msg.Block) * time.Millisecond,
	}).Result()

	if err != nil {
		if err != redis.Nil {
			log.Error("Redis XReadGroup error", err,
				zap.String("group", msg.Group),
				zap.String("consumer", msg.Consumer),
				zap.Strings("streams", msg.Streams))
		}
		return
	}

	// Process the results (in a real implementation, this would forward to interested actors)
	for _, stream := range streams {
		log.Debug("Redis XReadGroup success",
			zap.String("stream", stream.Stream),
			zap.Int("messages", len(stream.Messages)))
	}
}

// handleXAck processes an XAck request (acknowledge messages in stream)
func (a *RedisActor) handleXAck(msg RedisXAck) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StateUpdateTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XAck operation
	count, err := a.client.XAck(ctx, msg.Stream, msg.Group, msg.IDs...).Result()
	if err != nil {
		log.Error("Redis XAck error", err,
			zap.String("stream", msg.Stream),
			zap.String("group", msg.Group),
			zap.Strings("ids", msg.IDs))
		return
	}

	log.Debug("Redis XAck success",
		zap.String("stream", msg.Stream),
		zap.String("group", msg.Group),
		zap.Int64("count", count))
}

// handleClientStateUpdate processes a client state update request
func (a *RedisActor) handleClientStateUpdate(msg RedisClientStateUpdate) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StateUpdateTimeoutMs)*time.Millisecond)
	defer cancel()

	clientKey := ClientHashKey(msg.ClientID)

	// Update client state in Redis
	_, err := a.client.HSet(ctx, clientKey, msg.Fields).Result()
	if err != nil {
		log.Error("Redis client state update error", err, zap.String("clientID", msg.ClientID))
		return
	}

	// Set TTL on the client state hash
	_, err = a.client.Expire(ctx, clientKey, 24*time.Hour).Result()
	if err != nil {
		log.Warn("Failed to set TTL on client state",
			zap.Error(err),
			zap.String("clientID", msg.ClientID))
	}

	log.Debug("Redis client state update success", zap.String("clientID", msg.ClientID))
}

// handleClientStateDelete processes a client state delete request
func (a *RedisActor) handleClientStateDelete(msg RedisClientStateDelete) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StateUpdateTimeoutMs)*time.Millisecond)
	defer cancel()

	clientKey := ClientHashKey(msg.ClientID)
	subKey := ClientSubsSetKey(msg.ClientID)
	pubKey := ClientPubsSetKey(msg.ClientID)

	// Use pipeline for multiple operations
	pipe := a.client.Pipeline()
	pipe.Del(ctx, clientKey)
	pipe.Del(ctx, subKey)
	pipe.Del(ctx, pubKey)
	_, err := pipe.Exec(ctx)

	if err != nil {
		log.Error("Redis client state delete error", err, zap.String("clientID", msg.ClientID))
		return
	}

	log.Debug("Redis client state delete success", zap.String("clientID", msg.ClientID))
}

// RedisPubSubActor manages Redis PubSub operations
type RedisPubSubActor struct {
	*BaseActor
	client        ports.RedisClient
	config        *config.RedisConfig
	pubsub        *redis.PubSub
	parentActor   Actor
	subscriptions map[string]bool
	msgCh         <-chan *redis.Message
	stopCh        chan struct{}
	mu            sync.RWMutex
}

// NewRedisPubSubActor creates a new Redis PubSub actor
func NewRedisPubSubActor(ctx ActorContext, client ports.RedisClient, config *config.RedisConfig, parentActor Actor) *RedisPubSubActor {
	actor := &RedisPubSubActor{
		BaseActor:     NewBaseActor(ctx, WithMailboxSize(256)),
		client:        client,
		config:        config,
		parentActor:   parentActor,
		subscriptions: make(map[string]bool),
		stopCh:        make(chan struct{}),
	}

	actor.initializeBehaviors()
	return actor
}

// initializeBehaviors sets up message handlers
func (a *RedisPubSubActor) initializeBehaviors() {
	// Handle Redis publish
	a.Become(RedisPublishType, func(msg Message) {
		pubMsg := msg.(RedisPublish)
		a.handlePublish(pubMsg)
	})

	// Handle Redis subscribe
	a.Become(RedisSubscribeType, func(msg Message) {
		subMsg := msg.(RedisSubscribe)
		a.handleSubscribe(subMsg.Channels)
	})

	// Handle Redis unsubscribe
	a.Become(RedisUnsubscribeType, func(msg Message) {
		unsubMsg := msg.(RedisUnsubscribe)
		a.handleUnsubscribe(unsubMsg.Channels)
	})

	// System messages
	a.Become(StartMessage{}.MessageType(), func(msg Message) {
		a.startPubSub()
	})

	a.Become(StopMessage{}.MessageType(), func(msg Message) {
		a.stopPubSub()
	})
}

// Start initializes the actor
func (a *RedisPubSubActor) Start() error {
	log.Debug("Starting RedisPubSubActor")
	return a.BaseActor.Start()
}

// Stop terminates the actor and all resources
func (a *RedisPubSubActor) Stop() error {
	log.Debug("Stopping RedisPubSubActor")
	a.stopPubSub()
	return a.BaseActor.Stop()
}

// startPubSub initializes the PubSub connection
func (a *RedisPubSubActor) startPubSub() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create new PubSub client
	a.pubsub = a.client.Subscribe(a.context.Context())
	if a.pubsub == nil {
		log.Error("Failed to create PubSub client", fmt.Errorf("nil pubsub"))
		return
	}

	// Verify connection
	if _, err := a.pubsub.Receive(a.context.Context()); err != nil {
		log.Error("Failed to verify PubSub connection", err)
		a.pubsub.Close()
		a.pubsub = nil
		return
	}

	log.Info("PubSub connection established")

	// Get message channel
	a.msgCh = a.pubsub.Channel()

	// Start message processing goroutine
	go a.processMessages()
}

// stopPubSub closes the PubSub connection
func (a *RedisPubSubActor) stopPubSub() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Signal the message processing goroutine to stop
	select {
	case <-a.stopCh: // Already closed
	default:
		close(a.stopCh)
	}

	// Close PubSub connection if open
	if a.pubsub != nil {
		if err := a.pubsub.Close(); err != nil {
			log.Error("Error closing PubSub connection", err)
		}
		a.pubsub = nil
	}
}

// processMessages handles messages from the PubSub connection
func (a *RedisPubSubActor) processMessages() {
	if a.msgCh == nil {
		log.Error("PubSub message channel is nil", fmt.Errorf("nil message channel"))
		return
	}

	for {
		select {
		case <-a.context.Context().Done():
			return
		case <-a.stopCh:
			return
		case msg, ok := <-a.msgCh:
			if !ok {
				log.Warn("PubSub message channel closed, reconnecting...")
				a.reconnect()
				return
			}

			// Forward the message to the parent actor (RedisActor)
			a.context.Send(a.parentActor, RedisPubSubMessage{
				Channel: msg.Channel,
				Payload: msg.Payload,
			})
		}
	}
}

// reconnect attempts to reconnect the PubSub connection
func (a *RedisPubSubActor) reconnect() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Close existing connection if any
	if a.pubsub != nil {
		a.pubsub.Close()
		a.pubsub = nil
	}

	// Create new PubSub client
	a.pubsub = a.client.Subscribe(a.context.Context())
	if a.pubsub == nil {
		log.Error("Failed to reconnect PubSub client", fmt.Errorf("nil pubsub"))
		return
	}

	// Verify connection
	if _, err := a.pubsub.Receive(a.context.Context()); err != nil {
		log.Error("Failed to verify PubSub reconnection", err)
		a.pubsub.Close()
		a.pubsub = nil
		return
	}

	log.Info("PubSub connection re-established")

	// Re-subscribe to all channels
	if len(a.subscriptions) > 0 {
		channels := make([]string, 0, len(a.subscriptions))
		for channel := range a.subscriptions {
			channels = append(channels, channel)
		}

		if err := a.pubsub.Subscribe(a.context.Context(), channels...); err != nil {
			log.Error("Failed to re-subscribe to channels", err)
			a.pubsub.Close()
			a.pubsub = nil
			return
		}

		log.Info("Re-subscribed to channels", zap.Strings("channels", channels))
	}

	// Get new message channel
	a.msgCh = a.pubsub.Channel()

	// Start new message processing goroutine
	go a.processMessages()
}

// handlePublish processes a publish message
func (a *RedisPubSubActor) handlePublish(msg RedisPublish) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.PublishTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute publish operation
	count, err := a.client.Publish(ctx, msg.Channel, msg.Message).Result()
	if err != nil {
		log.Error("Redis publish error", err, zap.String("channel", msg.Channel))
		return
	}

	log.Debug("Redis publish success",
		zap.String("channel", msg.Channel),
		zap.Int64("subscribers", count))
}

// handleSubscribe processes a subscribe request
func (a *RedisPubSubActor) handleSubscribe(channels []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.pubsub == nil {
		log.Warn("Cannot subscribe, PubSub connection not available", zap.Strings("channels", channels))
		return
	}

	// Subscribe to channels
	if err := a.pubsub.Subscribe(a.context.Context(), channels...); err != nil {
		log.Error("Redis subscribe error", err, zap.Strings("channels", channels))
		return
	}

	// Update subscriptions map
	for _, channel := range channels {
		a.subscriptions[channel] = true
	}

	log.Debug("Redis subscribe success", zap.Strings("channels", channels))
}

// handleUnsubscribe processes an unsubscribe request
func (a *RedisPubSubActor) handleUnsubscribe(channels []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.pubsub == nil {
		log.Warn("Cannot unsubscribe, PubSub connection not available", zap.Strings("channels", channels))
		return
	}

	// Unsubscribe from channels
	if err := a.pubsub.Unsubscribe(a.context.Context(), channels...); err != nil {
		log.Error("Redis unsubscribe error", err, zap.Strings("channels", channels))
		return
	}

	// Update subscriptions map
	for _, channel := range channels {
		delete(a.subscriptions, channel)
	}

	log.Debug("Redis unsubscribe success", zap.Strings("channels", channels))
}

// RedisStreamReaderActor manages Redis Stream reading operations
type RedisStreamReaderActor struct {
	*BaseActor
	client      ports.RedisClient
	config      *config.RedisConfig
	parentActor Actor
	stopCh      chan struct{}
}

// NewRedisStreamReaderActor creates a new Redis Stream reader actor
func NewRedisStreamReaderActor(ctx ActorContext, client ports.RedisClient, config *config.RedisConfig, parentActor Actor) *RedisStreamReaderActor {
	actor := &RedisStreamReaderActor{
		BaseActor:   NewBaseActor(ctx, WithMailboxSize(256)),
		client:      client,
		config:      config,
		parentActor: parentActor,
		stopCh:      make(chan struct{}),
	}

	actor.initializeBehaviors()
	return actor
}

// initializeBehaviors sets up message handlers
func (a *RedisStreamReaderActor) initializeBehaviors() {
	// Handle XReadGroup request
	a.Become(RedisXReadGroupType, func(msg Message) {
		xreadMsg := msg.(RedisXReadGroup)
		a.handleXReadGroup(xreadMsg)
	})

	// System messages
	a.Become(StartMessage{}.MessageType(), func(msg Message) {
		// Nothing to do on start for now
	})

	a.Become(StopMessage{}.MessageType(), func(msg Message) {
		// Signal any long-running operations to stop
		select {
		case <-a.stopCh: // Already closed
		default:
			close(a.stopCh)
		}
	})
}

// handleXReadGroup processes an XReadGroup request
func (a *RedisStreamReaderActor) handleXReadGroup(msg RedisXReadGroup) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StreamReadTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XReadGroup operation
	streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    msg.Group,
		Consumer: msg.Consumer,
		Streams:  msg.Streams,
		Count:    msg.Count,
		Block:    time.Duration(msg.Block) * time.Millisecond,
	}).Result()

	if err != nil {
		if err != redis.Nil {
			log.Error("Redis XReadGroup error", err,
				zap.String("group", msg.Group),
				zap.String("consumer", msg.Consumer),
				zap.Strings("streams", msg.Streams))
		}
		return
	}

	// Process the results
	for _, stream := range streams {
		log.Debug("Redis XReadGroup success",
			zap.String("stream", stream.Stream),
			zap.Int("messages", len(stream.Messages)))

		// Forward each message to the parent actor
		for _, xmsg := range stream.Messages {
			// Parse the message from the "message" field
			messageStr, ok := xmsg.Values["message"].(string)
			if !ok {
				log.Warn("Invalid message format in stream",
					zap.String("stream", stream.Stream),
					zap.String("msgID", xmsg.ID))
				continue
			}

			// Forward to parent actor
			a.context.Send(a.parentActor, RedisStreamMessage{
				Stream: stream.Stream,
				ID:     xmsg.ID,
				Fields: map[string]string{
					"message": messageStr,
				},
			})
		}
	}
}

// RedisStreamWriterActor manages Redis Stream writing operations
type RedisStreamWriterActor struct {
	*BaseActor
	client      ports.RedisClient
	config      *config.RedisConfig
	parentActor Actor
}

// NewRedisStreamWriterActor creates a new Redis Stream writer actor
func NewRedisStreamWriterActor(ctx ActorContext, client ports.RedisClient, config *config.RedisConfig, parentActor Actor) *RedisStreamWriterActor {
	actor := &RedisStreamWriterActor{
		BaseActor:   NewBaseActor(ctx, WithMailboxSize(256)),
		client:      client,
		config:      config,
		parentActor: parentActor,
	}

	actor.initializeBehaviors()
	return actor
}

// initializeBehaviors sets up message handlers
func (a *RedisStreamWriterActor) initializeBehaviors() {
	// Handle XAdd request
	a.Become(RedisXAddType, func(msg Message) {
		xaddMsg := msg.(RedisXAdd)
		a.handleXAdd(xaddMsg)
	})

	// Handle XAck request
	a.Become(RedisXAckType, func(msg Message) {
		xackMsg := msg.(RedisXAck)
		a.handleXAck(xackMsg)
	})
}

// handleXAdd processes an XAdd request
func (a *RedisStreamWriterActor) handleXAdd(msg RedisXAdd) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.CacheWriteTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XAdd operation
	streamID, err := a.client.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Stream,
		MaxLen: msg.MaxLen,
		Approx: msg.Approx,
		Values: msg.Values,
	}).Result()

	if err != nil {
		log.Error("Redis XAdd error", err, zap.String("stream", msg.Stream))
		return
	}

	log.Debug("Redis XAdd success",
		zap.String("stream", msg.Stream),
		zap.String("id", streamID))

	// Set TTL on the stream if configured
	if a.config.StreamTTLSeconds > 0 {
		ttlDuration := time.Duration(a.config.StreamTTLSeconds) * time.Second
		_, err := a.client.Expire(ctx, msg.Stream, ttlDuration).Result()
		if err != nil {
			log.Warn("Failed to set TTL on stream after XAdd",
				zap.Error(err),
				zap.String("stream", msg.Stream))
		}
	}
}

// handleXAck processes an XAck request
func (a *RedisStreamWriterActor) handleXAck(msg RedisXAck) {
	ctx, cancel := context.WithTimeout(a.context.Context(), time.Duration(a.config.StateUpdateTimeoutMs)*time.Millisecond)
	defer cancel()

	// Execute XAck operation
	count, err := a.client.XAck(ctx, msg.Stream, msg.Group, msg.IDs...).Result()
	if err != nil {
		log.Error("Redis XAck error", err,
			zap.String("stream", msg.Stream),
			zap.String("group", msg.Group),
			zap.Strings("ids", msg.IDs))
		return
	}

	log.Debug("Redis XAck success",
		zap.String("stream", msg.Stream),
		zap.String("group", msg.Group),
		zap.Int64("count", count))
}
