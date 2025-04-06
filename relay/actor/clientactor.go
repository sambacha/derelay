package actor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/relay"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ClientActor represents a WebSocket client connection
type ClientActor struct {
	*BaseActor

	// Connection related state
	conn       *websocket.Conn
	remoteAddr string
	origin     string
	id         string
	role       relay.RoleType

	// Topic management
	pubTopics *relay.TopicSet
	subTopics *relay.TopicSet

	// Child actors
	readActor  Actor
	writeActor Actor

	// Parent references
	serverActor Actor
	redisActor  Actor

	// State tracking
	lastHeartbeat time.Time
}

// NewClientActor creates a new ClientActor
func NewClientActor(ctx ActorContext, conn *websocket.Conn, remoteAddr, origin string) *ClientActor {
	actor := &ClientActor{
		BaseActor:     NewBaseActor(ctx, WithMailboxSize(256)),
		conn:          conn,
		remoteAddr:    remoteAddr,
		origin:        origin,
		id:            GenerateRandomBytes16(),
		pubTopics:     relay.NewTopicSet(),
		subTopics:     relay.NewTopicSet(),
		lastHeartbeat: time.Now(),
	}

	// Set up default behaviors
	actor.initializeBehaviors()

	return actor
}

// ID returns a unique identifier for the client
func (a *ClientActor) ID() string {
	return a.id
}

// initializeBehaviors sets up message handlers
func (a *ClientActor) initializeBehaviors() {
	// WebSocket message handler
	a.Become(WebSocketMessageType, func(msg Message) {
		wsMsg := msg.(WebSocketMessage)
		a.handleWebSocketMessage(wsMsg)
	})

	// WebSocket disconnect handler
	a.Become(WebSocketDisconnectType, func(msg Message) {
		disconnectMsg := msg.(WebSocketDisconnect)
		a.handleDisconnect(disconnectMsg.Reason)
	})

	// WebSocket forward handler
	a.Become(WebSocketForwardType, func(msg Message) {
		forwardMsg := msg.(WebSocketForward)
		a.sendToClient(forwardMsg.Message)
	})

	// Handle client registration
	a.Become(WebSocketRegisterClientType, func(msg Message) {
		regMsg := msg.(WebSocketRegisterClient)
		a.role = regMsg.Role
		log.Debug("Client role registered", zap.String("clientID", a.id), zap.String("role", string(a.role)))
	})

	// WebSocket ping handler
	a.Become(WebSocketPingType, func(msg Message) {
		pingMsg := msg.(WebSocketPing)
		a.handlePing(pingMsg.Message)
	})

	// Handle system messages
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
func (a *ClientActor) Start() error {
	log.Debug("Starting ClientActor", zap.String("clientID", a.id), zap.String("remoteAddr", a.remoteAddr))
	return a.BaseActor.Start()
}

// Stop terminates the actor and all resources
func (a *ClientActor) Stop() error {
	log.Debug("Stopping ClientActor", zap.String("clientID", a.id))
	return a.BaseActor.Stop()
}

// startChildActors creates and starts reader/writer actors
func (a *ClientActor) startChildActors() {
	log.Debug("Starting ClientActor child actors", zap.String("clientID", a.id))

	// Create reader actor
	readerProps := ActorPropsFunc(func(ctx ActorContext) Actor {
		return NewReaderActor(ctx, a.conn, a.context.Self())
	})

	// Create writer actor
	writerProps := ActorPropsFunc(func(ctx ActorContext) Actor {
		return NewWriterActor(ctx, a.conn, a.context.Self())
	})

	// Spawn child actors
	var err error
	a.readActor, err = a.context.SpawnChild(readerProps)
	if err != nil {
		log.Error("Failed to start reader actor", err, zap.String("clientID", a.id))
		a.handleDisconnect(fmt.Errorf("failed to start reader: %w", err))
		return
	}

	a.writeActor, err = a.context.SpawnChild(writerProps)
	if err != nil {
		log.Error("Failed to start writer actor", err, zap.String("clientID", a.id))
		a.handleDisconnect(fmt.Errorf("failed to start writer: %w", err))
		return
	}

	// Watch child actors for termination
	a.context.Watch(a.readActor)
	a.context.Watch(a.writeActor)
}

// stopChildActors stops all child actors
func (a *ClientActor) stopChildActors() {
	if a.readActor != nil {
		if err := a.readActor.Stop(); err != nil {
			log.Warn("Error stopping reader actor", zap.String("clientID", a.id), zap.Error(err))
		}
	}

	if a.writeActor != nil {
		if err := a.writeActor.Stop(); err != nil {
			log.Warn("Error stopping writer actor", zap.String("clientID", a.id), zap.Error(err))
		}
	}
}

// handleWebSocketMessage processes a WebSocket message
func (a *ClientActor) handleWebSocketMessage(msg WebSocketMessage) {
	// Parse the message if not already parsed
	if msg.SocketMsg == nil {
		socketMsg := &relay.SocketMessage{}
		if err := json.Unmarshal(msg.Raw, socketMsg); err != nil {
			log.Warn("Received malformed WebSocket message",
				zap.Error(err),
				zap.String("clientID", a.id),
				zap.String("raw", string(msg.Raw)))
			return
		}
		msg.SocketMsg = socketMsg
	}

	socketMsg := msg.SocketMsg

	// Update client role if provided
	if socketMsg.Role != "" && a.role != relay.RoleType(socketMsg.Role) {
		a.role = relay.RoleType(socketMsg.Role)
		log.Debug("Updated client role from message",
			zap.String("clientID", a.id),
			zap.String("role", socketMsg.Role))
	}

	// Handle message based on type
	switch socketMsg.Type {
	case relay.Pub:
		// Update local topic registration
		a.pubTopics.Set(socketMsg.Topic)

		// Forward to server actor for processing
		a.context.Send(a.serverActor, WebSocketPublish{
			ClientID: a.id,
			Topic:    socketMsg.Topic,
			Message:  *socketMsg,
		})

	case relay.Sub:
		// Update local topic registration
		a.subTopics.Set(socketMsg.Topic)

		// Forward to server actor for processing
		a.context.Send(a.serverActor, WebSocketSubscribe{
			ClientID: a.id,
			Topic:    socketMsg.Topic,
		})

	case relay.Ping:
		// Handle ping message
		a.context.Send(a.context.Self(), WebSocketPing{
			ClientID: a.id,
			Message:  *socketMsg,
		})

	default:
		log.Debug("Received unhandled message type",
			zap.String("clientID", a.id),
			zap.String("type", string(socketMsg.Type)))
	}

	// Update heartbeat
	a.updateHeartbeat()
}

// handleDisconnect processes a disconnection event
func (a *ClientActor) handleDisconnect(reason error) {
	log.Info("Client disconnecting", zap.String("clientID", a.id), zap.Error(reason))

	// Notify server actor about disconnection
	if a.serverActor != nil {
		a.context.Send(a.serverActor, WebSocketDisconnect{
			ClientID: a.id,
			Reason:   reason,
		})
	}

	// Close connection if not already closed
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			log.Warn("Error closing WebSocket connection",
				zap.String("clientID", a.id),
				zap.Error(err))
		}
		a.conn = nil
	}

	// Self-terminate
	a.Stop()
}

// handleTerminated processes termination of child actors
func (a *ClientActor) handleTerminated(msg TerminatedMessage) {
	terminatedActor := msg.Actor

	switch {
	case a.readActor != nil && terminatedActor.ID() == a.readActor.ID():
		log.Debug("Reader actor terminated", zap.String("clientID", a.id))
		a.readActor = nil
		// Reader termination means connection is closed, so disconnect
		a.handleDisconnect(msg.Err)

	case a.writeActor != nil && terminatedActor.ID() == a.writeActor.ID():
		log.Debug("Writer actor terminated", zap.String("clientID", a.id))
		a.writeActor = nil
		// Writer termination means connection is closed, so disconnect
		a.handleDisconnect(msg.Err)
	}
}

// handlePing processes a ping message
func (a *ClientActor) handlePing(message relay.SocketMessage) {
	// Send pong response
	pongMsg := relay.SocketMessage{
		Type: relay.Pong,
		Role: string(relay.Relay),
	}

	a.sendToClient(pongMsg)
}

// sendToClient sends a message to the client through the writer actor
func (a *ClientActor) sendToClient(message relay.SocketMessage) {
	if a.writeActor != nil {
		// Wrap the SocketMessage in a WebSocketForward message
		a.context.Send(a.writeActor, WebSocketForward{
			ClientID: a.id,
			Message:  message,
		})
	} else {
		log.Warn("Cannot send to client, writer actor not available",
			zap.String("clientID", a.id),
			zap.String("messageType", string(message.Type)))
	}
}

// updateHeartbeat updates the client's last heartbeat time and Redis state
func (a *ClientActor) updateHeartbeat() {
	now := time.Now()

	// Update heartbeat no more than once per minute
	if a.redisActor != nil && now.Sub(a.lastHeartbeat) > time.Minute {
		fields := map[string]interface{}{
			"lastSeen": now.Unix(),
		}

		// Include role if set
		if a.role != "" {
			fields["role"] = string(a.role)
		}

		// Send heartbeat update to Redis actor
		a.context.Send(a.redisActor, RedisClientStateUpdate{
			ClientID: a.id,
			Fields:   fields,
		})

		a.lastHeartbeat = now
	}
}

// SetServerActor sets the parent server actor reference
func (a *ClientActor) SetServerActor(actor Actor) {
	a.serverActor = actor
}

// SetRedisActor sets the Redis actor reference
func (a *ClientActor) SetRedisActor(actor Actor) {
	a.redisActor = actor
}

// ReaderActor handles reading from the WebSocket connection
type ReaderActor struct {
	*BaseActor
	conn        *websocket.Conn
	clientActor Actor
}

// NewReaderActor creates a new actor for reading from a WebSocket
func NewReaderActor(ctx ActorContext, conn *websocket.Conn, clientActor Actor) *ReaderActor {
	actor := &ReaderActor{
		BaseActor:   NewBaseActor(ctx),
		conn:        conn,
		clientActor: clientActor,
	}

	// Set up behavior for start message
	actor.Become(StartMessage{}.MessageType(), func(message Message) {
		go actor.readLoop()
	})

	return actor
}

// readLoop continuously reads messages from the WebSocket
func (a *ReaderActor) readLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Panic in WebSocket reader",
				fmt.Errorf("panic: %v", r),
				zap.String("actorID", a.ID()))
			a.Stop()
		}
	}()

	for {
		select {
		case <-a.context.Context().Done():
			return
		default:
			// Read next message
			messageType, data, err := a.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err,
					websocket.CloseGoingAway,
					websocket.CloseNormalClosure,
					websocket.CloseNoStatusReceived) {
					log.Debug("WebSocket read error",
						zap.Error(err),
						zap.String("actorID", a.ID()))
				}
				// Notify client actor of disconnect
				a.context.Send(a.clientActor, WebSocketDisconnect{
					Reason: err,
				})
				return
			}

			// Process text messages
			if messageType == websocket.TextMessage {
				// Forward to client actor
				a.context.Send(a.clientActor, WebSocketMessage{
					Raw: data,
				})
			}
		}
	}
}

// WriterActor handles writing to the WebSocket connection
type WriterActor struct {
	*BaseActor
	conn         *websocket.Conn
	clientActor  Actor
	messageQueue chan relay.SocketMessage
	stopCh       chan struct{}
}

// NewWriterActor creates a new actor for writing to a WebSocket
func NewWriterActor(ctx ActorContext, conn *websocket.Conn, clientActor Actor) *WriterActor {
	actor := &WriterActor{
		BaseActor:    NewBaseActor(ctx),
		conn:         conn,
		clientActor:  clientActor,
		messageQueue: make(chan relay.SocketMessage, 32),
		stopCh:       make(chan struct{}),
	}

	// Set up behavior for receiving WebSocketForward messages
	actor.Become(WebSocketForwardType, func(message Message) {
		// Extract the SocketMessage from the WebSocketForward
		if forwardMsg, ok := message.(WebSocketForward); ok {
			select {
			case actor.messageQueue <- forwardMsg.Message:
				// Message queued
			default:
				log.Warn("Writer actor queue full, dropping message",
					zap.String("actorID", actor.ID()))
			}
		}
	})

	// Set up behavior for start message
	actor.Become(StartMessage{}.MessageType(), func(message Message) {
		go actor.writeLoop()
	})

	// Set up behavior for stop message
	actor.Become(StopMessage{}.MessageType(), func(message Message) {
		close(actor.stopCh)
	})

	return actor
}

// writeLoop continuously processes messages and writes them to the WebSocket
func (a *WriterActor) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Panic in WebSocket writer",
				fmt.Errorf("panic: %v", r),
				zap.String("actorID", a.ID()))
			a.Stop()
		}
	}()

	for {
		select {
		case <-a.context.Context().Done():
			return
		case <-a.stopCh:
			return
		case msg := <-a.messageQueue:
			// Marshal message to JSON
			data, err := json.Marshal(msg)
			if err != nil {
				log.Warn("Error marshaling WebSocket message",
					zap.Error(err),
					zap.String("actorID", a.ID()))
				continue
			}

			// Write to WebSocket
			if err := a.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Debug("WebSocket write error",
					zap.Error(err),
					zap.String("actorID", a.ID()))

				// Notify client actor of disconnect
				a.context.Send(a.clientActor, WebSocketDisconnect{
					Reason: err,
				})
				return
			}
		}
	}
}
