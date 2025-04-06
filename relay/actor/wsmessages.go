package actor

import (
	"github.com/RabbyHub/derelay/relay"
	"github.com/gorilla/websocket"
)

const (
	// Message types
	WebSocketMessageType        = "ws.message"
	WebSocketConnectType        = "ws.connect"
	WebSocketDisconnectType     = "ws.disconnect"
	WebSocketPublishType        = "ws.publish"
	WebSocketSubscribeType      = "ws.subscribe"
	WebSocketForwardType        = "ws.forward"
	WebSocketErrorType          = "ws.error"
	WebSocketRegisterClientType = "ws.register.client"
	WebSocketPingType           = "ws.ping"
	WebSocketPongType           = "ws.pong"

	// Redis message types
	RedisPubSubMessageType     = "redis.pubsub.message"
	RedisStreamMessageType     = "redis.stream.message"
	RedisPublishType           = "redis.publish"
	RedisSubscribeType         = "redis.subscribe"
	RedisUnsubscribeType       = "redis.unsubscribe"
	RedisXAddType              = "redis.xadd"
	RedisXReadGroupType        = "redis.xreadgroup"
	RedisXAckType              = "redis.xack"
	RedisClientStateUpdateType = "redis.client.stateupdate"
	RedisClientStateDeleteType = "redis.client.statedelete"
	RedisConnectionErrorType   = "redis.connection.error"

	// Rate limiting message types
	RateLimitCheckType  = "ratelimit.check"
	RateLimitResultType = "ratelimit.result"
)

// WebSocketMessage represents a message received from a WebSocket connection
type WebSocketMessage struct {
	ClientID  string
	Raw       []byte
	SocketMsg *relay.SocketMessage
}

// MessageType implements the Message interface
func (m WebSocketMessage) MessageType() string {
	return WebSocketMessageType
}

// WebSocketConnect represents a new WebSocket connection
type WebSocketConnect struct {
	Conn       *websocket.Conn
	RemoteAddr string
	Origin     string
}

// MessageType implements the Message interface
func (m WebSocketConnect) MessageType() string {
	return WebSocketConnectType
}

// WebSocketDisconnect represents a closed WebSocket connection
type WebSocketDisconnect struct {
	ClientID string
	Reason   error
}

// MessageType implements the Message interface
func (m WebSocketDisconnect) MessageType() string {
	return WebSocketDisconnectType
}

// WebSocketPublish represents a message to be published
type WebSocketPublish struct {
	ClientID string
	Topic    string
	Message  relay.SocketMessage
}

// MessageType implements the Message interface
func (m WebSocketPublish) MessageType() string {
	return WebSocketPublishType
}

// WebSocketSubscribe represents a subscription request
type WebSocketSubscribe struct {
	ClientID string
	Topic    string
}

// MessageType implements the Message interface
func (m WebSocketSubscribe) MessageType() string {
	return WebSocketSubscribeType
}

// WebSocketForward represents a message to be forwarded to a client
type WebSocketForward struct {
	ClientID string
	Message  relay.SocketMessage
}

// MessageType implements the Message interface
func (m WebSocketForward) MessageType() string {
	return WebSocketForwardType
}

// WebSocketError represents an error in WebSocket handling
type WebSocketError struct {
	ClientID string
	Err      error
	Message  string
}

// MessageType implements the Message interface
func (m WebSocketError) MessageType() string {
	return WebSocketErrorType
}

// WebSocketRegisterClient is sent to register a new client
type WebSocketRegisterClient struct {
	ClientID string
	Role     relay.RoleType
}

// MessageType implements the Message interface
func (m WebSocketRegisterClient) MessageType() string {
	return WebSocketRegisterClientType
}

// WebSocketPing represents a ping message from a client
type WebSocketPing struct {
	ClientID string
	Message  relay.SocketMessage
}

// MessageType implements the Message interface
func (m WebSocketPing) MessageType() string {
	return WebSocketPingType
}

// WebSocketPong represents a pong message to be sent to a client
type WebSocketPong struct {
	ClientID string
}

// MessageType implements the Message interface
func (m WebSocketPong) MessageType() string {
	return WebSocketPongType
}

// RedisPubSubMessage represents a message received from Redis Pub/Sub
type RedisPubSubMessage struct {
	Channel string
	Payload string
}

// MessageType implements the Message interface
func (m RedisPubSubMessage) MessageType() string {
	return RedisPubSubMessageType
}

// RedisStreamMessage represents a message read from a Redis Stream
type RedisStreamMessage struct {
	Stream  string
	ID      string
	Fields  map[string]string
	Message *relay.SocketMessage
}

// MessageType implements the Message interface
func (m RedisStreamMessage) MessageType() string {
	return RedisStreamMessageType
}

// RedisPublish represents a request to publish a message to Redis
type RedisPublish struct {
	Channel string
	Message interface{}
}

// MessageType implements the Message interface
func (m RedisPublish) MessageType() string {
	return RedisPublishType
}

// RedisSubscribe represents a request to subscribe to a Redis channel
type RedisSubscribe struct {
	Channels []string
}

// MessageType implements the Message interface
func (m RedisSubscribe) MessageType() string {
	return RedisSubscribeType
}

// RedisUnsubscribe represents a request to unsubscribe from a Redis channel
type RedisUnsubscribe struct {
	Channels []string
}

// MessageType implements the Message interface
func (m RedisUnsubscribe) MessageType() string {
	return RedisUnsubscribeType
}

// RedisXAdd represents a request to add a message to a Redis Stream
type RedisXAdd struct {
	Stream string
	MaxLen int64
	Approx bool
	Values map[string]interface{}
}

// MessageType implements the Message interface
func (m RedisXAdd) MessageType() string {
	return RedisXAddType
}

// RedisXReadGroup represents a request to read messages from a Redis Stream
type RedisXReadGroup struct {
	Group    string
	Consumer string
	Streams  []string
	Count    int64
	Block    int64
}

// MessageType implements the Message interface
func (m RedisXReadGroup) MessageType() string {
	return RedisXReadGroupType
}

// RedisXAck represents a request to acknowledge a message in a Redis Stream
type RedisXAck struct {
	Stream string
	Group  string
	IDs    []string
}

// MessageType implements the Message interface
func (m RedisXAck) MessageType() string {
	return RedisXAckType
}

// RedisClientStateUpdate represents a request to update client state in Redis
type RedisClientStateUpdate struct {
	ClientID string
	Fields   map[string]interface{}
}

// MessageType implements the Message interface
func (m RedisClientStateUpdate) MessageType() string {
	return RedisClientStateUpdateType
}

// RedisClientStateDelete represents a request to delete client state from Redis
type RedisClientStateDelete struct {
	ClientID string
}

// MessageType implements the Message interface
func (m RedisClientStateDelete) MessageType() string {
	return RedisClientStateDeleteType
}

// RedisConnectionError represents an error in Redis connection
type RedisConnectionError struct {
	Err       error
	Operation string
}

// MessageType implements the Message interface
func (m RedisConnectionError) MessageType() string {
	return RedisConnectionErrorType
}

// RateLimitCheck represents a request to check rate limit
type RateLimitCheck struct {
	Key  string
	Type string // "connection" or "message"
}

// MessageType implements the Message interface
func (m RateLimitCheck) MessageType() string {
	return RateLimitCheckType
}

// RateLimitResult represents the result of a rate limit check
type RateLimitResult struct {
	Key     string
	Type    string // "connection" or "message"
	Allowed bool
}

// MessageType implements the Message interface
func (m RateLimitResult) MessageType() string {
	return RateLimitResultType
}
