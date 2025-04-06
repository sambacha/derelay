package actor

// Redis message types for type identification
const (
	RedisPublishType           string = "RedisPublish"
	RedisSubscribeType         string = "RedisSubscribe"
	RedisUnsubscribeType       string = "RedisUnsubscribe"
	RedisXAddType              string = "RedisXAdd"
	RedisXReadGroupType        string = "RedisXReadGroup"
	RedisXAckType              string = "RedisXAck"
	RedisPubSubMessageType     string = "RedisPubSubMessage"
	RedisStreamMessageType     string = "RedisStreamMessage"
	RedisClientStateUpdateType string = "RedisClientStateUpdate"
	RedisClientStateDeleteType string = "RedisClientStateDelete"
)

// RedisPublish represents a request to publish a message to a Redis channel
type RedisPublish struct {
	Channel string
	Message string
}

func (m RedisPublish) MessageType() string {
	return RedisPublishType
}

// RedisSubscribe represents a request to subscribe to Redis channels
type RedisSubscribe struct {
	Channels []string
}

func (m RedisSubscribe) MessageType() string {
	return RedisSubscribeType
}

// RedisUnsubscribe represents a request to unsubscribe from Redis channels
type RedisUnsubscribe struct {
	Channels []string
}

func (m RedisUnsubscribe) MessageType() string {
	return RedisUnsubscribeType
}

// RedisXAdd represents a request to add a message to a Redis stream
type RedisXAdd struct {
	Stream string
	MaxLen int64
	Approx bool
	Values map[string]interface{}
}

func (m RedisXAdd) MessageType() string {
	return RedisXAddType
}

// RedisXReadGroup represents a request to read messages from a Redis stream using a consumer group
type RedisXReadGroup struct {
	Group    string
	Consumer string
	Streams  []string
	Count    int64
	Block    int64 // in milliseconds
}

func (m RedisXReadGroup) MessageType() string {
	return RedisXReadGroupType
}

// RedisXAck represents a request to acknowledge messages in a Redis stream
type RedisXAck struct {
	Stream string
	Group  string
	IDs    []string
}

func (m RedisXAck) MessageType() string {
	return RedisXAckType
}

// RedisPubSubMessage represents a message received from Redis Pub/Sub
type RedisPubSubMessage struct {
	Channel string
	Payload string
}

func (m RedisPubSubMessage) MessageType() string {
	return RedisPubSubMessageType
}

// RedisStreamMessage represents a message read from a Redis stream
type RedisStreamMessage struct {
	Stream string
	ID     string
	Fields map[string]string
}

func (m RedisStreamMessage) MessageType() string {
	return RedisStreamMessageType
}

// RedisClientStateUpdate represents a request to update client state in Redis
type RedisClientStateUpdate struct {
	ClientID string
	Fields   map[string]interface{}
}

func (m RedisClientStateUpdate) MessageType() string {
	return RedisClientStateUpdateType
}

// RedisClientStateDelete represents a request to delete client state from Redis
type RedisClientStateDelete struct {
	ClientID string
}

func (m RedisClientStateDelete) MessageType() string {
	return RedisClientStateDeleteType
}
