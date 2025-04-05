package relay

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9" // For type definitions like XAddArgs, XReadGroupArgs, XStreamSliceCmd etc.
)

// RelayRedisIO defines the interface for Redis operations needed by the relay handlers.
// This allows mocking/faking the Redis dependency for testing.
type RelayRedisIO interface {
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner // Including Pipeline if needed, otherwise remove
	// Add other necessary Redis commands used by handlers here...
	// Consider if specific commands from Pipeliner are needed as direct methods too.
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd // Added based on wsconn.go usage
	// Methods needed for PubSub management and shutdown
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub // Needed by managePubSubConnection
	Close() error                                                    // Needed by Shutdown
}

// ClientSender defines the interface for sending messages back to a specific client.
type ClientSender interface {
	// Send attempts to send a message to the client (non-blocking is typical).
	Send(message SocketMessage)
	// ID returns the unique identifier for the client. Needed for Redis keys etc.
	ID() string
	// Role returns the role (Dapp/Wallet) of the client. Needed for handler logic.
	Role() RoleType
}

// Note: SubscriptionManager interface might be added later if handler logic
// is further extracted from WsServer and needs to interact with subscribe/unsubscribe channels.
