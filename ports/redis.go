package ports

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient defines the interface for Redis operations needed by the relay package.
// This allows for dependency injection and mocking.
type RedisClient interface {
	// Pipeline methods
	Pipeline() redis.Pipeliner
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) // Needed if Pipeline() result is used directly

	// Key/Value/Set methods
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd // Added based on handleClientDisconnect usage

	// Pub/Sub methods
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub // Returns concrete type as its methods are specific

	// Stream methods
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd

	// Connection methods
	Close() error
}

// Ensure *redis.Client implements RedisClient (compile-time check)
var _ RedisClient = (*redis.Client)(nil)

// Note: Pipeliner interface might also be needed if its methods are called directly.
// For now, assuming Pipeline() is called and then methods on the returned Pipeliner.
// If Pipelined is used, the interface needs that too. Let's add Pipelined for safety.
