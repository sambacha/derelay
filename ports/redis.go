package ports // Changed package name

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient defines the interface for interacting with Redis,
// allowing for fake implementations during testing.
type RedisClient interface {
	// Pub/Sub
	Subscribe(ctx context.Context, channels ...string) RedisPubSub // Changed return type
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd

	// Streams
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd

	// Hashes
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd

	// Sets
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd

	// Keys
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	// Pipeline
	Pipeline() RedisPipeliner // Changed return type to custom interface
	// Note: Exec is called on the Pipeliner interface returned by Pipeline()

	// Connection
	Close() error
}

// RedisPipeliner defines the interface for Redis pipelines.
// This is needed because the Pipeline() method returns the concrete redis.Pipeliner type,
// but we need an interface to potentially fake pipeline execution.
// We only include methods used by the application (Exec).
type RedisPipeliner interface {
	// Commands added to the pipeline (mirroring RedisClient methods used in pipelines)
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	// Execution
	Exec(ctx context.Context) ([]redis.Cmder, error)
}

// Ensure redis.Pipeline implements RedisPipeliner (compile-time check)
var _ RedisPipeliner = (*redis.Pipeline)(nil)

// RedisPubSub defines the interface for Redis PubSub operations.
// This is needed because Subscribe returns the concrete *redis.PubSub type.
type RedisPubSub interface {
	Channel(opts ...redis.ChannelOption) <-chan *redis.Message
	Receive(ctx context.Context) (interface{}, error)
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	Close() error
}

// Ensure redis.PubSub implements RedisPubSub (compile-time check)
var _ RedisPubSub = (*redis.PubSub)(nil)

// --- Redis Client Adapter ---

// redisClientAdapter wraps a concrete *redis.Client to satisfy the RedisClient interface.
type redisClientAdapter struct {
	client *redis.Client
}

// NewRedisClientAdapter creates a new adapter.
func NewRedisClientAdapter(client *redis.Client) RedisClient {
	return &redisClientAdapter{client: client}
}

// Implement RedisClient interface for the adapter
func (a *redisClientAdapter) Subscribe(ctx context.Context, channels ...string) RedisPubSub {
	// The concrete client returns *redis.PubSub, which satisfies the RedisPubSub interface.
	return a.client.Subscribe(ctx, channels...)
}

func (a *redisClientAdapter) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	return a.client.Publish(ctx, channel, message)
}

func (a *redisClientAdapter) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	return a.client.XAdd(ctx, args)
}

func (a *redisClientAdapter) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd {
	return a.client.XGroupCreateMkStream(ctx, stream, group, start)
}

func (a *redisClientAdapter) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return a.client.XReadGroup(ctx, args)
}

func (a *redisClientAdapter) XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd {
	return a.client.XAck(ctx, stream, group, ids...)
}

func (a *redisClientAdapter) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return a.client.HSet(ctx, key, values...)
}

func (a *redisClientAdapter) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return a.client.SAdd(ctx, key, members...)
}

func (a *redisClientAdapter) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return a.client.Expire(ctx, key, expiration)
}

func (a *redisClientAdapter) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return a.client.Del(ctx, keys...)
}

func (a *redisClientAdapter) Pipeline() RedisPipeliner { // Changed return type to match interface
	// The concrete client's Pipeline() returns *redis.Pipeline,
	// which satisfies our RedisPipeliner interface (checked by var _ RedisPipeliner = (*redis.Pipeline)(nil))
	return a.client.Pipeline()
}

func (a *redisClientAdapter) Close() error {
	return a.client.Close()
}

// Ensure the adapter satisfies the interface (compile-time check)
var _ RedisClient = (*redisClientAdapter)(nil)
