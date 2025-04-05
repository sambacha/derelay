package rediscontract

import (
	"context"
	"testing"
	"time"

	"github.com/RabbyHub/derelay/relay" // Import the package where interfaces are defined
	"github.com/matryer/is"             // Use the chosen assertion library
	"github.com/redis/go-redis/v9"
)

// RedisClientContract defines the contract tests for any relay.RedisClient implementation.
type RedisClientContract struct {
	// NewClient creates a new client instance satisfying the relay.RedisClient interface for testing.
	// It should return a fresh instance for each test run.
	NewClient func(t *testing.T) relay.RedisClient

	// Cleanup is called after each test run to clean up any resources (e.g., close connection, clear data).
	Cleanup func(t *testing.T, client relay.RedisClient)
}

// Test runs all contract tests.
func (c RedisClientContract) Test(t *testing.T) {
	t.Helper()

	// --- Test Cases ---
	t.Run("basic HSet and Del", c.testHSetDel)
	t.Run("basic SAdd and Del", c.testSAddDel)
	t.Run("Expire and check", c.testExpire)
	t.Run("basic PubSub", c.testPubSub)
	t.Run("basic Streams (XAdd, XGroupCreate, XReadGroup, XAck)", c.testStreams)
	t.Run("basic Pipeline", c.testPipeline)
	// TODO: Add more tests for edge cases, error handling, interactions between commands.
}

// --- Individual Test Implementations ---

func (c RedisClientContract) testHSetDel(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	key := "test:hsetdel:hash"
	field1, val1 := "field1", "value1"
	field2, val2 := "field2", "value2"

	// HSet new fields
	added, err := client.HSet(ctx, key, field1, val1, field2, val2).Result()
	is.NoErr(err)
	is.Equal(added, int64(2)) // Should add 2 new fields

	// HSet update existing field, add new one
	field3, val3 := "field3", "value3"
	added, err = client.HSet(ctx, key, field1, "newvalue1", field3, val3).Result()
	is.NoErr(err)
	is.Equal(added, int64(1)) // Should add 1 new field (field3)

	// Del the key
	deleted, err := client.Del(ctx, key).Result()
	is.NoErr(err)
	is.Equal(deleted, int64(1)) // Should delete 1 key

	// Verify deletion (HSet should add again)
	added, err = client.HSet(ctx, key, field1, val1).Result()
	is.NoErr(err)
	is.Equal(added, int64(1))
}

func (c RedisClientContract) testSAddDel(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	key := "test:sadddel:set"
	m1, m2, m3 := "member1", "member2", "member3"

	// SAdd new members
	added, err := client.SAdd(ctx, key, m1, m2).Result()
	is.NoErr(err)
	is.Equal(added, int64(2))

	// SAdd existing and new member
	added, err = client.SAdd(ctx, key, m2, m3).Result()
	is.NoErr(err)
	is.Equal(added, int64(1)) // Only m3 should be added

	// Del the key
	deleted, err := client.Del(ctx, key).Result()
	is.NoErr(err)
	is.Equal(deleted, int64(1))

	// Verify deletion
	added, err = client.SAdd(ctx, key, m1).Result()
	is.NoErr(err)
	is.Equal(added, int64(1))
}

func (c RedisClientContract) testExpire(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	key := "test:expire:key"
	setKey := "test:expire:set"

	// Set a key (using HSet for example)
	_, err := client.HSet(ctx, key, "field", "value").Result()
	is.NoErr(err)
	_, err = client.SAdd(ctx, setKey, "member").Result()
	is.NoErr(err)

	// Set expiry
	expiredSet, err := client.Expire(ctx, key, 100*time.Millisecond).Result()
	is.NoErr(err)
	is.True(expiredSet) // Expire should return true if key exists and expiry was set

	expiredSet, err = client.Expire(ctx, setKey, 100*time.Millisecond).Result()
	is.NoErr(err)
	is.True(expiredSet)

	// Check key exists immediately
	_, err = client.HSet(ctx, key, "field2", "value2").Result() // HSet returns num added, not existence check directly
	is.NoErr(err)                                               // Should still exist
	added, err := client.SAdd(ctx, setKey, "member2").Result()
	is.NoErr(err)
	is.Equal(added, int64(1)) // Should still exist

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Check key is gone (Del should return 0)
	deleted, err := client.Del(ctx, key).Result()
	is.NoErr(err)
	is.Equal(deleted, int64(0))

	deleted, err = client.Del(ctx, setKey).Result()
	is.NoErr(err)
	is.Equal(deleted, int64(0))

	// Expire on non-existent key
	expiredSet, err = client.Expire(ctx, "nonexistent", 1*time.Second).Result()
	is.NoErr(err)
	is.True(!expiredSet) // Expire should return false if key doesn't exist
}

func (c RedisClientContract) testPubSub(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Test timeout
	defer cancel()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	channel1 := "test:pubsub:channel1"
	channel2 := "test:pubsub:channel2"
	msg1 := "hello world"
	msg2 := "goodbye world"

	// Subscriber 1
	ps1 := client.Subscribe(ctx, channel1)
	_, err := ps1.Receive(ctx) // Wait for initial confirmation
	is.NoErr(err)
	ch1 := ps1.Channel()

	// Subscriber 2
	ps2 := client.Subscribe(ctx, channel1, channel2)
	_, err = ps2.Receive(ctx)
	is.NoErr(err)
	ch2 := ps2.Channel()

	// Publish to channel1
	time.Sleep(50 * time.Millisecond) // Give time for subscriptions to register fully in fake/real
	numSubscribers, err := client.Publish(ctx, channel1, msg1).Result()
	is.NoErr(err)
	is.True(numSubscribers >= 2) // At least 2 subscribers expected

	// Publish to channel2
	numSubscribers, err = client.Publish(ctx, channel2, msg2).Result()
	is.NoErr(err)
	is.True(numSubscribers >= 1) // At least 1 subscriber expected

	// Check subscriber 1 received msg1
	select {
	case msg := <-ch1:
		is.Equal(msg.Channel, channel1)
		is.Equal(msg.Payload, msg1)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for message on channel1 for ps1")
	}

	// Check subscriber 2 received msg1 and msg2
	received1 := false
	received2 := false
	for i := 0; i < 2; i++ {
		select {
		case msg := <-ch2:
			if msg.Channel == channel1 && msg.Payload == msg1 {
				received1 = true
			} else if msg.Channel == channel2 && msg.Payload == msg2 {
				received2 = true
			} else {
				t.Fatalf("ps2 received unexpected message: %+v", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timeout waiting for message on channel1/2 for ps2")
		}
	}
	is.True(received1)
	is.True(received2)

	// Unsubscribe ps1
	err = ps1.Unsubscribe(ctx, channel1)
	is.NoErr(err)
	err = ps1.Close() // Close the pubsub connection
	is.NoErr(err)

	// Publish again to channel1
	numSubscribers, err = client.Publish(ctx, channel1, "msg3").Result()
	is.NoErr(err)
	is.True(numSubscribers >= 1) // Only ps2 should be left

	// Check ps2 received msg3
	select {
	case msg := <-ch2:
		is.Equal(msg.Channel, channel1)
		is.Equal(msg.Payload, "msg3")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for message on channel1 for ps2 after ps1 unsubscribe")
	}
}

func (c RedisClientContract) testStreams(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	streamKey := "test:stream:key"
	group := "test-group"
	consumer := "consumer-1"
	vals1 := map[string]interface{}{"message": `{"data":"one"}`}
	vals2 := map[string]interface{}{"message": `{"data":"two"}`}

	// XAdd messages
	id1, err := client.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: vals1}).Result()
	is.NoErr(err)
	is.True(id1 != "")
	id2, err := client.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: vals2}).Result()
	is.NoErr(err)
	is.True(id2 != "")
	is.True(id2 > id1) // Basic check for ID ordering

	// Create group
	_, err = client.XGroupCreateMkStream(ctx, streamKey, group, "0").Result()
	is.NoErr(err) // OK even if group/stream already exists

	// Read group - should get both messages
	results, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"}, // Read new messages
		Count:    10,
	}).Result()
	is.NoErr(err)
	is.Equal(len(results), 1)
	is.Equal(len(results[0].Messages), 2)
	is.Equal(results[0].Messages[0].ID, id1)
	is.Equal(results[0].Messages[0].Values["message"], vals1["message"])
	is.Equal(results[0].Messages[1].ID, id2)
	is.Equal(results[0].Messages[1].Values["message"], vals2["message"])

	// Read group again - should get no new messages
	results, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    10,
	}).Result()
	is.NoErr(err) // Should not error, just return empty
	is.Equal(len(results), 0)

	// Read pending messages (ID 0) - should get the two messages again
	results, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, "0"}, // Read pending messages
		Count:    10,
	}).Result()
	is.NoErr(err)
	is.Equal(len(results), 1)
	is.Equal(len(results[0].Messages), 2)
	is.Equal(results[0].Messages[0].ID, id1)
	is.Equal(results[0].Messages[1].ID, id2)

	// Ack the first message
	acked, err := client.XAck(ctx, streamKey, group, id1).Result()
	is.NoErr(err)
	is.Equal(acked, int64(1))

	// Read pending again - should only get the second message
	results, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, "0"},
		Count:    10,
	}).Result()
	is.NoErr(err)
	is.Equal(len(results), 1)
	is.Equal(len(results[0].Messages), 1)
	is.Equal(results[0].Messages[0].ID, id2)

	// Ack the second message
	acked, err = client.XAck(ctx, streamKey, group, id2).Result()
	is.NoErr(err)
	is.Equal(acked, int64(1))

	// Read pending again - should get nothing
	results, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, "0"},
		Count:    10,
	}).Result()
	is.NoErr(err)
	is.Equal(len(results), 0)
}

func (c RedisClientContract) testPipeline(t *testing.T) {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	client := c.NewClient(t)
	defer c.Cleanup(t, client)

	pipe := client.Pipeline()
	is.True(pipe != nil)

	key1 := "pipe:hset"
	// key2 := "pipe:set" // Removed unused variable
	key3 := "pipe:del"

	// Queue commands
	hsetCmd := pipe.HSet(ctx, key1, "f1", "v1")
	// saddCmd := pipe.SAdd(ctx, key2, "m1") // SAdd not in RedisPipeliner interface
	delCmd1 := pipe.Del(ctx, key3) // Delete non-existent key
	expireCmd := pipe.Expire(ctx, key1, 1*time.Minute)
	delCmd2 := pipe.Del(ctx, key1) // Only delete key1 now

	// Exec pipeline
	cmds, err := pipe.Exec(ctx)
	is.NoErr(err)          // Expect no error from Exec itself if commands are valid syntax
	is.Equal(len(cmds), 4) // Should have 4 results (HSet, Del, Expire, Del)

	// Check individual command results (values are populated after Exec)
	added, err := hsetCmd.Result()
	is.NoErr(err)
	is.Equal(added, int64(1))

	deleted, err := delCmd1.Result()
	is.NoErr(err)
	is.Equal(deleted, int64(0)) // key3 didn't exist

	expiredSet, err := expireCmd.Result()
	is.NoErr(err)
	is.True(expiredSet) // key1 existed

	deleted, err = delCmd2.Result()
	is.NoErr(err)
	is.Equal(deleted, int64(1)) // key1 deleted

	// Verify state after pipeline
	deleted, err = client.Del(ctx, key1).Result() // Try deleting key1 again
	is.NoErr(err)
	is.Equal(deleted, int64(0)) // Should already be deleted
}
