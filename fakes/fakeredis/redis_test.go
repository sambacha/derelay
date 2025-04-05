package fakeredis_test

import (
	"testing"

	"github.com/RabbyHub/derelay/contracts/rediscontract"
	"github.com/RabbyHub/derelay/fakes/fakeredis"
	"github.com/RabbyHub/derelay/relay" // For relay.RedisClient interface
)

func TestFakeRedisClient_Contract(t *testing.T) {
	contract := rediscontract.RedisClientContract{
		NewClient: func(t *testing.T) relay.RedisClient {
			// Return a new instance of the fake client for each test run
			return fakeredis.New()
		},
		Cleanup: func(t *testing.T, client relay.RedisClient) {
			// Cleanup for the fake client (if needed, e.g., closing channels)
			// The current fake Close() is a no-op, so this can be empty for now.
			// client.Close()
		},
	}

	// Run the contract tests against the fake implementation
	contract.Test(t)
}
