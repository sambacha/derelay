package actor

// Redis key/stream/channel prefixes
const (
	// redis client state tracking
	clientHashPrefix    = "wc:relay:client:"
	clientSubsSetPrefix = "wc:relay:client:subs:"
	clientPubsSetPrefix = "wc:relay:client:pubs:"
)

// ClientHashKey returns the hash key for client state
func ClientHashKey(clientID string) string {
	return clientHashPrefix + clientID
}

// ClientSubsSetKey returns the set key for client subscriptions
func ClientSubsSetKey(clientID string) string {
	return clientSubsSetPrefix + clientID
}

// ClientPubsSetKey returns the set key for client publications
func ClientPubsSetKey(clientID string) string {
	return clientPubsSetPrefix + clientID
}
