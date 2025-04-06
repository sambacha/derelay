package config

type RedisConfig struct {
	ServerAddr string `yaml:"server_addr"`
	Password   string `yaml:"password"`

	// Timeouts for Redis/DragonflyDB operations in milliseconds
	HeartbeatTimeoutMs   int `yaml:"heartbeat_timeout_ms"`    // Default: 1000
	StateUpdateTimeoutMs int `yaml:"state_update_timeout_ms"` // Default: 2000
	CacheWriteTimeoutMs  int `yaml:"cache_write_timeout_ms"`  // Default: 2000 (Used for XAdd)
	StreamReadTimeoutMs  int `yaml:"stream_read_timeout_ms"`  // Default: 3000 (Used for XReadGroup)
	PublishTimeoutMs     int `yaml:"publish_timeout_ms"`      // Default: 2000

	// Cache stream settings
	StreamMaxLength     int64  `yaml:"stream_max_length"`     // Default: 10000 (Approximate max length for message streams)
	StreamTTLSeconds    int    `yaml:"stream_ttl_seconds"`    // Default: 86400 (TTL for inactive streams in seconds, 0 to disable)
	StreamConsumerGroup string `yaml:"stream_consumer_group"` // Default: "derelay-cg" (Name for the stream consumer group)
}
