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
}
