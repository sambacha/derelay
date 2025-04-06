package config

type WsConfig struct {
	HeartbeatInterval          int      `yaml:"heartbeat_interval"`            // in seconds
	CheckSessionExpireInterval int      `yaml:"check_session_expire_interval"` // in seconds
	PendingSessionCacheTime    int      `yaml:"pending_session_cache_time"`    // in seconds
	MessageCacheTime           int      `yaml:"message_cache_time"`            //
	AllowedOrigins             []string `yaml:"allowed_origins"`

	// Rate Limiting Settings
	EnableConnectionRateLimit bool    `yaml:"enable_connection_rate_limit"` // Default: false
	ConnectionLimitPerIP      float64 `yaml:"connection_limit_per_ip"`      // Default: 10 (requests per second)
	ConnectionBurstPerIP      int     `yaml:"connection_burst_per_ip"`      // Default: 20
	EnableMessageRateLimit    bool    `yaml:"enable_message_rate_limit"`    // Default: false
	MessageLimitPerClient     float64 `yaml:"message_limit_per_client"`     // Default: 50 (messages per second)
	MessageBurstPerClient     int     `yaml:"message_burst_per_client"`     // Default: 100
}
