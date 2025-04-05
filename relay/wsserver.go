package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"sync" // Add sync import
	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/metrics"
	"github.com/google/uuid"
	"github.com/gorilla/websocket" // Re-add import
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type WsServer struct {
	instanceID  string // Unique ID for this server instance
	config      *config.WsConfig
	redisConfig *config.RedisConfig // Add Redis config reference

	// connection maintenance
	clients    map[*client]struct{}
	register   chan *client
	unregister chan ClientUnregisterEvent

	redisConn    *redis.Client
	redisSubConn *redis.PubSub // Still potentially useful for direct access if needed, managed by goroutine

	publishers  *TopicClientSet
	subscribers *TopicClientSet

	localCh             chan SocketMessage  // for handling message of local clients
	remoteMessages      chan *redis.Message // Messages received from PubSub manager
	subscribeRequests   chan []string       // Channel to request subscriptions
	unsubscribeRequests chan []string       // Channel to request unsubscriptions

	ctx    context.Context    // Context for managing goroutine lifecycle
	cancel context.CancelFunc // Func to cancel the context

	clientWG sync.WaitGroup // WaitGroup for active client goroutines
}

// upgrader is configured in NewWSServer now, as it needs access to config
// var upgrader = websocket.Upgrader{ ... } // Removed global variable

func NewWSServer(config *config.Config) *WsServer {
	ws := &WsServer{
		config:      &config.WsServerConfig,    // config
		redisConfig: &config.RedisServerConfig, // Store reference to redis config

		clients:    make(map[*client]struct{}),
		register:   make(chan *client, 4096),
		unregister: make(chan ClientUnregisterEvent, 4096),

		publishers:  NewTopicClientSet(),
		subscribers: NewTopicClientSet(),

		localCh: make(chan SocketMessage, 2),
	}
	ws.instanceID = uuid.NewString() // Generate unique ID for this instance
	log.Info("Initializing WsServer", zap.String("instanceID", ws.instanceID))

	ws.redisConn = redis.NewClient(&redis.Options{
		Addr:     config.RedisServerConfig.ServerAddr,
		Password: config.RedisServerConfig.Password,
		DB:       0,
	})
	// ws.redisSubConn = ws.redisConn.Subscribe(context.TODO()) // Removed: Manager goroutine handles this

	// Initialize context and channels for the manager goroutine
	ws.ctx, ws.cancel = context.WithCancel(context.Background())
	ws.remoteMessages = make(chan *redis.Message, 128) // Buffered channel
	ws.subscribeRequests = make(chan []string, 32)
	ws.unsubscribeRequests = make(chan []string, 32)

	// Start the PubSub connection manager goroutine
	go ws.managePubSubConnection()

	return ws
}

func (ws *WsServer) NewClientConn(w http.ResponseWriter, r *http.Request) {
	// Configure upgrader dynamically based on ws.config
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			// If AllowedOrigins is empty or nil, allow all origins
			if len(ws.config.AllowedOrigins) == 0 {
				return true
			}
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Allow requests with no Origin header? Or deny? Denying is safer.
				return false
			}
			for _, allowed := range ws.config.AllowedOrigins {
				// Current logic: exact match or wildcard "*"
				if allowed == "*" || allowed == origin {
					return true
				}
			}
			log.Warn("WebSocket origin denied", zap.String("origin", origin), zap.Strings("allowed", ws.config.AllowedOrigins))
			return false
		},
		// Add other upgrader options if needed (ReadBufferSize, WriteBufferSize)
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Log upgrade errors for debugging, but don't flood logs for non-websocket requests
		// Check for specific websocket handshake errors if possible
		log.Debug("WebSocket upgrade failed", zap.Error(err), zap.String("remoteAddr", r.RemoteAddr), zap.String("origin", r.Header.Get("Origin")))
		// Return directly without sending an HTTP error response, as Upgrade handles that.
		return
	}

	client := &client{
		conn:      conn,
		id:        generateRandomBytes16(),
		ws:        ws,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
		sendbuf:   make(chan SocketMessage, 8),
		quit:      make(chan struct{}),
	}

	ws.register <- client

	// Add client to WaitGroup before starting goroutines
	ws.clientWG.Add(1)
	go client.read()
	go client.write()
}

func (ws *WsServer) Run() {
	log.Info("Websocket server Run loop started")
	// remoteCh := ws.redisSubConn.Channel() // Removed: Using ws.remoteMessages now

	for {
		select {
		// Handle shutdown signal
		case <-ws.ctx.Done():
			log.Info("WsServer Run loop stopping due to context cancellation.")
			return

		case message := <-ws.localCh:
			// local message could be "pub", "sub" or "ack" or "ping"
			// pub/sub message handler may contain time-consuming operations(e.g. read/write redis)
			// so put them in separate goroutine to avoid blocking wsserver main loop
			switch message.Type {
			case Pub:
				// do not modify wsserver's local variable in seperate goroutine
				message.client.pubTopics.Set(message.Topic)
				ws.publishers.Set(message.Topic, message.client)
				go ws.pubMessage(message)
				log.Info("local message", zap.Any("client", message.client), zap.Any("message", message))
			case Sub:
				message.client.subTopics.Set(message.Topic)
				ws.subscribers.Set(message.Topic, message.client)
				go ws.subMessage(message)
				log.Info("local message", zap.Any("client", message.client), zap.Any("message", message))
			case Ping:
				ws.handlePingMessage(message)
			}
		// Receive messages from the PubSub manager goroutine
		case msg := <-ws.remoteMessages:
			// Reconnection logic is now handled by managePubSubConnection goroutine

			// Process the received message
			message := SocketMessage{}
			err := json.Unmarshal([]byte(msg.Payload), &message) // Use msg.Payload
			if err != nil {
				log.Warn("malformed message from remote", zap.String("payload", msg.Payload), zap.Error(err))
				continue
			}
			log.Debug("remote message received", zap.Any("message", message), zap.String("channel", msg.Channel)) // Log channel too

			// if message is not from `dappNotifyChan`, then must be from `messageChan` and must be a "pub" message
			if !fromDappNotifyChan(msg.Channel) { // Check msg.Channel
				for _, subscriber := range ws.GetSubscriber(message.Topic) {
					log.Debug("forwarding remote pub to subscriber", zap.Any("client", subscriber), zap.Any("message", message))
					subscriber.send(message) // Forward the unmarshaled SocketMessage
				}
				continue
			}

			// otherwise the message must be from the `dappNofityChan` channel
			// messages from chanNotifyDapp could be:
			//  * SessionReceived
			//	* SessionSuspended
			//	* SessionResumed
			// 	* relay generated fake "ack" for the wallet
			for _, publisher := range ws.GetDappPublisher(message.Topic) {
				log.Debug("wallet updates, notify dapp", zap.Any("client", publisher), zap.Any("message", message))
				publisher.send(message)
			}

		case client := <-ws.register:
			metrics.IncNewConnection()
			ws.clients[client] = struct{}{}
			metrics.SetCurrentConnections(len(ws.clients))

			// Add client state tracking in Redis/DragonflyDB
			clientKey := clientHashKey(client.id)
			now := time.Now().Unix()
			// Use pipeline for efficiency
			pipe := ws.redisConn.Pipeline()
			// Role might be unknown initially, set later in client.read heartbeat
			pipe.HSet(context.TODO(), clientKey, map[string]interface{}{
				"instanceID":  ws.instanceID,
				"connectedAt": now,
				"lastSeen":    now,
				"role":        "", // Initialize role as empty
			})
			// Set a TTL for the client state hash (e.g., 24 hours)
			// This ensures stale client state is eventually removed if heartbeats stop
			pipe.Expire(context.TODO(), clientKey, 24*time.Hour)
			_, err := pipe.Exec(context.TODO())
			if err != nil {
				log.Error("failed to set initial client state in redis", err, zap.Any("client", client))
			} else {
				log.Debug("set initial client state in redis", zap.Any("client", client))
			}

		case unregisterEvent := <-ws.unregister:
			client, reason := unregisterEvent.client, unregisterEvent.reason

			ws.handleClientDisconnect(client)
			delete(ws.clients, client)

			metrics.IncClosedConnection()
			metrics.SetCurrentConnections(len(ws.clients))
			log.Info("client disconnected", zap.Any("client", client), zap.String("reason", reason.Error()))

			// Decrement WaitGroup after handling unregistration
			ws.clientWG.Done()
		}
	}
}

// Helper function to get all unique topics currently subscribed or published to locally
func (ws *WsServer) getAllActiveTopics() []string {
	activeTopics := make(map[string]struct{})

	ws.subscribers.RLock()
	for topic := range ws.subscribers.Data {
		if len(ws.subscribers.Data[topic]) > 0 {
			activeTopics[messageChanKey(topic)] = struct{}{}
		}
	}
	ws.subscribers.RUnlock()

	ws.publishers.RLock()
	for topic := range ws.publishers.Data {
		if len(ws.publishers.Data[topic]) > 0 {
			// Check if messageChanKey is already added
			if _, exists := activeTopics[messageChanKey(topic)]; !exists {
				activeTopics[messageChanKey(topic)] = struct{}{}
			}
			// Add dappNotifyChanKey if any publisher is a Dapp
			isDappPublisherPresent := false
			for client := range ws.publishers.Data[topic] {
				if client.role == Dapp {
					isDappPublisherPresent = true
					break
				}
			}
			if isDappPublisherPresent {
				activeTopics[dappNotifyChanKey(topic)] = struct{}{}
			}
		}
	}
	ws.publishers.RUnlock()

	topicList := make([]string, 0, len(activeTopics))
	for topic := range activeTopics {
		topicList = append(topicList, topic)
	}
	return topicList
}

func (ws *WsServer) GetSubscriber(topic string) []*client {
	clients := []*client{}
	for client := range ws.subscribers.Get(topic) {
		clients = append(clients, client)
	}
	return clients
}

// GetDappPulisher gets the topic publisher and check whether its role is "dapp"
// the return value indicates whether the notification target has been found
func (ws *WsServer) GetDappPublisher(topic string) []*client {
	dapps := []*client{}
	for client := range ws.publishers.Get(topic) {
		if client.role == Dapp {
			dapps = append(dapps, client)
		}
	}
	return dapps
}

// getCachedMessages function is removed as caching is now handled by reading from streams in subMessage

func (ws *WsServer) Shutdown() {
	// Graceful shutdown logic: Signal the manager goroutine to stop and close connections.
	log.Info("WsServer shutting down", zap.String("instanceID", ws.instanceID))

	// Signal the PubSub manager goroutine to stop
	if ws.cancel != nil {
		ws.cancel()
	}

	// Close the main Redis connection
	if ws.redisConn != nil {
		ws.redisConn.Close()
	}
	// The managePubSubConnection goroutine is responsible for closing its own redisSubConn

	// Wait for all client goroutines to finish
	log.Info("Waiting for client goroutines to shut down...")
	ws.clientWG.Wait()
	log.Info("All client goroutines shut down.")
}

// managePubSubConnection runs in a background goroutine to handle the Redis PubSub connection.
func (ws *WsServer) managePubSubConnection() {
	defer log.Info("PubSub connection manager stopped.")
	log.Info("PubSub connection manager started.")

	var currentSubConn *redis.PubSub
	var msgCh <-chan *redis.Message
	backoff := time.Second // Initial backoff duration

	for {
		select {
		case <-ws.ctx.Done(): // Check for shutdown signal first
			if currentSubConn != nil {
				currentSubConn.Close()
			}
			return
		default:
			// Attempt to establish connection if not currently connected
			if currentSubConn == nil {
				log.Info("Attempting to establish PubSub connection...")
				subConn := ws.redisConn.Subscribe(ws.ctx) // Use ws.ctx
				if subConn == nil {                       // Check if Subscribe itself failed immediately
					log.Error("Failed to initiate PubSub subscription.", nil)
					// Wait before retrying
					time.Sleep(backoff)
					backoff = min(backoff*2, 30*time.Second) // Exponential backoff up to 30s
					continue
				}

				// Verify connection by trying to receive confirmation (optional but good practice)
				_, err := subConn.Receive(ws.ctx)
				if err != nil {
					log.Error("Failed to verify PubSub subscription.", err)
					subConn.Close()
					time.Sleep(backoff)
					backoff = min(backoff*2, 30*time.Second)
					continue
				}

				log.Info("PubSub connection established.")
				currentSubConn = subConn
				msgCh = currentSubConn.Channel()
				backoff = time.Second // Reset backoff on successful connection

				// Re-subscribe to all necessary topics
				topicsToResubscribe := ws.getAllActiveTopics()
				if len(topicsToResubscribe) > 0 {
					log.Info("Re-subscribing to active topics", zap.Strings("topics", topicsToResubscribe))
					err := currentSubConn.Subscribe(ws.ctx, topicsToResubscribe...)
					if err != nil {
						log.Error("Failed to re-subscribe to topics after connection.", err)
						// Connection might be unhealthy, close and retry
						currentSubConn.Close()
						currentSubConn = nil
						msgCh = nil
						continue // Retry connection
					}
				}
			}

			// Inner select loop for active connection
			select {
			case <-ws.ctx.Done():
				if currentSubConn != nil {
					currentSubConn.Close()
				}
				return

			case msg, ok := <-msgCh:
				if !ok {
					log.Warn("PubSub message channel closed. Connection lost.")
					currentSubConn.Close() // Ensure closed
					currentSubConn = nil
					msgCh = nil
					// No sleep here, outer loop will handle backoff retry
				} else {
					// Forward message to main Run loop
					select {
					case ws.remoteMessages <- msg:
					// Message forwarded
					case <-ws.ctx.Done():
						// Shutdown during forward attempt
						if currentSubConn != nil {
							currentSubConn.Close()
						}
						return
					}
				}

			case topics := <-ws.subscribeRequests:
				if currentSubConn != nil {
					log.Debug("Processing subscribe request", zap.Strings("topics", topics))
					err := currentSubConn.Subscribe(ws.ctx, topics...)
					if err != nil {
						log.Error("Failed to subscribe to topics", err, zap.Strings("topics", topics))
						// Assume connection is broken, trigger reconnect
						currentSubConn.Close()
						currentSubConn = nil
						msgCh = nil
					}
				} else {
					log.Warn("Received subscribe request while PubSub disconnected.", zap.Strings("topics", topics))
					// Topics will be subscribed on next successful connection
				}

			case topics := <-ws.unsubscribeRequests:
				if currentSubConn != nil {
					log.Debug("Processing unsubscribe request", zap.Strings("topics", topics))
					err := currentSubConn.Unsubscribe(ws.ctx, topics...)
					if err != nil {
						log.Error("Failed to unsubscribe from topics", err, zap.Strings("topics", topics))
						// Assume connection is broken, trigger reconnect
						currentSubConn.Close()
						currentSubConn = nil
						msgCh = nil
					}
				} else {
					log.Warn("Received unsubscribe request while PubSub disconnected.", zap.Strings("topics", topics))
					// No action needed if disconnected
				}
			}
		}
	}
}

// min helper for backoff calculation
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
