package relay

import (
	"context"
	"encoding/json"
	"net/http"

	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/metrics"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type WsServer struct {
	instanceID string // Unique ID for this server instance
	config     *config.WsConfig

	// connection maintenance
	clients    map[*client]struct{}
	register   chan *client
	unregister chan ClientUnregisterEvent

	redisConn    *redis.Client
	redisSubConn *redis.PubSub

	publishers  *TopicClientSet
	subscribers *TopicClientSet

	localCh chan SocketMessage // for handling message of local clients
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // TODO Only white list allowed origins
	},
}

func NewWSServer(config *config.Config) *WsServer {
	ws := &WsServer{
		config: &config.WsServerConfig, // config

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
	ws.redisSubConn = ws.redisConn.Subscribe(context.TODO())

	return ws
}

func (ws *WsServer) NewClientConn(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// ignore the clients who ain't mean to do websocket communication with us
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

	go client.read()
	go client.write()
}

func (ws *WsServer) Run() {
	log.Info("Websocket server has been started")

	remoteCh := ws.redisSubConn.Channel()

	for {
		select {
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
		case chmessage, ok := <-remoteCh:
			// Check if the channel is closed (might indicate PubSub connection issue)
			if !ok {
				log.Warn("Redis PubSub channel closed. Attempting to re-subscribe.")
				// Attempt to re-establish the subscription
				// Note: This is a basic recovery attempt. A more robust solution
				// might involve a dedicated connection manager goroutine.
				ws.redisSubConn = ws.redisConn.Subscribe(context.TODO()) // Re-subscribe (might block)
				if ws.redisSubConn == nil {
					log.Error("Failed to re-subscribe to Redis PubSub. Exiting Run loop.", nil) // Or handle differently
					// Consider more drastic action like server restart or backoff retry
					return // Exit loop if re-subscription fails critically
				}
				// Need to re-subscribe to all topics currently tracked
				topicsToResubscribe := ws.getAllActiveTopics()
				if len(topicsToResubscribe) > 0 {
					err := ws.redisSubConn.Subscribe(context.TODO(), topicsToResubscribe...)
					if err != nil {
						log.Error("Failed to re-subscribe to topics after connection loss", err, zap.Strings("topics", topicsToResubscribe))
						// Potentially exit or retry
					} else {
						log.Info("Successfully re-subscribed to topics", zap.Strings("topics", topicsToResubscribe))
					}
				}
				remoteCh = ws.redisSubConn.Channel() // Get the new channel
				continue                             // Continue to next select iteration
			}

			// Process the received message
			message := SocketMessage{}
			err := json.Unmarshal([]byte(chmessage.Payload), &message)
			if err != nil {
				log.Warn("malformed message from remote", zap.String("payload", chmessage.Payload), zap.Error(err))
				continue
			}
			log.Info("remote message", zap.Any("message", message))

			// if message is not from `dappNotifyChan`, then must be from `messageChan` and must be a "pub" message
			if !fromDappNotifyChan(chmessage.Channel) {
				for _, subscriber := range ws.GetSubscriber(message.Topic) {
					log.Info("forward to subscriber", zap.Any("client", subscriber), zap.Any("message", message))
					subscriber.send(message)
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
	// TODO: Implement graceful shutdown logic if needed
	// - Close redis connections?
	// - Wait for client goroutines? (terminate signals them)
	log.Info("WsServer shutting down", zap.String("instanceID", ws.instanceID))
	if ws.redisSubConn != nil {
		ws.redisSubConn.Close()
	}
	if ws.redisConn != nil {
		ws.redisConn.Close()
	}
}
