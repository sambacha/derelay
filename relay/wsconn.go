package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/metrics"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const heartbeatInterval = 1 * time.Minute // How often to update client state in Redis/DragonflyDB

type client struct {
	conn *websocket.Conn
	ws   *WsServer

	id        string   // randomly generate, just for logging
	role      RoleType // dapp or wallet
	pubTopics *TopicSet
	subTopics *TopicSet

	sendbuf       chan SocketMessage // send buffer
	quit          chan struct{}
	lastHeartbeat time.Time // Track last heartbeat sent to Redis/DragonflyDB
}

func (c *client) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	if c != nil {
		encoder.AddString("id", c.id)
		encoder.AddString("role", string(c.role))
		// Check errors from AddArray
		if err := encoder.AddArray("pubTopics", c.pubTopics); err != nil {
			return err
		}
		if err := encoder.AddArray("subTopics", c.subTopics); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) read() {
	c.lastHeartbeat = time.Now() // Initialize heartbeat time on read start
	// ctx := context.TODO() // Removed unused variable

	for {
		_, m, err := c.conn.ReadMessage()
		if err != nil {
			c.terminate(err) // terminate handles sending unregister event
			return           // Exit read loop on error
		}

		// Decode message
		message := SocketMessage{}
		if err := json.NewDecoder(bytes.NewReader(m)).Decode(&message); err != nil {
			log.Warn("[wsconn] received malformed text message", zap.Error(err), zap.String("raw", string(m)), zap.Any("client", c))
			continue // Skip malformed message
		}

		// Determine/update client role based on message
		// Only update if role is not yet set or changes (unlikely but possible)
		newRole := RoleType(strings.ToLower(message.Role))
		roleUpdated := false
		if newRole != "" && c.role != newRole {
			c.role = newRole
			roleUpdated = true
			log.Debug("determined client role", zap.Any("client", c), zap.String("role", string(c.role)))
		}

		// Send message to central processing channel
		message.client = c
		c.ws.localCh <- message

		// Periodic heartbeat update to Redis/DragonflyDB
		if time.Since(c.lastHeartbeat) > heartbeatInterval || roleUpdated {
			// Correctly access redisConfig from WsServer
			ctxHeartbeat, cancelHB := context.WithTimeout(c.ws.ctx, time.Duration(c.ws.redisConfig.HeartbeatTimeoutMs)*time.Millisecond)
			defer cancelHB()

			pipe := c.ws.redisConn.Pipeline()
			clientKey := clientHashKey(c.id)
			updates := map[string]interface{}{
				"lastSeen": time.Now().Unix(),
			}
			if roleUpdated && c.role != "" {
				updates["role"] = string(c.role)
			}

			pipe.HSet(ctxHeartbeat, clientKey, updates)
			// Extend the TTL every heartbeat to keep the state alive
			pipe.Expire(ctxHeartbeat, clientKey, 24*time.Hour)
			// Also extend TTL for subscription/publication sets if they exist
			pipe.Expire(ctxHeartbeat, clientSubsSetKey(c.id), 24*time.Hour)
			pipe.Expire(ctxHeartbeat, clientPubsSetKey(c.id), 24*time.Hour)

			_, err := pipe.Exec(ctxHeartbeat) // Use ctxHeartbeat
			if err != nil {
				log.Warn("failed to update client heartbeat state in redis", zap.Error(err), zap.Any("client", c))
			}
			c.lastHeartbeat = time.Now() // Update last heartbeat time after attempting update (even if Exec failed)
		}
	}
}

func (c *client) write() {
	for {
		select {
		case message := <-c.sendbuf:
			m := new(bytes.Buffer)
			if err := json.NewEncoder(m).Encode(message); err != nil {
				log.Warn("sending malformed text message", zap.Error(err))
				continue
			}
			err := c.conn.WriteMessage(websocket.TextMessage, m.Bytes())
			if err != nil {
				log.Error("client write error", err, zap.Any("client", c), zap.Any("message", message))
				continue
			}
		case <-c.quit:
			return
		}
	}
}

// send implements a non-blocking sending
func (c *client) send(message SocketMessage) {
	select {
	case c.sendbuf <- message:
	default:
		metrics.IncSendBlocking()
	}
}

func (c *client) terminate(reason error) {
	c.quit <- struct{}{}
	c.conn.Close()
	c.ws.unregister <- ClientUnregisterEvent{client: c, reason: reason}
}
