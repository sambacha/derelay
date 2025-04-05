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

// --- ClientSender Interface Implementation ---

// Send implements the ClientSender interface.
// It's already defined below as the primary send method.

// ID implements the ClientSender interface.
func (c *client) ID() string {
	return c.id
}

// Role implements the ClientSender interface.
func (c *client) Role() RoleType {
	return c.role
}

// --- Other Methods ---

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

// decodeSocketMessage attempts to decode a byte slice into a SocketMessage.
func decodeSocketMessage(data []byte) (SocketMessage, error) {
	var message SocketMessage
	// Use DisallowUnknownFields to be stricter about the input structure
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&message)
	return message, err // Return the message and any decoding error
}

func (c *client) read() {
	c.lastHeartbeat = time.Now() // Initialize heartbeat time on read start

	for {
		_, m, err := c.conn.ReadMessage()
		if err != nil {
			c.terminate(err) // terminate handles sending unregister event
			return           // Exit read loop on error
		}

		// Decode message using the new function
		message, err := decodeSocketMessage(m)
		if err != nil {
			log.Warn(
				"[wsconn] received malformed text message",
				zap.Error(err), // Log the decoding error
				zap.String("raw", string(m)),
				zap.Any("client", c),
			)
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
			ctxHeartbeat, cancelHB := context.WithTimeout(
				c.ws.ctx,
				time.Duration(c.ws.redisConfig.HeartbeatTimeoutMs)*time.Millisecond,
			)
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

			// Use '=' to avoid shadowing outer 'err'
			if _, err = pipe.Exec(ctxHeartbeat); err != nil {
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

// send implements a non-blocking sending. It also fulfills the Send method
// requirement of the ClientSender interface.
func (c *client) Send(message SocketMessage) {
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
