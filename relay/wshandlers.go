package relay

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/RabbyHub/derelay/log"
	"github.com/RabbyHub/derelay/metrics"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ApproxMaxStreamLen defines the approximate maximum length for message streams.
// Needs to be configurable or derived from cache time / expected rate.
const ApproxMaxStreamLen = 10000

// WsMessageHandler func(*WsServer, SocketMessage) // This type seems unused, can be removed if confirmed.

func (ws *WsServer) pubMessage(message SocketMessage) {
	topic := message.Topic
	publisher := message.client

	if message.Role == string(Dapp) {
		// this `notifyDappChanName(topic)` redis channel is used to notify dapp the wallet's status
		ws.redisSubConn.Subscribe(context.TODO(), dappNotifyChanKey(topic))
		if message.Phase == string(SessionStart) {
			metrics.IncEstablishedSessions()
			return
		}
	}

	log.Debug("publish message", zap.Any("client", publisher), zap.Any("topic", message.Topic))

	metrics.IncTotalMessages()
	key := messageChanKey(topic)
	if count, _ := ws.redisConn.Publish(context.TODO(), key, message).Result(); count >= 1 {
		log.Debug("message published", zap.Any("client", publisher), zap.Any("topic", topic))
		if publisher.role == Dapp {
			publisher.send(SocketMessage{
				Topic: message.Topic,
				Type:  Ack,
				Role:  string(Wallet),
			})
		}
	} else {
		log.Debug("cache message", zap.Any("client", publisher), zap.Any("topic", topic))
		metrics.IncCachedMessages()
		if message.Phase == string(SessionRequest) {
			metrics.IncNewRequestedSessions()
		}
		// Cache message using XADD to the stream
		streamKey := streamMessageKey(topic)
		// Marshal the message to store in the stream
		// Storing as a single JSON string in a 'message' field
		msgBytes, err := json.Marshal(message)
		if err != nil {
			// Corrected log.Error call
			log.Error("failed to marshal message for stream cache", err, zap.Any("message", message))
		} else {
			_, err = ws.redisConn.XAdd(context.TODO(), &redis.XAddArgs{
				Stream: streamKey,
				MaxLen: ApproxMaxStreamLen, // Trim stream approximately
				Approx: true,
				Values: map[string]interface{}{"message": string(msgBytes)},
			}).Result()
			if err != nil {
				log.Warn("failed to cache message to stream", zap.Error(err), zap.Any("message", message))
			} else {
				log.Debug("message cached to stream", zap.Any("client", publisher), zap.Any("topic", topic))
			}
		}
	}

	// Add topic to client's published topics set in Redis
	if _, err := ws.redisConn.SAdd(context.TODO(), clientPubsSetKey(publisher.id), topic).Result(); err == nil {
		// Set TTL on first add or periodically refresh
		ws.redisConn.Expire(context.TODO(), clientPubsSetKey(publisher.id), 24*time.Hour)
	} else {
		log.Warn("failed to add topic to client pubs set", zap.Error(err), zap.Any("client", publisher), zap.String("topic", topic))
	}
}

func (ws *WsServer) subMessage(message SocketMessage) {
	// Removed duplicate declaration
	topic := message.Topic
	subscriber := message.client
	ctx := context.TODO() // Use a proper context

	// Subscribe to Pub/Sub channel
	if err := ws.redisSubConn.Subscribe(ctx, messageChanKey(topic)); err != nil {
		log.Warn("[redisSub] subscribe to topic fail", zap.String("topic", topic), zap.Any("client", subscriber), zap.Error(err))
	} else {
		log.Debug("subscribed to pub/sub topic", zap.String("topic", topic), zap.Any("client", subscriber))
	}

	// Add topic to client's subscribed topics set in Redis
	if _, err := ws.redisConn.SAdd(ctx, clientSubsSetKey(subscriber.id), topic).Result(); err == nil {
		// Set TTL on first add or periodically refresh
		ws.redisConn.Expire(ctx, clientSubsSetKey(subscriber.id), 24*time.Hour)
	} else {
		log.Warn("failed to add topic to client subs set", zap.Error(err), zap.Any("client", subscriber), zap.String("topic", topic))
	}

	// Read pending messages from stream for this client
	streamKey := streamMessageKey(topic)
	groupName := "derelay-cg"     // Consider making group name configurable or more dynamic if needed
	consumerName := subscriber.id // Use client ID as consumer name

	// Ensure stream and group exist (ignore errors if they already do)
	// Using MkStream ensures the stream is created if it doesn't exist before reading
	ws.redisConn.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Result() // Ignore error

	pendingMessages := 0
	processedIDs := []string{} // Keep track of IDs to ACK

	// Loop to read pending messages (start from 0-0 for this consumer in the group)
	for {
		results, err := ws.redisConn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamKey, "0-0"}, // Read pending messages (ID > 0-0)
			Count:    10,                         // Read in batches
			Block:    0,                          // Don't block if no messages initially
		}).Result()

		if err != nil {
			// If no stream exists yet, that's fine. Otherwise log error.
			if !errors.Is(err, redis.Nil) && !strings.Contains(err.Error(), "NOGROUP") { // NOGROUP check might be needed depending on redis version/client behavior
				log.Warn("failed to read stream group", zap.Error(err), zap.String("stream", streamKey), zap.String("group", groupName), zap.Any("client", subscriber))
			}
			break // Exit loop on error or no messages
		}

		if len(results) == 0 || len(results[0].Messages) == 0 {
			break // No more pending messages
		}

		streamMessages := results[0].Messages
		pendingMessages += len(streamMessages)

		for _, streamMsg := range streamMessages {
			msgData, ok := streamMsg.Values["message"].(string)
			if !ok {
				log.Warn("invalid message format in stream", zap.String("stream", streamKey), zap.String("msgID", streamMsg.ID))
				// Consider acknowledging malformed messages to avoid reprocessing
				processedIDs = append(processedIDs, streamMsg.ID)
				continue
			}

			var notification SocketMessage
			if err := json.Unmarshal([]byte(msgData), &notification); err != nil {
				// Corrected log.Error call
				log.Error("failed to unmarshal message from stream", err, zap.String("stream", streamKey), zap.String("msgID", streamMsg.ID))
				// Consider acknowledging malformed messages
				processedIDs = append(processedIDs, streamMsg.ID)
				continue
			}

			// Send the retrieved message to the client
			subscriber.send(notification)
			processedIDs = append(processedIDs, streamMsg.ID) // Mark for ACK

			// Wallet-specific logic for SessionRequest found in stream
			if subscriber.role != Dapp && notification.Phase == string(SessionRequest) {
				// When a wallet subscribes to a topic, either it've just scanned the QRCode to receive the session request
				// or it've just waken up from hibernation and trying to recovering the connection
				// This handles the first case (scanning QR code and finding the request in the stream)
				metrics.IncReceivedSessions()
				log.Debug("session request received from stream", zap.Any("topic", topic), zap.Any("client", subscriber))

				// notify the topic publisher, aka the dapp, that the session request has been received by wallet
				dappNotifyKey := dappNotifyChanKey(notification.Topic)
				ws.redisConn.Publish(ctx, dappNotifyKey, SocketMessage{
					Topic: notification.Topic,
					Phase: string(SessionReceived),
					Type:  Ack, // Send ACK type for SessionReceived notification
					Role:  string(Relay),
				})
			}
		}

		// Acknowledge the processed batch
		if len(processedIDs) > 0 {
			ws.redisConn.XAck(ctx, streamKey, groupName, processedIDs...)
			processedIDs = []string{} // Reset for next batch
		}

		// If we read less than requested, we've likely processed all pending
		if len(streamMessages) < 10 {
			break
		}
	}
	log.Debug("processed pending stream messages", zap.String("topic", topic), zap.Int("count", pendingMessages), zap.Any("client", subscriber))

	// Wallet-specific logic for SessionResumed (always send on subscribe if wallet)
	// This handles the case where a wallet reconnects (wakes up from hibernation)
	if subscriber.role != Dapp {
		// NOTE we could check for whether the notifications of this topic is session request, we don't need reply `sessionResumed`
		// for sessionRequest message, but for simplity we don't do that check here
		dappNotifyKey := dappNotifyChanKey(message.Topic)
		ws.redisConn.Publish(ctx, dappNotifyKey, SocketMessage{
			Topic: message.Topic,
			Type:  Pub, // Send Pub type for SessionResumed notification
			Role:  string(Relay),
			Phase: string(SessionResumed),
		})
		log.Debug("published session resumed notification", zap.String("topic", topic), zap.Any("client", subscriber))
	}
}

//

func (ws *WsServer) handlePingMessage(message SocketMessage) {
	// response to application layer ping message
	client := message.client
	client.send(SocketMessage{
		Type: Pong,
		Role: string(Relay), // Identify relay in pong
	})
}

// cacheMessage function is removed, logic moved to pubMessage using XAdd

func (ws *WsServer) handleClientDisconnect(client *client) {
	ctx := context.TODO() // Use a proper context

	// Cleanup local topic maps
	subscribedTopicsMap := client.subTopics.Get() // Get subscribed topics before clearing
	publishedTopicsMap := client.pubTopics.Get()  // Get published topics before clearing

	channelsToUnsubscribe := []string{}

	for topic := range subscribedTopicsMap {
		ws.subscribers.Unset(topic, client)
		if ws.subscribers.Len(topic) == 0 {
			ws.subscribers.Clear(topic)
			// Only unsubscribe if no local clients are left for this topic
			if ws.publishers.Len(topic) == 0 { // Check publishers too
				channelsToUnsubscribe = append(channelsToUnsubscribe, messageChanKey(topic))
			}
		}
	}
	for topic := range publishedTopicsMap {
		ws.publishers.Unset(topic, client)
		if ws.publishers.Len(topic) == 0 {
			ws.publishers.Clear(topic)
			// Only unsubscribe if no local clients are left for this topic
			if ws.subscribers.Len(topic) == 0 { // Check subscribers too
				// Avoid adding duplicate channel keys
				isAlreadyAdded := false
				for _, ch := range channelsToUnsubscribe {
					if ch == messageChanKey(topic) {
						isAlreadyAdded = true
						break
					}
				}
				if !isAlreadyAdded {
					channelsToUnsubscribe = append(channelsToUnsubscribe, messageChanKey(topic))
				}
			}
			// Unsubscribe from dapp notify channel if it was a DApp publisher and no one else is publishing/subscribing
			if client.role == Dapp && ws.subscribers.Len(topic) == 0 && ws.publishers.Len(topic) == 0 {
				channelsToUnsubscribe = append(channelsToUnsubscribe, dappNotifyChanKey(topic))
			}
		}
	}

	// Unsubscribe from Redis Pub/Sub channels if no clients are left for those topics on this instance
	if len(channelsToUnsubscribe) > 0 {
		// Use a set to ensure unique channels before unsubscribing
		uniqueChannels := make(map[string]struct{})
		for _, ch := range channelsToUnsubscribe {
			uniqueChannels[ch] = struct{}{}
		}
		finalChannels := make([]string, 0, len(uniqueChannels))
		for ch := range uniqueChannels {
			finalChannels = append(finalChannels, ch)
		}

		if len(finalChannels) > 0 {
			log.Info("unsubscribing from redis channels", zap.Any("client", client), zap.Strings("channels", finalChannels))
			// Run in goroutine to avoid blocking the main loop
			go func() {
				err := ws.redisSubConn.Unsubscribe(ctx, finalChannels...)
				if err != nil {
					// Corrected log.Error call
					log.Error("failed to unsubscribe from redis channels", err, zap.Strings("channels", finalChannels))
				}
			}()
		}
	}

	// Cleanup client state in Redis
	pipe := ws.redisConn.Pipeline()
	pipe.Del(ctx, clientHashKey(client.id))
	pipe.Del(ctx, clientSubsSetKey(client.id))
	pipe.Del(ctx, clientPubsSetKey(client.id))
	_, err := pipe.Exec(ctx)
	if err != nil {
		// Corrected log.Error call
		log.Error("failed to cleanup client state in redis", err, zap.Any("client", client))
	} else {
		log.Debug("cleaned up client state in redis", zap.Any("client", client))
	}

	// Notify DApp if a Wallet disconnected
	if client.role != Dapp { // If it's a Wallet or unknown role that disconnected
		for topic := range subscribedTopicsMap { // Use the map captured before clearing
			go func(topic string) {
				dappNotifyKey := dappNotifyChanKey(topic)
				// Publish SessionSuspended notification
				ws.redisConn.Publish(ctx, dappNotifyKey, SocketMessage{
					Topic: topic,
					Type:  Pub,
					Role:  string(Wallet), // Indicate Wallet is the source of the state change
					Phase: string(SessionSuspended),
				})
				log.Debug("notified dapp about wallet suspension", zap.String("topic", topic), zap.Any("client", client))
			}(topic)
		}
	}
}
