//go:build e2e

package relay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
)

const (
	derelayServerURL = "ws://localhost:8080" // Assumes docker-compose exposes port 8080
	testTimeout      = 10 * time.Second      // Timeout for the entire test
	messageTimeout   = 5 * time.Second       // Timeout for waiting for a specific message
)

// Helper function to create and connect a WebSocket client
func connectWebSocket(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", err)
	}
	conn.SetReadLimit(65535) // Increase read limit if needed
	return conn, nil
}

// Helper function to send a message
func sendMessage(ctx context.Context, conn *websocket.Conn, msg SocketMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	err = conn.Write(ctx, websocket.MessageText, data)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

// Helper function to read a message with timeout
func readMessage(ctx context.Context, t *testing.T, conn *websocket.Conn) SocketMessage {
	msgCtx, cancel := context.WithTimeout(ctx, messageTimeout)
	defer cancel()

	msgType, data, err := conn.Read(msgCtx)
	require.NoError(t, err, "Failed to read message")
	require.Equal(t, websocket.MessageText, msgType, "Unexpected message type")

	var receivedMsg SocketMessage
	err = json.Unmarshal(data, &receivedMsg)
	require.NoError(t, err, "Failed to unmarshal received message")

	return receivedMsg
}

func TestE2ESessionLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	topic := fmt.Sprintf("e2e-test-topic-%d", time.Now().UnixNano())

	var wg sync.WaitGroup
	wg.Add(2) // Wait for DApp and Wallet goroutines

	// --- DApp Client ---
	go func() {
		defer wg.Done()
		dappCtx, dappCancel := context.WithCancel(ctx)
		defer dappCancel()

		dappConn, err := connectWebSocket(dappCtx, derelayServerURL)
		require.NoError(t, err, "DApp failed to connect")
		defer dappConn.Close(websocket.StatusNormalClosure, "")
		t.Log("DApp connected")

		// 1. DApp sends SessionRequest
		sessionRequest := SocketMessage{
			Topic: topic,
			Type:  Pub,
			Role:  string(Dapp),
			Phase: string(SessionRequest),
		}
		err = sendMessage(dappCtx, dappConn, sessionRequest)
		require.NoError(t, err, "DApp failed to send SessionRequest")
		t.Logf("DApp sent SessionRequest for topic %s", topic)

		// 4. DApp expects SessionReceived notification
		receivedMsg := readMessage(dappCtx, t, dappConn)
		t.Logf("DApp received message: %+v", receivedMsg)
		require.Equal(t, topic, receivedMsg.Topic, "DApp received wrong topic")
		require.Equal(t, Ack, receivedMsg.Type, "DApp received wrong type") // Should be ACK for SessionReceived
		require.Equal(t, string(Relay), receivedMsg.Role, "DApp received wrong role")
		require.Equal(t, string(SessionReceived), receivedMsg.Phase, "DApp received wrong phase")
		t.Log("DApp received SessionReceived notification")
	}()

	// Give DApp a moment to send its message before Wallet connects
	time.Sleep(100 * time.Millisecond)

	// --- Wallet Client ---
	go func() {
		defer wg.Done()
		walletCtx, walletCancel := context.WithCancel(ctx)
		defer walletCancel()

		walletConn, err := connectWebSocket(walletCtx, derelayServerURL)
		require.NoError(t, err, "Wallet failed to connect")
		defer walletConn.Close(websocket.StatusNormalClosure, "")
		t.Log("Wallet connected")

		// 2. Wallet subscribes to the topic
		subscribeRequest := SocketMessage{
			Topic: topic,
			Type:  Sub,
			Role:  string(Wallet),
		}
		err = sendMessage(walletCtx, walletConn, subscribeRequest)
		require.NoError(t, err, "Wallet failed to send Subscribe request")
		t.Logf("Wallet subscribed to topic %s", topic)

		// 3. Wallet expects SessionRequest
		receivedMsg := readMessage(walletCtx, t, walletConn)
		t.Logf("Wallet received message: %+v", receivedMsg)
		require.Equal(t, topic, receivedMsg.Topic, "Wallet received wrong topic")
		require.Equal(t, Pub, receivedMsg.Type, "Wallet received wrong type")
		require.Equal(t, string(Dapp), receivedMsg.Role, "Wallet received wrong role")
		require.Equal(t, string(SessionRequest), receivedMsg.Phase, "Wallet received wrong phase")
		t.Log("Wallet received SessionRequest")
	}()

	// Wait for both clients to finish or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("DApp and Wallet finished")
	case <-ctx.Done():
		require.Fail(t, "Test timed out")
	}
}
