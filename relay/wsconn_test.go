package relay

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/RabbyHub/derelay/config"          // Needed for WsServer config
	"github.com/RabbyHub/derelay/fakes/fakeredis" // Use fake redis for WsServer
	"github.com/gorilla/websocket"
	"github.com/matryer/is"
)

// --- Fake wsConnection Implementation ---

type fakeWsConnection struct {
	mu                 sync.Mutex
	readBuffer         chan []byte // Channel to simulate incoming messages
	readError          error       // Error to return on next ReadMessage
	writeBuffer        [][]byte    // Messages "written" by writePump
	writeError         error       // Error to return on next WriteMessage/WriteControl
	pongHandler        func(string) error
	closeCalled        chan struct{} // Channel to signal Close was called
	writeMessageCalled chan struct{} // Channel to signal WriteMessage was called
	readMessageCalled  chan struct{} // Channel to signal ReadMessage was called
}

func newFakeWsConnection(bufferSize int) *fakeWsConnection {
	return &fakeWsConnection{
		readBuffer:         make(chan []byte, bufferSize),
		writeBuffer:        [][]byte{},
		closeCalled:        make(chan struct{}, 1),
		writeMessageCalled: make(chan struct{}, bufferSize*2), // Buffer to avoid blocking test
		readMessageCalled:  make(chan struct{}, bufferSize*2), // Buffer to avoid blocking test
	}
}

func (f *fakeWsConnection) ReadMessage() (int, []byte, error) {
	f.mu.Lock()
	err := f.readError
	f.mu.Unlock() // Unlock before potentially blocking read

	if err != nil {
		f.mu.Lock()
		f.readError = nil // Consume error
		f.mu.Unlock()
		return 0, nil, err
	}

	// Block until a message is available or the channel is closed
	msg, ok := <-f.readBuffer
	if !ok {
		return 0, nil, io.EOF // Simulate connection closed by peer
	}
	f.readMessageCalled <- struct{}{} // Signal read occurred
	return websocket.TextMessage, msg, nil
}

func (f *fakeWsConnection) WriteMessage(messageType int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeError != nil {
		return f.writeError
	}
	// Copy data to avoid race conditions if caller reuses buffer
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	f.writeBuffer = append(f.writeBuffer, dataCopy)
	// Non-blocking signal that write occurred
	select {
	case f.writeMessageCalled <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeWsConnection) SetPongHandler(h func(string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pongHandler = h // Store pong handler if needed for specific tests
}

func (f *fakeWsConnection) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case f.closeCalled <- struct{}{}: // Signal close
	default: // Avoid blocking if already signaled
	}
	// Close the read buffer channel to unblock ReadMessage
	close(f.readBuffer)
	return nil
}

// Helper methods for tests
func (f *fakeWsConnection) InjectReadMessage(msg []byte) {
	f.readBuffer <- msg // Send message to the read channel
}

func (f *fakeWsConnection) InjectReadError(err error) {
	f.mu.Lock()
	f.readError = err
	f.mu.Unlock()
	// Closing the channel might also be necessary to unblock ReadMessage immediately
	// close(f.readBuffer) // Consider if error should also close the read side
}

func (f *fakeWsConnection) CloseReadChannel() {
	close(f.readBuffer) // Explicitly close read side
}

func (f *fakeWsConnection) GetWrittenMessages() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Return a copy to avoid race conditions
	msgs := make([][]byte, len(f.writeBuffer))
	copy(msgs, f.writeBuffer)
	return msgs
}

func (f *fakeWsConnection) ClearWrittenMessages() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeBuffer = [][]byte{}
}

// --- Test Cases ---

func TestClientReadPump(t *testing.T) {
	is := is.New(t)

	// Setup fake WsServer and Client
	fakeRedis := fakeredis.New()
	// Use default config for simplicity, customize if needed
	cfg := config.LoadConfig("")
	wsServer := NewWSServer(cfg)
	wsServer.redisConn = fakeRedis // Inject fake redis

	fakeConn := newFakeWsConnection(10)
	// Manually create client as NewClientConn does websocket upgrade
	client := &client{
		conn:      fakeConn,
		id:        "test-client-read",
		ws:        wsServer,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
		sendbuf:   make(chan SocketMessage, 8),
		quit:      make(chan struct{}),
	}
	wsServer.register <- client // Register manually

	go wsServer.Run()         // Run server loop in background
	defer wsServer.Shutdown() // Ensure server shutdown

	go client.read() // Start the read pump

	// Test sending a valid message
	validMsg := SocketMessage{Topic: "t1", Type: Pub, Payload: "p1", Role: string(Dapp)}
	validMsgBytes, _ := json.Marshal(validMsg)
	fakeConn.InjectReadMessage(validMsgBytes)

	// Wait for message to be processed by wsServer (check localCh)
	select {
	case received := <-wsServer.localCh:
		is.Equal(received.Topic, validMsg.Topic)
		is.Equal(received.Type, validMsg.Type)
		is.Equal(received.Payload, validMsg.Payload)
		is.Equal(received.Role, validMsg.Role)
		is.Equal(received.client, client) // Should be associated with the correct client
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for message in wsServer.localCh")
	}

	// Test sending malformed JSON
	fakeConn.InjectReadMessage([]byte("{invalid json"))
	// No easy way to assert log output without more complex setup, assume it continues

	// Test read error triggers termination
	readErr := errors.New("simulated read error")
	fakeConn.InjectReadError(readErr)

	// Check if client was unregistered
	select {
	case unregEvent := <-wsServer.unregister:
		is.Equal(unregEvent.client, client)
		is.Equal(unregEvent.reason, readErr)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for client unregister")
	}

	// Ensure read pump goroutine exited (check quit channel - tricky without access)
	// Check closeCalled on fakeConn as a proxy for termination sequence
	select {
	case <-fakeConn.closeCalled:
		// Success
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Timeout waiting for fakeConn.Close() to be called")
	}
}

func TestClientWritePump(t *testing.T) {
	is := is.New(t)

	// Setup fake WsServer and Client
	fakeRedis := fakeredis.New()
	cfg := config.LoadConfig("")
	wsServer := NewWSServer(cfg)
	wsServer.redisConn = fakeRedis

	fakeConn := newFakeWsConnection(10)
	client := &client{
		conn:      fakeConn,
		id:        "test-client-write",
		ws:        wsServer,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
		sendbuf:   make(chan SocketMessage, 8), // Use client's sendbuf
		quit:      make(chan struct{}),
	}
	// No need to register with wsServer for write test unless interaction is needed

	go client.write() // Start write pump

	// Send a message via client.send()
	msgToSend := SocketMessage{Topic: "t2", Type: Pub, Payload: "p2"}
	client.send(msgToSend)

	// Wait for WriteMessage to be called
	select {
	case <-fakeConn.writeMessageCalled:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for WriteMessage to be called")
	}

	// Verify written message
	written := fakeConn.GetWrittenMessages()
	is.Equal(len(written), 1)
	var receivedMsg SocketMessage
	err := json.Unmarshal(written[0], &receivedMsg)
	is.NoErr(err)
	is.Equal(receivedMsg.Topic, msgToSend.Topic)
	is.Equal(receivedMsg.Type, msgToSend.Type)
	is.Equal(receivedMsg.Payload, msgToSend.Payload)

	// Test termination via quit channel
	close(client.quit) // Signal quit

	// Allow time for write pump to exit (no direct way to confirm without modifying code)
	time.Sleep(50 * time.Millisecond)

	// Try sending another message - should not be written
	fakeConn.ClearWrittenMessages()
	client.send(SocketMessage{Topic: "t3", Type: Sub})
	time.Sleep(50 * time.Millisecond)               // Wait to ensure no write happens
	is.Equal(len(fakeConn.GetWrittenMessages()), 0) // Should be empty

}

// TestClientSendNonBlocking tests the non-blocking nature of send()
func TestClientSendNonBlocking(t *testing.T) {
	is := is.New(t)

	fakeConn := newFakeWsConnection(1) // Small buffer
	client := &client{
		conn:    fakeConn,
		id:      "test-client-send",
		sendbuf: make(chan SocketMessage, 1), // Small buffer matching fakeConn
		quit:    make(chan struct{}),
	}

	// Fill the buffer
	client.send(SocketMessage{Topic: "t1"})

	// This send should block initially, but default case should trigger
	// We can't easily test the block, but we test the default path
	startTime := time.Now()
	client.send(SocketMessage{Topic: "t2"}) // This should hit default
	duration := time.Since(startTime)

	// Check that the second send returned quickly (didn't block indefinitely)
	is.True(duration < 50*time.Millisecond) // Should be almost instantaneous

	// We expect metrics.IncSendBlocking() to have been called, but testing globals is hard.
}
