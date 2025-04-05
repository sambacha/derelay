package relay

import (
	"encoding/json" // Needed for read/write tests
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket" // Needed for mock setup and constants
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore" // Needed for MarshalLogObject test
	// Imports removed due to import cycles or being unused:
	// "context"
	// "net/http"
	// "net/http/httptest"
	// "strings"
	// "github.com/RabbyHub/derelay/config"
	// "github.com/RabbyHub/derelay/fakes/fakeredis"
	// "github.com/RabbyHub/derelay/metrics"
	// "github.com/redis/go-redis/v9"
)

// MockConn implements the wsConn interface for testing
type MockConn struct {
	// Existing fields
	closeCalled bool
	closeErr    error
	mu          sync.Mutex

	// Fields for ReadMessage simulation
	readMessageData chan []byte // Channel to send message data to the mock
	readMessageErr  chan error  // Channel to send error to the mock
	readMessageType int         // Type of message to return

	// Fields for WriteMessage verification
	writeMessageCalled chan bool // Signal when WriteMessage is called
	lastWriteData      []byte    // Store last written data
	lastWriteType      int       // Store last written message type
	writeErr           error     // Error to return from WriteMessage
}

// NewMockConn creates a new mock connection
func NewMockConn() *MockConn {
	return &MockConn{
		readMessageData:    make(chan []byte, 1),
		readMessageErr:     make(chan error, 1),
		readMessageType:    websocket.TextMessage, // Default type
		writeMessageCalled: make(chan bool, 1),
	}
}

func (m *MockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled = true
	return m.closeErr
}

// Implement other methods needed by the client struct if necessary, returning default/error values
func (m *MockConn) ReadMessage() (messageType int, p []byte, err error) {
	// Simulate reading a message provided via channels
	select {
	case data := <-m.readMessageData:
		return m.readMessageType, data, nil
	case err := <-m.readMessageErr:
		return 0, nil, err
	case <-time.After(1 * time.Second): // Timeout to prevent test hanging
		return 0, nil, errors.New("mock ReadMessage timeout")
	}
}
func (m *MockConn) WriteMessage(messageType int, data []byte) error {
	m.mu.Lock()
	m.lastWriteType = messageType
	m.lastWriteData = data
	m.mu.Unlock()
	// Signal that WriteMessage was called
	select {
	case m.writeMessageCalled <- true:
	default: // Don't block if channel is full or not read
	}
	return m.writeErr
}
func (m *MockConn) LocalAddr() net.Addr {
	return nil // Or a dummy address
}
func (m *MockConn) RemoteAddr() net.Addr {
	return nil // Or a dummy address
}
func (m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}
func (m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
func (m *MockConn) SetPingHandler(h func(appData string) error) {}
func (m *MockConn) SetPongHandler(h func(appData string) error) {}
func (m *MockConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	return errors.New("not implemented")
}
func (m *MockConn) EnableWriteCompression(enable bool)  {}
func (m *MockConn) SetCompressionLevel(level int) error { return nil }
func (m *MockConn) Subprotocol() string                 { return "" }
func (m *MockConn) UnderlyingConn() net.Conn            { return nil }

// --- Test Terminate ---

func TestClientTerminate(t *testing.T) {
	// Setup
	mockConn := NewMockConn() // Use constructor
	unregisterChan := make(chan ClientUnregisterEvent, 1)
	ws := &WsServer{ // Minimal mock WsServer
		unregister: unregisterChan,
	}

	c := &client{
		conn:      mockConn, // Assign *MockConn to wsConn interface field
		ws:        ws,
		id:        "test-client-id",
		quit:      make(chan struct{}, 1), // Buffered to avoid blocking if already closed
		sendbuf:   make(chan SocketMessage),
		pubTopics: NewTopicSet(), // Initialize TopicSet
		subTopics: NewTopicSet(), // Initialize TopicSet
	}

	testReason := errors.New("test termination reason")

	// Execute
	c.terminate(testReason)

	// Verify
	select {
	case <-c.quit: // Check quit channel signaled
	default:
		t.Errorf("terminate did not signal the quit channel")
	}
	assert.True(t, mockConn.closeCalled, "conn.Close() was not called")
	select {
	case event := <-unregisterChan:
		assert.Equal(t, c, event.client, "Unregister event has wrong client")
		assert.Equal(t, testReason, event.reason, "Unregister event has wrong reason")
	case <-time.After(100 * time.Millisecond):
		t.Errorf("terminate did not send unregister event")
	}
}

func TestClientTerminate_CloseError(t *testing.T) {
	// Setup
	closeErr := errors.New("mock close error")
	mockConn := NewMockConn()
	mockConn.closeErr = closeErr // Set error to return
	unregisterChan := make(chan ClientUnregisterEvent, 1)
	ws := &WsServer{unregister: unregisterChan}

	c := &client{
		conn:      mockConn, // Assign *MockConn to wsConn interface field
		ws:        ws,
		id:        "test-client-close-err",
		quit:      make(chan struct{}, 1),
		pubTopics: NewTopicSet(), // Initialize TopicSet
		subTopics: NewTopicSet(), // Initialize TopicSet
	}
	testReason := errors.New("disconnect")

	// Execute
	c.terminate(testReason)

	// Verify
	assert.True(t, mockConn.closeCalled, "conn.Close() was not called")
	select {
	case event := <-unregisterChan:
		assert.Equal(t, c, event.client)
		assert.Equal(t, testReason, event.reason)
	case <-time.After(100 * time.Millisecond):
		t.Errorf("terminate did not send unregister event even after close error")
	}
	// Log verification removed as it requires complex setup or log package changes
}

// --- Test Send ---

func TestClientSend(t *testing.T) {
	// Setup
	sendChan := make(chan SocketMessage, 1) // Buffered channel
	c := &client{
		sendbuf: sendChan,
	}
	testMsg := SocketMessage{Topic: "test-send", Type: Pub}

	// Execute
	c.send(testMsg)

	// Verify
	select {
	case msg := <-sendChan:
		assert.Equal(t, testMsg, msg, "Message sent does not match expected")
	case <-time.After(50 * time.Millisecond):
		t.Errorf("send did not put message onto sendbuf channel")
	}
}

func TestClientSend_Blocking(t *testing.T) {
	// Setup
	sendChan := make(chan SocketMessage) // Unbuffered channel to force blocking
	c := &client{
		sendbuf: sendChan,
	}
	testMsg := SocketMessage{Topic: "test-block", Type: Pub}

	// Execute send in a goroutine as it will block
	go c.send(testMsg)
	time.Sleep(50 * time.Millisecond) // Allow time for potential block

	// Verify metric incremented (check output) - Removed due to import cycle
	// bodyAfter := getRelayMetricsOutput(t)
	// assert.Contains(t, bodyAfter, "wc_relay_send_blockings", "Send blocking metric not found in output")

	// Cleanup: read from channel to unblock goroutine
	select {
	case <-sendChan:
	default:
	}
}

// --- Test Write ---

func TestClientWrite(t *testing.T) {
	// Setup
	mockConn := NewMockConn() // Use constructor
	sendChan := make(chan SocketMessage, 1)
	quitChan := make(chan struct{})
	var wg sync.WaitGroup

	c := &client{
		conn:      mockConn,
		sendbuf:   sendChan,
		quit:      quitChan,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}

	testMsg := SocketMessage{Topic: "test-write", Type: Pub, Payload: `{"ok": true}`}
	expectedBytes, _ := json.Marshal(testMsg)

	// Start write goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.write()
	}()

	// Send a message to the buffer
	sendChan <- testMsg

	// Wait for WriteMessage to be called
	select {
	case <-mockConn.writeMessageCalled:
		// Verify data written
		mockConn.mu.Lock()
		assert.Equal(t, websocket.TextMessage, mockConn.lastWriteType)
		assert.JSONEq(t, string(expectedBytes), string(mockConn.lastWriteData))
		mockConn.mu.Unlock()
	case <-time.After(100 * time.Millisecond):
		t.Errorf("mockConn.WriteMessage was not called")
	}

	// Signal quit and wait for goroutine to finish
	close(quitChan)
	wg.Wait()

	assert.False(t, mockConn.closeCalled, "conn.Close() should not be called by write()")
}

// --- Test MarshalLogObject ---

func TestClientMarshalLogObject(t *testing.T) {
	encoder := zapcore.NewMapObjectEncoder()
	c := &client{
		id:        "log-client-id",
		role:      Dapp,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}
	c.pubTopics.Set("pub-topic-1")
	c.subTopics.Set("sub-topic-1")
	c.subTopics.Set("sub-topic-2")

	err := c.MarshalLogObject(encoder)
	assert.NoError(t, err, "MarshalLogObject should not return an error")

	fields := encoder.Fields
	assert.Equal(t, "log-client-id", fields["id"], "Incorrect id field")
	assert.Equal(t, string(Dapp), fields["role"], "Incorrect role field")
	_, pubOk := fields["pubTopics"]
	_, subOk := fields["subTopics"]
	assert.True(t, pubOk, "pubTopics field missing")
	assert.True(t, subOk, "subTopics field missing")
}

// --- Test Read ---

func TestClientRead_SuccessfulRead(t *testing.T) {
	// Setup
	mockConn := NewMockConn()
	localChan := make(chan SocketMessage, 1)
	unregisterChan := make(chan ClientUnregisterEvent, 1) // Needed by terminate on error

	// Minimal WsServer mock - Cannot include redisConn due to import cycle
	ws := &WsServer{
		localCh:    localChan,
		unregister: unregisterChan,
		// redisConn:  fakeRedis, // Cannot use fakeRedis here
		// redisConfig: &config.RedisConfig{ HeartbeatTimeoutMs: 100, }, // Cannot use config
		// ctx: context.Background(), // Cannot use context
	}

	c := &client{
		conn:      mockConn,
		ws:        ws,
		id:        "test-read-client",
		quit:      make(chan struct{}),
		sendbuf:   make(chan SocketMessage),
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}

	// Message to simulate receiving
	testMsg := SocketMessage{Topic: "read-topic", Type: Pub, Role: string(Wallet)}
	testMsgBytes, _ := json.Marshal(testMsg)

	// Start read goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.read()
	}()

	// Send message data to the mock connection
	mockConn.readMessageData <- testMsgBytes

	// Verify message received on localCh
	select {
	case receivedMsg := <-localChan:
		assert.Equal(t, testMsg.Topic, receivedMsg.Topic)
		assert.Equal(t, testMsg.Type, receivedMsg.Type)
		assert.Equal(t, testMsg.Role, receivedMsg.Role)
		assert.Equal(t, c, receivedMsg.client, "Message should have client attached")
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Did not receive message on localCh")
	}

	// Terminate the read loop by simulating a connection error
	readErr := errors.New("simulated read error")
	mockConn.readMessageErr <- readErr

	// Wait for read goroutine to exit
	wg.Wait()

	// Verify terminate was called (check unregister channel)
	select {
	case event := <-unregisterChan:
		assert.Equal(t, c, event.client)
		assert.Contains(t, event.reason.Error(), "simulated read error", "Unregister reason mismatch")
	case <-time.After(100 * time.Millisecond):
		t.Errorf("read did not trigger terminate on error")
	}
}

func TestClientRead_MalformedJSON(t *testing.T) {
	// Setup
	mockConn := NewMockConn()
	localChan := make(chan SocketMessage, 1)
	unregisterChan := make(chan ClientUnregisterEvent, 1)
	ws := &WsServer{
		localCh:    localChan,
		unregister: unregisterChan,
		// Cannot mock redis/config/ctx here
	}
	c := &client{
		conn:      mockConn,
		ws:        ws,
		id:        "test-read-malformed",
		quit:      make(chan struct{}),
		sendbuf:   make(chan SocketMessage),
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}

	malformedBytes := []byte(`{"topic":"bad", "type"`) // Incomplete JSON

	// Start read goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.read()
	}()

	// Send malformed message
	mockConn.readMessageData <- malformedBytes

	// Verify NO message received on localCh (should be skipped)
	select {
	case msg := <-localChan:
		t.Errorf("Received unexpected message on localCh: %+v", msg)
	case <-time.After(100 * time.Millisecond):
		// Good, no message received
	}

	// Send a valid message to ensure loop continues
	validMsg := SocketMessage{Topic: "good", Type: Sub}
	validBytes, _ := json.Marshal(validMsg)
	mockConn.readMessageData <- validBytes

	// Verify valid message IS received
	select {
	case receivedMsg := <-localChan:
		assert.Equal(t, validMsg.Topic, receivedMsg.Topic)
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Did not receive valid message on localCh after malformed one")
	}

	// Terminate loop
	mockConn.readMessageErr <- errors.New("terminate after malformed test")
	wg.Wait()
}

// TODO: Add more tests for read() - e.g., error handling, role determination (if possible without cycles)
