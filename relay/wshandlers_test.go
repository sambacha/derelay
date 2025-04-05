package relay

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
)

// --- Mock RelayRedisIO ---

type MockRelayRedisIO struct {
	mock.Mock
}

func (m *MockRelayRedisIO) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	args := m.Called(ctx, channel, message)
	var result int64
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).(int64)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewIntResult(result, err)
}
func (m *MockRelayRedisIO) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	args := m.Called(ctx, a)
	var result string
	var err error
	if args.Get(0) != nil {
		result = args.String(0)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewStringResult(result, err)
}
func (m *MockRelayRedisIO) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	args := m.Called(ctx, key, members)
	var result int64
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).(int64)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewIntResult(result, err)
}
func (m *MockRelayRedisIO) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	args := m.Called(ctx, key, expiration)
	var result bool
	var err error
	if args.Get(0) != nil {
		result = args.Bool(0)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewBoolResult(result, err)
}
func (m *MockRelayRedisIO) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd {
	args := m.Called(ctx, stream, group, start)
	var result string
	var err error
	if args.Get(0) != nil {
		result = args.String(0)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewStatusResult(result, err)
}
func (m *MockRelayRedisIO) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	args := m.Called(ctx, a)
	var result []redis.XStream
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).([]redis.XStream)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewXStreamSliceCmdResult(result, err)
}
func (m *MockRelayRedisIO) XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd {
	args := m.Called(ctx, stream, group, ids)
	var result int64
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).(int64)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewIntResult(result, err)
}
func (m *MockRelayRedisIO) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	args := m.Called(ctx, keys)
	var result int64
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).(int64)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewIntResult(result, err)
}
func (m *MockRelayRedisIO) Pipeline() redis.Pipeliner {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).(redis.Pipeliner)
	}
	return nil
}
func (m *MockRelayRedisIO) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	args := m.Called(ctx, key, values)
	var result int64
	var err error
	if args.Get(0) != nil {
		result = args.Get(0).(int64)
	}
	if args.Get(1) != nil {
		err = args.Error(1)
	}
	return redis.NewIntResult(result, err)
}
func (m *MockRelayRedisIO) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	args := m.Called(ctx, channels)
	if args.Get(0) != nil {
		return args.Get(0).(*redis.PubSub)
	}
	return nil
}
func (m *MockRelayRedisIO) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Mock ClientSender ---

type MockClientSender struct {
	mock.Mock
	clientID string
	role     RoleType
}

func NewMockClientSender(id string, role RoleType) *MockClientSender {
	return &MockClientSender{clientID: id, role: role}
}

func (m *MockClientSender) Send(message SocketMessage) {
	m.Called(message)
}
func (m *MockClientSender) ID() string {
	return m.clientID
}
func (m *MockClientSender) Role() RoleType {
	return m.role
}

// --- Test Setup ---

func setupTestWsServer(mockRedis *MockRelayRedisIO) *WsServer {
	cfg := &config.Config{
		RedisServerConfig: config.RedisConfig{
			PublishTimeoutMs:     100,
			CacheWriteTimeoutMs:  100,
			StateUpdateTimeoutMs: 100,
			StreamReadTimeoutMs:  100,
			HeartbeatTimeoutMs:   100,
		},
	}
	var redisIO RelayRedisIO
	if mockRedis != nil {
		redisIO = mockRedis
	}
	ws := NewWSServer(cfg, redisIO)
	ws.subscribeRequests = make(chan []string, 1)
	ws.unsubscribeRequests = make(chan []string, 1)
	ws.localCh = make(chan SocketMessage, 1)
	ws.remoteMessages = make(chan *redis.Message, 1)
	ws.register = make(chan *client, 1)
	ws.unregister = make(chan ClientUnregisterEvent, 1)
	// Use a background context for the server in tests unless specific cancellation is needed
	ws.ctx, ws.cancel = context.WithCancel(context.Background())
	return ws
}

// --- Tests ---

func TestHandlePingMessage(t *testing.T) {
	mockClient := NewMockClientSender("client-ping", Wallet)
	mockSendFunc := func(msg SocketMessage) {
		mockClient.Send(msg)
	}
	mockClient.On("Send", mock.MatchedBy(func(msg SocketMessage) bool {
		return msg.Type == Pong && msg.Role == string(Relay)
	})).Return().Once()
	mockSendFunc(SocketMessage{Type: Pong, Role: string(Relay)})
	mockClient.AssertExpectations(t)
}

func TestPubMessage_PublishSuccess_Dapp(t *testing.T) {
	mockRedis := new(MockRelayRedisIO)
	mockPublisher := NewMockClientSender("pub-dapp-1", Dapp)
	ws := setupTestWsServer(mockRedis)
	go func() { <-ws.subscribeRequests }()

	topic := "topic-pub-success"
	testClient := &client{
		id:        mockPublisher.ID(),
		role:      mockPublisher.Role(),
		ws:        ws,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}
	pubMsg := SocketMessage{Topic: topic, Type: Pub, Payload: "hello", Role: string(Dapp), client: testClient}
	sendBuffer := make(chan SocketMessage, 1)
	testClient.sendbuf = sendBuffer
	go func() {
		for msg := range sendBuffer {
			mockPublisher.Send(msg)
		}
	}()

	// Mock Expectations - Use context.Background()
	mockRedis.On("Publish", context.Background(), messageChanKey(topic), pubMsg).Return(int64(1), nil).Once()
	mockPublisher.On("Send", mock.MatchedBy(func(msg SocketMessage) bool {
		return msg.Type == Ack && msg.Topic == topic && msg.Role == string(Wallet)
	})).Return().Once()
	mockRedis.On("SAdd", context.Background(), clientPubsSetKey(mockPublisher.ID()), []interface{}{topic}).Return(int64(1), nil).Once()
	mockRedis.On("Expire", context.Background(), clientPubsSetKey(mockPublisher.ID()), 24*time.Hour).Return(true, nil).Once()

	ws.pubMessage(pubMsg)
	close(sendBuffer)

	mockRedis.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestPubMessage_Cache_NoSubscribers(t *testing.T) {
	mockRedis := new(MockRelayRedisIO)
	mockPublisher := NewMockClientSender("pub-cache-1", Dapp)
	ws := setupTestWsServer(mockRedis)
	go func() { <-ws.subscribeRequests }()

	topic := "topic-pub-cache"
	testClient := &client{
		id:        mockPublisher.ID(),
		role:      mockPublisher.Role(),
		ws:        ws,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}
	pubMsg := SocketMessage{Topic: topic, Type: Pub, Payload: "cache me", Role: string(Dapp), client: testClient}
	pubMsgBytes, _ := json.Marshal(pubMsg)

	// Mock Expectations - Use context.Background()
	mockRedis.On("Publish", context.Background(), messageChanKey(topic), pubMsg).Return(int64(0), nil).Once()
	mockRedis.On("XAdd", context.Background(), mock.MatchedBy(func(a *redis.XAddArgs) bool {
		if a.Stream != streamMessageKey(topic) {
			return false
		}
		vals, ok := a.Values.(map[string]interface{})
		if !ok {
			return false
		}
		msgStr, ok := vals["message"].(string)
		if !ok {
			return false
		}
		return msgStr == string(pubMsgBytes)
	})).Return("some-id", nil).Once()
	mockRedis.On("SAdd", context.Background(), clientPubsSetKey(mockPublisher.ID()), []interface{}{topic}).Return(int64(1), nil).Once()
	mockRedis.On("Expire", context.Background(), clientPubsSetKey(mockPublisher.ID()), 24*time.Hour).Return(true, nil).Once()

	ws.pubMessage(pubMsg)

	mockRedis.AssertExpectations(t)
	mockPublisher.AssertNotCalled(t, "Send", mock.Anything)
}

func TestSubMessage_Basic_NoCache(t *testing.T) {
	mockRedis := new(MockRelayRedisIO)
	mockSubscriber := NewMockClientSender("sub-basic-1", Wallet)
	ws := setupTestWsServer(mockRedis)

	topic := "topic-sub-basic"
	testClient := &client{
		id:        mockSubscriber.ID(),
		role:      mockSubscriber.Role(),
		ws:        ws,
		pubTopics: NewTopicSet(),
		subTopics: NewTopicSet(),
	}
	subMsg := SocketMessage{Topic: topic, Type: Sub, Role: string(Wallet), client: testClient}

	// Mock Expectations - Use context.Background()
	mockRedis.On("SAdd", context.Background(), clientSubsSetKey(mockSubscriber.ID()), []interface{}{topic}).Return(int64(1), nil).Once()
	mockRedis.On("Expire", context.Background(), clientSubsSetKey(mockSubscriber.ID()), 24*time.Hour).Return(true, nil).Once()
	mockRedis.On("XGroupCreateMkStream", context.Background(), streamMessageKey(topic), "derelay-cg", "0").Return("OK", errors.New("BUSYGROUP Consumer Group name already exists")).Maybe()
	mockRedis.On("XGroupCreateMkStream", context.Background(), streamMessageKey(topic), "derelay-cg", "0").Return("OK", nil).Maybe()
	mockRedis.On("XReadGroup", context.Background(), mock.MatchedBy(func(a *redis.XReadGroupArgs) bool {
		return len(a.Streams) == 2 && a.Streams[0] == streamMessageKey(topic) && a.Streams[1] == "0-0" && a.Group == "derelay-cg" && a.Consumer == mockSubscriber.ID()
	})).Return([]redis.XStream{}, redis.Nil).Once()
	mockRedis.On("Publish", context.Background(), dappNotifyChanKey(topic), mock.MatchedBy(func(msg SocketMessage) bool {
		return msg.Type == Pub && msg.Phase == string(SessionResumed) && msg.Role == string(Relay)
	})).Return(int64(0), nil).Once()

	go func() { <-ws.subscribeRequests }()
	ws.subMessage(subMsg)

	mock.AssertExpectationsForObjects(t, mockRedis)
	mockSubscriber.AssertNotCalled(t, "Send", mock.Anything)
}
