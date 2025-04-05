package relay_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/RabbyHub/derelay/relay"
	"github.com/redis/go-redis/v9"
	"pgregory.net/rapid" // Correct import path
)

// --- Placeholder Fake Types ---
type FakeRedisIO struct {
	mu                sync.Mutex
	PublishedMessages map[string][]interface{}
	AddedToStreams    map[string][]redis.XAddArgs
	AddedToSets       map[string][]interface{}
	ExpiredKeys       map[string]time.Duration
	nextPublishResult map[string]struct {
		Count int64
		Err   error
	}
}

func NewFakeRedisIO() *FakeRedisIO { /* ... */
	return &FakeRedisIO{
		PublishedMessages: make(map[string][]interface{}),
		AddedToStreams:    make(map[string][]redis.XAddArgs),
		AddedToSets:       make(map[string][]interface{}),
		ExpiredKeys:       make(map[string]time.Duration),
		nextPublishResult: make(map[string]struct {
			Count int64
			Err   error
		}),
	}
}
func (f *FakeRedisIO) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PublishedMessages[channel] = append(f.PublishedMessages[channel], message)
	res, ok := f.nextPublishResult[channel]
	if ok {
		delete(f.nextPublishResult, channel)
		return redis.NewIntResult(res.Count, res.Err)
	}
	return redis.NewIntResult(0, nil)
}
func (f *FakeRedisIO) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.AddedToStreams[a.Stream] = append(f.AddedToStreams[a.Stream], *a)
	return redis.NewStringResult("fake-id-123", nil)
}
func (f *FakeRedisIO) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.AddedToSets[key] = append(f.AddedToSets[key], members...)
	return redis.NewIntResult(int64(len(members)), nil)
}
func (f *FakeRedisIO) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ExpiredKeys[key] = expiration
	return redis.NewBoolResult(true, nil)
}
func (f *FakeRedisIO) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd {
	return redis.NewStatusResult("OK", nil)
}
func (f *FakeRedisIO) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return redis.NewXStreamSliceCmdResult(nil, nil)
}
func (f *FakeRedisIO) XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd {
	return redis.NewIntResult(int64(len(ids)), nil)
}
func (f *FakeRedisIO) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return redis.NewIntResult(int64(len(keys)), nil)
}
func (f *FakeRedisIO) Pipeline() redis.Pipeliner { return nil }
func (f *FakeRedisIO) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return redis.NewIntResult(int64(len(values)/2), nil)
}
func (f *FakeRedisIO) Subscribe(ctx context.Context, channels ...string) *redis.PubSub { return nil }
func (f *FakeRedisIO) Close() error                                                    { return nil }
func (f *FakeRedisIO) SetNextPublishResult(channel string, count int64, err error) { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextPublishResult[channel] = struct {
		Count int64
		Err   error
	}{count, err}
}
func (f *FakeRedisIO) GetPublishedMessages(channel string) []interface{} { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	msgs := make([]interface{}, len(f.PublishedMessages[channel]))
	copy(msgs, f.PublishedMessages[channel])
	return msgs
}
func (f *FakeRedisIO) GetAddedToStreams(stream string) []redis.XAddArgs { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	args := make([]redis.XAddArgs, len(f.AddedToStreams[stream]))
	copy(args, f.AddedToStreams[stream])
	return args
}
func (f *FakeRedisIO) ClearRecordedCalls() { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PublishedMessages = make(map[string][]interface{})
	f.AddedToStreams = make(map[string][]redis.XAddArgs)
	f.AddedToSets = make(map[string][]interface{})
	f.ExpiredKeys = make(map[string]time.Duration)
}

type FakeWsIO struct{}

func NewFakeWsIO() *FakeWsIO { return &FakeWsIO{} }

// TODO: Implement WsIO interface if defined

type FakeClientSender struct {
	ClientID     ClientID
	ClientRole   relay.RoleType
	SentMessages []relay.SocketMessage
	mu           sync.Mutex
}

func NewFakeClientSender(id ClientID, role relay.RoleType) *FakeClientSender { /* ... */
	return &FakeClientSender{ClientID: id, ClientRole: role}
}
func (f *FakeClientSender) Send(message relay.SocketMessage) { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.SentMessages = append(f.SentMessages, message)
}
func (f *FakeClientSender) ID() string           { return string(f.ClientID) }
func (f *FakeClientSender) Role() relay.RoleType { return f.ClientRole }
func (f *FakeClientSender) GetSentMessages() []relay.SocketMessage { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	msgs := make([]relay.SocketMessage, len(f.SentMessages))
	copy(msgs, f.SentMessages)
	return msgs
}
func (f *FakeClientSender) ClearSentMessages() { /* ... */
	f.mu.Lock()
	defer f.mu.Unlock()
	f.SentMessages = nil
}

// --- System Under Test ---
type SystemUnderTest struct {
	WsServer    *relay.WsServer
	FakeRedis   *FakeRedisIO
	FakeWsIO    *FakeWsIO
	FakeClients map[ClientID]*FakeClientSender
}

func NewSystemUnderTest(t *rapid.T) *SystemUnderTest { /* ... */
	fakeRedis := NewFakeRedisIO()
	fakeWsIO := NewFakeWsIO()
	fakeClients := make(map[ClientID]*FakeClientSender)
	cfg := config.Config{ /* Minimal config */ }
	ws := relay.NewWSServer(&cfg, fakeRedis)
	return &SystemUnderTest{WsServer: ws, FakeRedis: fakeRedis, FakeWsIO: fakeWsIO, FakeClients: fakeClients}
}
func (s *SystemUnderTest) ClearTestedSideEffects() { /* ... */
	s.FakeRedis.ClearRecordedCalls()
	for _, clientFake := range s.FakeClients {
		clientFake.ClearSentMessages()
	}
}

// --- Model Definition ---
type ClientID string
type Topic string
type PublishOutcome int

const (
	PublishOutcomeSucceedSubscribers PublishOutcome = iota
	PublishOutcomeSucceedNoSubscribers
	PublishOutcomeFail
)

type ModelState struct {
	ConnectedClients        map[ClientID]relay.RoleType
	Subscriptions           map[Topic]map[ClientID]bool
	CachedMessages          map[Topic][]relay.SocketMessage
	ExpectedDirectSends     map[ClientID][]relay.SocketMessage
	SimulatedPublishOutcome map[Topic]PublishOutcome
}

func NewModelState() *ModelState { /* ... */
	return &ModelState{
		ConnectedClients:        make(map[ClientID]relay.RoleType),
		Subscriptions:           make(map[Topic]map[ClientID]bool),
		CachedMessages:          make(map[Topic][]relay.SocketMessage),
		ExpectedDirectSends:     make(map[ClientID][]relay.SocketMessage),
		SimulatedPublishOutcome: make(map[Topic]PublishOutcome),
	}
}

// --- State Transition Methods ---
func (m *ModelState) ApplyConnect(clientID ClientID, role relay.RoleType) { /* ... */
	if m.ConnectedClients == nil {
		m.ConnectedClients = make(map[ClientID]relay.RoleType)
	}
	if m.ExpectedDirectSends == nil {
		m.ExpectedDirectSends = make(map[ClientID][]relay.SocketMessage)
	}
	m.ConnectedClients[clientID] = role
	if _, ok := m.ExpectedDirectSends[clientID]; !ok {
		m.ExpectedDirectSends[clientID] = []relay.SocketMessage{}
	}
}
func (m *ModelState) ApplyDisconnect(clientID ClientID) { /* ... */
	delete(m.ConnectedClients, clientID)
	if m.Subscriptions != nil {
		for topic, clients := range m.Subscriptions {
			if _, ok := clients[clientID]; ok {
				delete(clients, clientID)
				if len(clients) == 0 {
					delete(m.Subscriptions, topic)
				}
			}
		}
	}
	delete(m.ExpectedDirectSends, clientID)
}
func (m *ModelState) ApplySubscribe(clientID ClientID, topic Topic) { /* ... */
	if m.Subscriptions == nil {
		m.Subscriptions = make(map[Topic]map[ClientID]bool)
	}
	if m.Subscriptions[topic] == nil {
		m.Subscriptions[topic] = make(map[ClientID]bool)
	}
	if m.ExpectedDirectSends == nil {
		m.ExpectedDirectSends = make(map[ClientID][]relay.SocketMessage)
	}
	if m.CachedMessages == nil {
		m.CachedMessages = make(map[Topic][]relay.SocketMessage)
	}
	m.Subscriptions[topic][clientID] = true
	if cached, ok := m.CachedMessages[topic]; ok && len(cached) > 0 {
		if _, clientOk := m.ExpectedDirectSends[clientID]; !clientOk {
			m.ExpectedDirectSends[clientID] = []relay.SocketMessage{}
		}
		m.ExpectedDirectSends[clientID] = append(m.ExpectedDirectSends[clientID], cached...)
		delete(m.CachedMessages, topic)
	}
}
func (m *ModelState) ApplySetPublishOutcome(topic Topic, outcome PublishOutcome) { /* ... */
	if m.SimulatedPublishOutcome == nil {
		m.SimulatedPublishOutcome = make(map[Topic]PublishOutcome)
	}
	m.SimulatedPublishOutcome[topic] = outcome
}
func (m *ModelState) ApplyPublish(publisherID ClientID, msg relay.SocketMessage) { /* ... */
	publisherRole := m.ConnectedClients[publisherID]
	topic := Topic(msg.Topic)
	outcome, ok := m.SimulatedPublishOutcome[topic]
	if !ok {
		outcome = PublishOutcomeSucceedSubscribers
	}
	delete(m.SimulatedPublishOutcome, topic)
	if m.CachedMessages == nil {
		m.CachedMessages = make(map[Topic][]relay.SocketMessage)
	}
	if m.ExpectedDirectSends == nil {
		m.ExpectedDirectSends = make(map[ClientID][]relay.SocketMessage)
	}

	if outcome == PublishOutcomeFail || outcome == PublishOutcomeSucceedNoSubscribers {
		m.CachedMessages[topic] = append(m.CachedMessages[topic], msg)
	} else {
		subscribersExist := false
		if subs, topicExists := m.Subscriptions[topic]; topicExists && len(subs) > 0 {
			subscribersExist = true
		}
		if subscribersExist {
			for subID := range m.Subscriptions[topic] {
				if subID == publisherID {
					continue
				}
				if _, clientOk := m.ExpectedDirectSends[subID]; !clientOk {
					m.ExpectedDirectSends[subID] = []relay.SocketMessage{}
				}
				m.ExpectedDirectSends[subID] = append(m.ExpectedDirectSends[subID], msg)
			}
			if publisherRole == relay.Dapp {
				ackMsg := relay.SocketMessage{Topic: msg.Topic, Type: relay.Ack, Role: string(relay.Wallet)}
				if _, clientOk := m.ExpectedDirectSends[publisherID]; !clientOk {
					m.ExpectedDirectSends[publisherID] = []relay.SocketMessage{}
				}
				m.ExpectedDirectSends[publisherID] = append(m.ExpectedDirectSends[publisherID], ackMsg)
			}
		} else {
			m.CachedMessages[topic] = append(m.CachedMessages[topic], msg)
		}
	}
}

// --- Rapid State Machine ---

// RelayStateMachine implements rapid.StateMachine
type RelayStateMachine struct {
	sut   *SystemUnderTest
	model *ModelState
}

// Init initializes the state machine.
func (sm *RelayStateMachine) Init(t *rapid.T) {
	sm.sut = NewSystemUnderTest(t)
	sm.model = NewModelState()
}

// Cleanup is called after the test sequence finishes.
func (sm *RelayStateMachine) Cleanup(t *rapid.T) {
	// TODO: Shutdown WsServer goroutine if started in Init.
}

// --- Rapid Generators ---
// Corrected generator definitions using rapid API
var genClientID = rapid.Map(rapid.StringMatching(`^[a-zA-Z0-9_]{1,10}$`), func(s string) ClientID { return ClientID(s) })
var genTopic = rapid.Map(rapid.StringMatching(`^[a-zA-Z0-9_]{1,10}$`), func(s string) Topic { return Topic(s) })
var genRole = rapid.SampledFrom([]relay.RoleType{relay.Dapp, relay.Wallet})
var genPublishOutcome = rapid.SampledFrom([]PublishOutcome{PublishOutcomeSucceedSubscribers, PublishOutcomeSucceedNoSubscribers, PublishOutcomeFail})
var genMessageType = rapid.SampledFrom([]relay.MessageType{relay.Pub, relay.Sub, relay.Ack, relay.Ping})

var genSocketMessage = rapid.Custom(func(t *rapid.T) relay.SocketMessage {
	// Draw returns the concrete type, no assertion needed
	topic := genTopic.Draw(t, "msg.topic")
	msgType := genMessageType.Draw(t, "msg.type")
	payload := rapid.String().Draw(t, "msg.payload")
	role := rapid.String().Draw(t, "msg.role")
	phase := rapid.String().Draw(t, "msg.phase")
	silent := rapid.Bool().Draw(t, "msg.silent")
	return relay.SocketMessage{Topic: string(topic), Type: msgType, Payload: payload, Role: role, Phase: phase, Silent: silent}
})

// --- Rapid Actions ---

func (sm *RelayStateMachine) Connect(t *rapid.T) {
	clientID := genClientID.Draw(t, "clientID")
	role := genRole.Draw(t, "role")
	if _, exists := sm.model.ConnectedClients[clientID]; exists {
		t.Skip("Client already connected in model")
	}

	sm.sut.FakeClients[clientID] = NewFakeClientSender(clientID, role)
	// TODO: Run SUT logic for connect
	t.Logf("Action: Connect Client %s (%s)", clientID, role)
	sm.model.ApplyConnect(clientID, role)
	// TODO: Add Check logic
	sm.sut.ClearTestedSideEffects()
}

func (sm *RelayStateMachine) Disconnect(t *rapid.T) {
	if len(sm.model.ConnectedClients) == 0 {
		t.Skip("No clients connected to disconnect")
	}
	clients := make([]ClientID, 0, len(sm.model.ConnectedClients))
	for id := range sm.model.ConnectedClients {
		clients = append(clients, id)
	}
	// Draw returns int directly
	idx := rapid.IntRange(0, len(clients)-1).Draw(t, "clientIndex")
	clientID := clients[idx]

	delete(sm.sut.FakeClients, clientID)
	// TODO: Run SUT logic for disconnect
	t.Logf("Action: Disconnect Client %s", clientID)
	sm.model.ApplyDisconnect(clientID)
	// TODO: Add Check logic (e.g., check Redis DEL calls)
	sm.sut.ClearTestedSideEffects()
}

func (sm *RelayStateMachine) Subscribe(t *rapid.T) {
	if len(sm.model.ConnectedClients) == 0 {
		t.Skip("No clients connected to subscribe")
	}
	clients := make([]ClientID, 0, len(sm.model.ConnectedClients))
	for id := range sm.model.ConnectedClients {
		clients = append(clients, id)
	}
	idx := rapid.IntRange(0, len(clients)-1).Draw(t, "clientIndex")
	clientID := clients[idx]
	topic := genTopic.Draw(t, "topic")

	// TODO: Run SUT logic for subscribe
	t.Logf("Action: Client %s Subscribe Topic %s", clientID, topic)
	sm.model.ApplySubscribe(clientID, topic)

	// Check direct sends from cache
	clientSender := sm.sut.FakeClients[clientID]
	expectedSends := sm.model.ExpectedDirectSends[clientID]
	actualSends := clientSender.GetSentMessages()
	if !reflect.DeepEqual(expectedSends, actualSends) {
		// Use t.Fatalf for fatal errors in rapid checks
		t.Fatalf("Subscribe Check Failed: Client %s on Topic %s\nExpected Sends: %+v\nActual Sends: %+v", clientID, topic, expectedSends, actualSends)
	}
	clientSender.ClearSentMessages()
	sm.model.ExpectedDirectSends[clientID] = nil
	sm.sut.ClearTestedSideEffects()
}

func (sm *RelayStateMachine) SetPublishOutcome(t *rapid.T) {
	topic := genTopic.Draw(t, "topic")
	outcome := genPublishOutcome.Draw(t, "outcome")

	// Run: Configure FakeRedisIO
	sm.sut.FakeRedis.SetNextPublishResult(string(topic), 0, nil) // Reset
	switch outcome {
	case PublishOutcomeSucceedSubscribers:
		sm.sut.FakeRedis.SetNextPublishResult(string(topic), 1, nil)
	case PublishOutcomeSucceedNoSubscribers:
		sm.sut.FakeRedis.SetNextPublishResult(string(topic), 0, nil)
	case PublishOutcomeFail:
		sm.sut.FakeRedis.SetNextPublishResult(string(topic), 0, errors.New("simulated redis publish error"))
	}
	t.Logf("Action: SetPublishOutcome for Topic %s to %v", topic, outcome)
	sm.model.ApplySetPublishOutcome(topic, outcome)
}

func (sm *RelayStateMachine) Publish(t *rapid.T) {
	if len(sm.model.ConnectedClients) == 0 {
		t.Skip("No clients connected to publish")
	}
	clients := make([]ClientID, 0, len(sm.model.ConnectedClients))
	for id := range sm.model.ConnectedClients {
		clients = append(clients, id)
	}
	idx := rapid.IntRange(0, len(clients)-1).Draw(t, "clientIndex")
	publisherID := clients[idx]
	msg := genSocketMessage.Draw(t, "message")
	msg.Role = string(sm.model.ConnectedClients[publisherID])

	// TODO: Run SUT logic for publish
	t.Logf("Action: Client %s Publish Topic %s", publisherID, msg.Topic)
	sm.model.ApplyPublish(publisherID, msg)

	// Check: Compare model expectations vs fake states
	topic := Topic(msg.Topic)
	publisherSender := sm.sut.FakeClients[publisherID]
	// Check Redis calls (Publish, XAdd)
	// TODO: Implement checks using sm.sut.FakeRedis getters
	// Check Direct Sends (ACKs)
	expectedDirect := sm.model.ExpectedDirectSends[publisherID]
	actualDirect := publisherSender.GetSentMessages()
	if !reflect.DeepEqual(expectedDirect, actualDirect) {
		t.Fatalf("Publish Check Failed (Direct Send): Publisher %s on Topic %s\nExpected: %+v\nActual: %+v", publisherID, topic, expectedDirect, actualDirect)
	}
	publisherSender.ClearSentMessages()
	sm.model.ExpectedDirectSends[publisherID] = nil

	// Check Sends to Subscribers
	if subs, ok := sm.model.Subscriptions[topic]; ok {
		for subID := range subs {
			if subID == publisherID {
				continue
			}
			subSender := sm.sut.FakeClients[subID]
			expectedSubSends := sm.model.ExpectedDirectSends[subID]
			actualSubSends := subSender.GetSentMessages()
			if !reflect.DeepEqual(expectedSubSends, actualSubSends) {
				t.Fatalf("Publish Check Failed (Subscriber Send): Subscriber %s on Topic %s\nExpected: %+v\nActual: %+v", subID, topic, expectedSubSends, actualSubSends)
			}
			subSender.ClearSentMessages()
			sm.model.ExpectedDirectSends[subID] = nil
		}
	}
	sm.sut.ClearTestedSideEffects()
}

// --- Rapid Test Runner ---

func TestRapidRelayStateMachine(t *testing.T) {
	// Pass the *testing.T to rapid.Run
	rapid.Run(t, &RelayStateMachine{})
}

// Note: rapid discovers actions via reflection on the StateMachine struct.
// No explicit init() registration is typically needed like in gopter's command pattern.
