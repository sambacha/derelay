package relay

import (
	"encoding/json"
	"testing"
	"testing/quick"

	// "sync" // No longer needed

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore" // Needed for MarshalLogArray test
)

// Removed testClient helper struct

func TestTopicClientSetOperations(t *testing.T) {
	ts := NewTopicClientSet()
	// Use actual *client struct since test is in the same package
	c1 := &client{id: "1"}
	c2 := &client{id: "2"}
	c3 := &client{id: "3"}

	// Test initial state
	assert.Equal(t, 0, ts.Len("topicA"), "Initial length should be 0")
	assert.Nil(t, ts.Get("topicA"), "Getting non-existent topic should return nil")

	// Test Set
	ts.Set("topicA", c1) // No cast needed
	assert.Equal(t, 1, ts.Len("topicA"), "Length after adding c1")
	ts.Set("topicA", c2)
	assert.Equal(t, 2, ts.Len("topicA"), "Length after adding c2")
	ts.Set("topicB", c1)
	assert.Equal(t, 1, ts.Len("topicB"), "Length of topicB after adding c1")
	assert.Equal(t, 2, ts.Len("topicA"), "Length of topicA should be unchanged")

	// Test Set duplicate
	ts.Set("topicA", c1)
	assert.Equal(t, 2, ts.Len("topicA"), "Length after adding duplicate c1")

	// Test Get
	clientsA := ts.Get("topicA")
	require.NotNil(t, clientsA, "Get should not return nil for existing topic")
	assert.Equal(t, 2, len(clientsA), "Get should return correct number of clients")
	_, ok1 := clientsA[c1]
	_, ok2 := clientsA[c2]
	assert.True(t, ok1, "Client 1 should be in the set")
	assert.True(t, ok2, "Client 2 should be in the set")

	// Test Unset
	ts.Unset("topicA", c1)
	assert.Equal(t, 1, ts.Len("topicA"), "Length after unsetting c1")
	clientsA = ts.Get("topicA")
	_, ok1 = clientsA[c1]
	_, ok2 = clientsA[c2]
	assert.False(t, ok1, "Client 1 should not be in the set after unset")
	assert.True(t, ok2, "Client 2 should still be in the set after unset")

	// Test Unset non-existent client/topic
	ts.Unset("topicA", c3) // Client not in topic
	assert.Equal(t, 1, ts.Len("topicA"))
	ts.Unset("topicC", c1) // Topic doesn't exist
	assert.Equal(t, 0, ts.Len("topicC"))

	// Test Clear
	ts.Set("topicA", c1)
	ts.Set("topicA", c3)
	assert.Equal(t, 3, ts.Len("topicA"))
	ts.Clear("topicA")
	assert.Equal(t, 0, ts.Len("topicA"), "Length after clear")
	assert.Nil(t, ts.Get("topicA"), "Get after clear should return nil")

	// Test Clear non-existent topic
	ts.Clear("topicD") // Should not panic
}

func TestTopicClientSet_GetTopicsByClient(t *testing.T) {
	ts := NewTopicClientSet()
	c1 := &client{id: "1"}
	c2 := &client{id: "2"}

	// Setup
	ts.Set("topicA", c1)
	ts.Set("topicB", c1)
	ts.Set("topicC", c1)
	ts.Set("topicA", c2)
	ts.Set("topicD", c2)

	// Get without clearing
	topicsC1 := ts.GetTopicsByClient(c1, false)
	assert.ElementsMatch(t, []string{"topicA", "topicB", "topicC"}, topicsC1)
	// Verify state unchanged
	assert.Equal(t, 2, ts.Len("topicA"))
	assert.Equal(t, 1, ts.Len("topicB"))
	assert.Equal(t, 1, ts.Len("topicC"))
	assert.Equal(t, 1, ts.Len("topicD"))

	// Get with clearing
	topicsC1Cleared := ts.GetTopicsByClient(c1, true)
	assert.ElementsMatch(t, []string{"topicA", "topicB", "topicC"}, topicsC1Cleared)
	// Verify c1 removed
	assert.Equal(t, 1, ts.Len("topicA"), "Length of topicA after clearing c1")
	assert.Equal(t, 0, ts.Len("topicB"), "Length of topicB after clearing c1")
	assert.Equal(t, 0, ts.Len("topicC"), "Length of topicC after clearing c1")
	// Verify c2 unaffected
	assert.Equal(t, 1, ts.Len("topicD"))
	clientsA := ts.Get("topicA")
	_, ok1 := clientsA[c1]
	_, ok2 := clientsA[c2]
	assert.False(t, ok1, "c1 should be removed from topicA")
	assert.True(t, ok2, "c2 should remain in topicA")

	// Get again after clearing should yield empty
	topicsC1Again := ts.GetTopicsByClient(c1, false)
	assert.Empty(t, topicsC1Again)
}

func TestTopicSetBasic(t *testing.T) { // Renamed old test
	ts := NewTopicSet() // Use NewTopicSet

	ts.Set("hello")
	ts.Set("hello") // Add duplicate
	ts.Set("world")

	assert.Equal(t, 2, len(ts.Data), "Length should ignore duplicate add")

	data := ts.Get()
	assert.Equal(t, 2, len(data), "Length from Get()")
	_, ok1 := data["hello"]
	_, ok2 := data["world"]
	assert.True(t, ok1)
	assert.True(t, ok2)

	// Test Get returns a copy
	data["new_key"] = struct{}{}
	assert.Equal(t, 2, len(ts.Data), "Original map should not be modified")
}

// Mock ArrayEncoder for testing MarshalLogArray
type mockArrayEncoder struct {
	zapcore.ArrayEncoder // Embed to satisfy interface easily
	elements             []string
}

func (m *mockArrayEncoder) AppendString(s string) {
	m.elements = append(m.elements, s)
}

// Implement other AppendXxx methods as needed, potentially just panicking or no-op

func TestTopicSet_MarshalLogArray(t *testing.T) {
	ts := NewTopicSet()
	ts.Set("topic1")
	ts.Set("topic2")
	ts.Set("topic3")

	encoder := &mockArrayEncoder{}
	err := ts.MarshalLogArray(encoder)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"topic1", "topic2", "topic3"}, encoder.elements)

	// Test empty set
	tsEmpty := NewTopicSet()
	encoderEmpty := &mockArrayEncoder{}
	errEmpty := tsEmpty.MarshalLogArray(encoderEmpty)
	assert.NoError(t, errEmpty)
	assert.Empty(t, encoderEmpty.elements)
}

// --- Old Tests Below (kept for reference, can be removed) ---

/* Commenting out old tests as they are superseded by the enhanced ones above
func TestTopicSetBasic(t *testing.T) {
	ts := NewTopicClientSet()

	c := &client{id: "1"} // This uses the actual unexported client
	ts.Set("hello", c)
	ts.Set("hello", &client{id: "2"})
	ts.Set("hello", &client{id: "3"})

	actualLen := ts.Len("hello")
	expectedLen := 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}

	ts.Unset("hello", c)
	expectedLen = 2
	actualLen = ts.Len("hello")
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}
}

func TestTopicSetReference(t *testing.T) {
	ts := NewTopicClientSet()

	c := &client{id: "1"} // This uses the actual unexported client
	ts.Set("hello", c)
	ts.Set("hello", &client{id: "2"})
	ts.Set("hello", &client{id: "3"})

	actualLen := ts.Len("hello")
	expectedLen := 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}

	newts := ts

	newts.Unset("hello", c)
	expectedLen = 2
	actualLen = ts.Len("hello")
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}
}

func TestGetTopicsByClientAndClear(t *testing.T) {
	ts := NewTopicClientSet()

	c := &client{id: "1"} // This uses the actual unexported client
	ts.Set("hello", c)
	ts.Set("hello1", c)
	ts.Set("hello2", c)
	ts.Set("hello", &client{id: "2"})
	ts.Set("hello", &client{id: "3"})

	actualLen := ts.Len("hello")
	expectedLen := 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}

	topics := ts.GetTopicsByClient(c, false)
	actualLen = len(topics)
	expectedLen = 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}

	topics = ts.GetTopicsByClient(c, true)
	actualLen = len(topics)
	expectedLen = 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}

	topics = ts.GetTopicsByClient(c, true)
	actualLen = len(topics)
	expectedLen = 0
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}
}

func TestTopicGet_Old(t *testing.T) { // Renamed old test
	ts := NewTopicSet()

	ts.Set("hello")
	ts.Set("hello1")
	ts.Set("hello2")

	topics := ts.Get()
	actualLen := len(topics)
	expectedLen := 3
	if actualLen != expectedLen {
		t.Errorf("length error, expected: %v, actual: %v", expectedLen, actualLen)
	}
	if _, ok := topics["hello"]; !ok {
		t.Errorf("key does not exists")
	}
	if _, ok := topics["hello1"]; !ok {
		t.Errorf("key does not exists")
	}
	if _, ok := topics["hello2"]; !ok {
		t.Errorf("key does not exists")
	}
}
*/ // Correctly terminated comment block

func TestKeyGenerationFunctions(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		function  func(string) string
		expected  string
		expectNil bool // For generateRandomBytes16 edge case
	}{
		{"streamMessageKey", "topic1", streamMessageKey, "wc:relay:stream:messages:topic1", false},
		{"messageChanKey", "topic2", messageChanKey, "wc:relay:chan:messages:topic2", false},
		{"dappNotifyChanKey", "topic3", dappNotifyChanKey, "wc:relay:chan:dappNotify:topic3", false},
		{"clientHashKey", "clientA", clientHashKey, "wc:relay:client:clientA", false},
		{"clientSubsSetKey", "clientB", clientSubsSetKey, "wc:relay:client:subs:clientB", false},
		{"clientPubsSetKey", "clientC", clientPubsSetKey, "wc:relay:client:pubs:clientC", false},
		{"streamMessageKey empty", "", streamMessageKey, "wc:relay:stream:messages:", false},
		{"messageChanKey empty", "", messageChanKey, "wc:relay:chan:messages:", false},
		{"dappNotifyChanKey empty", "", dappNotifyChanKey, "wc:relay:chan:dappNotify:", false},
		{"clientHashKey empty", "", clientHashKey, "wc:relay:client:", false},
		{"clientSubsSetKey empty", "", clientSubsSetKey, "wc:relay:client:subs:", false},
		{"clientPubsSetKey empty", "", clientPubsSetKey, "wc:relay:client:pubs:", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.function(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}

	t.Run("generateRandomBytes16", func(t *testing.T) {
		val1 := generateRandomBytes16()
		val2 := generateRandomBytes16()
		assert.NotEmpty(t, val1)
		assert.NotEmpty(t, val2)
		assert.NotEqual(t, val1, val2)
		assert.Len(t, val1, 24, "Expected base64 encoded length of 24")
		assert.Len(t, val2, 24, "Expected base64 encoded length of 24")
	})
}

func TestFromDappNotifyChan(t *testing.T) {
	t.Logf("Constant dappNotifyChan: %q", dappNotifyChan)
	input1 := "wc:relay:chan:dappNotify:topic123"
	t.Logf("Testing input: %q, Expected: true, Got: %v", input1, fromDappNotifyChan(input1))
	assert.True(t, fromDappNotifyChan(input1))

	input2 := "wc:relay:chan:messages:topic123"
	t.Logf("Testing input: %q, Expected: false, Got: %v", input2, fromDappNotifyChan(input2))
	assert.False(t, fromDappNotifyChan(input2))

	input3 := "some:other:prefix:topic123"
	t.Logf("Testing input: %q, Expected: false, Got: %v", input3, fromDappNotifyChan(input3))
	assert.False(t, fromDappNotifyChan(input3))

	input4 := "wc:relay:chan:dappNotify:"                                                    // Prefix only
	t.Logf("Testing input: %q, Expected: true, Got: %v", input4, fromDappNotifyChan(input4)) // Corrected expectation
	assert.True(t, fromDappNotifyChan(input4))                                               // Corrected assertion

	input5 := "" // Empty string
	t.Logf("Testing input: %q, Expected: false, Got: %v", input5, fromDappNotifyChan(input5))
	assert.False(t, fromDappNotifyChan(input5))
}

// Property-based test for SocketMessage serialization/deserialization
func TestSocketMessageMarshalUnmarshalProperty(t *testing.T) {
	property := func(original SocketMessage) bool {
		data, err := original.MarshalBinary()
		if err != nil {
			t.Logf("MarshalBinary failed for input %v: %v", original, err)
			return false
		}
		reconstructed, err := decodeSocketMessage(data)
		if err != nil {
			t.Logf("Unmarshal failed for data %s: %v", string(data), err)
			return false
		}
		return original.Topic == reconstructed.Topic &&
			original.Type == reconstructed.Type &&
			original.Payload == reconstructed.Payload &&
			original.Role == reconstructed.Role &&
			original.Phase == reconstructed.Phase &&
			original.Silent == reconstructed.Silent
	}
	config := &quick.Config{MaxCount: 1000}
	if err := quick.Check(property, config); err != nil {
		t.Errorf("SocketMessage marshal/unmarshal property failed: %v", err)
	}
}

// Benchmark for SocketMessage serialization/deserialization
func BenchmarkSocketMessageMarshalUnmarshal(b *testing.B) {
	msg := SocketMessage{
		Topic:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		Type:    Pub,
		Payload: `{"jsonrpc":"2.0","method":"some_method","params":{"data":"0x...","more_data":123},"id":1}`,
		Role:    string(Dapp),
		Phase:   string(SessionRequest),
		Silent:  false,
	}
	var data []byte
	var err error
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err = msg.MarshalBinary()
		if err != nil {
			b.Fatalf("MarshalBinary failed: %v", err)
		}
		var reconstructed SocketMessage
		err = json.Unmarshal(data, &reconstructed)
		if err != nil {
			b.Fatalf("Unmarshal failed: %v", err)
		}
	}
}
