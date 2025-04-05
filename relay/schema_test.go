package relay

import (
	"encoding/base64" // Add base64 import
	"encoding/json"
	"testing"
	"time" // Ensure time is imported

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap" // Add zap import
	"go.uber.org/zap/zapcore"
)

// Removed unused "strings" import

func TestTopicSetBasic(t *testing.T) {
	ts := NewTopicClientSet()

	c := &client{id: "1"}
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
	// Test Get
	clients := ts.Get("hello")
	assert.Equal(t, 2, len(clients), "Get should return correct number of clients")

	// Test Clear
	ts.Clear("hello")
	assert.Equal(t, 0, ts.Len("hello"), "Clear should remove all clients for the topic")
	assert.Equal(t, 0, len(ts.Get("hello")), "Get after Clear should return empty set")
}

func TestTopicSetReference(t *testing.T) {
	ts := NewTopicClientSet()

	c := &client{id: "1"}
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

	c := &client{id: "1"}
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

// TestGetTopicsByClientAndClear is already present and seems okay.

func TestTopicGet(t *testing.T) {
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

func TestKeyGenerationFunctions(t *testing.T) {
	topic := "my-topic-123"
	clientID := "client-abc-xyz"

	// Use the actual prefixes from schema.go
	assert.Equal(t, "wc:relay:stream:messages:"+topic, streamMessageKey(topic), "streamMessageKey mismatch")
	assert.Equal(t, "wc:relay:chan:messages:"+topic, messageChanKey(topic), "messageChanKey mismatch")
	assert.Equal(t, "wc:relay:chan:dappNotify:"+topic, dappNotifyChanKey(topic), "dappNotifyChanKey mismatch") // Note: dappNotifyChan constant name vs key prefix
	assert.Equal(t, "wc:relay:client:"+clientID, clientHashKey(clientID), "clientHashKey mismatch")
	assert.Equal(t, "wc:relay:client:subs:"+clientID, clientSubsSetKey(clientID), "clientSubsSetKey mismatch") // Note: order difference
	assert.Equal(t, "wc:relay:client:pubs:"+clientID, clientPubsSetKey(clientID), "clientPubsSetKey mismatch") // Note: order difference
}

func TestFromDappNotifyChan(t *testing.T) {
	// Use the actual prefixes from schema.go
	assert.True(t, fromDappNotifyChan("wc:relay:chan:dappNotify:some_topic"), "Should identify dapp notify channel")
	assert.False(t, fromDappNotifyChan("wc:relay:chan:messages:some_topic"), "Should not identify message channel")
	assert.False(t, fromDappNotifyChan("wc:relay:stream:messages:some_topic"), "Should not identify stream key")
	assert.False(t, fromDappNotifyChan("invalid_key"), "Should not identify invalid key")
}

func TestMarshalBinary(t *testing.T) {
	payloadStr := `{"data":"value"}`
	msg := SocketMessage{
		Topic:   "test-topic",
		Type:    Pub,
		Role:    string(Dapp),
		Phase:   string(SessionRequest),
		Payload: payloadStr, // Use Payload field
	}

	data, err := msg.MarshalBinary()
	assert.NoError(t, err, "MarshalBinary should not return error")
	assert.NotEmpty(t, data, "MarshalBinary should return non-empty data")

	// Unmarshal back to verify
	var decodedMsg SocketMessage
	err = json.Unmarshal(data, &decodedMsg)
	assert.NoError(t, err, "Unmarshal should succeed")
	assert.Equal(t, msg.Topic, decodedMsg.Topic)
	assert.Equal(t, msg.Type, decodedMsg.Type)
	assert.Equal(t, msg.Role, decodedMsg.Role)
	assert.Equal(t, msg.Phase, decodedMsg.Phase)
	assert.Equal(t, msg.Payload, decodedMsg.Payload) // Compare Payload string
}

func TestMarshalLogArray(t *testing.T) {
	ts := NewTopicSet()
	ts.Set("topic1")
	ts.Set("topic2")

	// Use a JSONEncoder
	cfg := zap.NewDevelopmentEncoderConfig()
	// buf := &bytes.Buffer{} // Remove unused buf
	encoder := zapcore.NewJSONEncoder(cfg)

	// Call AddArray, which will invoke ts.MarshalLogArray
	err := encoder.AddArray("topics", ts)
	assert.NoError(t, err, "encoder.AddArray should not return error")

	// To check the output, we need to write the encoded fields to the buffer
	// This requires a bit more setup or using a different encoder approach.
	// Alternative: Use zaptest/observer again if testing the logging output is the goal.

	// Simpler check: Ensure MarshalLogArray doesn't panic or error with a valid encoder.
	// Create a dummy ArrayEncoder for testing the method directly.
	dummyArrEncoder := &dummyArrayEncoder{}
	errDirect := ts.MarshalLogArray(dummyArrEncoder)
	assert.NoError(t, errDirect, "MarshalLogArray should not error with a valid encoder")
	assert.Contains(t, dummyArrEncoder.elements, "topic1", "Dummy encoder should contain topic1")
	assert.Contains(t, dummyArrEncoder.elements, "topic2", "Dummy encoder should contain topic2")
	assert.Len(t, dummyArrEncoder.elements, 2, "Dummy encoder should have 2 elements")

}

// dummyArrayEncoder for testing MarshalLogArray directly
type dummyArrayEncoder struct {
	elements []string
}

func (d *dummyArrayEncoder) AppendArray(marshaler zapcore.ArrayMarshaler) error   { return nil } // Not needed for this test
func (d *dummyArrayEncoder) AppendObject(marshaler zapcore.ObjectMarshaler) error { return nil } // Not needed
func (d *dummyArrayEncoder) AppendBool(v bool)                                    {}
func (d *dummyArrayEncoder) AppendByteString(v []byte)                            {}
func (d *dummyArrayEncoder) AppendComplex128(v complex128)                        {}
func (d *dummyArrayEncoder) AppendComplex64(v complex64)                          {}
func (d *dummyArrayEncoder) AppendDuration(v time.Duration)                       {}
func (d *dummyArrayEncoder) AppendFloat64(v float64)                              {}
func (d *dummyArrayEncoder) AppendFloat32(v float32)                              {}
func (d *dummyArrayEncoder) AppendInt(v int)                                      {}
func (d *dummyArrayEncoder) AppendInt64(v int64)                                  {}
func (d *dummyArrayEncoder) AppendInt32(v int32)                                  {}
func (d *dummyArrayEncoder) AppendInt16(v int16)                                  {}
func (d *dummyArrayEncoder) AppendInt8(v int8)                                    {}
func (d *dummyArrayEncoder) AppendString(v string) { // This is the one we care about
	d.elements = append(d.elements, v)
}
func (d *dummyArrayEncoder) AppendTime(v time.Time)              {}
func (d *dummyArrayEncoder) AppendUint(v uint)                   {}
func (d *dummyArrayEncoder) AppendUint64(v uint64)               {}
func (d *dummyArrayEncoder) AppendUint32(v uint32)               {}
func (d *dummyArrayEncoder) AppendUint16(v uint16)               {}
func (d *dummyArrayEncoder) AppendUint8(v uint8)                 {}
func (d *dummyArrayEncoder) AppendUintptr(v uintptr)             {}
func (d *dummyArrayEncoder) AppendReflected(v interface{}) error { return nil }

func TestGenerateRandomBytes16(t *testing.T) {
	b1Str := generateRandomBytes16() // Returns only string
	b2Str := generateRandomBytes16() // Returns only string

	// Check if strings are generated and have expected format (base64 of 16 bytes)
	assert.NotEmpty(t, b1Str, "generateRandomBytes16 (1) should not be empty")
	assert.NotEmpty(t, b2Str, "generateRandomBytes16 (2) should not be empty")

	// Decode base64 to check length
	b1Bytes, err1 := base64.StdEncoding.DecodeString(b1Str)
	b2Bytes, err2 := base64.StdEncoding.DecodeString(b2Str)
	assert.NoError(t, err1, "Decoding b1Str failed")
	assert.NoError(t, err2, "Decoding b2Str failed")

	assert.Len(t, b1Bytes, 16, "Decoded bytes should have length 16")
	assert.Len(t, b2Bytes, 16, "Decoded bytes should have length 16")
	assert.NotEqual(t, b1Str, b2Str, "Generated strings should be different")

	// Old hex check removed as it returns base64
	// assert.Regexp(t, `^[0-9a-f]{32}$`, b1, "Should look like hex string")
	// assert.Regexp(t, `^[0-9a-f]{32}$`, b2, "Should look like hex string")
}
