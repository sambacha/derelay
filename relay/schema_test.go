package relay

import (
	"encoding/json"
	"testing"
	"testing/quick"
)

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

// Property-based test for SocketMessage serialization/deserialization
func TestSocketMessageMarshalUnmarshalProperty(t *testing.T) {
	property := func(original SocketMessage) bool {
		// Marshal the original message
		data, err := original.MarshalBinary()
		if err != nil {
			// If marshaling fails for a generated message, quick.Check treats it as a failure
			// but we might want specific logging or handling depending on why it could fail.
			// For now, returning false indicates a problem.
			t.Logf("MarshalBinary failed for input %v: %v", original, err)
			return false
		}

		// Unmarshal back into a new message
		var reconstructed SocketMessage
		err = json.Unmarshal(data, &reconstructed)
		if err != nil {
			t.Logf("Unmarshal failed for data %s: %v", string(data), err)
			return false
		}

		// Compare the exported fields. We ignore the unexported 'client' field.
		return original.Topic == reconstructed.Topic &&
			original.Type == reconstructed.Type &&
			original.Payload == reconstructed.Payload &&
			original.Role == reconstructed.Role &&
			original.Phase == reconstructed.Phase &&
			original.Silent == reconstructed.Silent
	}

	// Configure quick.Check if needed (e.g., number of iterations)
	config := &quick.Config{
		// Values: func(values []reflect.Value, rand *rand.Rand) {}, // Custom generator hook if needed
		// MaxCount: 1000, // Increase iterations if desired
	}

	if err := quick.Check(property, config); err != nil {
		t.Errorf("SocketMessage marshal/unmarshal property failed: %v", err)
	}
}

// Benchmark for SocketMessage serialization/deserialization
func BenchmarkSocketMessageMarshalUnmarshal(b *testing.B) {
	// Create a sample message to use for benchmarking
	// Using realistic-ish values
	msg := SocketMessage{
		Topic:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", // 64 hex chars
		Type:    Pub,
		Payload: `{"jsonrpc":"2.0","method":"some_method","params":{"data":"0x...","more_data":123},"id":1}`,
		Role:    string(Dapp),
		Phase:   string(SessionRequest),
		Silent:  false,
	}
	var data []byte
	var err error

	b.ReportAllocs() // Report memory allocations
	b.ResetTimer()   // Start timing after setup

	for i := 0; i < b.N; i++ {
		// Marshal
		data, err = msg.MarshalBinary()
		if err != nil {
			b.Fatalf("MarshalBinary failed: %v", err)
		}

		// Unmarshal (into a throwaway variable to simulate the full cycle)
		var reconstructed SocketMessage
		err = json.Unmarshal(data, &reconstructed)
		if err != nil {
			b.Fatalf("Unmarshal failed: %v", err)
		}
	}
}
