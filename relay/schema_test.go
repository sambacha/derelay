package relay

import "testing"

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

func TestTopicClientSetClear(t *testing.T) {
	ts := NewTopicClientSet()

	c1 := &client{id: "1"}
	c2 := &client{id: "2"}
	topic1 := "topic1"
	topic2 := "topic2"

	ts.Set(topic1, c1)
	ts.Set(topic1, c2)
	ts.Set(topic2, c1)

	if ts.Len(topic1) != 2 {
		t.Errorf("Expected length 2 for topic1, got %d", ts.Len(topic1))
	}
	if ts.Len(topic2) != 1 {
		t.Errorf("Expected length 1 for topic2, got %d", ts.Len(topic2))
	}

	ts.Clear(topic1)

	if ts.Len(topic1) != 0 {
		t.Errorf("Expected length 0 for topic1 after clear, got %d", ts.Len(topic1))
	}
	if _, ok := ts.Data[topic1]; ok {
		t.Errorf("Topic1 key should not exist after clear")
	}
	// Ensure topic2 is unaffected
	if ts.Len(topic2) != 1 {
		t.Errorf("Expected length 1 for topic2 after clearing topic1, got %d", ts.Len(topic2))
	}
	if len(ts.GetTopicsByClient(c1, false)) != 1 {
		t.Errorf("Client c1 should only be associated with topic2 now")
	}
}

func TestTopicClientSetEmpty(t *testing.T) {
	ts := NewTopicClientSet()
	c1 := &client{id: "1"}
	topic := "empty_topic"

	if ts.Len(topic) != 0 {
		t.Errorf("Expected length 0 for non-existent topic, got %d", ts.Len(topic))
	}
	if len(ts.Get(topic)) != 0 {
		t.Errorf("Expected empty map for non-existent topic get")
	}
	if len(ts.GetTopicsByClient(c1, false)) != 0 {
		t.Errorf("Expected empty topic list for client not in set")
	}
	ts.Unset(topic, c1) // Should not panic
	ts.Clear(topic)     // Should not panic
}

// Note: Concurrency tests would require running Set/Unset/Get/Len/Clear
// from multiple goroutines and checking for race conditions using `go test -race`.
// This basic test suite doesn't include explicit concurrency tests.
