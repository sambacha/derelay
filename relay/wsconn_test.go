package relay

import (
	"encoding/json" // Needed for test cases
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/stretchr/testify/assert" // Added for assertions
)

// TestSendChanWithNoReceiver... (existing test)

func TestSendChanWithNoReceiver(t *testing.T) {

	var wg sync.WaitGroup
	send := make(chan int, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			i := <-send
			t.Logf("received: %v", i) // Use t.Logf
			if i == 3 {
				t.Logf("receiving routine exit") // Use t.Logf
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		i := 0
		for {
			select {
			case send <- i:
				t.Logf("send: %v", i) // Use t.Logf
				i++
				time.Sleep(1 * time.Second)
			default:
				t.Logf("send buffer is full") // Use t.Logf
				return
			}
		}
	}()

	wg.Wait()
}

// Property test for decodeSocketMessage
func TestDecodeSocketMessageProperties(t *testing.T) {
	property := func(original SocketMessage) bool {
		data, err := original.MarshalBinary()
		if err != nil {
			t.Logf("MarshalBinary failed unexpectedly: %v", err)
			return false
		}
		reconstructed, err := decodeSocketMessage(data)
		if err != nil {
			t.Logf("decodeSocketMessage failed for valid input: %v\nData: %s", err, string(data))
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
		t.Errorf("Property test failed for decoding valid SocketMessage: %v", err)
	}
}

func TestDecodeSocketMessage(t *testing.T) {
	validMsg := SocketMessage{Topic: "test-topic", Type: Pub, Payload: `{"data": 123}`, Role: string(Dapp), Phase: string(SessionRequest), Silent: false}
	validMsgBytes, _ := json.Marshal(validMsg)
	var tempMap map[string]interface{}
	_ = json.Unmarshal(validMsgBytes, &tempMap)
	tempMap["extraField"] = "should cause error"
	extraFieldBytes, _ := json.Marshal(tempMap)

	tests := []struct {
		name        string
		input       []byte
		expectMsg   SocketMessage
		expectError bool
	}{
		{name: "Valid Message", input: validMsgBytes, expectMsg: validMsg, expectError: false},
		{name: "Invalid JSON Syntax", input: []byte(`{"topic":"test", "type":`), expectMsg: SocketMessage{}, expectError: true},
		{name: "Empty Input", input: []byte{}, expectMsg: SocketMessage{}, expectError: true},                                           // EOF error
		{name: "Null Input", input: []byte(`null`), expectMsg: SocketMessage{}, expectError: false},                                     // Corrected expectation
		{name: "JSON with Extra Field", input: extraFieldBytes, expectMsg: SocketMessage{}, expectError: true},                          // Due to DisallowUnknownFields
		{name: "Different JSON Structure", input: []byte(`{"some_other_key": "value"}`), expectMsg: SocketMessage{}, expectError: true}, // Due to DisallowUnknownFields
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := decodeSocketMessage(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectMsg.Topic, msg.Topic)
				assert.Equal(t, tt.expectMsg.Type, msg.Type)
				assert.Equal(t, tt.expectMsg.Payload, msg.Payload)
				assert.Equal(t, tt.expectMsg.Role, msg.Role)
				assert.Equal(t, tt.expectMsg.Phase, msg.Phase)
				assert.Equal(t, tt.expectMsg.Silent, msg.Silent)
			}
		})
	}
}

func TestClientGetters(t *testing.T) {
	c := &client{id: "client-123", role: Dapp}
	assert.Equal(t, "client-123", c.ID())
	assert.Equal(t, Dapp, c.Role())
	c2 := &client{id: "client-456", role: Wallet}
	assert.Equal(t, "client-456", c2.ID())
	assert.Equal(t, Wallet, c2.Role())
}
