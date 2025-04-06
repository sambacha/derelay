package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Helper function to get metrics output
func getMetricsOutput(t *testing.T) string {
	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	handler := Handler()
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
	return rr.Body.String()
}

// Reset metrics (Note: This is tricky with the default registry.
// For more complex scenarios, a custom registry is better.
// Here, we'll rely on testing increments from zero or known states).
// We can reset counters by trying to CollectAndCompare with 0, effectively ignoring previous state.
// Gauges need explicit setting.

func TestConnectionMetrics(t *testing.T) {
	// --- Test Increments ---
	// Note: Assumes metrics start at 0 or a known state before this test.
	IncNewConnection()
	IncNewConnection()    // 2
	IncClosedConnection() // 1
	IncSendBlocking()
	IncSendBlocking()
	IncSendBlocking() // 3
	SetCurrentConnections(15)
	SetCurrentConnections(10) // Final value 10

	// --- Verify via Handler ---
	body := getMetricsOutput(t)

	// Check specific lines
	if !strings.Contains(body, "wc_relay_new_connections 2") {
		t.Errorf("handler body does not contain expected new connections metric (wc_relay_new_connections 2):\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_closed_connections 1") {
		t.Errorf("handler body does not contain expected closed connections metric (wc_relay_closed_connections 1):\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_send_blockings 3") {
		t.Errorf("handler body does not contain expected send blocking metric (wc_relay_send_blockings 3):\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_current_connections 10") {
		t.Errorf("handler body does not contain expected current connections metric (wc_relay_current_connections 10):\n%s", body)
	}
}

func TestMessageMetrics(t *testing.T) {
	// --- Test Increments ---
	// Note: Assumes metrics start at 0 or a known state before this test.
	IncTotalMessages()
	IncTotalMessages() // 2 (wc_relay_total_messages, wc_relay_messages{phase="total"})
	IncCachedMessages()
	IncCachedMessages()
	IncCachedMessages()       // 3 (wc_relay_new_cached_messages, wc_relay_messages{phase="pending"})
	DecCachedMessages()       // 1 (wc_relay_new_uncached_messages, wc_relay_messages{phase="delay_delivered"})
	IncNewRequestedSessions() // 1 (wc_relay_new_sessions, wc_relay_sessions{phase="new"})
	IncReceivedSessions()     // 1 (wc_relay_received_sessions, wc_relay_sessions{phase="received"}) - Note: Also increments wc_relay_new_sessions
	IncEstablishedSessions()  // 1 (wc_relay_established_sessions, wc_relay_sessions{phase="established"})
	IncExpiredSessions()      // 1 (wc_relay_expired_sessions, wc_relay_sessions{phase="expired"})

	// --- Verify via Handler ---
	body := getMetricsOutput(t)

	// Check specific lines
	if !strings.Contains(body, "wc_relay_total_messages 2") {
		t.Errorf("handler body does not contain expected total messages metric (wc_relay_total_messages 2):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_messages{phase="total"} 2`) {
		t.Errorf("handler body does not contain expected messages{phase=\"total\"} metric:\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_new_cached_messages 3") {
		t.Errorf("handler body does not contain expected new cached messages metric (wc_relay_new_cached_messages 3):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_messages{phase="pending"} 3`) {
		t.Errorf("handler body does not contain expected messages{phase=\"pending\"} metric:\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_new_uncached_messages 1") {
		t.Errorf("handler body does not contain expected new uncached messages metric (wc_relay_new_uncached_messages 1):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_messages{phase="delay_delivered"} 1`) {
		t.Errorf("handler body does not contain expected messages{phase=\"delay_delivered\"} metric:\n%s", body)
	}
	// Note: IncReceivedSessions NO LONGER increments wc_relay_new_sessions after bug fix.
	if !strings.Contains(body, "wc_relay_new_sessions 1") { // Expect 1 now
		t.Errorf("handler body does not contain expected new sessions metric (wc_relay_new_sessions 1):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_sessions{phase="new"} 1`) {
		t.Errorf("handler body does not contain expected sessions{phase=\"new\"} metric:\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_received_sessions 1") {
		t.Errorf("handler body does not contain expected received sessions metric (wc_relay_received_sessions 1):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_sessions{phase="received"} 1`) {
		t.Errorf("handler body does not contain expected sessions{phase=\"received\"} metric:\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_established_sessions 1") {
		t.Errorf("handler body does not contain expected established sessions metric (wc_relay_established_sessions 1):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_sessions{phase="established"} 1`) {
		t.Errorf("handler body does not contain expected sessions{phase=\"established\"} metric:\n%s", body)
	}
	if !strings.Contains(body, "wc_relay_expired_sessions 1") {
		t.Errorf("handler body does not contain expected expired sessions metric (wc_relay_expired_sessions 1):\n%s", body)
	}
	if !strings.Contains(body, `wc_relay_sessions{phase="expired"} 1`) {
		t.Errorf("handler body does not contain expected sessions{phase=\"expired\"} metric:\n%s", body)
	}
}

// TestMetricsHandler checks if the handler exposes the metrics correctly
func TestMetricsHandler(t *testing.T) {
	// Set some metric values relative to the current state
	// Note: This test might be flaky if run concurrently with others due to shared default registry state.
	// We increment known metrics and check if they appear correctly.
	// Get initial state to calculate relative increments if needed, but simple check is often enough.
	// bodyBefore := getMetricsOutput(t) // Optional: Get state before

	IncNewConnection()       // Increment new connections
	SetCurrentConnections(5) // Set current connections
	IncTotalMessages()       // Increment total messages

	req := httptest.NewRequest("GET", "/metrics", nil) // Changed variable name
	rr := httptest.NewRecorder()

	// Use the Handler() function from the metrics package
	handler := Handler()
	handler.ServeHTTP(rr, req)

	// Check the status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Check if the body contains expected metric strings (relative to previous state or absolute if possible)
	bodyAfter := rr.Body.String() // Changed variable name
	// These checks assume the metrics were 0 before this test function ran, which might not be true.
	// A more robust test would parse the 'before' and 'after' states.
	// For now, we check if the lines exist with *some* value, and check specific values known from this test run.
	if !strings.Contains(bodyAfter, "wc_relay_new_connections") { // Check existence
		t.Errorf("handler body does not contain new connections metric:\n%s", bodyAfter)
	}
	if !strings.Contains(bodyAfter, "wc_relay_current_connections 5") { // Check specific value set
		t.Errorf("handler body does not contain expected current connections metric (wc_relay_current_connections 5):\n%s", bodyAfter)
	}
	if !strings.Contains(bodyAfter, "wc_relay_total_messages") { // Check existence
		t.Errorf("handler body does not contain total messages metric:\n%s", bodyAfter)
	}
}
