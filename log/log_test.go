package log

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// setupObserver sets up an observer core for capturing logs and replaces the global logger.
// It returns the observed logs and a function to restore the original logger.
func setupObserver(t *testing.T) (*observer.ObservedLogs, func()) {
	originalLogger := logger // Store original logger
	core, recorded := observer.New(zapcore.DebugLevel)
	observedLogger := zap.New(core, zap.AddCallerSkip(1)) // Add skip to match original logger setup
	logger = observedLogger                               // Replace global logger

	// Return the recorded logs and a cleanup function to restore the original logger
	return recorded, func() {
		logger = originalLogger
	}
}

func TestInfo(t *testing.T) {
	recorded, restore := setupObserver(t)
	defer restore()

	Info("test info message", zap.String("key", "value"))

	logs := recorded.AllUntimed() // Use AllUntimed to ignore timestamps
	assert.Equal(t, 1, len(logs), "Expected one log entry")
	if len(logs) > 0 {
		entry := logs[0]
		assert.Equal(t, zapcore.InfoLevel, entry.Level, "Incorrect log level")
		assert.Equal(t, "test info message", entry.Message, "Incorrect message")
		// Check context field (Note: field order isn't guaranteed)
		found := false
		for _, field := range entry.Context {
			if field.Key == "key" && field.String == "value" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected field 'key'='value' not found")
	}
}

func TestWarn(t *testing.T) {
	recorded, restore := setupObserver(t)
	defer restore()

	Warn("test warn message", zap.Int("code", 123))

	logs := recorded.AllUntimed()
	assert.Equal(t, 1, len(logs), "Expected one log entry")
	if len(logs) > 0 {
		entry := logs[0]
		assert.Equal(t, zapcore.WarnLevel, entry.Level, "Incorrect log level")
		assert.Equal(t, "test warn message", entry.Message, "Incorrect message")
		found := false
		for _, field := range entry.Context {
			if field.Key == "code" && field.Integer == 123 {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected field 'code'=123 not found")
	}
}

func TestError(t *testing.T) {
	recorded, restore := setupObserver(t)
	defer restore()

	testErr := errors.New("this is a test error")
	Error("test error message", testErr, zap.Bool("critical", true))

	logs := recorded.AllUntimed()
	assert.Equal(t, 1, len(logs), "Expected one log entry")
	if len(logs) > 0 {
		entry := logs[0]
		assert.Equal(t, zapcore.ErrorLevel, entry.Level, "Incorrect log level")
		assert.Equal(t, "test error message", entry.Message, "Incorrect message")

		foundErr := false
		foundCritical := false
		for _, field := range entry.Context {
			if field.Key == "error" && field.Interface != nil {
				errField, ok := field.Interface.(error)
				if ok && errField.Error() == testErr.Error() {
					foundErr = true
				}
			}
			if field.Key == "critical" && field.Integer == 1 { // zapcore encodes bool as int
				foundCritical = true
			}
		}
		assert.True(t, foundErr, "Expected 'error' field not found or incorrect")
		assert.True(t, foundCritical, "Expected field 'critical'=true not found")
	}
}

func TestDebug(t *testing.T) {
	recorded, restore := setupObserver(t)
	defer restore()

	Debug("test debug message", zap.Any("data", map[string]int{"a": 1}))

	logs := recorded.AllUntimed()
	assert.Equal(t, 1, len(logs), "Expected one log entry")
	if len(logs) > 0 {
		entry := logs[0]
		assert.Equal(t, zapcore.DebugLevel, entry.Level, "Incorrect log level")
		assert.Equal(t, "test debug message", entry.Message, "Incorrect message")
		// Checking complex fields like maps requires more involved assertion or marshaling
		foundData := false
		for _, field := range entry.Context {
			if field.Key == "data" {
				foundData = true
				// Further checks on field.Interface could be added here if needed
				break
			}
		}
		assert.True(t, foundData, "Expected field 'data' not found")
	}
}

// Note: Testing Fatal requires more setup to prevent os.Exit, possibly using zapcore.RegisterHooks
// or replacing the logger core with one that panics instead of exits for testing.
// Skipping direct Fatal test for now.

// Test Logger() function returns a logger instance
func TestLoggerFunc(t *testing.T) {
	l := Logger()
	assert.NotNil(t, l, "Logger() should return a non-nil logger")
	// We can't easily assert the AddCallerSkip option without complex reflection.
	// The NotNil check confirms it's a *zap.Logger as expected by the function signature.
}

// Test Any() helper
func TestAny(t *testing.T) {
	field := Any("testKey", 123)
	assert.Equal(t, "testKey", field.Key)
	assert.Equal(t, zapcore.Int64Type, field.Type) // zap promotes int to int64
	assert.Equal(t, int64(123), field.Integer)
}

// We can't easily test the init() or buildLoggerWithConfig directly without
// potentially interfering with other tests or requiring build tags.
// The ProductionModeWithoutStackTrace is implicitly tested by the setupObserver
// if the global logger is indeed configured that way initially.
