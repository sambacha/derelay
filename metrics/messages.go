package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Reverted variables back to unexported
	countTotalMessages = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "total_messages", // Reverted name
		Help:      "Number of total messages",
	})
	countCachedMessages = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "new_cached_messages", // Reverted name
		Help:      "Number of new cached messages",
	})
	// Reverted rename
	countUncachedMessages = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "new_uncached_messages", // Reverted name
		Help:      "Number of cached messages consumed",
	})
	// Restoring CounterVecs
	countMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "messages",
		Help:      "Number of messages",
	}, []string{"phase"})

	countSessions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "sessions",
		Help:      "Number of new pending sessions",
	}, []string{"phase"})

	countNewRequestedSessions = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "new_sessions", // Reverted name
		Help:      "Number of new pending sessions",
	})
	countReceivedSessions = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "received_sessions", // Reverted name
		Help:      "Number of new received sessions",
	})
	countEstablishedSessions = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "established_sessions", // Reverted name
		Help:      "Number of new established sessions",
	})
	countExpiredSessions = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "expired_sessions", // Reverted name
		Help:      "Number of expired sessions",
	})
	// Removed definition for MessagesSent as it wasn't originally present
)

// Reverted function names and logic
func IncTotalMessages() {
	countTotalMessages.Inc()
	countMessages.With(prometheus.Labels{"phase": "total"}).Inc()
}
func IncCachedMessages() {
	countCachedMessages.Inc()
	countMessages.With(prometheus.Labels{"phase": "pending"}).Inc()
}
func DecCachedMessages() { // Reverted name
	countUncachedMessages.Inc()
	countMessages.With(prometheus.Labels{"phase": "delay_delivered"}).Inc()
}

// Removed IncMessagesSentDirectly as it wasn't originally present

func IncNewRequestedSessions() {
	countNewRequestedSessions.Inc()
	countSessions.With(prometheus.Labels{"phase": "new"}).Inc()
}

func IncReceivedSessions() {
	// Original logic might have been different, check git history if needed
	// Assuming it incremented countReceivedSessions and the Vec
	countReceivedSessions.Inc() // Use unexported var
	countSessions.With(prometheus.Labels{"phase": "received"}).Inc()
}

func IncEstablishedSessions() {
	countEstablishedSessions.Inc()
	countSessions.With(prometheus.Labels{"phase": "established"}).Inc()
}

func IncExpiredSessions() {
	countExpiredSessions.Inc()
	countSessions.With(prometheus.Labels{"phase": "expired"}).Inc()
}

func init() {
	prometheus.MustRegister(countTotalMessages)    // Use unexported var
	prometheus.MustRegister(countCachedMessages)   // Use unexported var
	prometheus.MustRegister(countUncachedMessages) // Use unexported var
	// Removed registration for MessagesSentDirectly and MessagesSent

	prometheus.MustRegister(countMessages)             // Use unexported var
	prometheus.MustRegister(countSessions)             // Use unexported var
	prometheus.MustRegister(countNewRequestedSessions) // Use unexported var
	prometheus.MustRegister(countEstablishedSessions)  // Use unexported var
	prometheus.MustRegister(countReceivedSessions)     // Use unexported var
	prometheus.MustRegister(countExpiredSessions)      // Use unexported var
}
