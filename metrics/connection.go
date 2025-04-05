package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Reverted variables back to unexported
	countNewConnections = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "new_connections", // Reverted suffix
		Help:      "Number of new connections",
	})
	countClosedConnections = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "closed_connections", // Reverted suffix
		Help:      "Number of closed connections",
	})
	gaugeCurrentConnections = prometheus.NewGauge(prometheus.GaugeOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "current_connections",
		Help:      "Number of current connections",
	})

	countSendBlocking = prometheus.NewCounter(prometheus.CounterOpts{ // Reverted rename
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "send_blockings", // Reverted suffix
		Help:      "Number of send blocking connections",
	})
)

func IncNewConnection() {
	countNewConnections.Inc() // Use unexported var
}

func IncClosedConnection() {
	countClosedConnections.Inc() // Use unexported var
}

func IncSendBlocking() {
	countSendBlocking.Inc() // Use unexported var
}

func SetCurrentConnections(num int) {
	gaugeCurrentConnections.Set(float64(num)) // Use unexported var
}

func init() {
	prometheus.MustRegister(countNewConnections)     // Use unexported var
	prometheus.MustRegister(countClosedConnections)  // Use unexported var
	prometheus.MustRegister(gaugeCurrentConnections) // Use unexported var
	prometheus.MustRegister(countSendBlocking)       // Use unexported var
}
