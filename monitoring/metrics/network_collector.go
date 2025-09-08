// monitoring/metrics/network_collector.go

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// NetworkCollector collects network-related metrics
type NetworkCollector struct {
	// Network metrics
	PeerConnections  prometheus.Gauge
	MessagesSent     prometheus.Counter
	MessagesReceived prometheus.Counter
	BytesSent        prometheus.Counter
	BytesReceived    prometheus.Counter
	ConnectionErrors prometheus.Counter
	Latency          prometheus.Histogram

	logger *logrus.Logger
	mu     sync.RWMutex
}

// NewNetworkCollector creates a new network metrics collector
func NewNetworkCollector() *NetworkCollector {
	return &NetworkCollector{
		PeerConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_peer_connections",
			Help: "Current number of peer connections",
		}),
		MessagesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_messages_sent_total",
			Help: "Total number of messages sent",
		}),
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_messages_received_total",
			Help: "Total number of messages received",
		}),
		BytesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_bytes_sent_total",
			Help: "Total number of bytes sent",
		}),
		BytesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_bytes_received_total",
			Help: "Total number of bytes received",
		}),
		ConnectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_connection_errors_total",
			Help: "Total number of connection errors",
		}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_network_latency_seconds",
			Help:    "Network latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		logger: logrus.New(),
	}
}

// Register registers all metrics with the provided registry
func (c *NetworkCollector) Register(registry prometheus.Registerer) error {
	metrics := []prometheus.Collector{
		c.PeerConnections,
		c.MessagesSent,
		c.MessagesReceived,
		c.BytesSent,
		c.BytesReceived,
		c.ConnectionErrors,
		c.Latency,
	}

	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			return err
		}
	}

	return nil
}

// SetPeerConnections sets the current number of peer connections
func (c *NetworkCollector) SetPeerConnections(count int) {
	c.PeerConnections.Set(float64(count))
}

// IncMessagesSent increments the messages sent counter
func (c *NetworkCollector) IncMessagesSent() {
	c.MessagesSent.Inc()
}

// IncMessagesReceived increments the messages received counter
func (c *NetworkCollector) IncMessagesReceived() {
	c.MessagesReceived.Inc()
}

// AddBytesSent adds to the bytes sent counter
func (c *NetworkCollector) AddBytesSent(bytes uint64) {
	c.BytesSent.Add(float64(bytes))
}

// AddBytesReceived adds to the bytes received counter
func (c *NetworkCollector) AddBytesReceived(bytes uint64) {
	c.BytesReceived.Add(float64(bytes))
}

// IncConnectionErrors increments the connection errors counter
func (c *NetworkCollector) IncConnectionErrors() {
	c.ConnectionErrors.Inc()
}

// ObserveLatency records network latency
func (c *NetworkCollector) ObserveLatency(seconds float64) {
	c.Latency.Observe(seconds)
}

// UpdatePeers updates peer-related metrics
func (c *NetworkCollector) UpdatePeers(count int) {
	c.SetPeerConnections(count)
}

// UpdateHealth updates network health metrics
func (c *NetworkCollector) UpdateHealth(health bool) {
	if health {
		c.logger.Debug("Network health: healthy")
	} else {
		c.logger.Warn("Network health: unhealthy")
		c.IncConnectionErrors()
	}
}

// UpdatePartitionStatus updates partition status metrics
func (c *NetworkCollector) UpdatePartitionStatus(isPartitioned bool) {
	if isPartitioned {
		c.logger.Warn("Network partition detected")
	} else {
		c.logger.Info("Network partition resolved")
	}
}
