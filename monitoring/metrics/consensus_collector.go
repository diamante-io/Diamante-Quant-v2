// monitoring/metrics/consensus_collector.go

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// ConsensusCollector collects consensus-related metrics
type ConsensusCollector struct {
	// Consensus metrics
	RoundsTotal      prometheus.Counter
	LatencyHistogram prometheus.Histogram
	ValidatorsActive prometheus.Gauge
	ValidatorsTotal  prometheus.Gauge
	EventsFinalized  prometheus.Counter
	EventsPending    prometheus.Gauge
	BlocksProposed   prometheus.Counter
	BlocksAccepted   prometheus.Counter
	BlocksRejected   prometheus.Counter

	logger *logrus.Logger
	mu     sync.RWMutex
}

// NewConsensusCollector creates a new consensus metrics collector
func NewConsensusCollector() *ConsensusCollector {
	return &ConsensusCollector{
		RoundsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_consensus_rounds_total",
			Help: "Total number of consensus rounds",
		}),
		LatencyHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_consensus_latency_seconds",
			Help:    "Consensus round latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		ValidatorsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_validators_active",
			Help: "Number of active validators",
		}),
		ValidatorsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_validators_total",
			Help: "Total number of validators",
		}),
		EventsFinalized: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_events_finalized_total",
			Help: "Total number of finalized events",
		}),
		EventsPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_events_pending",
			Help: "Number of pending events",
		}),
		BlocksProposed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_proposed_total",
			Help: "Total number of blocks proposed",
		}),
		BlocksAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_accepted_total",
			Help: "Total number of blocks accepted",
		}),
		BlocksRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_rejected_total",
			Help: "Total number of blocks rejected",
		}),
		logger: logrus.New(),
	}
}

// Register registers all metrics with the provided registry
func (c *ConsensusCollector) Register(registry prometheus.Registerer) error {
	metrics := []prometheus.Collector{
		c.RoundsTotal,
		c.LatencyHistogram,
		c.ValidatorsActive,
		c.ValidatorsTotal,
		c.EventsFinalized,
		c.EventsPending,
		c.BlocksProposed,
		c.BlocksAccepted,
		c.BlocksRejected,
	}

	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			return err
		}
	}

	return nil
}

// IncRounds increments the consensus rounds counter
func (c *ConsensusCollector) IncRounds() {
	c.RoundsTotal.Inc()
}

// ObserveLatency records consensus latency
func (c *ConsensusCollector) ObserveLatency(duration time.Duration) {
	c.LatencyHistogram.Observe(duration.Seconds())
}

// SetActiveValidators sets the number of active validators
func (c *ConsensusCollector) SetActiveValidators(count int) {
	c.ValidatorsActive.Set(float64(count))
}

// SetTotalValidators sets the total number of validators
func (c *ConsensusCollector) SetTotalValidators(count int) {
	c.ValidatorsTotal.Set(float64(count))
}

// IncEventsFinalized increments the finalized events counter
func (c *ConsensusCollector) IncEventsFinalized() {
	c.EventsFinalized.Inc()
}

// SetEventsPending sets the number of pending events
func (c *ConsensusCollector) SetEventsPending(count int) {
	c.EventsPending.Set(float64(count))
}

// IncBlocksProposed increments the proposed blocks counter
func (c *ConsensusCollector) IncBlocksProposed() {
	c.BlocksProposed.Inc()
}

// IncBlocksAccepted increments the accepted blocks counter
func (c *ConsensusCollector) IncBlocksAccepted() {
	c.BlocksAccepted.Inc()
}

// IncBlocksRejected increments the rejected blocks counter
func (c *ConsensusCollector) IncBlocksRejected() {
	c.BlocksRejected.Inc()
}

// ConsensusMetricsUpdate contains typed consensus metrics for updates
type ConsensusMetricsUpdate struct {
	ActiveValidators int `json:"active_validators"`
	TotalValidators  int `json:"total_validators"`
	PendingEvents    int `json:"pending_events"`
}

// Update updates consensus metrics with current state
func (c *ConsensusCollector) Update(metrics ConsensusMetricsUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update metrics based on the provided data
	if metrics.ActiveValidators > 0 {
		c.SetActiveValidators(metrics.ActiveValidators)
	}
	if metrics.TotalValidators > 0 {
		c.SetTotalValidators(metrics.TotalValidators)
	}
	if metrics.PendingEvents >= 0 {
		c.SetEventsPending(metrics.PendingEvents)
	}
}
