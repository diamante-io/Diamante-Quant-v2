// monitoring/metrics/business_collector.go

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
)

// BusinessCollector collects business-level metrics for the blockchain
type BusinessCollector struct {
	// Transaction metrics
	Processed prometheus.Counter
	Failed    prometheus.Counter

	// Block metrics
	BlocksProduced  prometheus.Counter
	BlocksValidated prometheus.Counter
	BlocksRejected  prometheus.Counter

	// Consensus metrics
	ConsensusRounds  prometheus.Counter
	ConsensusLatency prometheus.Histogram

	// Network metrics
	PeerConnections  prometheus.Gauge
	MessagesSent     prometheus.Counter
	MessagesReceived prometheus.Counter

	// EVM metrics
	ContractsDeployed prometheus.Counter
	ContractCalls     prometheus.Counter
	GasUsed           prometheus.Counter

	// Storage metrics
	StorageReads          prometheus.Counter
	StorageWrites         prometheus.Counter
	CacheHits             prometheus.Counter
	CacheMisses           prometheus.Counter
	DatabaseQueries       prometheus.Counter
	DatabaseQueryDuration prometheus.Histogram

	logger *logrus.Logger
	mu     sync.RWMutex
}

// NewBusinessCollector creates a new business metrics collector
func NewBusinessCollector(logger *logrus.Logger) *BusinessCollector {
	if logger == nil {
		logger = logrus.New()
	}

	return &BusinessCollector{
		// Transaction metrics
		Processed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_transactions_processed_total",
			Help: "Total number of transactions processed",
		}),
		Failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_transactions_failed_total",
			Help: "Total number of failed transactions",
		}),

		// Block metrics
		BlocksProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_produced_total",
			Help: "Total number of blocks produced",
		}),
		BlocksValidated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_validated_total",
			Help: "Total number of blocks validated",
		}),
		BlocksRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_blocks_rejected_total",
			Help: "Total number of blocks rejected",
		}),

		// Consensus metrics
		ConsensusRounds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_consensus_rounds_total",
			Help: "Total number of consensus rounds",
		}),
		ConsensusLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_consensus_latency_seconds",
			Help:    "Consensus round latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),

		// Network metrics
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

		// EVM metrics
		ContractsDeployed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_contracts_deployed_total",
			Help: "Total number of smart contracts deployed",
		}),
		ContractCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_contract_calls_total",
			Help: "Total number of smart contract calls",
		}),
		GasUsed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_gas_used_total",
			Help: "Total amount of gas used",
		}),

		// Storage metrics
		StorageReads: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_storage_reads_total",
			Help: "Total number of storage read operations",
		}),
		StorageWrites: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_storage_writes_total",
			Help: "Total number of storage write operations",
		}),
		CacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_cache_hits_total",
			Help: "Total number of cache hits",
		}),
		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_cache_misses_total",
			Help: "Total number of cache misses",
		}),
		DatabaseQueries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_database_queries_total",
			Help: "Total number of database queries",
		}),
		DatabaseQueryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_database_query_duration_seconds",
			Help:    "Duration of database queries in seconds",
			Buckets: prometheus.DefBuckets,
		}),

		logger: logger,
	}
}

// Register registers all metrics with the provided registry
func (c *BusinessCollector) Register(registry prometheus.Registerer) error {
	metrics := []prometheus.Collector{
		c.Processed,
		c.Failed,
		c.BlocksProduced,
		c.BlocksValidated,
		c.BlocksRejected,
		c.ConsensusRounds,
		c.ConsensusLatency,
		c.PeerConnections,
		c.MessagesSent,
		c.MessagesReceived,
		c.ContractsDeployed,
		c.ContractCalls,
		c.GasUsed,
		c.StorageReads,
		c.StorageWrites,
		c.CacheHits,
		c.CacheMisses,
		c.DatabaseQueries,
		c.DatabaseQueryDuration,
	}

	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			return err
		}
	}

	return nil
}

// IncProcessed increments processed counter
func (c *BusinessCollector) IncProcessed() {
	c.Processed.Inc()
}

// IncFailed increments failed counter
func (c *BusinessCollector) IncFailed() {
	c.Failed.Inc()
}

// IncBlocksProduced increments blocks produced counter
func (c *BusinessCollector) IncBlocksProduced() {
	c.BlocksProduced.Inc()
}

// IncBlocksValidated increments blocks validated counter
func (c *BusinessCollector) IncBlocksValidated() {
	c.BlocksValidated.Inc()
}

// IncBlocksRejected increments blocks rejected counter
func (c *BusinessCollector) IncBlocksRejected() {
	c.BlocksRejected.Inc()
}

// IncConsensusRounds increments consensus rounds counter
func (c *BusinessCollector) IncConsensusRounds() {
	c.ConsensusRounds.Inc()
}

// ObserveConsensusLatency records consensus latency
func (c *BusinessCollector) ObserveConsensusLatency(duration time.Duration) {
	c.ConsensusLatency.Observe(duration.Seconds())
}

// SetPeerConnections sets the current number of peer connections
func (c *BusinessCollector) SetPeerConnections(count int) {
	c.PeerConnections.Set(float64(count))
}

// IncMessagesSent increments messages sent counter
func (c *BusinessCollector) IncMessagesSent() {
	c.MessagesSent.Inc()
}

// IncMessagesReceived increments messages received counter
func (c *BusinessCollector) IncMessagesReceived() {
	c.MessagesReceived.Inc()
}

// IncContractsDeployed increments contracts deployed counter
func (c *BusinessCollector) IncContractsDeployed() {
	c.ContractsDeployed.Inc()
}

// IncContractCalls increments contract calls counter
func (c *BusinessCollector) IncContractCalls() {
	c.ContractCalls.Inc()
}

// AddGasUsed adds to the total gas used
func (c *BusinessCollector) AddGasUsed(gas uint64) {
	c.GasUsed.Add(float64(gas))
}

// IncStorageReads increments storage reads counter
func (c *BusinessCollector) IncStorageReads() {
	c.StorageReads.Inc()
}

// IncStorageWrites increments storage writes counter
func (c *BusinessCollector) IncStorageWrites() {
	c.StorageWrites.Inc()
}

// IncCacheHits increments cache hits counter
func (c *BusinessCollector) IncCacheHits() {
	c.CacheHits.Inc()
}

// IncCacheMisses increments cache misses counter
func (c *BusinessCollector) IncCacheMisses() {
	c.CacheMisses.Inc()
}

// IncDatabaseQueries increments database queries counter
func (c *BusinessCollector) IncDatabaseQueries() {
	c.DatabaseQueries.Inc()
}

// ObserveDatabaseQuery records a database query duration
func (c *BusinessCollector) ObserveDatabaseQuery(d time.Duration) {
	c.DatabaseQueryDuration.Observe(d.Seconds())
}

// GetMetrics returns a snapshot of current metrics
func (c *BusinessCollector) GetMetrics() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := make(map[string]float64)

	metrics["transactions_processed"] = getCounterValue(c.Processed, c.logger)
	metrics["transactions_failed"] = getCounterValue(c.Failed, c.logger)
	metrics["blocks_produced"] = getCounterValue(c.BlocksProduced, c.logger)
	metrics["blocks_validated"] = getCounterValue(c.BlocksValidated, c.logger)
	metrics["blocks_rejected"] = getCounterValue(c.BlocksRejected, c.logger)
	metrics["consensus_rounds"] = getCounterValue(c.ConsensusRounds, c.logger)
	metrics["messages_sent"] = getCounterValue(c.MessagesSent, c.logger)
	metrics["messages_received"] = getCounterValue(c.MessagesReceived, c.logger)
	metrics["contracts_deployed"] = getCounterValue(c.ContractsDeployed, c.logger)
	metrics["contract_calls"] = getCounterValue(c.ContractCalls, c.logger)
	metrics["gas_used"] = getCounterValue(c.GasUsed, c.logger)
	metrics["storage_reads"] = getCounterValue(c.StorageReads, c.logger)
	metrics["storage_writes"] = getCounterValue(c.StorageWrites, c.logger)
	metrics["cache_hits"] = getCounterValue(c.CacheHits, c.logger)
	metrics["cache_misses"] = getCounterValue(c.CacheMisses, c.logger)
	metrics["database_queries"] = getCounterValue(c.DatabaseQueries, c.logger)
	metrics["database_query_duration_sum"] = getHistogramSum(c.DatabaseQueryDuration, c.logger)
	metrics["database_query_duration_count"] = float64(getHistogramCount(c.DatabaseQueryDuration, c.logger))
	metrics["peer_connections"] = getGaugeValue(c.PeerConnections, c.logger)

	// Histogram metrics expose count and sum; return the average latency
	metrics["consensus_latency_sum"] = getHistogramSum(c.ConsensusLatency, c.logger)
	metrics["consensus_latency_count"] = float64(getHistogramCount(c.ConsensusLatency, c.logger))

	return metrics
}

// Reset resets all metrics to zero
func (c *BusinessCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Note: Prometheus counters cannot be reset to zero
	// This is a limitation of the Prometheus model
	// In practice, you would create new metric instances
	c.logger.Warn("Metrics reset requested, but Prometheus counters cannot be reset")
}

// LogMetrics logs current metrics values
func (c *BusinessCollector) LogMetrics() {
	metrics := c.GetMetrics()

	// Convert map[string]float64 to logrus.Fields
	fields := make(logrus.Fields)
	for k, v := range metrics {
		fields[k] = v
	}

	c.logger.WithFields(fields).Info("Current business metrics")
}

// getCounterValue reads the current value of a Prometheus counter with error handling
func getCounterValue(c prometheus.Counter, logger *logrus.Logger) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to read counter value")
		}
		return 0
	}
	if m.Counter == nil {
		if logger != nil {
			logger.Warn("counter metric is nil")
		}
		return 0
	}
	return m.GetCounter().GetValue()
}

// getGaugeValue reads the current value of a Prometheus gauge with error handling
func getGaugeValue(g prometheus.Gauge, logger *logrus.Logger) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to read gauge value")
		}
		return 0
	}
	if m.Gauge == nil {
		if logger != nil {
			logger.Warn("gauge metric is nil")
		}
		return 0
	}
	return m.GetGauge().GetValue()
}

// getHistogramCount reads the sample count of a Prometheus histogram with error handling
func getHistogramCount(h prometheus.Histogram, logger *logrus.Logger) uint64 {
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to read histogram count")
		}
		return 0
	}
	if m.Histogram == nil {
		if logger != nil {
			logger.Warn("histogram metric is nil")
		}
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

// getHistogramSum reads the sum of a Prometheus histogram with error handling
func getHistogramSum(h prometheus.Histogram, logger *logrus.Logger) float64 {
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to read histogram sum")
		}
		return 0
	}
	if m.Histogram == nil {
		if logger != nil {
			logger.Warn("histogram metric is nil")
		}
		return 0
	}
	return m.GetHistogram().GetSampleSum()
}
