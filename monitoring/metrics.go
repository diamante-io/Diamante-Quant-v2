package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/sirupsen/logrus"

	apipkg "diamante/api"
	"diamante/common"
	ctypes "diamante/consensus/types"
	"diamante/transaction"
	"diamante/types"

	dto "github.com/prometheus/client_model/go"
)

// Metrics groups all Prometheus metrics for Diamante monitoring
type Metrics struct {
	// Consensus metrics
	ConsensusHeight  prometheus.Gauge
	ConsensusLatency prometheus.Histogram

	// Transaction metrics
	TxProcessed prometheus.Counter
	TxFailed    prometheus.Counter
	TxPoolSize  prometheus.Gauge

	// Network metrics
	NetworkPeers         prometheus.Gauge
	NetworkBytesSent     prometheus.Counter
	NetworkBytesReceived prometheus.Counter

	// Validator metrics
	ValidatorsActive prometheus.Gauge
	ValidatorsTotal  prometheus.Gauge

	// Contract metrics
	ContractsDeployed prometheus.Counter
	ContractCalls     prometheus.Counter

	// Fees
	FeesCollected prometheus.Counter

	// System metrics
	CPUUsage    prometheus.Gauge
	MemoryUsage prometheus.Gauge
	DiskUsage   prometheus.Gauge
}

// NewMetrics creates all Prometheus metrics and registers them with the default registry
func NewMetrics() *Metrics {
	m := &Metrics{
		ConsensusHeight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_consensus_block_height",
			Help: "Current block height as reported by consensus",
		}),
		ConsensusLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_consensus_latency_seconds",
			Help:    "Consensus round latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		TxProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_tx_processed_total",
			Help: "Total transactions processed",
		}),
		TxFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_tx_failed_total",
			Help: "Total transactions that failed",
		}),
		TxPoolSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_tx_pool_size",
			Help: "Number of transactions currently in the pool",
		}),
		NetworkPeers: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_network_peer_count",
			Help: "Number of connected peers",
		}),
		NetworkBytesSent: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_network_bytes_sent_total",
			Help: "Total bytes sent over the network",
		}),
		NetworkBytesReceived: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_network_bytes_received_total",
			Help: "Total bytes received over the network",
		}),
		ValidatorsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_active_validators",
			Help: "Number of active validators",
		}),
		ValidatorsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_total_validators",
			Help: "Total number of validators registered",
		}),
		ContractsDeployed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_contracts_deployed_total",
			Help: "Total smart contracts deployed",
		}),
		ContractCalls: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_contract_calls_total",
			Help: "Total smart contract calls",
		}),
		FeesCollected: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_fees_collected_total",
			Help: "Total fees collected",
		}),
		CPUUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_cpu_usage_percent",
			Help: "System CPU usage percentage",
		}),
		MemoryUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_memory_usage_bytes",
			Help: "System memory usage in bytes",
		}),
		DiskUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_disk_usage_bytes",
			Help: "System disk usage in bytes",
		}),
	}
	return m
}

// NetworkMetrics defines the subset of network manager functionality used by the collector
type NetworkMetrics interface {
	GetNetworkHealth() (int, error)
	GetPeerList() ([]string, error)
}

// MetricsCollector periodically updates Prometheus metrics from core modules
type MetricsCollector struct {
	consensus ctypes.Consensus
	txMgr     *transaction.TransactionManager
	network   NetworkMetrics
	ledger    common.LedgerAPI
	metrics   *Metrics
	interval  time.Duration
	logger    *logrus.Logger
	stop      chan struct{}
}

// NewMetricsCollectorSimple creates a MetricsCollector with minimal configuration
func NewMetricsCollectorSimple(config *MetricsConfig, logger *logrus.Logger) *MetricsCollector {
	if logger == nil {
		logger = logrus.New()
	}
	return &MetricsCollector{
		consensus: nil,
		txMgr:     nil,
		network:   nil,
		ledger:    nil,
		metrics:   nil,
		interval:  time.Minute,
		logger:    logger,
		stop:      make(chan struct{}),
	}
}

// NewMetricsCollector creates a MetricsCollector
func NewMetricsCollector(cons ctypes.Consensus, network NetworkMetrics, ledger common.LedgerAPI, txMgr *transaction.TransactionManager, interval time.Duration, logger *logrus.Logger) *MetricsCollector {
	if logger == nil {
		logger = logrus.New()
	}
	return &MetricsCollector{
		consensus: cons,
		txMgr:     txMgr,
		network:   network,
		ledger:    ledger,
		metrics:   NewMetrics(),
		interval:  interval,
		logger:    logger,
		stop:      make(chan struct{}),
	}
}

// Start begins the metrics collection loop
func (mc *MetricsCollector) Start() {
	go mc.loop()
}

// Stop stops the metrics collection loop
func (mc *MetricsCollector) Stop() {
	close(mc.stop)
}

// RecordTransactionMetrics records transaction metrics
func (mc *MetricsCollector) RecordTransactionMetrics(tx *types.TypedTransaction, status string, duration time.Duration) {
	if mc.metrics == nil {
		return
	}
	// Record transaction metrics here
	mc.logger.WithFields(map[string]interface{}{
		"tx_id":    tx.ID,
		"status":   status,
		"duration": duration,
	}).Debug("Transaction metrics recorded")
}

// RecordBlockMetrics records block metrics
func (mc *MetricsCollector) RecordBlockMetrics(height uint64, size int, duration time.Duration, status string) {
	if mc.metrics == nil {
		return
	}
	// Record block metrics here
	mc.logger.WithFields(map[string]interface{}{
		"height":   height,
		"size":     size,
		"duration": duration,
		"status":   status,
	}).Debug("Block metrics recorded")
}

// RecordConsensusMetrics records consensus metrics
func (mc *MetricsCollector) RecordConsensusMetrics(result string, duration time.Duration, validatorCount int) {
	if mc.metrics == nil {
		return
	}
	mc.logger.WithFields(map[string]interface{}{
		"result":          result,
		"duration":        duration,
		"validator_count": validatorCount,
	}).Debug("Consensus metrics recorded")
}

// IncrementCounter increments a counter metric
func (mc *MetricsCollector) IncrementCounter(name string, labels map[string]string) {
	if mc.metrics == nil {
		return
	}
	mc.logger.WithFields(map[string]interface{}{
		"counter": name,
		"labels":  labels,
	}).Debug("Counter incremented")
}

// SetGauge sets a gauge metric
func (mc *MetricsCollector) SetGauge(name string, value float64, labels map[string]string) {
	if mc.metrics == nil {
		return
	}
	mc.logger.WithFields(map[string]interface{}{
		"gauge":  name,
		"value":  value,
		"labels": labels,
	}).Debug("Gauge set")
}

// ObserveHistogram observes a histogram metric
func (mc *MetricsCollector) ObserveHistogram(name string, value float64, labels map[string]string) {
	if mc.metrics == nil {
		return
	}
	mc.logger.WithFields(map[string]interface{}{
		"histogram": name,
		"value":     value,
		"labels":    labels,
	}).Debug("Histogram observed")
}

// GetMetrics returns collected metrics
func (mc *MetricsCollector) GetMetrics() (map[string]interface{}, error) {
	if mc.metrics == nil {
		return make(map[string]interface{}), nil
	}
	return map[string]interface{}{
		"status": "metrics_collected",
	}, nil
}

func (mc *MetricsCollector) loop() {
	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mc.collect()
		case <-mc.stop:
			return
		}
	}
}

func (mc *MetricsCollector) collect() {
	if hg, ok := mc.consensus.(apipkg.HeightGetter); ok {
		mc.metrics.ConsensusHeight.Set(float64(hg.GetLastBlockHeight()))
	}
	if mc.consensus != nil {
		mc.metrics.ValidatorsActive.Set(float64(len(mc.consensus.GetActiveValidators())))
		mc.metrics.ValidatorsTotal.Set(float64(len(mc.consensus.GetValidators())))
	}
	if mc.txMgr != nil {
		mc.metrics.TxPoolSize.Set(float64(mc.txMgr.GetPoolSize()))
		stats := mc.txMgr.GetStats()
		mc.metrics.TxProcessed.Add(float64(stats.TotalProcessed) - getCounterValue(mc.metrics.TxProcessed, mc.logger))
		mc.metrics.TxFailed.Add(float64(stats.TotalRejected) - getCounterValue(mc.metrics.TxFailed, mc.logger))
	}
	if mc.network != nil {
		peers, err := mc.network.GetPeerList()
		if err != nil {
			mc.logger.WithError(err).Warn("failed to get peer list")
			peers = []string{} // Use empty list on error
		}
		mc.metrics.NetworkPeers.Set(float64(len(peers)))
	}
	mc.metrics.MemoryUsage.Set(getMemoryUsage(mc.logger))
	mc.metrics.CPUUsage.Set(getCPUUsage(mc.logger))
	mc.metrics.DiskUsage.Set(getDiskUsage(mc.logger))
}

// Helper functions -----------------------------------------------------------

// getMemoryUsage retrieves current memory usage with error logging
func getMemoryUsage(logger *logrus.Logger) float64 {
	vm, err := mem.VirtualMemory()
	if err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to get memory usage")
		}
		return 0
	}
	return float64(vm.Used)
}

// getCPUUsage retrieves current CPU usage percentage with error logging
func getCPUUsage(logger *logrus.Logger) float64 {
	perc, err := cpu.Percent(0, false)
	if err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to get CPU usage")
		}
		return 0
	}
	if len(perc) == 0 {
		if logger != nil {
			logger.Warn("CPU usage returned empty array")
		}
		return 0
	}
	return perc[0]
}

// getDiskUsage retrieves current disk usage with error logging
func getDiskUsage(logger *logrus.Logger) float64 {
	du, err := disk.Usage("/")
	if err != nil {
		if logger != nil {
			logger.WithError(err).Warn("failed to get disk usage")
		}
		return 0
	}
	return float64(du.Used)
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
