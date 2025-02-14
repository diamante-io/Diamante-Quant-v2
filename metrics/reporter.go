// metrics/reporter.go
package metrics

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"diamante/api"
	ctypes "diamante/consensus/types"
	"diamante/ledger"
	"diamante/transaction"
)

// ValidatorMetrics defines the methods needed for gathering validator metrics.
type ValidatorMetrics interface {
	GetActiveNodes() [][32]byte
	GetGossipPeers() [][32]byte
}

// Reporter periodically collects metrics from core modules and updates Prometheus gauges.
type Reporter struct {
	mu              sync.RWMutex
	logger          *logrus.Logger
	consensus       ctypes.Consensus // Should implement GetNetworkLoad() and, if possible, ValidatorMetrics.
	ledger          ledger.LedgerAPI
	txManager       *transaction.TransactionManager // Must expose GetPoolSize() method.
	updateInterval  time.Duration
	prometheusStats *PrometheusStats
	stopChan        chan struct{}
}

// PrometheusStats holds our Prometheus metrics.
type PrometheusStats struct {
	// Consensus metrics.
	blockHeight      prometheus.Gauge
	networkLoad      prometheus.Gauge
	activeValidators prometheus.Gauge

	// Transaction metrics.
	txPoolSize  prometheus.Gauge
	txProcessed prometheus.Counter
	txFailed    prometheus.Counter

	// Network metrics.
	peerCount prometheus.Gauge

	// System metrics (placeholders; integrate real metrics later).
	memoryUsage prometheus.Gauge
	cpuUsage    prometheus.Gauge
	diskUsage   prometheus.Gauge
}

// NewReporter creates a new Reporter instance and registers metrics.
func NewReporter(
	cons ctypes.Consensus,
	ledgerAPI ledger.LedgerAPI,
	txManager *transaction.TransactionManager,
	updateInterval time.Duration,
	logger *logrus.Logger,
) *Reporter {

	stats := &PrometheusStats{
		blockHeight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_block_height",
			Help: "Current block height as reported by consensus (via HeightGetter)",
		}),
		networkLoad: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_network_load",
			Help: "Current network load from consensus",
		}),
		activeValidators: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_active_validators",
			Help: "Number of active validators (via ValidatorMetrics)",
		}),
		txPoolSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_tx_pool_size",
			Help: "Current number of transactions in the pool",
		}),
		txProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_tx_processed_total",
			Help: "Total number of transactions processed",
		}),
		txFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_tx_failed_total",
			Help: "Total number of transactions that failed processing",
		}),
		peerCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_peer_count",
			Help: "Number of peers connected via Gossip",
		}),
		memoryUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_memory_usage_bytes",
			Help: "System memory usage in bytes (placeholder)",
		}),
		cpuUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_cpu_usage_percent",
			Help: "System CPU usage percentage (placeholder)",
		}),
		diskUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_disk_usage_bytes",
			Help: "System disk usage in bytes (placeholder)",
		}),
	}

	prometheus.MustRegister(
		stats.blockHeight,
		stats.networkLoad,
		stats.activeValidators,
		stats.txPoolSize,
		stats.txProcessed,
		stats.txFailed,
		stats.peerCount,
		stats.memoryUsage,
		stats.cpuUsage,
		stats.diskUsage,
	)

	return &Reporter{
		consensus:       cons,
		ledger:          ledgerAPI,
		txManager:       txManager,
		updateInterval:  updateInterval,
		logger:          logger,
		prometheusStats: stats,
		stopChan:        make(chan struct{}),
	}
}

// Start launches the metrics reporting loop.
func (r *Reporter) Start() error {
	r.logger.Info("Starting Diamante V2 metrics reporter...")
	go r.reportingLoop()
	return nil
}

// Stop halts the reporting loop.
func (r *Reporter) Stop() error {
	r.logger.Info("Stopping metrics reporter...")
	close(r.stopChan)
	return nil
}

// reportingLoop collects metrics periodically.
func (r *Reporter) reportingLoop() {
	ticker := time.NewTicker(r.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.collectAndReportMetrics()
		case <-r.stopChan:
			r.logger.Info("Metrics reporter loop stopped.")
			return
		}
	}
}

// collectAndReportMetrics gathers metrics from consensus, transaction manager, and network.
func (r *Reporter) collectAndReportMetrics() {
	// Consensus metrics:
	// Use the HeightGetter interface from the API package.
	var height float64
	if getter, ok := r.consensus.(api.HeightGetter); ok {
		height = float64(getter.GetLastBlockHeight())
	}
	r.prometheusStats.blockHeight.Set(height)
	r.prometheusStats.networkLoad.Set(r.consensus.GetNetworkLoad())

	// If the consensus object supports ValidatorMetrics, get active validator and peer count.
	if vm, ok := r.consensus.(ValidatorMetrics); ok {
		activeNodes := vm.GetActiveNodes()
		r.prometheusStats.activeValidators.Set(float64(len(activeNodes)))
		peerIDs := vm.GetGossipPeers()
		r.prometheusStats.peerCount.Set(float64(len(peerIDs)))
	}

	// Transaction metrics:
	// Get pool size via TransactionManager.GetPoolSize() (make sure to add this method as explained).
	r.prometheusStats.txPoolSize.Set(float64(r.txManager.GetPoolSize()))

	// System metrics: placeholders (update with real system calls later).
	// r.prometheusStats.memoryUsage.Set(getMemoryUsage())
	// r.prometheusStats.cpuUsage.Set(getCPUUsage())
	// r.prometheusStats.diskUsage.Set(getDiskUsage())

	// Log a snapshot of key metrics.
	snapshot, _ := json.Marshal(r.GetMetricsSnapshot())
	r.logger.Info("Metrics updated", "snapshot", string(snapshot))
}

// GetMetricsSnapshot returns a snapshot of key metric values.
func (r *Reporter) GetMetricsSnapshot() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["blockHeight"] = r.prometheusStats.blockHeight
	stats["networkLoad"] = r.prometheusStats.networkLoad
	stats["activeValidators"] = r.prometheusStats.activeValidators
	stats["txPoolSize"] = r.prometheusStats.txPoolSize
	stats["peerCount"] = r.prometheusStats.peerCount
	return stats
}
