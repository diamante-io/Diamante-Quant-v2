// metrics/reporter.go
package metrics

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/sirupsen/logrus"

	apipkg "diamante/api"
	"diamante/common"
	ctypes "diamante/consensus/types"
	monitoralert "diamante/monitoring/alerting"
	monitormetrics "diamante/monitoring/metrics"
	"diamante/transaction"
)

// Constants for configuration and magic numbers
const (
	// DefaultUpdateInterval is the default interval for metrics collection
	DefaultUpdateInterval = 30 * time.Second

	// DefaultCircuitBreakerTimeout is the default timeout for circuit breaker
	DefaultCircuitBreakerTimeout = 5 * time.Second

	// DefaultMaxRetries is the default maximum number of retries for operations
	DefaultMaxRetries = 3

	// DefaultRetryDelay is the default initial delay for retry operations
	DefaultRetryDelay = 100 * time.Millisecond

	// DefaultMaxConcurrentOperations limits concurrent metric collection operations
	DefaultMaxConcurrentOperations = 10

	// MetricsNamespace is the namespace prefix for all metrics
	MetricsNamespace = "diamante"
)

// Partition status constants
const (
	PartitionStatusNormal     = 0
	PartitionStatusSuspected  = 1
	PartitionStatusConfirmed  = 2
	PartitionStatusRecovering = 3
	PartitionStatusUnknown    = -1
)

// ValidatorMetrics defines the methods needed for gathering validator metrics with proper error handling.
type ValidatorMetrics interface {
	// GetActiveNodes returns the list of active validator nodes with error handling.
	GetActiveNodes() ([][32]byte, error)

	// GetGossipPeers returns the list of gossip peers with error handling.
	GetGossipPeers() ([][32]byte, error)
}

// NetworkMetrics defines methods for gathering network statistics with proper error handling.
type NetworkMetrics interface {
	// GetNetworkHealth returns the network health score with error handling.
	GetNetworkHealth() (int, error)

	// GetPeerList returns the list of connected peers with error handling.
	GetPeerList() ([]string, error)
}

// CircuitBreaker implements the circuit breaker pattern for external dependencies
type CircuitBreaker struct {
	mu           sync.RWMutex
	state        int32 // 0 = closed, 1 = open, 2 = half-open
	failures     int32
	lastFailTime time.Time
	timeout      time.Duration
	threshold    int32
}

// NewCircuitBreaker creates a new circuit breaker with the specified threshold and timeout
func NewCircuitBreaker(threshold int32, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Execute runs the given function through the circuit breaker
func (cb *CircuitBreaker) Execute(fn func() error) error {
	state := atomic.LoadInt32(&cb.state)

	switch state {
	case 0: // closed
		err := fn()
		if err != nil {
			cb.recordFailure()
			return errors.Wrap(err, "circuit breaker: operation failed")
		}
		cb.recordSuccess()
		return nil

	case 1: // open
		cb.mu.RLock()
		canRetry := time.Since(cb.lastFailTime) > cb.timeout
		cb.mu.RUnlock()

		if canRetry {
			atomic.CompareAndSwapInt32(&cb.state, 1, 2) // open -> half-open
			return cb.Execute(fn)
		}
		return errors.New("circuit breaker: circuit is open")

	case 2: // half-open
		err := fn()
		if err != nil {
			cb.recordFailure()
			return errors.Wrap(err, "circuit breaker: half-open test failed")
		}
		cb.recordSuccess()
		atomic.StoreInt32(&cb.state, 0) // half-open -> closed
		return nil

	default:
		return errors.New("circuit breaker: invalid state")
	}
}

func (cb *CircuitBreaker) recordFailure() {
	failures := atomic.AddInt32(&cb.failures, 1)
	if failures >= cb.threshold {
		cb.mu.Lock()
		cb.lastFailTime = common.ConsensusNow()
		cb.mu.Unlock()
		atomic.StoreInt32(&cb.state, 1) // closed/half-open -> open
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	atomic.StoreInt32(&cb.failures, 0)
}

// RetryConfig holds configuration for retry operations
type RetryConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   DefaultMaxRetries,
		InitialDelay: DefaultRetryDelay,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
	}
}

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, config RetryConfig, fn func() error) error {
	var lastErr error
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "retry cancelled by context")
			case <-time.After(delay):
				// Continue with retry
			}

			delay = time.Duration(float64(delay) * config.Multiplier)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}

		if err := fn(); err != nil {
			lastErr = err
			continue
		}

		return nil // Success
	}

	return errors.Wrapf(lastErr, "operation failed after %d attempts", config.MaxRetries+1)
}

// Reporter periodically collects metrics from core modules and updates Prometheus gauges.
// This implementation includes production-ready features like circuit breakers, retry logic,
// proper error handling, and resource management.
type Reporter struct {
	mu              sync.RWMutex
	logger          *logrus.Logger
	consensus       ctypes.Consensus                // Should implement GetNetworkLoad() and, if possible, ValidatorMetrics
	network         NetworkMetrics                  // Provides network level metrics
	ledger          common.LedgerAPI                // Ledger API for blockchain metrics
	txManager       *transaction.TransactionManager // Transaction manager for pool metrics
	updateInterval  time.Duration
	prometheusStats *PrometheusStats
	stopChan        chan struct{}
	monitoring      *monitormetrics.Registry
	alerts          *monitoralert.Manager
	healthFn        func() int

	// Production features
	circuitBreakers    map[string]*CircuitBreaker
	retryConfig        RetryConfig
	semaphore          chan struct{} // Limit concurrent operations
	metricsErrors      *prometheus.CounterVec
	operationDurations *prometheus.HistogramVec

	// State management
	started     int32
	stopping    int32
	lastMetrics map[string]interface{}
}

// PrometheusStats holds our Prometheus metrics with comprehensive coverage.
type PrometheusStats struct {
	// Consensus metrics
	blockHeight      prometheus.Gauge
	networkLoad      prometheus.Gauge
	activeValidators prometheus.Gauge

	// Ledger metrics
	ledgerHeight       prometheus.Gauge
	ledgerAccounts     prometheus.Gauge
	ledgerBlocks       prometheus.Gauge
	ledgerTransactions prometheus.Gauge

	// Network metrics
	networkHealth prometheus.Gauge
	peerCount     prometheus.Gauge

	// Transaction metrics
	txPoolSize  prometheus.Gauge
	txProcessed prometheus.Counter
	txFailed    prometheus.Counter

	// System metrics
	memoryUsage prometheus.Gauge
	cpuUsage    prometheus.Gauge
	diskUsage   prometheus.Gauge

	// Meta-metrics for monitoring the metrics system itself
	metricsCollectionDuration prometheus.Histogram
	metricsCollectionErrors   prometheus.Counter
	metricsCollectionSuccess  prometheus.Counter
}

// NewReporter creates a new Reporter instance with comprehensive validation and error handling.
// This constructor implements all production requirements including parameter validation,
// circuit breaker initialization, and resource management.
//
// Parameters:
//   - cons: Consensus interface (must not be nil)
//   - networkMetrics: Network metrics interface (optional)
//   - ledgerAPI: Ledger API interface (optional)
//   - txManager: Transaction manager (must not be nil)
//   - updateInterval: Metrics update interval (must be > 0)
//   - logger: Logger instance (optional, will create default if nil)
//   - registry: Monitoring registry (optional)
//   - alertMgr: Alert manager (optional)
//   - healthFn: Health check function (optional)
//
// Returns:
//   - *Reporter: The created reporter instance
//   - error: Validation error if any required parameter is invalid
func NewReporter(
	cons ctypes.Consensus,
	networkMetrics NetworkMetrics,
	ledgerAPI common.LedgerAPI,
	txManager *transaction.TransactionManager,
	updateInterval time.Duration,
	logger *logrus.Logger,
	registry *monitormetrics.Registry,
	alertMgr *monitoralert.Manager,
	healthFn func() int,
) (*Reporter, error) {
	// Validate required parameters
	if cons == nil {
		return nil, errors.New("consensus interface cannot be nil")
	}

	if txManager == nil {
		return nil, errors.New("transaction manager cannot be nil")
	}

	if updateInterval <= 0 {
		return nil, errors.New("update interval must be greater than zero")
	}

	// Use defaults for optional parameters
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	if updateInterval < time.Second {
		logger.Warning("Update interval is very short, consider using at least 1 second")
	}

	// Create meta-metrics for monitoring the metrics system itself
	metricsErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "metrics_collection_errors_total",
			Help:      "Total number of errors during metrics collection",
		},
		[]string{"component", "operation"},
	)

	operationDurations := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: MetricsNamespace,
			Name:      "metrics_operation_duration_seconds",
			Help:      "Duration of metrics collection operations",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"component", "operation"},
	)

	stats := &PrometheusStats{
		blockHeight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "block_height",
			Help:      "Current block height as reported by consensus",
		}),
		networkLoad: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "network_load",
			Help:      "Current network load from consensus",
		}),
		activeValidators: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "active_validators",
			Help:      "Number of active validators",
		}),
		ledgerHeight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "ledger_height",
			Help:      "Current block height from the ledger",
		}),
		ledgerAccounts: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "ledger_accounts",
			Help:      "Total accounts in the ledger",
		}),
		ledgerBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "ledger_blocks",
			Help:      "Total blocks stored in the ledger",
		}),
		ledgerTransactions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "ledger_transactions",
			Help:      "Total transactions stored in the ledger",
		}),
		networkHealth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "network_health",
			Help:      "Overall network health score",
		}),
		peerCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "peer_count",
			Help:      "Number of connected peers",
		}),
		txPoolSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "tx_pool_size",
			Help:      "Current number of transactions in the pool",
		}),
		txProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "tx_processed_total",
			Help:      "Total number of transactions processed",
		}),
		txFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "tx_failed_total",
			Help:      "Total number of transactions that failed processing",
		}),
		memoryUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "memory_usage_bytes",
			Help:      "System memory usage in bytes",
		}),
		cpuUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "cpu_usage_percent",
			Help:      "System CPU usage percentage",
		}),
		diskUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "disk_usage_bytes",
			Help:      "System disk usage in bytes",
		}),
		metricsCollectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: MetricsNamespace,
			Name:      "metrics_collection_duration_seconds",
			Help:      "Duration of metrics collection cycles",
			Buckets:   prometheus.DefBuckets,
		}),
		metricsCollectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "metrics_collection_errors_total",
			Help:      "Total number of metrics collection errors",
		}),
		metricsCollectionSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "metrics_collection_success_total",
			Help:      "Total number of successful metrics collection cycles",
		}),
	}

	// Safely register metrics with Prometheus (replace MustRegister with graceful error handling)
	metrics := []prometheus.Collector{
		stats.blockHeight,
		stats.networkLoad,
		stats.activeValidators,
		stats.ledgerHeight,
		stats.ledgerAccounts,
		stats.ledgerBlocks,
		stats.ledgerTransactions,
		stats.networkHealth,
		stats.txPoolSize,
		stats.txProcessed,
		stats.txFailed,
		stats.peerCount,
		stats.memoryUsage,
		stats.cpuUsage,
		stats.diskUsage,
		stats.metricsCollectionDuration,
		stats.metricsCollectionErrors,
		stats.metricsCollectionSuccess,
		metricsErrors,
		operationDurations,
	}

	for _, metric := range metrics {
		if err := prometheus.Register(metric); err != nil {
			// Check if already registered (which is acceptable)
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				return nil, errors.Wrapf(err, "failed to register Prometheus metric")
			}
			logger.WithError(err).Debug("Metric already registered, continuing")
		}
	}

	// Initialize circuit breakers for different components
	circuitBreakers := map[string]*CircuitBreaker{
		"consensus": NewCircuitBreaker(3, DefaultCircuitBreakerTimeout),
		"network":   NewCircuitBreaker(3, DefaultCircuitBreakerTimeout),
		"ledger":    NewCircuitBreaker(3, DefaultCircuitBreakerTimeout),
		"system":    NewCircuitBreaker(5, DefaultCircuitBreakerTimeout), // More lenient for system metrics
	}

	return &Reporter{
		consensus:          cons,
		network:            networkMetrics,
		ledger:             ledgerAPI,
		txManager:          txManager,
		updateInterval:     updateInterval,
		logger:             logger,
		prometheusStats:    stats,
		stopChan:           make(chan struct{}),
		monitoring:         registry,
		alerts:             alertMgr,
		healthFn:           healthFn,
		circuitBreakers:    circuitBreakers,
		retryConfig:        DefaultRetryConfig(),
		semaphore:          make(chan struct{}, DefaultMaxConcurrentOperations),
		metricsErrors:      metricsErrors,
		operationDurations: operationDurations,
		lastMetrics:        make(map[string]interface{}),
	}, nil
}

// Start launches the metrics reporting loop with proper state management.
// This method ensures thread-safe startup and prevents multiple concurrent starts.
func (r *Reporter) Start() error {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return errors.New("reporter is already started")
	}

	r.logger.Info("Starting Diamante V2 metrics reporter with production features...")
	go r.reportingLoop()
	return nil
}

// Stop halts the reporting loop with graceful shutdown and resource cleanup.
// This method ensures all resources are properly cleaned up and prevents data races.
func (r *Reporter) Stop() error {
	if !atomic.CompareAndSwapInt32(&r.stopping, 0, 1) {
		return errors.New("reporter is already stopping")
	}

	r.logger.Info("Stopping metrics reporter...")
	close(r.stopChan)

	// Wait a short time for cleanup using a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	<-ctx.Done()

	atomic.StoreInt32(&r.started, 0)
	atomic.StoreInt32(&r.stopping, 0)

	return nil
}

// reportingLoop collects metrics periodically with comprehensive error handling and resource management.
func (r *Reporter) reportingLoop() {
	ticker := time.NewTicker(r.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			start := common.ConsensusNow()

			// Use semaphore to limit concurrent operations
			select {
			case r.semaphore <- struct{}{}:
				func() {
					defer func() { <-r.semaphore }()
					defer func() {
						duration := time.Since(start)
						r.prometheusStats.metricsCollectionDuration.Observe(duration.Seconds())
					}()

					if err := r.collectAndReportMetrics(); err != nil {
						r.logger.WithError(err).Error("Failed to collect metrics")
						r.prometheusStats.metricsCollectionErrors.Inc()
					} else {
						r.prometheusStats.metricsCollectionSuccess.Inc()
					}
				}()
			default:
				r.logger.Warning("Metrics collection skipped due to resource limit")
				r.metricsErrors.WithLabelValues("reporter", "resource_limit").Inc()
			}

		case <-r.stopChan:
			r.logger.Info("Metrics reporter loop stopped")
			return
		}
	}
}

// collectAndReportMetrics gathers metrics from all components with comprehensive error handling.
func (r *Reporter) collectAndReportMetrics() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.updateInterval/2)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	collectionErrors := make([]error, 0)

	// Collect consensus metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.collectConsensusMetrics(ctx); err != nil {
			mu.Lock()
			collectionErrors = append(collectionErrors, errors.Wrap(err, "consensus metrics collection failed"))
			mu.Unlock()
		}
	}()

	// Collect network metrics
	if r.network != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.collectNetworkMetrics(ctx); err != nil {
				mu.Lock()
				collectionErrors = append(collectionErrors, errors.Wrap(err, "network metrics collection failed"))
				mu.Unlock()
			}
		}()
	}

	// Collect ledger metrics
	if r.ledger != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.collectLedgerMetrics(ctx); err != nil {
				mu.Lock()
				collectionErrors = append(collectionErrors, errors.Wrap(err, "ledger metrics collection failed"))
				mu.Unlock()
			}
		}()
	}

	// Collect transaction metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.collectTransactionMetrics(ctx); err != nil {
			mu.Lock()
			collectionErrors = append(collectionErrors, errors.Wrap(err, "transaction metrics collection failed"))
			mu.Unlock()
		}
	}()

	// Collect system metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.collectSystemMetrics(ctx); err != nil {
			mu.Lock()
			collectionErrors = append(collectionErrors, errors.Wrap(err, "system metrics collection failed"))
			mu.Unlock()
		}
	}()

	wg.Wait()

	// Update monitoring and alerts
	if r.monitoring != nil || r.alerts != nil {
		r.updateMonitoringAndAlerts()
	}

	// Log metrics snapshot
	r.logMetricsSnapshot()

	if len(collectionErrors) > 0 {
		return errors.Errorf("metrics collection completed with %d errors: %v", len(collectionErrors), collectionErrors)
	}

	return nil
}

// collectConsensusMetrics collects metrics from the consensus layer with circuit breaker protection.
func (r *Reporter) collectConsensusMetrics(ctx context.Context) error {
	return r.circuitBreakers["consensus"].Execute(func() error {
		return RetryWithBackoff(ctx, r.retryConfig, func() error {
			start := common.ConsensusNow()
			defer func() {
				r.operationDurations.WithLabelValues("consensus", "collect").Observe(time.Since(start).Seconds())
			}()

			// Get block height
			var height float64
			if getter, ok := r.consensus.(apipkg.HeightGetter); ok {
				height = float64(getter.GetLastBlockHeight())
			}
			r.prometheusStats.blockHeight.Set(height)

			// Get network load
			networkLoad := r.consensus.GetNetworkLoad()
			r.prometheusStats.networkLoad.Set(networkLoad)

			// Get validator metrics if available
			if vm, ok := r.consensus.(ValidatorMetrics); ok {
				activeNodes, err := vm.GetActiveNodes()
				if err != nil {
					r.metricsErrors.WithLabelValues("consensus", "active_nodes").Inc()
					return errors.Wrap(err, "failed to get active nodes")
				}
				r.prometheusStats.activeValidators.Set(float64(len(activeNodes)))

				peerIDs, err := vm.GetGossipPeers()
				if err != nil {
					r.metricsErrors.WithLabelValues("consensus", "gossip_peers").Inc()
					return errors.Wrap(err, "failed to get gossip peers")
				}
				r.prometheusStats.peerCount.Set(float64(len(peerIDs)))
			}

			return nil
		})
	})
}

// collectNetworkMetrics collects network-related metrics with error handling.
func (r *Reporter) collectNetworkMetrics(ctx context.Context) error {
	return r.circuitBreakers["network"].Execute(func() error {
		return RetryWithBackoff(ctx, r.retryConfig, func() error {
			start := common.ConsensusNow()
			defer func() {
				r.operationDurations.WithLabelValues("network", "collect").Observe(time.Since(start).Seconds())
			}()

			// Get network health
			var health int
			var err error
			if r.healthFn != nil {
				health = r.healthFn()
			} else {
				health, err = r.network.GetNetworkHealth()
				if err != nil {
					r.metricsErrors.WithLabelValues("network", "health").Inc()
					return errors.Wrap(err, "failed to get network health")
				}
			}
			r.prometheusStats.networkHealth.Set(float64(health))

			// Get peer list
			peers, err := r.network.GetPeerList()
			if err != nil {
				r.metricsErrors.WithLabelValues("network", "peers").Inc()
				return errors.Wrap(err, "failed to get peer list")
			}
			r.prometheusStats.peerCount.Set(float64(len(peers)))

			return nil
		})
	})
}

// collectLedgerMetrics collects ledger-related metrics with comprehensive error handling.
func (r *Reporter) collectLedgerMetrics(ctx context.Context) error {
	return r.circuitBreakers["ledger"].Execute(func() error {
		return RetryWithBackoff(ctx, r.retryConfig, func() error {
			start := common.ConsensusNow()
			defer func() {
				r.operationDurations.WithLabelValues("ledger", "collect").Observe(time.Since(start).Seconds())
			}()

			stats, err := r.ledger.GetStats()
			if err != nil {
				r.metricsErrors.WithLabelValues("ledger", "stats").Inc()
				return errors.Wrap(err, "failed to get ledger stats")
			}

			// Use proper struct field access instead of map indexing
			r.prometheusStats.ledgerHeight.Set(float64(stats.LastBlockHeight))
			r.prometheusStats.ledgerAccounts.Set(float64(stats.TotalAccounts))
			r.prometheusStats.ledgerBlocks.Set(float64(stats.LastBlockHeight)) // Using block height as block count
			r.prometheusStats.ledgerTransactions.Set(float64(stats.TotalTransactions))

			return nil
		})
	})
}

// collectTransactionMetrics collects transaction pool metrics.
func (r *Reporter) collectTransactionMetrics(ctx context.Context) error {
	start := common.ConsensusNow()
	defer func() {
		r.operationDurations.WithLabelValues("transaction", "collect").Observe(time.Since(start).Seconds())
	}()

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	poolSize := r.txManager.GetPoolSize()
	r.prometheusStats.txPoolSize.Set(float64(poolSize))

	return nil
}

// collectSystemMetrics collects system-level metrics with proper error handling.
func (r *Reporter) collectSystemMetrics(ctx context.Context) error {
	return r.circuitBreakers["system"].Execute(func() error {
		return RetryWithBackoff(ctx, r.retryConfig, func() error {
			start := common.ConsensusNow()
			defer func() {
				r.operationDurations.WithLabelValues("system", "collect").Observe(time.Since(start).Seconds())
			}()

			// Memory usage
			memUsage, err := getMemoryUsageWithError()
			if err != nil {
				r.metricsErrors.WithLabelValues("system", "memory").Inc()
				r.logger.WithError(err).Warning("Failed to get memory usage")
				memUsage = -1 // Indicate error state
			}
			r.prometheusStats.memoryUsage.Set(memUsage)

			// CPU usage
			cpuUsage, err := getCPUUsageWithError()
			if err != nil {
				r.metricsErrors.WithLabelValues("system", "cpu").Inc()
				r.logger.WithError(err).Warning("Failed to get CPU usage")
				cpuUsage = -1 // Indicate error state
			}
			r.prometheusStats.cpuUsage.Set(cpuUsage)

			// Disk usage
			diskUsage, err := getDiskUsageWithError()
			if err != nil {
				r.metricsErrors.WithLabelValues("system", "disk").Inc()
				r.logger.WithError(err).Warning("Failed to get disk usage")
				diskUsage = -1 // Indicate error state
			}
			r.prometheusStats.diskUsage.Set(diskUsage)

			return nil
		})
	})
}

// updateMonitoringAndAlerts updates external monitoring systems and evaluates alerts.
func (r *Reporter) updateMonitoringAndAlerts() {
	snapshot := r.GetMetricsSnapshot()

	if r.monitoring != nil {
		if r.monitoring.Consensus != nil {
			// Convert snapshot values to ConsensusMetricsUpdate
			consensusUpdate := monitormetrics.ConsensusMetricsUpdate{}
			if activeValidators, ok := snapshot["activeValidators"].(float64); ok {
				consensusUpdate.ActiveValidators = int(activeValidators)
			}
			// Note: We don't have TotalValidators or PendingEvents in our snapshot
			// You might need to add these to the collection process if needed

			r.monitoring.Consensus.Update(consensusUpdate)
		}

		if r.monitoring.Network != nil {
			if peerCount, ok := snapshot["peerCount"].(float64); ok {
				r.monitoring.Network.UpdatePeers(int(peerCount))
			}
			if health, ok := snapshot["networkHealth"].(float64); ok {
				// UpdateHealth expects bool, so convert based on a threshold
				// For example, health > 0 means healthy
				r.monitoring.Network.UpdateHealth(health > 0)
			}
		}

		if r.monitoring.Performance != nil {
			// Convert snapshot values to PerformanceMetrics
			perfMetrics := monitormetrics.PerformanceMetrics{}
			if cpuUsage, ok := snapshot["cpuUsage"].(float64); ok {
				perfMetrics.CPUUsage = cpuUsage
			}
			if memoryUsage, ok := snapshot["memoryUsage"].(float64); ok {
				perfMetrics.MemoryUsage = uint64(memoryUsage)
			}
			if diskUsage, ok := snapshot["diskUsage"].(float64); ok {
				perfMetrics.DiskUsage = uint64(diskUsage)
			}
			// Note: GoroutineCount is not in our snapshot
			// You might need to add it to the collection process if needed

			r.monitoring.Performance.Update(perfMetrics)
		}
	}

	if r.alerts != nil {
		// Convert snapshot to MetricsSnapshot
		metricsSnapshot := monitoralert.MetricsSnapshot{
			SystemMetrics: monitoralert.SystemMetrics{
				CPUUsagePercent:  getFloat64FromSnapshot(snapshot, "cpuUsage", 0),
				MemoryUsageBytes: int64(getFloat64FromSnapshot(snapshot, "memoryUsage", 0)),
				DiskUsageBytes:   int64(getFloat64FromSnapshot(snapshot, "diskUsage", 0)),
			},
			BusinessMetrics: monitoralert.BusinessMetrics{
				// These values aren't in our current snapshot, but we can add some
				TransactionsProcessed: int64(getFloat64FromSnapshot(snapshot, "ledgerTransactions", 0)),
			},
			NetworkMetrics: monitoralert.NetworkMetrics{
				PeerCount: int64(getFloat64FromSnapshot(snapshot, "peerCount", 0)),
			},
			ConsensusMetrics: monitoralert.ConsensusMetrics{
				BlockHeight:      int64(getFloat64FromSnapshot(snapshot, "blockHeight", 0)),
				ValidatorCount:   int64(getFloat64FromSnapshot(snapshot, "activeValidators", 0)),
				ActiveValidators: int64(getFloat64FromSnapshot(snapshot, "activeValidators", 0)),
			},
		}

		r.alerts.Evaluate(metricsSnapshot)
	}
}

// logMetricsSnapshot logs a snapshot of current metrics for debugging and monitoring.
func (r *Reporter) logMetricsSnapshot() {
	r.mu.Lock()
	r.lastMetrics = r.GetMetricsSnapshot()
	r.mu.Unlock()

	snapshot, err := json.Marshal(r.lastMetrics)
	if err != nil {
		r.logger.WithError(err).Error("Failed to marshal metrics snapshot")
		return
	}

	r.logger.Debug("Metrics snapshot", "data", string(snapshot))
}

// GetMetricsSnapshot returns a snapshot of key metric values with proper error handling.
func (r *Reporter) GetMetricsSnapshot() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["blockHeight"] = getGaugeValue(r.prometheusStats.blockHeight)
	stats["networkLoad"] = getGaugeValue(r.prometheusStats.networkLoad)
	stats["activeValidators"] = getGaugeValue(r.prometheusStats.activeValidators)
	stats["txPoolSize"] = getGaugeValue(r.prometheusStats.txPoolSize)
	stats["peerCount"] = getGaugeValue(r.prometheusStats.peerCount)
	stats["ledgerHeight"] = getGaugeValue(r.prometheusStats.ledgerHeight)
	stats["ledgerAccounts"] = getGaugeValue(r.prometheusStats.ledgerAccounts)
	stats["ledgerBlocks"] = getGaugeValue(r.prometheusStats.ledgerBlocks)
	stats["ledgerTransactions"] = getGaugeValue(r.prometheusStats.ledgerTransactions)
	stats["networkHealth"] = getGaugeValue(r.prometheusStats.networkHealth)
	stats["memoryUsage"] = getGaugeValue(r.prometheusStats.memoryUsage)
	stats["cpuUsage"] = getGaugeValue(r.prometheusStats.cpuUsage)
	stats["diskUsage"] = getGaugeValue(r.prometheusStats.diskUsage)

	return stats
}

// getGaugeValue extracts the current float64 value from a Prometheus gauge with error handling.
func getGaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		return -1 // Indicate error state
	}
	return m.GetGauge().GetValue()
}

// toFloat64 attempts to convert various numeric types to float64 with comprehensive type support.
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// getMemoryUsageWithError returns the system memory usage in bytes with proper error handling.
func getMemoryUsageWithError() (float64, error) {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get virtual memory stats")
	}
	return float64(vm.Used), nil
}

// getCPUUsageWithError returns the current CPU usage percentage with proper error handling.
func getCPUUsageWithError() (float64, error) {
	perc, err := cpu.Percent(0, false)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get CPU percentage")
	}
	if len(perc) == 0 {
		return 0, errors.New("no CPU percentage data available")
	}
	return perc[0], nil
}

// getDiskUsageWithError returns disk usage in bytes for the root filesystem with proper error handling.
func getDiskUsageWithError() (float64, error) {
	du, err := disk.Usage("/")
	if err != nil {
		return 0, errors.Wrap(err, "failed to get disk usage")
	}
	return float64(du.Used), nil
}

// ValidateReporter performs comprehensive validation of a reporter instance.
func ValidateReporter(r *Reporter) error {
	if r == nil {
		return errors.New("reporter is nil")
	}

	if r.consensus == nil {
		return errors.New("consensus interface is nil")
	}

	if r.txManager == nil {
		return errors.New("transaction manager is nil")
	}

	if r.updateInterval <= 0 {
		return errors.New("update interval must be positive")
	}

	if r.logger == nil {
		return errors.New("logger is nil")
	}

	return nil
}

// GetCircuitBreakerStatus returns the status of all circuit breakers for monitoring.
func (r *Reporter) GetCircuitBreakerStatus() map[string]map[string]interface{} {
	status := make(map[string]map[string]interface{})

	for name, cb := range r.circuitBreakers {
		state := atomic.LoadInt32(&cb.state)
		failures := atomic.LoadInt32(&cb.failures)

		var stateStr string
		switch state {
		case 0:
			stateStr = "closed"
		case 1:
			stateStr = "open"
		case 2:
			stateStr = "half-open"
		default:
			stateStr = "unknown"
		}

		status[name] = map[string]interface{}{
			"state":    stateStr,
			"failures": failures,
		}
	}

	return status
}

// getFloat64FromSnapshot safely extracts a float64 value from the snapshot
func getFloat64FromSnapshot(snapshot map[string]interface{}, key string, defaultValue float64) float64 {
	if val, ok := snapshot[key].(float64); ok {
		return val
	}
	return defaultValue
}
