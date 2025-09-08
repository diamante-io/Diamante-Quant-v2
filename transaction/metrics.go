package transaction

import (
	"sync"
	"time"

	"diamante/consensus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TransactionMetricsCollector provides comprehensive Prometheus metrics for transaction processing
type TransactionMetricsCollector struct {
	// Transaction counters
	transactionsProcessed *prometheus.CounterVec
	transactionsRejected  *prometheus.CounterVec
	transactionsConfirmed *prometheus.CounterVec
	transactionsFailed    *prometheus.CounterVec
	transactionsExpired   prometheus.Counter
	transactionsEvicted   prometheus.Counter

	// Processing durations
	processingDuration    *prometheus.HistogramVec
	validationDuration    *prometheus.HistogramVec
	signingDuration       prometheus.Histogram
	confirmationDuration  prometheus.Histogram
	smartContractDuration *prometheus.HistogramVec

	// Queue metrics
	queueWaitTime       *prometheus.HistogramVec
	poolSize            prometheus.Gauge
	pendingTransactions *prometheus.GaugeVec
	queueDepth          *prometheus.GaugeVec

	// Fee metrics
	feeCollected    *prometheus.CounterVec
	averageFee      prometheus.Gauge
	minFeeThreshold prometheus.Gauge
	feePerByte      *prometheus.HistogramVec

	// Rejection reason tracking
	rejectionReasons *prometheus.CounterVec

	// Gas metrics for smart contracts
	gasUsed          *prometheus.HistogramVec
	gasEstimated     *prometheus.HistogramVec
	gasLimitExceeded prometheus.Counter

	// Nonce tracking
	nonceGaps       prometheus.Counter
	nonceReuse      prometheus.Counter
	nonceSyncErrors prometheus.Counter

	// System metrics
	activeConnections prometheus.Gauge
	memoryUsage       prometheus.Gauge
	goroutineCount    prometheus.Gauge

	// Priority metrics
	priorityDistribution *prometheus.GaugeVec
	priorityQueueLatency *prometheus.HistogramVec

	// Batch processing metrics
	batchSize           prometheus.Histogram
	batchProcessingTime prometheus.Histogram
	batchSuccessRate    prometheus.Gauge

	// Internal state
	mu                    sync.RWMutex
	lastUpdateTime        time.Time
	rejectionReasonCounts map[string]uint64
}

// NewTransactionMetricsCollector creates a new metrics collector with Prometheus metrics
func NewTransactionMetricsCollector(namespace string) *TransactionMetricsCollector {
	if namespace == "" {
		namespace = "diamante"
	}

	subsystem := "transaction"

	return &TransactionMetricsCollector{
		// Transaction counters
		transactionsProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "processed_total",
				Help:      "Total number of transactions processed",
			},
			[]string{"type", "status"},
		),
		transactionsRejected: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "rejected_total",
				Help:      "Total number of transactions rejected",
			},
			[]string{"reason"},
		),
		transactionsConfirmed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "confirmed_total",
				Help:      "Total number of transactions confirmed",
			},
			[]string{"type"},
		),
		transactionsFailed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "failed_total",
				Help:      "Total number of transactions that failed",
			},
			[]string{"type", "reason"},
		),
		transactionsExpired: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "expired_total",
				Help:      "Total number of transactions that expired",
			},
		),
		transactionsEvicted: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "evicted_total",
				Help:      "Total number of transactions evicted from pool",
			},
		),

		// Processing durations
		processingDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "processing_duration_seconds",
				Help:      "Transaction processing duration in seconds",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"type", "status"},
		),
		validationDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "validation_duration_seconds",
				Help:      "Transaction validation duration in seconds",
				Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1},
			},
			[]string{"type"},
		),
		signingDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "signing_duration_seconds",
				Help:      "Transaction signing duration in seconds",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
			},
		),
		confirmationDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "confirmation_duration_seconds",
				Help:      "Transaction confirmation duration in seconds",
				Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
			},
		),
		smartContractDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "smart_contract_duration_seconds",
				Help:      "Smart contract execution duration in seconds",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"method", "contract_type"},
		),

		// Queue metrics
		queueWaitTime: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queue_wait_time_seconds",
				Help:      "Time transactions spend waiting in queue",
				Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300, 600},
			},
			[]string{"priority"},
		),
		poolSize: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "pool_size",
				Help:      "Current size of the transaction pool",
			},
		),
		pendingTransactions: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "pending_total",
				Help:      "Number of pending transactions by type",
			},
			[]string{"type"},
		),
		queueDepth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queue_depth",
				Help:      "Depth of transaction queues by priority",
			},
			[]string{"priority"},
		),

		// Fee metrics
		feeCollected: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "fee_collected_total",
				Help:      "Total transaction fees collected",
			},
			[]string{"currency"},
		),
		averageFee: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "average_fee",
				Help:      "Average transaction fee",
			},
		),
		minFeeThreshold: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "min_fee_threshold",
				Help:      "Current minimum fee threshold",
			},
		),
		feePerByte: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "fee_per_byte",
				Help:      "Transaction fee per byte distribution",
				Buckets:   []float64{0.001, 0.01, 0.1, 1, 10, 100, 1000},
			},
			[]string{"type"},
		),

		// Rejection tracking
		rejectionReasons: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "rejection_reasons_total",
				Help:      "Count of transaction rejections by reason",
			},
			[]string{"reason"},
		),

		// Gas metrics
		gasUsed: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "gas_used",
				Help:      "Gas used by smart contract transactions",
				Buckets:   []float64{21000, 50000, 100000, 200000, 500000, 1000000, 2000000, 5000000},
			},
			[]string{"contract_type", "method"},
		),
		gasEstimated: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "gas_estimated",
				Help:      "Gas estimated for smart contract transactions",
				Buckets:   []float64{21000, 50000, 100000, 200000, 500000, 1000000, 2000000, 5000000},
			},
			[]string{"contract_type", "method"},
		),
		gasLimitExceeded: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "gas_limit_exceeded_total",
				Help:      "Number of transactions that exceeded gas limit",
			},
		),

		// Nonce tracking
		nonceGaps: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "nonce_gaps_total",
				Help:      "Number of nonce gaps detected",
			},
		),
		nonceReuse: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "nonce_reuse_total",
				Help:      "Number of nonce reuse attempts",
			},
		),
		nonceSyncErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "nonce_sync_errors_total",
				Help:      "Number of nonce synchronization errors",
			},
		),

		// System metrics
		activeConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "active_connections",
				Help:      "Number of active connections",
			},
		),
		memoryUsage: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "memory_usage_bytes",
				Help:      "Memory usage in bytes",
			},
		),
		goroutineCount: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "goroutine_count",
				Help:      "Number of goroutines",
			},
		),

		// Priority metrics
		priorityDistribution: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "priority_distribution",
				Help:      "Distribution of transactions by priority",
			},
			[]string{"priority"},
		),
		priorityQueueLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "priority_queue_latency_seconds",
				Help:      "Latency for priority queue operations",
				Buckets:   []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01},
			},
			[]string{"operation"},
		),

		// Batch metrics
		batchSize: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "batch_size",
				Help:      "Size of transaction batches",
				Buckets:   []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000},
			},
		),
		batchProcessingTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "batch_processing_time_seconds",
				Help:      "Time to process transaction batches",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
		),
		batchSuccessRate: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "batch_success_rate",
				Help:      "Success rate of batch processing",
			},
		),

		rejectionReasonCounts: make(map[string]uint64),
		lastUpdateTime:        consensus.ConsensusNow(),
	}
}

// RecordTransactionProcessed records a processed transaction
func (mc *TransactionMetricsCollector) RecordTransactionProcessed(txType, status string, duration time.Duration) {
	mc.transactionsProcessed.WithLabelValues(txType, status).Inc()
	mc.processingDuration.WithLabelValues(txType, status).Observe(duration.Seconds())
}

// RecordTransactionRejected records a rejected transaction
func (mc *TransactionMetricsCollector) RecordTransactionRejected(reason string) {
	mc.transactionsRejected.WithLabelValues(reason).Inc()
	mc.rejectionReasons.WithLabelValues(reason).Inc()

	mc.mu.Lock()
	mc.rejectionReasonCounts[reason]++
	mc.mu.Unlock()
}

// RecordTransactionConfirmed records a confirmed transaction
func (mc *TransactionMetricsCollector) RecordTransactionConfirmed(txType string, duration time.Duration) {
	mc.transactionsConfirmed.WithLabelValues(txType).Inc()
	mc.confirmationDuration.Observe(duration.Seconds())
}

// RecordValidationDuration records validation duration
func (mc *TransactionMetricsCollector) RecordValidationDuration(txType string, duration time.Duration) {
	mc.validationDuration.WithLabelValues(txType).Observe(duration.Seconds())
}

// RecordSigningDuration records signing duration
func (mc *TransactionMetricsCollector) RecordSigningDuration(duration time.Duration) {
	mc.signingDuration.Observe(duration.Seconds())
}

// RecordQueueWaitTime records time spent in queue
func (mc *TransactionMetricsCollector) RecordQueueWaitTime(priority string, duration time.Duration) {
	mc.queueWaitTime.WithLabelValues(priority).Observe(duration.Seconds())
}

// UpdatePoolSize updates the current pool size
func (mc *TransactionMetricsCollector) UpdatePoolSize(size int) {
	mc.poolSize.Set(float64(size))
}

// UpdatePendingTransactions updates pending transaction counts
func (mc *TransactionMetricsCollector) UpdatePendingTransactions(txType string, count int) {
	mc.pendingTransactions.WithLabelValues(txType).Set(float64(count))
}

// RecordFeeCollected records collected fees
func (mc *TransactionMetricsCollector) RecordFeeCollected(amount float64, currency string) {
	mc.feeCollected.WithLabelValues(currency).Add(amount)
}

// UpdateAverageFee updates the average fee metric
func (mc *TransactionMetricsCollector) UpdateAverageFee(fee float64) {
	mc.averageFee.Set(fee)
}

// UpdateMinFeeThreshold updates the minimum fee threshold
func (mc *TransactionMetricsCollector) UpdateMinFeeThreshold(threshold float64) {
	mc.minFeeThreshold.Set(threshold)
}

// RecordFeePerByte records fee per byte for a transaction
func (mc *TransactionMetricsCollector) RecordFeePerByte(txType string, feePerByte float64) {
	mc.feePerByte.WithLabelValues(txType).Observe(feePerByte)
}

// RecordGasUsed records gas used by a smart contract transaction
func (mc *TransactionMetricsCollector) RecordGasUsed(contractType, method string, gasUsed uint64) {
	mc.gasUsed.WithLabelValues(contractType, method).Observe(float64(gasUsed))
}

// RecordGasEstimated records estimated gas for a smart contract transaction
func (mc *TransactionMetricsCollector) RecordGasEstimated(contractType, method string, gasEstimated uint64) {
	mc.gasEstimated.WithLabelValues(contractType, method).Observe(float64(gasEstimated))
}

// IncrementGasLimitExceeded increments gas limit exceeded counter
func (mc *TransactionMetricsCollector) IncrementGasLimitExceeded() {
	mc.gasLimitExceeded.Inc()
}

// IncrementNonceGaps increments nonce gap counter
func (mc *TransactionMetricsCollector) IncrementNonceGaps() {
	mc.nonceGaps.Inc()
}

// IncrementNonceReuse increments nonce reuse counter
func (mc *TransactionMetricsCollector) IncrementNonceReuse() {
	mc.nonceReuse.Inc()
}

// IncrementNonceSyncErrors increments nonce sync error counter
func (mc *TransactionMetricsCollector) IncrementNonceSyncErrors() {
	mc.nonceSyncErrors.Inc()
}

// UpdateSystemMetrics updates system-level metrics
func (mc *TransactionMetricsCollector) UpdateSystemMetrics(connections int, memoryBytes uint64, goroutines int) {
	mc.activeConnections.Set(float64(connections))
	mc.memoryUsage.Set(float64(memoryBytes))
	mc.goroutineCount.Set(float64(goroutines))
}

// UpdatePriorityDistribution updates transaction priority distribution
func (mc *TransactionMetricsCollector) UpdatePriorityDistribution(distribution map[string]int) {
	for priority, count := range distribution {
		mc.priorityDistribution.WithLabelValues(priority).Set(float64(count))
	}
}

// RecordPriorityQueueOperation records priority queue operation latency
func (mc *TransactionMetricsCollector) RecordPriorityQueueOperation(operation string, duration time.Duration) {
	mc.priorityQueueLatency.WithLabelValues(operation).Observe(duration.Seconds())
}

// RecordBatchProcessing records batch processing metrics
func (mc *TransactionMetricsCollector) RecordBatchProcessing(batchSize int, duration time.Duration, successRate float64) {
	mc.batchSize.Observe(float64(batchSize))
	mc.batchProcessingTime.Observe(duration.Seconds())
	mc.batchSuccessRate.Set(successRate)
}

// IncrementTransactionsExpired increments expired transaction counter
func (mc *TransactionMetricsCollector) IncrementTransactionsExpired() {
	mc.transactionsExpired.Inc()
}

// IncrementTransactionsEvicted increments evicted transaction counter
func (mc *TransactionMetricsCollector) IncrementTransactionsEvicted() {
	mc.transactionsEvicted.Inc()
}

// RecordSmartContractExecution records smart contract execution metrics
func (mc *TransactionMetricsCollector) RecordSmartContractExecution(method, contractType string, duration time.Duration) {
	mc.smartContractDuration.WithLabelValues(method, contractType).Observe(duration.Seconds())
}

// UpdateQueueDepth updates queue depth for a priority level
func (mc *TransactionMetricsCollector) UpdateQueueDepth(priority string, depth int) {
	mc.queueDepth.WithLabelValues(priority).Set(float64(depth))
}

// GetRejectionReasonStats returns rejection reason statistics
func (mc *TransactionMetricsCollector) GetRejectionReasonStats() map[string]uint64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := make(map[string]uint64)
	for reason, count := range mc.rejectionReasonCounts {
		stats[reason] = count
	}
	return stats
}

// Reset resets internal counters (for testing)
func (mc *TransactionMetricsCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.rejectionReasonCounts = make(map[string]uint64)
	mc.lastUpdateTime = consensus.ConsensusNow()
}
