// Package benchmarks provides transaction processing benchmarks
package benchmarks

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/transaction"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// TransactionBenchmark benchmarks transaction processing performance
type TransactionBenchmark struct {
	pool   *transaction.TypedPool
	config *TransactionBenchmarkConfig
	logger *logrus.Logger

	// Metrics
	submitted atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
	latencies []time.Duration
	mu        sync.Mutex
}

// TransactionBenchmarkConfig contains configuration for transaction benchmarks
type TransactionBenchmarkConfig struct {
	PoolSize         int     `json:"pool_size"`
	WorkerCount      int     `json:"worker_count"`
	TransactionSize  int     `json:"transaction_size"`
	BatchSize        int     `json:"batch_size"`
	SimulateFailures bool    `json:"simulate_failures"`
	FailureRate      float64 `json:"failure_rate"`
}

// NewTransactionBenchmark creates a new transaction benchmark
func NewTransactionBenchmark(config *TransactionBenchmarkConfig, logger *logrus.Logger) *TransactionBenchmark {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = &TransactionBenchmarkConfig{
			PoolSize:        10000,
			WorkerCount:     10,
			TransactionSize: 256,
			BatchSize:       100,
		}
	}

	return &TransactionBenchmark{
		config:    config,
		logger:    logger,
		latencies: make([]time.Duration, 0, config.PoolSize),
	}
}

// Name returns the benchmark name
func (b *TransactionBenchmark) Name() string {
	return "transaction_processing"
}

// Description returns the benchmark description
func (b *TransactionBenchmark) Description() string {
	return "Benchmarks transaction pool processing performance"
}

// Setup prepares the benchmark
func (b *TransactionBenchmark) Setup(ctx context.Context) error {
	// Create transaction pool
	maxPoolSize := b.config.PoolSize
	txTimeout := 5 * time.Minute
	minFee := 0.001

	pool := transaction.NewTypedPool(maxPoolSize, txTimeout, minFee, b.logger)

	b.pool = pool

	// Pool is ready to use (no Start method needed)

	return nil
}

// Run executes the benchmark
func (b *TransactionBenchmark) Run(ctx context.Context, iterations int) (*BenchmarkMetrics, error) {
	startTime := common.ConsensusNow()

	// Reset metrics
	b.submitted.Store(0)
	b.processed.Store(0)
	b.failed.Store(0)
	b.latencies = b.latencies[:0]

	// Create workers
	var wg sync.WaitGroup
	workerCount := b.config.WorkerCount
	txPerWorker := iterations / workerCount

	// Channel for collecting latencies
	latencyChan := make(chan time.Duration, iterations)

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			b.runWorker(ctx, workerID, txPerWorker, latencyChan)
		}(i)
	}

	// Collect latencies
	go func() {
		for latency := range latencyChan {
			b.mu.Lock()
			b.latencies = append(b.latencies, latency)
			b.mu.Unlock()
		}
	}()

	// Wait for completion
	wg.Wait()
	close(latencyChan)

	// Calculate metrics
	duration := time.Since(startTime)
	metrics := b.calculateMetrics(duration)

	return metrics, nil
}

// runWorker runs a benchmark worker
func (b *TransactionBenchmark) runWorker(ctx context.Context, workerID int, count int, latencyChan chan<- time.Duration) {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			// Create transaction
			tx := b.createTransaction(workerID, i)

			// Measure submission latency
			submitStart := common.ConsensusNow()
			err := b.pool.AddTransaction(tx)
			submitLatency := time.Since(submitStart)

			if err != nil {
				b.failed.Add(1)
				b.logger.WithError(err).Debug("Failed to submit transaction")
				continue
			}

			b.submitted.Add(1)
			latencyChan <- submitLatency

			// Simulate processing
			if b.config.BatchSize > 0 && (i+1)%b.config.BatchSize == 0 {
				// Process batch
				b.processBatch()
			}
		}
	}

	// Process remaining transactions
	b.processBatch()
}

// createTransaction creates a test transaction
func (b *TransactionBenchmark) createTransaction(workerID, index int) *types.TypedTransaction {
	return &types.TypedTransaction{
		ID:        fmt.Sprintf("tx-%d-%d-%d", workerID, index, common.ConsensusNow().UnixNano()),
		Type:      types.TransactionTypeTransfer,
		From:      fmt.Sprintf("sender-%d", workerID),
		To:        fmt.Sprintf("recipient-%d", index%100),
		Value:     uint64(1000 + index),
		GasLimit:  21000,
		GasPrice:  1000000000,
		Nonce:     uint64(index),
		Timestamp: common.ConsensusNow().Unix(),
		Data: &types.TypedTransactionData{
			RawData: make([]byte, b.config.TransactionSize),
		},
		Priority: b.calculatePriority(index),
	}
}

// calculatePriority calculates transaction priority
func (b *TransactionBenchmark) calculatePriority(index int) types.TransactionPriority {
	switch index % 10 {
	case 0:
		return types.TransactionPriorityHigh
	case 1, 2:
		return types.TransactionPriorityNormal
	default:
		return types.TransactionPriorityLow
	}
}

// processBatch simulates batch processing
func (b *TransactionBenchmark) processBatch() {
	// Get pending transactions
	pending := b.pool.GetPendingTransactions(b.config.BatchSize)

	for _, tx := range pending {
		// Simulate processing
		if b.shouldFail() {
			b.failed.Add(1)
		} else {
			b.processed.Add(1)
		}

		// Remove from pool
		b.pool.RemoveTransaction(tx.ID)
	}
}

// shouldFail determines if a transaction should fail
func (b *TransactionBenchmark) shouldFail() bool {
	if !b.config.SimulateFailures {
		return false
	}

	// Simple failure simulation
	return common.ConsensusNow().UnixNano()%100 < int64(b.config.FailureRate*100)
}

// calculateMetrics calculates benchmark metrics
func (b *TransactionBenchmark) calculateMetrics(duration time.Duration) *BenchmarkMetrics {
	submitted := b.submitted.Load()
	processed := b.processed.Load()
	failed := b.failed.Load()

	tps := float64(submitted) / duration.Seconds()

	// Calculate latency metrics
	latencyMetrics := b.calculateLatencyMetrics()

	// Calculate throughput
	throughput := &ThroughputMetrics{
		MessagesPerSecond: float64(processed) / duration.Seconds(),
		BytesPerSecond:    float64(processed*int64(b.config.TransactionSize)) / duration.Seconds(),
	}

	// Calculate error metrics
	errorMetrics := &ErrorMetrics{
		TotalErrors:  failed,
		ErrorRate:    float64(failed) / float64(submitted),
		ErrorsByType: map[string]int64{"submission_failure": failed},
	}

	// Get resource metrics
	resourceMetrics := b.getResourceMetrics()

	return &BenchmarkMetrics{
		TotalOperations: submitted,
		TotalDuration:   duration,
		TPS:             tps,
		Latency:         latencyMetrics,
		Throughput:      throughput,
		Resources:       resourceMetrics,
		Errors:          errorMetrics,
		Custom: map[string]float64{
			"processed": float64(processed),
			// Pool size metrics not available in TypedPool
			// "pool_size":         float64(b.pool.Size()),
			// "pending_size":      float64(b.pool.PendingSize()),
			"submission_rate": float64(submitted) / duration.Seconds(),
			"processing_rate": float64(processed) / duration.Seconds(),
		},
	}
}

// calculateLatencyMetrics calculates latency statistics
func (b *TransactionBenchmark) calculateLatencyMetrics() *LatencyMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.latencies) == 0 {
		return &LatencyMetrics{}
	}

	// Sort latencies
	sortedLatencies := make([]time.Duration, len(b.latencies))
	copy(sortedLatencies, b.latencies)
	sortDurations(sortedLatencies)

	// Calculate percentiles
	count := len(sortedLatencies)

	return &LatencyMetrics{
		Min:    sortedLatencies[0],
		Max:    sortedLatencies[count-1],
		Mean:   calculateMean(sortedLatencies),
		Median: sortedLatencies[count/2],
		P90:    sortedLatencies[int(float64(count)*0.90)],
		P95:    sortedLatencies[int(float64(count)*0.95)],
		P99:    sortedLatencies[int(float64(count)*0.99)],
		StdDev: calculateStdDev(sortedLatencies),
	}
}

// getResourceMetrics gets current resource usage
func (b *TransactionBenchmark) getResourceMetrics() *ResourceMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return &ResourceMetrics{
		MemoryUsageMB:  float64(m.Sys) / 1024 / 1024,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
		GCCount:        m.NumGC,
		GCPauseTotal:   time.Duration(m.PauseTotalNs),
	}
}

// Cleanup cleans up after the benchmark
func (b *TransactionBenchmark) Cleanup(ctx context.Context) error {
	// TypedPool doesn't have a Stop method - it cleans up automatically
	return nil
}

// Validate validates the benchmark results
func (b *TransactionBenchmark) Validate(metrics *BenchmarkMetrics) error {
	if metrics.TotalOperations == 0 {
		return fmt.Errorf("no transactions were submitted")
	}

	if metrics.Errors != nil && metrics.Errors.ErrorRate > 0.1 {
		return fmt.Errorf("error rate too high: %.2f%%", metrics.Errors.ErrorRate*100)
	}

	successRate := metrics.Custom["processed"] / float64(metrics.TotalOperations)
	if successRate < 0.9 {
		return fmt.Errorf("success rate too low: %.2f%%", successRate*100)
	}

	return nil
}

// Helper functions for latency calculations

func sortDurations(durations []time.Duration) {
	// Simple sort implementation
	for i := 0; i < len(durations); i++ {
		for j := i + 1; j < len(durations); j++ {
			if durations[i] > durations[j] {
				durations[i], durations[j] = durations[j], durations[i]
			}
		}
	}
}

func calculateMean(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	return sum / time.Duration(len(durations))
}

func calculateStdDev(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	mean := calculateMean(durations)
	var sumSquares float64

	for _, d := range durations {
		diff := float64(d - mean)
		sumSquares += diff * diff
	}

	variance := sumSquares / float64(len(durations))
	return time.Duration(math.Sqrt(variance))
}
