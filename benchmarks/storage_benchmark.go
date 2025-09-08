// Package benchmarks provides storage performance benchmarks
package benchmarks

import (
	"context"
	"diamante/common"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/consensus"
	"diamante/storage"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// StorageBenchmark benchmarks storage performance
type StorageBenchmark struct {
	adapter storage.LedgerStore
	config  *StorageBenchmarkConfig
	logger  *logrus.Logger

	// Metrics
	writes         atomic.Int64
	reads          atomic.Int64
	deletes        atomic.Int64
	queries        atomic.Int64
	writeLatencies []time.Duration
	readLatencies  []time.Duration
	queryLatencies []time.Duration
	mu             sync.Mutex
}

// StorageBenchmarkConfig contains configuration for storage benchmarks
type StorageBenchmarkConfig struct {
	StorageType     string  `json:"storage_type"` // mongodb, lmdb, memory
	DataSize        int     `json:"data_size"`    // Size of each data entry
	KeyPrefix       string  `json:"key_prefix"`
	ReadWriteRatio  float64 `json:"read_write_ratio"` // Ratio of reads to writes
	QueryComplexity int     `json:"query_complexity"`
	BatchSize       int     `json:"batch_size"`
	ConcurrentOps   int     `json:"concurrent_ops"`
}

// NewStorageBenchmark creates a new storage benchmark
func NewStorageBenchmark(config *StorageBenchmarkConfig, logger *logrus.Logger) *StorageBenchmark {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = &StorageBenchmarkConfig{
			StorageType:     "memory",
			DataSize:        1024, // 1KB
			KeyPrefix:       "bench",
			ReadWriteRatio:  0.8, // 80% reads, 20% writes
			QueryComplexity: 10,
			BatchSize:       100,
			ConcurrentOps:   10,
		}
	}

	return &StorageBenchmark{
		config:         config,
		logger:         logger,
		writeLatencies: make([]time.Duration, 0, 10000),
		readLatencies:  make([]time.Duration, 0, 10000),
		queryLatencies: make([]time.Duration, 0, 10000),
	}
}

// Name returns the benchmark name
func (b *StorageBenchmark) Name() string {
	return fmt.Sprintf("storage_%s_performance", b.config.StorageType)
}

// Description returns the benchmark description
func (b *StorageBenchmark) Description() string {
	return fmt.Sprintf("Benchmarks %s storage adapter performance", b.config.StorageType)
}

// Setup prepares the benchmark
func (b *StorageBenchmark) Setup(ctx context.Context) error {
	// Create storage adapter based on type
	switch b.config.StorageType {
	case "mongodb":
		// MongoDB adapter needs to implement LedgerStore interface
		// adapter, err = storage.NewTypedMongoAdapter(
		//	"mongodb://localhost:27017",
		//	"diamante_bench",
		//	b.logger)
		return fmt.Errorf("mongodb adapter interface mismatch - needs BatchWrite method")
	case "memory":
		// Memory adapter needs to be implemented or interface corrected
		// adapter = storage.NewMemoryAdapter()
		return fmt.Errorf("memory adapter not available")
	case "lmdb":
		// TODO: Implement LMDB adapter for benchmarking
		return fmt.Errorf("lmdb adapter not yet implemented for benchmarks")
	default:
		return fmt.Errorf("unsupported storage type: %s", b.config.StorageType)
	}

	// The following code is unreachable until adapters are implemented
	// // Adapter is ready for use
	// b.adapter = adapter
	//
	// // LedgerStore doesn't have Initialize method
	// // Need to use Open() instead
	//
	// // Pre-populate some data for read benchmarks
	// if err := b.prepopulateData(ctx); err != nil {
	// 	return fmt.Errorf("failed to prepopulate data: %w", err)
	// }
	//
	// return nil
}

// prepopulateData prepopulates storage with test data
func (b *StorageBenchmark) prepopulateData(ctx context.Context) error {
	prepopCount := 1000

	for i := 0; i < prepopCount; i++ {
		_ = fmt.Sprintf("%s:prepop:%d", b.config.KeyPrefix, i)
		_ = b.generateTestData(i)

		// Use SaveTransaction for benchmark data
		tx := &common.Transaction{
			ID:        fmt.Sprintf("%s:prepop:%d", b.config.KeyPrefix, i),
			Sender:    "benchmark_sender",
			Receiver:  "benchmark_receiver",
			Amount:    float64(i),
			Timestamp: consensus.ConsensusNow().Unix(),
			Data:      b.generateTestData(i).Bytes(),
		}

		if err := b.adapter.SaveTransaction(tx, i); err != nil {
			return fmt.Errorf("failed to save transaction: %w", err)
		}
	}

	b.logger.WithField("count", prepopCount).Debug("Prepopulated test data")
	return nil
}

// Run executes the benchmark
func (b *StorageBenchmark) Run(ctx context.Context, iterations int) (*BenchmarkMetrics, error) {
	startTime := common.ConsensusNow()

	// Reset metrics
	b.writes.Store(0)
	b.reads.Store(0)
	b.deletes.Store(0)
	b.queries.Store(0)
	b.writeLatencies = b.writeLatencies[:0]
	b.readLatencies = b.readLatencies[:0]
	b.queryLatencies = b.queryLatencies[:0]

	// Create workers
	var wg sync.WaitGroup
	workerCount := b.config.ConcurrentOps
	opsPerWorker := iterations / workerCount

	// Channels for collecting latencies
	writeLatencyChan := make(chan time.Duration, iterations)
	readLatencyChan := make(chan time.Duration, iterations)
	queryLatencyChan := make(chan time.Duration, iterations)

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			b.runWorker(ctx, workerID, opsPerWorker, writeLatencyChan, readLatencyChan, queryLatencyChan)
		}(i)
	}

	// Collect latencies
	go func() {
		for {
			select {
			case latency := <-writeLatencyChan:
				b.mu.Lock()
				b.writeLatencies = append(b.writeLatencies, latency)
				b.mu.Unlock()
			case latency := <-readLatencyChan:
				b.mu.Lock()
				b.readLatencies = append(b.readLatencies, latency)
				b.mu.Unlock()
			case latency := <-queryLatencyChan:
				b.mu.Lock()
				b.queryLatencies = append(b.queryLatencies, latency)
				b.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Calculate metrics
	duration := time.Since(startTime)
	metrics := b.calculateMetrics(duration)

	return metrics, nil
}

// runWorker runs a benchmark worker
func (b *StorageBenchmark) runWorker(ctx context.Context, workerID, count int, writeLatencyChan, readLatencyChan, queryLatencyChan chan<- time.Duration) {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			// Determine operation type based on read/write ratio
			if rand.Float64() < b.config.ReadWriteRatio {
				b.performRead(workerID, i, readLatencyChan)
			} else {
				b.performWrite(workerID, i, writeLatencyChan)
			}

			// Occasionally perform queries and deletes
			if i%10 == 0 {
				b.performQuery(workerID, i, queryLatencyChan)
			}

			if i%100 == 0 {
				b.performDelete(workerID, i)
			}
		}
	}
}

// performWrite performs a write operation
func (b *StorageBenchmark) performWrite(workerID, index int, latencyChan chan<- time.Duration) {
	key := fmt.Sprintf("%s:w%d:i%d", b.config.KeyPrefix, workerID, index)

	start := common.ConsensusNow()
	// Use SaveTransaction for write operations
	tx := &common.Transaction{
		ID:        key,
		Sender:    fmt.Sprintf("worker_%d", workerID),
		Receiver:  "benchmark_receiver",
		Amount:    float64(index),
		Timestamp: start.Unix(),
		Data:      b.generateTestData(index).Bytes(),
	}
	err := b.adapter.SaveTransaction(tx, index)
	latency := time.Since(start)

	if err != nil {
		b.logger.WithError(err).Debug("Write failed")
		return
	}

	b.writes.Add(1)
	latencyChan <- latency
}

// performRead performs a read operation
func (b *StorageBenchmark) performRead(workerID, index int, latencyChan chan<- time.Duration) {
	// Read either prepopulated data or recently written data
	var key string
	if rand.Float64() < 0.5 {
		// Read prepopulated data
		key = fmt.Sprintf("%s:prepop:%d", b.config.KeyPrefix, rand.Intn(1000))
	} else {
		// Read recently written data
		key = fmt.Sprintf("%s:w%d:i%d", b.config.KeyPrefix, rand.Intn(b.config.ConcurrentOps), rand.Intn(index+1))
	}

	start := common.ConsensusNow()
	// Use GetTransaction for read operations
	tx, err := b.adapter.GetTransaction(key)
	if tx != nil {
		_ = tx.Data // Access the data to simulate usage
	}
	latency := time.Since(start)

	if err != nil && err.Error() != "key not found" {
		b.logger.WithError(err).Debug("Read failed")
		return
	}

	b.reads.Add(1)
	latencyChan <- latency
}

// performQuery performs a query operation
func (b *StorageBenchmark) performQuery(workerID, index int, latencyChan chan<- time.Duration) {
	// Create query filter
	_ = &types.StorageQuery{
		Namespace: fmt.Sprintf("%s:w%d", b.config.KeyPrefix, workerID),
		Limit:     uint32(b.config.QueryComplexity),
	}

	start := common.ConsensusNow()
	// Use GetTransactionsByAddress for query operations
	addr := fmt.Sprintf("worker_%d", workerID)
	txs, err := b.adapter.GetTransactionsByAddress(addr, b.config.QueryComplexity, 0)
	var results []interface{}
	for _, tx := range txs {
		results = append(results, tx)
	}
	latency := time.Since(start)

	if err != nil {
		b.logger.WithError(err).Debug("Query failed")
		return
	}

	b.queries.Add(1)
	latencyChan <- latency

	// Validate results
	if len(results) == 0 {
		b.logger.Debug("Query returned no results")
	}
}

// performDelete performs a delete operation
func (b *StorageBenchmark) performDelete(workerID, index int) {
	// Create storage key for deletion
	// key := &types.StorageKey{
	// 	Namespace: "benchmark",
	// 	Category:  "test",
	// 	ID:        fmt.Sprintf("w%d:i%d", workerID, index-50),
	// 	Version:   1,
	// }

	// LedgerStore doesn't have SetState method - skip delete operation for now
	// TODO: Implement proper delete functionality for benchmarks
	// err := b.adapter.SetState([]byte(key.ID), nil)
	// if err != nil && err.Error() != "key not found" {
	// 	b.logger.WithError(err).Debug("Delete failed")
	// 	return
	// }

	b.deletes.Add(1)
}

// generateTestData generates test data for storage
func (b *StorageBenchmark) generateTestData(index int) *types.StorageValue {
	data := make([]byte, b.config.DataSize)
	rand.Read(data)

	return &types.StorageValue{
		Key: &types.StorageKey{
			Namespace: "benchmark",
			Category:  "test",
			ID:        fmt.Sprintf("key-%d", index),
			Version:   uint64(index),
		},
		Data:      data,
		Timestamp: consensus.ConsensusNow().Unix(),
		Metadata:  types.NewMetadata("benchmark"),
	}
}

// calculateMetrics calculates benchmark metrics
func (b *StorageBenchmark) calculateMetrics(duration time.Duration) *BenchmarkMetrics {
	writes := b.writes.Load()
	reads := b.reads.Load()
	deletes := b.deletes.Load()
	queries := b.queries.Load()

	totalOps := writes + reads + deletes + queries
	opsPerSecond := float64(totalOps) / duration.Seconds()

	// Calculate latency metrics
	writeLatency := b.calculateLatencyMetrics(b.writeLatencies)
	readLatency := b.calculateLatencyMetrics(b.readLatencies)
	queryLatency := b.calculateLatencyMetrics(b.queryLatencies)

	// Combine latencies for overall metric
	allLatencies := make([]time.Duration, 0)
	allLatencies = append(allLatencies, b.writeLatencies...)
	allLatencies = append(allLatencies, b.readLatencies...)
	overallLatency := b.calculateLatencyMetrics(allLatencies)

	// Calculate throughput
	throughput := &ThroughputMetrics{
		MessagesPerSecond: opsPerSecond,
		BytesPerSecond:    float64(writes*int64(b.config.DataSize)) / duration.Seconds(),
	}

	// Get resource metrics
	resourceMetrics := b.getResourceMetrics()

	return &BenchmarkMetrics{
		TotalOperations: totalOps,
		TotalDuration:   duration,
		TPS:             opsPerSecond,
		Latency:         overallLatency,
		Throughput:      throughput,
		Resources:       resourceMetrics,
		Custom: map[string]float64{
			"writes":            float64(writes),
			"reads":             float64(reads),
			"deletes":           float64(deletes),
			"queries":           float64(queries),
			"write_ops_per_sec": float64(writes) / duration.Seconds(),
			"read_ops_per_sec":  float64(reads) / duration.Seconds(),
			"avg_write_latency": float64(writeLatency.Mean),
			"avg_read_latency":  float64(readLatency.Mean),
			"avg_query_latency": float64(queryLatency.Mean),
			"p99_write_latency": float64(writeLatency.P99),
			"p99_read_latency":  float64(readLatency.P99),
		},
	}
}

// calculateLatencyMetrics calculates latency statistics
func (b *StorageBenchmark) calculateLatencyMetrics(latencies []time.Duration) *LatencyMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(latencies) == 0 {
		return &LatencyMetrics{}
	}

	// Make a copy and sort
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sortDurations(sorted)

	count := len(sorted)

	return &LatencyMetrics{
		Min:    sorted[0],
		Max:    sorted[count-1],
		Mean:   calculateMean(sorted),
		Median: sorted[count/2],
		P90:    sorted[int(float64(count)*0.90)],
		P95:    sorted[int(float64(count)*0.95)],
		P99:    sorted[int(float64(count)*0.99)],
		StdDev: calculateStdDev(sorted),
	}
}

// getResourceMetrics gets current resource usage
func (b *StorageBenchmark) getResourceMetrics() *ResourceMetrics {
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
func (b *StorageBenchmark) Cleanup(ctx context.Context) error {
	if b.adapter != nil {
		// Clean up test data - NOTE: Query and Delete methods not available in LedgerStore interface
		// This would need to be implemented with available LedgerStore methods
		b.logger.Info("Cleanup would remove benchmark data - not implemented")

		// Close adapter
		return b.adapter.Close()
	}
	return nil
}

// Validate validates the benchmark results
func (b *StorageBenchmark) Validate(metrics *BenchmarkMetrics) error {
	if metrics.TotalOperations == 0 {
		return fmt.Errorf("no operations were performed")
	}

	// Check if read/write ratio is approximately correct
	actualReadRatio := metrics.Custom["reads"] / (metrics.Custom["reads"] + metrics.Custom["writes"])
	expectedRatio := b.config.ReadWriteRatio

	if actualReadRatio < expectedRatio-0.1 || actualReadRatio > expectedRatio+0.1 {
		b.logger.WithFields(logrus.Fields{
			"expected": expectedRatio,
			"actual":   actualReadRatio,
		}).Warn("Read/write ratio deviated from expected")
	}

	// Check latencies
	if metrics.Latency.P99 > 100*time.Millisecond {
		return fmt.Errorf("P99 latency too high: %v", metrics.Latency.P99)
	}

	return nil
}
