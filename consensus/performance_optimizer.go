// consensus/performance_optimizer.go
package consensus

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/consensus/types"
	dtypes "diamante/types"
)

// Local types to avoid import cycle
type OptimizerBlock struct {
	Number       uint64                 `json:"number"`
	Hash         string                 `json:"hash"`
	PreviousHash string                 `json:"previous_hash"`
	Timestamp    int64                  `json:"timestamp"`
	Transactions []OptimizerTransaction `json:"transactions"`
	Proposer     string                 `json:"proposer"`
}

type OptimizerTransaction struct {
	ID              string                      `json:"id"`
	Sender          string                      `json:"sender"`
	Receiver        string                      `json:"receiver"`
	Amount          uint64                      `json:"amount"`
	Fee             uint64                      `json:"fee"`
	Data            []byte                      `json:"data"`
	Timestamp       int64                       `json:"timestamp"`
	SmartContractID string                      `json:"smart_contract_id,omitempty"`
	Metadata        *dtypes.TransactionMetadata `json:"metadata,omitempty"`
}

// Local error functions to avoid import cycle
func OptimizerConsensusError(err error, msg string) error {
	if err == nil {
		return fmt.Errorf("consensus error: %s", msg)
	}
	return fmt.Errorf("consensus error: %s: %w", msg, err)
}

func OptimizerWrapError(err error, msg string) error {
	return fmt.Errorf("%s: %w", msg, err)
}

// PerformanceOptimizer provides advanced performance optimizations for consensus
type PerformanceOptimizer struct {
	// Configuration
	config *OptimizerConfig
	logger *hybridConsensusLogger

	// Memory management
	eventPool     sync.Pool
	blockPool     sync.Pool
	validatorPool sync.Pool

	// Concurrency optimization
	workerPool *WorkerPool
	semaphore  chan struct{}

	// Caching
	eventCache     *LRUCache
	blockCache     *LRUCache
	validatorCache *LRUCache

	// Metrics
	metrics *OptimizerMetrics

	// State
	running int32
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Adaptive optimization
	adaptiveManager *AdaptiveOptimizer

	// Memory pressure monitoring
	memoryMonitor *MemoryMonitor
}

// OptimizerConfig holds configuration for the performance optimizer
type OptimizerConfig struct {
	// Memory pools
	EventPoolSize     int
	BlockPoolSize     int
	ValidatorPoolSize int

	// Caching
	EventCacheSize     int
	BlockCacheSize     int
	ValidatorCacheSize int
	CacheTTL           time.Duration

	// Concurrency
	MaxWorkers       int
	MaxConcurrentOps int
	WorkerQueueSize  int

	// Adaptive optimization
	EnableAdaptive     bool
	AdaptationInterval time.Duration

	// Memory monitoring
	EnableMemoryMonitor bool
	MemoryThreshold     float64 // Percentage of available memory
	GCThreshold         float64 // When to trigger GC
}

// DefaultOptimizerConfig returns production-optimized configuration
func DefaultOptimizerConfig() *OptimizerConfig {
	return &OptimizerConfig{
		EventPoolSize:       1000,
		BlockPoolSize:       100,
		ValidatorPoolSize:   50,
		EventCacheSize:      5000,
		BlockCacheSize:      1000,
		ValidatorCacheSize:  100,
		CacheTTL:            5 * time.Minute,
		MaxWorkers:          runtime.NumCPU() * 2,
		MaxConcurrentOps:    1000,
		WorkerQueueSize:     10000,
		EnableAdaptive:      true,
		AdaptationInterval:  30 * time.Second,
		EnableMemoryMonitor: true,
		MemoryThreshold:     0.8,
		GCThreshold:         0.9,
	}
}

// OptimizerMetrics tracks performance optimization metrics
type OptimizerMetrics struct {
	// Pool metrics
	EventPoolHits   int64
	EventPoolMisses int64
	BlockPoolHits   int64
	BlockPoolMisses int64

	// Cache metrics
	EventCacheHits   int64
	EventCacheMisses int64
	BlockCacheHits   int64
	BlockCacheMisses int64

	// Worker metrics
	WorkersActive   int64
	TasksQueued     int64
	TasksCompleted  int64
	AvgTaskDuration time.Duration

	// Memory metrics
	MemoryUsage int64
	GCCount     int64
	GCDuration  time.Duration

	// Adaptation metrics
	AdaptationCount int64
	LastAdaptation  time.Time
}

// NewPerformanceOptimizer creates a new performance optimizer
func NewPerformanceOptimizer(config *OptimizerConfig, logger *hybridConsensusLogger) *PerformanceOptimizer {
	if config == nil {
		config = DefaultOptimizerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	po := &PerformanceOptimizer{
		config:    config,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		semaphore: make(chan struct{}, config.MaxConcurrentOps),
		metrics:   &OptimizerMetrics{},
	}

	// Initialize memory pools
	po.initializeMemoryPools()

	// Initialize caches
	po.initializeCaches()

	// Initialize worker pool
	po.workerPool = NewWorkerPool(config.MaxWorkers, config.WorkerQueueSize, logger)

	// Initialize adaptive manager
	if config.EnableAdaptive {
		po.adaptiveManager = NewAdaptiveOptimizer(po, logger)
	}

	// Initialize memory monitor
	if config.EnableMemoryMonitor {
		po.memoryMonitor = NewMemoryMonitor(config.MemoryThreshold, config.GCThreshold, logger)
	}

	return po
}

// initializeMemoryPools sets up object pools for memory optimization
func (po *PerformanceOptimizer) initializeMemoryPools() {
	// Event pool
	po.eventPool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&po.metrics.EventPoolMisses, 1)
			return &types.Event{}
		},
	}

	// Block pool
	po.blockPool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&po.metrics.BlockPoolMisses, 1)
			return &OptimizerBlock{}
		},
	}

	// Validator pool
	po.validatorPool = sync.Pool{
		New: func() interface{} {
			return &types.Validator{}
		},
	}

	// Pre-populate pools
	for i := 0; i < po.config.EventPoolSize; i++ {
		po.eventPool.Put(&types.Event{})
	}
	for i := 0; i < po.config.BlockPoolSize; i++ {
		po.blockPool.Put(&OptimizerBlock{})
	}
	for i := 0; i < po.config.ValidatorPoolSize; i++ {
		po.validatorPool.Put(&types.Validator{})
	}
}

// initializeCaches sets up LRU caches for frequently accessed data
func (po *PerformanceOptimizer) initializeCaches() {
	po.eventCache = NewLRUCache(po.config.EventCacheSize)
	po.blockCache = NewLRUCache(po.config.BlockCacheSize)
	po.validatorCache = NewLRUCache(po.config.ValidatorCacheSize)
}

// Start begins the performance optimizer
func (po *PerformanceOptimizer) Start() error {
	if !atomic.CompareAndSwapInt32(&po.running, 0, 1) {
		return OptimizerConsensusError(nil, "performance optimizer already running")
	}

	// Start worker pool
	if err := po.workerPool.Start(); err != nil {
		return OptimizerWrapError(err, "failed to start worker pool")
	}

	// Start adaptive manager
	if po.adaptiveManager != nil {
		po.wg.Add(1)
		go po.runAdaptiveOptimization()
	}

	// Start memory monitor
	if po.memoryMonitor != nil {
		po.wg.Add(1)
		go po.runMemoryMonitoring()
	}

	po.logger.Info("Performance optimizer started")
	return nil
}

// Stop shuts down the performance optimizer
func (po *PerformanceOptimizer) Stop() error {
	if !atomic.CompareAndSwapInt32(&po.running, 1, 0) {
		return nil // Already stopped
	}

	po.cancel()

	// Stop worker pool
	if err := po.workerPool.Stop(); err != nil {
		po.logger.Error("Error stopping worker pool", LogKeyValue{Key: "error", Value: err.Error()})
	}

	// Wait for goroutines
	po.wg.Wait()

	po.logger.Info("Performance optimizer stopped")
	return nil
}

// GetEvent retrieves an event from the pool
func (po *PerformanceOptimizer) GetEvent() *types.Event {
	event := po.eventPool.Get().(*types.Event)
	atomic.AddInt64(&po.metrics.EventPoolHits, 1)

	// Reset event for reuse
	*event = types.Event{}
	return event
}

// PutEvent returns an event to the pool
func (po *PerformanceOptimizer) PutEvent(event *types.Event) {
	if event != nil {
		po.eventPool.Put(event)
	}
}

// GetBlock retrieves a block from the pool
func (po *PerformanceOptimizer) GetBlock() *OptimizerBlock {
	block := po.blockPool.Get().(*OptimizerBlock)
	atomic.AddInt64(&po.metrics.BlockPoolHits, 1)

	// Reset block for reuse
	*block = OptimizerBlock{}
	return block
}

// PutBlock returns a block to the pool
func (po *PerformanceOptimizer) PutBlock(block *OptimizerBlock) {
	if block != nil {
		po.blockPool.Put(block)
	}
}

// GetValidator retrieves a validator from the pool
func (po *PerformanceOptimizer) GetValidator() *types.Validator {
	validator := po.validatorPool.Get().(*types.Validator)

	// Reset validator for reuse
	*validator = types.Validator{}
	return validator
}

// PutValidator returns a validator to the pool
func (po *PerformanceOptimizer) PutValidator(validator *types.Validator) {
	if validator != nil {
		po.validatorPool.Put(validator)
	}
}

// CacheEvent stores an event in cache
func (po *PerformanceOptimizer) CacheEvent(eventID [32]byte, event *types.Event) {
	po.eventCache.Put(fmt.Sprintf("%x", eventID), event)
}

// GetCachedEvent retrieves an event from cache
func (po *PerformanceOptimizer) GetCachedEvent(eventID [32]byte) (*types.Event, bool) {
	value, found := po.eventCache.Get(fmt.Sprintf("%x", eventID))
	if found {
		atomic.AddInt64(&po.metrics.EventCacheHits, 1)
		return value.(*types.Event), true
	}
	atomic.AddInt64(&po.metrics.EventCacheMisses, 1)
	return nil, false
}

// CacheBlock stores a block in cache
func (po *PerformanceOptimizer) CacheBlock(blockNumber uint64, block *OptimizerBlock) {
	po.blockCache.Put(fmt.Sprintf("%d", blockNumber), block)
}

// GetCachedBlock retrieves a block from cache
func (po *PerformanceOptimizer) GetCachedBlock(blockNumber uint64) (*OptimizerBlock, bool) {
	value, found := po.blockCache.Get(fmt.Sprintf("%d", blockNumber))
	if found {
		atomic.AddInt64(&po.metrics.BlockCacheHits, 1)
		return value.(*OptimizerBlock), true
	}
	atomic.AddInt64(&po.metrics.BlockCacheMisses, 1)
	return nil, false
}

// SubmitTask submits a task to the worker pool
func (po *PerformanceOptimizer) SubmitTask(task Task) error {
	select {
	case po.semaphore <- struct{}{}:
		atomic.AddInt64(&po.metrics.TasksQueued, 1)

		wrappedTask := func() {
			defer func() { <-po.semaphore }()

			start := ConsensusNow()
			task.Execute()
			duration := ConsensusSince(start)

			atomic.AddInt64(&po.metrics.TasksCompleted, 1)
			po.updateTaskDuration(duration)
		}

		return po.workerPool.Submit(wrappedTask)

	case <-po.ctx.Done():
		return OptimizerConsensusError(nil, "optimizer shutting down")
	}
}

// updateTaskDuration updates the average task duration
func (po *PerformanceOptimizer) updateTaskDuration(duration time.Duration) {
	// Simple moving average for task duration
	currentAvg := atomic.LoadInt64((*int64)(&po.metrics.AvgTaskDuration))
	newAvg := (time.Duration(currentAvg) + duration) / 2
	atomic.StoreInt64((*int64)(&po.metrics.AvgTaskDuration), int64(newAvg))
}

// runAdaptiveOptimization runs the adaptive optimization loop
func (po *PerformanceOptimizer) runAdaptiveOptimization() {
	defer po.wg.Done()

	ticker := time.NewTicker(po.config.AdaptationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if po.adaptiveManager != nil {
				po.adaptiveManager.Optimize()
				atomic.AddInt64(&po.metrics.AdaptationCount, 1)
				po.metrics.LastAdaptation = ConsensusNow()
			}

		case <-po.ctx.Done():
			return
		}
	}
}

// runMemoryMonitoring runs the memory monitoring loop
func (po *PerformanceOptimizer) runMemoryMonitoring() {
	defer po.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if po.memoryMonitor != nil {
				po.memoryMonitor.Check()
			}

		case <-po.ctx.Done():
			return
		}
	}
}

// GetMetrics returns current optimizer metrics
func (po *PerformanceOptimizer) GetMetrics() OptimizerMetrics {
	return *po.metrics
}

// GetPoolUtilization returns current pool utilization statistics
func (po *PerformanceOptimizer) GetPoolUtilization() map[string]float64 {
	return map[string]float64{
		"event_pool_hit_rate":  float64(po.metrics.EventPoolHits) / float64(po.metrics.EventPoolHits+po.metrics.EventPoolMisses),
		"block_pool_hit_rate":  float64(po.metrics.BlockPoolHits) / float64(po.metrics.BlockPoolHits+po.metrics.BlockPoolMisses),
		"event_cache_hit_rate": float64(po.metrics.EventCacheHits) / float64(po.metrics.EventCacheHits+po.metrics.EventCacheMisses),
		"block_cache_hit_rate": float64(po.metrics.BlockCacheHits) / float64(po.metrics.BlockCacheHits+po.metrics.BlockCacheMisses),
		"workers_active":       float64(atomic.LoadInt64(&po.metrics.WorkersActive)),
		"tasks_queued":         float64(atomic.LoadInt64(&po.metrics.TasksQueued)),
	}
}

// Task interface for background processing
type Task interface {
	Execute()
}

// Simple task implementation
type SimpleTask struct {
	Fn func()
}

func (st SimpleTask) Execute() {
	st.Fn()
}

// OptimizationHint provides hints for performance optimization
type OptimizationHint struct {
	Type        string
	Severity    int // 1=low, 5=critical
	Message     string
	Suggestion  string
	MetricValue float64
}

// GetOptimizationHints returns performance optimization suggestions
func (po *PerformanceOptimizer) GetOptimizationHints() []OptimizationHint {
	hints := make([]OptimizationHint, 0)
	util := po.GetPoolUtilization()

	// Check cache hit rates
	if eventCacheHitRate := util["event_cache_hit_rate"]; eventCacheHitRate < 0.8 {
		hints = append(hints, OptimizationHint{
			Type:        "cache",
			Severity:    3,
			Message:     "Low event cache hit rate",
			Suggestion:  "Consider increasing event cache size",
			MetricValue: eventCacheHitRate,
		})
	}

	// Check worker utilization
	if workersActive := util["workers_active"]; workersActive > float64(po.config.MaxWorkers)*0.9 {
		hints = append(hints, OptimizationHint{
			Type:        "concurrency",
			Severity:    4,
			Message:     "High worker utilization",
			Suggestion:  "Consider increasing worker pool size",
			MetricValue: workersActive,
		})
	}

	// Check task queue
	if tasksQueued := util["tasks_queued"]; tasksQueued > float64(po.config.WorkerQueueSize)*0.8 {
		hints = append(hints, OptimizationHint{
			Type:        "queue",
			Severity:    5,
			Message:     "Task queue nearly full",
			Suggestion:  "Increase worker count or queue size",
			MetricValue: tasksQueued,
		})
	}

	return hints
}
