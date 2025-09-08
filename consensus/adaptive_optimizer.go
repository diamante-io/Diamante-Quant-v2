// consensus/adaptive_optimizer.go
package consensus

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	dtypes "diamante/types"
)

// AdaptiveOptimizer automatically adjusts system parameters based on performance metrics
type AdaptiveOptimizer struct {
	performanceOptimizer *PerformanceOptimizer
	logger               *hybridConsensusLogger

	// Configuration
	config *AdaptiveConfig

	// Historical data
	metricsHistory   []OptimizationMetrics
	parameterHistory map[string][]ParameterChange
	historyMu        sync.RWMutex

	// Current parameters
	currentParams map[string]*dtypes.Value
	paramsMu      sync.RWMutex

	// Optimization state
	lastOptimization  time.Time
	optimizationCount int64
	bestScore         float64

	// Learning algorithm state
	learningState *LearningState
}

// AdaptiveConfig holds configuration for adaptive optimization
type AdaptiveConfig struct {
	// Optimization intervals
	MinOptimizationInterval time.Duration
	MaxOptimizationInterval time.Duration

	// Parameter adjustment limits
	MaxParameterChange float64 // Maximum percentage change per optimization
	MinParameterValue  map[string]float64
	MaxParameterValue  map[string]float64

	// Learning parameters
	LearningRate    float64
	ExplorationRate float64
	MemoryWindow    int // Number of past optimizations to consider

	// Performance thresholds
	PerformanceThreshold float64 // Minimum performance improvement to continue optimization
	StabilityThreshold   float64 // Minimum stability required before optimization

	// Feature flags
	EnableBatchSizeOptimization   bool
	EnableWorkerCountOptimization bool
	EnableCacheSizeOptimization   bool
	EnableTimeoutOptimization     bool
}

// DefaultAdaptiveConfig returns a default adaptive configuration
func DefaultAdaptiveConfig() *AdaptiveConfig {
	return &AdaptiveConfig{
		MinOptimizationInterval: 30 * time.Second,
		MaxOptimizationInterval: 5 * time.Minute,
		MaxParameterChange:      0.2, // 20% max change
		MinParameterValue: map[string]float64{
			"batch_size":   10,
			"worker_count": 1,
			"cache_size":   100,
			"timeout_ms":   100,
		},
		MaxParameterValue: map[string]float64{
			"batch_size":   10000,
			"worker_count": 100,
			"cache_size":   100000,
			"timeout_ms":   30000,
		},
		LearningRate:                  0.1,
		ExplorationRate:               0.1,
		MemoryWindow:                  20,
		PerformanceThreshold:          0.05, // 5% improvement
		StabilityThreshold:            0.95, // 95% stability
		EnableBatchSizeOptimization:   true,
		EnableWorkerCountOptimization: true,
		EnableCacheSizeOptimization:   true,
		EnableTimeoutOptimization:     true,
	}
}

// OptimizationMetrics contains metrics for a single optimization cycle
type OptimizationMetrics struct {
	Timestamp         time.Time
	ThroughputTPS     float64
	LatencyMs         float64
	MemoryUsageMB     float64
	CPUUsagePercent   float64
	ErrorRate         float64
	CacheHitRate      float64
	QueueUtilization  float64
	WorkerUtilization float64
	PerformanceScore  float64
}

// ParameterChange represents a change to a system parameter
type ParameterChange struct {
	Timestamp time.Time
	Parameter string
	OldValue  float64
	NewValue  float64
	Reason    string
	Impact    float64 // Measured performance impact
}

// LearningState maintains the state of the learning algorithm
type LearningState struct {
	ParameterWeights   map[string]float64
	PerformanceHistory []float64
	ExplorationCount   map[string]int
	LastExploration    map[string]time.Time
	Convergence        map[string]bool
}

// NewAdaptiveOptimizer creates a new adaptive optimizer
func NewAdaptiveOptimizer(po *PerformanceOptimizer, logger *hybridConsensusLogger) *AdaptiveOptimizer {
	config := DefaultAdaptiveConfig()

	return &AdaptiveOptimizer{
		performanceOptimizer: po,
		logger:               logger,
		config:               config,
		metricsHistory:       make([]OptimizationMetrics, 0),
		parameterHistory:     make(map[string][]ParameterChange),
		currentParams:        make(map[string]*dtypes.Value),
		learningState: &LearningState{
			ParameterWeights:   make(map[string]float64),
			PerformanceHistory: make([]float64, 0),
			ExplorationCount:   make(map[string]int),
			LastExploration:    make(map[string]time.Time),
			Convergence:        make(map[string]bool),
		},
	}
}

// Optimize performs adaptive optimization of system parameters
func (ao *AdaptiveOptimizer) Optimize() error {
	// Check if enough time has passed since last optimization
	if ConsensusSince(ao.lastOptimization) < ao.config.MinOptimizationInterval {
		return nil
	}

	// Collect current metrics
	metrics := ao.collectMetrics()

	// Store metrics in history
	ao.historyMu.Lock()
	ao.metricsHistory = append(ao.metricsHistory, metrics)
	if len(ao.metricsHistory) > ao.config.MemoryWindow {
		ao.metricsHistory = ao.metricsHistory[1:]
	}
	ao.historyMu.Unlock()

	// Check if system is stable enough for optimization
	if !ao.isSystemStable() {
		ao.logger.Debug("System not stable enough for optimization")
		return nil
	}

	// Perform optimization
	optimizations := ao.performOptimization(metrics)

	ao.lastOptimization = ConsensusNow()
	ao.optimizationCount++

	// Update best score if current score is better
	if metrics.PerformanceScore > ao.bestScore {
		ao.bestScore = metrics.PerformanceScore
	}

	if len(optimizations) > 0 {
		ao.logger.Info("Applied adaptive optimizations",
			LogKeyValue{Key: "count", Value: fmt.Sprintf("%d", len(optimizations))},
			LogKeyValue{Key: "cycle", Value: fmt.Sprintf("%d", ao.optimizationCount)})
	}

	return nil
}

// collectMetrics gathers current system performance metrics
func (ao *AdaptiveOptimizer) collectMetrics() OptimizationMetrics {
	// Get metrics from performance optimizer
	optimizerMetrics := ao.performanceOptimizer.GetMetrics()
	utilization := ao.performanceOptimizer.GetPoolUtilization()

	// Calculate performance score (simplified)
	performanceScore := ao.calculatePerformanceScore(optimizerMetrics, utilization)

	now := ConsensusNow()
	return OptimizationMetrics{
		Timestamp:         now,
		ThroughputTPS:     float64(optimizerMetrics.TasksCompleted) / ConsensusSince(now.Add(-time.Minute)).Seconds(),
		LatencyMs:         float64(optimizerMetrics.AvgTaskDuration.Nanoseconds()) / 1e6,
		MemoryUsageMB:     float64(optimizerMetrics.MemoryUsage) / (1024 * 1024),
		CPUUsagePercent:   utilization["workers_active"],
		ErrorRate:         0, // Would be calculated from actual error metrics
		CacheHitRate:      utilization["event_cache_hit_rate"],
		QueueUtilization:  utilization["tasks_queued"] / 100, // Normalize to percentage
		WorkerUtilization: utilization["workers_active"] / 100,
		PerformanceScore:  performanceScore,
	}
}

// calculatePerformanceScore computes an overall performance score
func (ao *AdaptiveOptimizer) calculatePerformanceScore(metrics OptimizerMetrics, utilization map[string]float64) float64 {
	// Weighted combination of various metrics
	// Higher is better

	cacheScore := utilization["event_cache_hit_rate"] + utilization["block_cache_hit_rate"]
	utilizationScore := math.Min(utilization["workers_active"], 80)              // Optimal utilization around 80%
	memoryScore := math.Max(0, 100-float64(metrics.MemoryUsage)/(1024*1024*100)) // Lower memory usage is better

	// Combine scores (weights can be tuned)
	score := (cacheScore*0.3 + utilizationScore*0.4 + memoryScore*0.3)

	return math.Max(0, math.Min(100, score))
}

// isSystemStable checks if the system is stable enough for optimization
func (ao *AdaptiveOptimizer) isSystemStable() bool {
	ao.historyMu.RLock()
	defer ao.historyMu.RUnlock()

	if len(ao.metricsHistory) < 3 {
		return false // Need at least 3 data points
	}

	// Calculate stability based on variance in performance score
	scores := make([]float64, len(ao.metricsHistory))
	for i, metrics := range ao.metricsHistory {
		scores[i] = metrics.PerformanceScore
	}

	stability := ao.calculateStability(scores)
	return stability >= ao.config.StabilityThreshold
}

// calculateStability calculates stability based on variance
func (ao *AdaptiveOptimizer) calculateStability(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	// Calculate mean
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	// Calculate variance
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance /= float64(len(values))

	// Convert variance to stability (0-1 scale)
	// Lower variance = higher stability
	stability := 1.0 / (1.0 + variance/100.0)

	return stability
}

// performOptimization performs the actual parameter optimization
func (ao *AdaptiveOptimizer) performOptimization(currentMetrics OptimizationMetrics) []ParameterChange {
	changes := make([]ParameterChange, 0)

	// Optimize batch size
	if ao.config.EnableBatchSizeOptimization {
		if change := ao.optimizeBatchSize(currentMetrics); change != nil {
			changes = append(changes, *change)
		}
	}

	// Optimize worker count
	if ao.config.EnableWorkerCountOptimization {
		if change := ao.optimizeWorkerCount(currentMetrics); change != nil {
			changes = append(changes, *change)
		}
	}

	// Optimize cache sizes
	if ao.config.EnableCacheSizeOptimization {
		if change := ao.optimizeCacheSize(currentMetrics); change != nil {
			changes = append(changes, *change)
		}
	}

	// Apply changes and record them
	for _, change := range changes {
		ao.applyParameterChange(change)
		ao.recordParameterChange(change)
	}

	return changes
}

// optimizeBatchSize optimizes batch processing parameters
func (ao *AdaptiveOptimizer) optimizeBatchSize(metrics OptimizationMetrics) *ParameterChange {
	currentBatchSize := ao.getCurrentParameter("batch_size", 100.0)

	// Simple heuristic: if queue utilization is high, increase batch size
	// If latency is high, decrease batch size
	var newBatchSize float64
	var reason string

	if metrics.QueueUtilization > 80 {
		newBatchSize = currentBatchSize * 1.1
		reason = "high_queue_utilization"
	} else if metrics.LatencyMs > 100 {
		newBatchSize = currentBatchSize * 0.9
		reason = "high_latency"
	} else {
		return nil // No change needed
	}

	// Apply bounds
	newBatchSize = ao.applyParameterBounds("batch_size", newBatchSize)

	if newBatchSize != currentBatchSize {
		return &ParameterChange{
			Timestamp: ConsensusNow(),
			Parameter: "batch_size",
			OldValue:  currentBatchSize,
			NewValue:  newBatchSize,
			Reason:    reason,
		}
	}

	return nil
}

// optimizeWorkerCount optimizes the number of worker threads
func (ao *AdaptiveOptimizer) optimizeWorkerCount(metrics OptimizationMetrics) *ParameterChange {
	currentWorkerCount := ao.getCurrentParameter("worker_count", 4.0)

	var newWorkerCount float64
	var reason string

	// Increase workers if utilization is high and performance is suffering
	if metrics.WorkerUtilization > 90 && metrics.LatencyMs > 50 {
		newWorkerCount = currentWorkerCount + 1
		reason = "high_utilization_high_latency"
	} else if metrics.WorkerUtilization < 30 && currentWorkerCount > 1 {
		newWorkerCount = currentWorkerCount - 1
		reason = "low_utilization"
	} else {
		return nil
	}

	// Apply bounds
	newWorkerCount = ao.applyParameterBounds("worker_count", newWorkerCount)

	if newWorkerCount != currentWorkerCount {
		return &ParameterChange{
			Timestamp: ConsensusNow(),
			Parameter: "worker_count",
			OldValue:  currentWorkerCount,
			NewValue:  newWorkerCount,
			Reason:    reason,
		}
	}

	return nil
}

// optimizeCacheSize optimizes cache sizes based on hit rates
func (ao *AdaptiveOptimizer) optimizeCacheSize(metrics OptimizationMetrics) *ParameterChange {
	currentCacheSize := ao.getCurrentParameter("cache_size", 1000.0)

	var newCacheSize float64
	var reason string

	// Increase cache size if hit rate is low
	if metrics.CacheHitRate < 70 {
		newCacheSize = currentCacheSize * 1.2
		reason = "low_cache_hit_rate"
	} else if metrics.CacheHitRate > 95 && metrics.MemoryUsageMB > 500 {
		newCacheSize = currentCacheSize * 0.9
		reason = "high_memory_usage"
	} else {
		return nil
	}

	// Apply bounds
	newCacheSize = ao.applyParameterBounds("cache_size", newCacheSize)

	if newCacheSize != currentCacheSize {
		return &ParameterChange{
			Timestamp: ConsensusNow(),
			Parameter: "cache_size",
			OldValue:  currentCacheSize,
			NewValue:  newCacheSize,
			Reason:    reason,
		}
	}

	return nil
}

// getCurrentParameter gets the current value of a parameter
func (ao *AdaptiveOptimizer) getCurrentParameter(name string, defaultValue float64) float64 {
	ao.paramsMu.RLock()
	defer ao.paramsMu.RUnlock()

	if value, exists := ao.currentParams[name]; exists {
		if value.Type == dtypes.ValueTypeFloat64 {
			if floatValue, err := strconv.ParseFloat(string(value.Data), 64); err == nil {
				return floatValue
			}
		}
	}

	return defaultValue
}

// applyParameterBounds ensures parameter values stay within configured bounds
func (ao *AdaptiveOptimizer) applyParameterBounds(name string, value float64) float64 {
	if min, exists := ao.config.MinParameterValue[name]; exists {
		value = math.Max(value, min)
	}

	if max, exists := ao.config.MaxParameterValue[name]; exists {
		value = math.Min(value, max)
	}

	return value
}

// applyParameterChange applies a parameter change to the system
func (ao *AdaptiveOptimizer) applyParameterChange(change ParameterChange) error {
	ao.paramsMu.Lock()
	ao.currentParams[change.Parameter] = dtypes.NewValue(dtypes.ValueTypeFloat64, []byte(strconv.FormatFloat(change.NewValue, 'f', -1, 64)))
	ao.paramsMu.Unlock()

	// Apply the change to the actual system components
	switch change.Parameter {
	case "worker_count":
		if pool := ao.performanceOptimizer.workerPool; pool != nil {
			return pool.Resize(int(change.NewValue))
		}
	case "cache_size":
		// Cache size changes would require cache recreation
		// For now, just log the change
		ao.logger.Info("Cache size change requested",
			LogKeyValue{Key: "new_size", Value: fmt.Sprintf("%.0f", change.NewValue)})
	}

	return nil
}

// recordParameterChange records a parameter change in history
func (ao *AdaptiveOptimizer) recordParameterChange(change ParameterChange) {
	ao.historyMu.Lock()
	defer ao.historyMu.Unlock()

	if ao.parameterHistory[change.Parameter] == nil {
		ao.parameterHistory[change.Parameter] = make([]ParameterChange, 0)
	}

	ao.parameterHistory[change.Parameter] = append(ao.parameterHistory[change.Parameter], change)

	// Keep only recent history
	if len(ao.parameterHistory[change.Parameter]) > ao.config.MemoryWindow {
		ao.parameterHistory[change.Parameter] = ao.parameterHistory[change.Parameter][1:]
	}
}

// GetOptimizationHistory returns the optimization history
func (ao *AdaptiveOptimizer) GetOptimizationHistory() ([]OptimizationMetrics, map[string][]ParameterChange) {
	ao.historyMu.RLock()
	defer ao.historyMu.RUnlock()

	// Return copies to avoid race conditions
	metricsCopy := make([]OptimizationMetrics, len(ao.metricsHistory))
	copy(metricsCopy, ao.metricsHistory)

	paramsCopy := make(map[string][]ParameterChange)
	for param, changes := range ao.parameterHistory {
		paramsCopy[param] = make([]ParameterChange, len(changes))
		copy(paramsCopy[param], changes)
	}

	return metricsCopy, paramsCopy
}

// GetCurrentParameters returns the current parameter values
func (ao *AdaptiveOptimizer) GetCurrentParameters() map[string]*dtypes.Value {
	ao.paramsMu.RLock()
	defer ao.paramsMu.RUnlock()

	result := make(map[string]*dtypes.Value)
	for k, v := range ao.currentParams {
		result[k] = v
	}

	return result
}

// getCurrentScore returns the current performance score
func (ao *AdaptiveOptimizer) getCurrentScore() float64 {
	ao.historyMu.RLock()
	defer ao.historyMu.RUnlock()

	if len(ao.metricsHistory) == 0 {
		return 0.0
	}

	// Return the most recent performance score
	return ao.metricsHistory[len(ao.metricsHistory)-1].PerformanceScore
}

// GetOptimizationStats returns optimization statistics
func (ao *AdaptiveOptimizer) GetOptimizationStats() *dtypes.AdaptiveOptimizerStats {
	ao.historyMu.RLock()
	metricsCount := len(ao.metricsHistory)
	ao.historyMu.RUnlock()

	parameterChangeCount := 0
	for _, changes := range ao.parameterHistory {
		parameterChangeCount += len(changes)
	}

	// Calculate average performance score
	avgScore := 0.0
	if metricsCount > 0 {
		for _, m := range ao.metricsHistory {
			avgScore += m.PerformanceScore
		}
		avgScore /= float64(metricsCount)
	}

	return &dtypes.AdaptiveOptimizerStats{
		OptimizationCount:    int(ao.optimizationCount),
		LastOptimization:     ao.lastOptimization,
		CurrentScore:         ao.getCurrentScore(),
		BestScore:            ao.bestScore,
		AverageScore:         avgScore,
		ParameterChanges:     parameterChangeCount,
		LearningRate:         ao.config.LearningRate,
		ExplorationRate:      ao.config.ExplorationRate,
		ConvergedParams:      len(ao.learningState.Convergence),
		MetricsHistoryLength: metricsCount,
	}
}
