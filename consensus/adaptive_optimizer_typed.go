// Package consensus provides typed adaptive optimization
package consensus

import (
	"fmt"
	"math"
	"sync"
	"time"

	"diamante/types"
)

// TypedAdaptiveOptimizer automatically adjusts system parameters based on performance metrics
type TypedAdaptiveOptimizer struct {
	performanceOptimizer *PerformanceOptimizer
	logger               *hybridConsensusLogger

	// Configuration
	config *types.OptimizerConfig

	// Historical data
	metricsHistory   []types.ConsensusMetrics
	parameterHistory map[string][]TypedParameterChange
	historyMu        sync.RWMutex

	// Current parameters using typed map
	currentParams *types.TypedMap
	paramsMu      sync.RWMutex

	// Optimization state
	lastOptimization  time.Time
	optimizationCount int64

	// Learning algorithm state
	learningState *TypedLearningState
}

// TypedParameterChange represents a parameter change with typed values
type TypedParameterChange struct {
	Timestamp time.Time
	Parameter string
	OldValue  *types.Value
	NewValue  *types.Value
	Reason    string
	Impact    float64 // Measured performance impact
}

// TypedLearningState represents the learning algorithm state with typed values
type TypedLearningState struct {
	ParameterWeights   map[string]float64
	PerformanceHistory []float64
	BestConfiguration  *types.TypedMap
	BestPerformance    float64
}

// TypedAdaptiveConfig holds typed configuration for adaptive optimization
type TypedAdaptiveConfig struct {
	// Optimization intervals
	MinOptimizationInterval time.Duration
	MaxOptimizationInterval time.Duration

	// Parameter adjustment limits
	MaxParameterChange float64 // Maximum percentage change per optimization
	MinParameterValue  map[string]*types.Value
	MaxParameterValue  map[string]*types.Value

	// Learning parameters
	LearningRate    float64
	ExplorationRate float64
	MemoryWindow    int // Number of past optimizations to consider

	// Target metrics
	TargetBlockTime   time.Duration
	TargetThroughput  float64
	TargetLatency     time.Duration
	TargetSuccessRate float64
}

// NewTypedAdaptiveOptimizer creates a new typed adaptive optimizer
func NewTypedAdaptiveOptimizer(performanceOptimizer *PerformanceOptimizer, config *types.OptimizerConfig) *TypedAdaptiveOptimizer {
	// Convert config to typed config
	_ = &TypedAdaptiveConfig{
		MinOptimizationInterval: config.MinTimeout,
		MaxOptimizationInterval: config.MaxTimeout,
		MaxParameterChange:      config.AdjustmentFactor,
		MinParameterValue:       make(map[string]*types.Value),
		MaxParameterValue:       make(map[string]*types.Value),
		LearningRate:            0.01,
		ExplorationRate:         0.1,
		MemoryWindow:            int(config.MetricWindow / time.Minute),
		TargetBlockTime:         config.TargetBlockTime,
		TargetLatency:           config.LatencyThreshold,
	}

	return &TypedAdaptiveOptimizer{
		performanceOptimizer: performanceOptimizer,
		config:               config,
		metricsHistory:       make([]types.ConsensusMetrics, 0),
		parameterHistory:     make(map[string][]TypedParameterChange),
		currentParams:        types.NewTypedMap(),
		learningState: &TypedLearningState{
			ParameterWeights:   make(map[string]float64),
			PerformanceHistory: make([]float64, 0),
			BestConfiguration:  types.NewTypedMap(),
			BestPerformance:    0,
		},
		lastOptimization: ConsensusNow(),
	}
}

// InitializeParameters sets up initial parameter values
func (ao *TypedAdaptiveOptimizer) InitializeParameters() {
	ao.paramsMu.Lock()
	defer ao.paramsMu.Unlock()

	// Initialize consensus parameters
	ao.currentParams.Set("block_size", types.Uint64ToValue(1048576))        // 1MB
	ao.currentParams.Set("block_interval", types.Int64ToValue(5000))        // 5s in ms
	ao.currentParams.Set("validator_timeout", types.Int64ToValue(10000))    // 10s in ms
	ao.currentParams.Set("network_latency_buffer", types.Int64ToValue(500)) // 500ms
	ao.currentParams.Set("transaction_batch_size", types.Uint64ToValue(1000))
	ao.currentParams.Set("consensus_round_timeout", types.Int64ToValue(30000)) // 30s in ms

	// Initialize parameter weights
	ao.learningState.ParameterWeights["block_size"] = 0.8
	ao.learningState.ParameterWeights["block_interval"] = 1.0
	ao.learningState.ParameterWeights["validator_timeout"] = 0.9
	ao.learningState.ParameterWeights["network_latency_buffer"] = 0.7
	ao.learningState.ParameterWeights["transaction_batch_size"] = 0.6
	ao.learningState.ParameterWeights["consensus_round_timeout"] = 0.85
}

// OptimizeParameters performs parameter optimization based on metrics
func (ao *TypedAdaptiveOptimizer) OptimizeParameters(metrics *types.ConsensusMetrics) error {
	ao.historyMu.Lock()
	defer ao.historyMu.Unlock()

	// Check if enough time has passed since last optimization
	if ConsensusSince(ao.lastOptimization) < ao.config.MinTimeout {
		return nil
	}

	// Add metrics to history
	ao.metricsHistory = append(ao.metricsHistory, *metrics)
	if len(ao.metricsHistory) > int(ao.config.MetricWindow.Milliseconds()/1000) {
		ao.metricsHistory = ao.metricsHistory[1:]
	}

	// Calculate performance score
	performanceScore := ao.calculatePerformanceScore(metrics)

	// Update best configuration if this is better
	if performanceScore > ao.learningState.BestPerformance {
		ao.learningState.BestPerformance = performanceScore
		ao.learningState.BestConfiguration = ao.cloneCurrentParams()
	}

	// Optimize each parameter
	parameters := []string{
		"block_size",
		"block_interval",
		"validator_timeout",
		"network_latency_buffer",
		"transaction_batch_size",
		"consensus_round_timeout",
	}

	for _, param := range parameters {
		if err := ao.optimizeParameter(param, metrics, performanceScore); err != nil {
			ao.logger.Warn("Failed to optimize parameter", LogKeyValue{Key: "param", Value: param}, LogKeyValue{Key: "error", Value: err.Error()})
		}
	}

	ao.lastOptimization = ConsensusNow()
	ao.optimizationCount++

	return nil
}

// optimizeParameter optimizes a single parameter
func (ao *TypedAdaptiveOptimizer) optimizeParameter(param string, metrics *types.ConsensusMetrics, performanceScore float64) error {
	ao.paramsMu.Lock()
	defer ao.paramsMu.Unlock()

	currentValue, exists := ao.currentParams.Get(param)
	if !exists {
		return fmt.Errorf("parameter %s not found", param)
	}

	// Calculate adjustment based on parameter type and metrics
	var newValue *types.Value
	var adjustment float64

	switch param {
	case "block_size":
		// Adjust based on transaction count and block time
		currentSize, _ := currentValue.Uint64()
		targetTPS := float64(metrics.TransactionCount) / metrics.BlockTime.Seconds()

		if targetTPS > 10000 && metrics.BlockTime < ao.config.TargetBlockTime {
			adjustment = 1.1 // Increase by 10%
		} else if metrics.BlockTime > ao.config.TargetBlockTime {
			adjustment = 0.9 // Decrease by 10%
		} else {
			adjustment = 1.0
		}

		newSize := uint64(float64(currentSize) * adjustment)
		newSize = ao.clampUint64(newSize, 524288, 5242880) // 512KB to 5MB
		newValue = types.Uint64ToValue(newSize)

	case "block_interval":
		// Adjust based on block time performance
		currentInterval, _ := currentValue.Int64()

		if metrics.BlockTime > ao.config.TargetBlockTime*2 {
			adjustment = 1.2 // Increase interval
		} else if metrics.BlockTime < ao.config.TargetBlockTime/2 {
			adjustment = 0.8 // Decrease interval
		} else {
			adjustment = 1.0
		}

		newInterval := int64(float64(currentInterval) * adjustment)
		newInterval = ao.clampInt64(newInterval, 1000, 30000) // 1s to 30s
		newValue = types.Int64ToValue(newInterval)

	case "validator_timeout":
		// Adjust based on network latency and missed blocks
		currentTimeout, _ := currentValue.Int64()

		if metrics.MissedBlocks > 0 {
			adjustment = 1.1 // Increase timeout
		} else if metrics.NetworkLatency < ao.config.LatencyThreshold/2 {
			adjustment = 0.95 // Slightly decrease timeout
		} else {
			adjustment = 1.0
		}

		newTimeout := int64(float64(currentTimeout) * adjustment)
		newTimeout = ao.clampInt64(newTimeout, 5000, 60000) // 5s to 60s
		newValue = types.Int64ToValue(newTimeout)

	default:
		// For other parameters, use gradient-based optimization
		gradient := ao.estimateGradient(param, performanceScore)
		currentVal, _ := currentValue.Int64()

		adjustment = 1.0 + (gradient * ao.config.AdjustmentFactor)
		newVal := int64(float64(currentVal) * adjustment)
		newValue = types.Int64ToValue(newVal)
	}

	// Record the change
	if adjustment != 1.0 {
		change := TypedParameterChange{
			Timestamp: ConsensusNow(),
			Parameter: param,
			OldValue:  currentValue,
			NewValue:  newValue,
			Reason:    fmt.Sprintf("Performance optimization (score: %.2f)", performanceScore),
			Impact:    adjustment - 1.0,
		}

		ao.parameterHistory[param] = append(ao.parameterHistory[param], change)
		ao.currentParams.Set(param, newValue)

		ao.logger.Info("Parameter optimized",
			LogKeyValue{Key: "param", Value: param},
			LogKeyValue{Key: "adjustment", Value: fmt.Sprintf("%.2f%%", (adjustment-1.0)*100)},
			LogKeyValue{Key: "reason", Value: change.Reason})
	}

	return nil
}

// calculatePerformanceScore calculates overall performance score
func (ao *TypedAdaptiveOptimizer) calculatePerformanceScore(metrics *types.ConsensusMetrics) float64 {
	// Normalize metrics to 0-1 range
	blockTimeScore := 1.0 - math.Min(1.0, metrics.BlockTime.Seconds()/ao.config.TargetBlockTime.Seconds())
	throughputScore := math.Min(1.0, float64(metrics.TransactionCount)/10000.0)
	latencyScore := 1.0 - math.Min(1.0, metrics.NetworkLatency.Seconds()/ao.config.LatencyThreshold.Seconds())
	reliabilityScore := 1.0 - (float64(metrics.MissedBlocks) / 100.0)

	// Weighted average
	score := (blockTimeScore * 0.3) +
		(throughputScore * 0.3) +
		(latencyScore * 0.2) +
		(reliabilityScore * 0.2)

	return math.Max(0, math.Min(1, score))
}

// estimateGradient estimates the gradient for a parameter
func (ao *TypedAdaptiveOptimizer) estimateGradient(param string, currentScore float64) float64 {
	// Look at recent parameter changes and their impact
	history := ao.parameterHistory[param]
	if len(history) < 2 {
		// Not enough history, use exploration
		if ao.shouldExplore() {
			return (ao.random() - 0.5) * 0.2
		}
		return 0
	}

	// Calculate gradient from recent changes
	recent := history[len(history)-1]
	if recent.Impact != 0 {
		performanceChange := currentScore - ao.learningState.BestPerformance
		gradient := performanceChange / recent.Impact

		// Apply learning rate
		return gradient * 0.1 // Conservative learning rate
	}

	return 0
}

// Helper methods

func (ao *TypedAdaptiveOptimizer) clampInt64(value, min, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func (ao *TypedAdaptiveOptimizer) clampUint64(value, min, max uint64) uint64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func (ao *TypedAdaptiveOptimizer) shouldExplore() bool {
	return ao.random() < ao.config.AdjustmentFactor*0.1
}

func (ao *TypedAdaptiveOptimizer) random() float64 {
	// Use consensus-based deterministic randomness
	return float64(ConsensusUnixNano()%1000) / 1000.0
}

func (ao *TypedAdaptiveOptimizer) cloneCurrentParams() *types.TypedMap {
	ao.paramsMu.RLock()
	defer ao.paramsMu.RUnlock()

	clone := types.NewTypedMap()
	for _, key := range ao.currentParams.Keys() {
		if value, exists := ao.currentParams.Get(key); exists {
			clone.Set(key, value)
		}
	}

	return clone
}

// GetCurrentParameters returns the current parameter values as a typed map
func (ao *TypedAdaptiveOptimizer) GetCurrentParameters() *types.TypedMap {
	ao.paramsMu.RLock()
	defer ao.paramsMu.RUnlock()

	return ao.cloneCurrentParams()
}

// GetOptimizationStats returns optimization statistics
func (ao *TypedAdaptiveOptimizer) GetOptimizationStats() *types.OptimizerMetrics {
	ao.historyMu.RLock()
	defer ao.historyMu.RUnlock()

	// Calculate average block time from history
	var totalBlockTime time.Duration
	if len(ao.metricsHistory) > 0 {
		for _, m := range ao.metricsHistory {
			totalBlockTime += m.BlockTime
		}
		totalBlockTime /= time.Duration(len(ao.metricsHistory))
	}

	// Get current timeout from parameters
	var optimalTimeout time.Duration
	if timeoutVal, exists := ao.currentParams.Get("consensus_round_timeout"); exists {
		if timeout, err := timeoutVal.Int64(); err == nil {
			optimalTimeout = time.Duration(timeout) * time.Millisecond
		}
	}

	return &types.OptimizerMetrics{
		AverageBlockTime:   totalBlockTime,
		TimeoutAdjustments: uint64(ao.optimizationCount),
		NetworkCondition:   ao.assessNetworkCondition(),
		OptimalTimeout:     optimalTimeout,
		SuccessRate:        ao.learningState.BestPerformance,
	}
}

// assessNetworkCondition assesses the current network condition
func (ao *TypedAdaptiveOptimizer) assessNetworkCondition() string {
	if len(ao.metricsHistory) == 0 {
		return "unknown"
	}

	latest := ao.metricsHistory[len(ao.metricsHistory)-1]

	if latest.NetworkLatency < 100*time.Millisecond && latest.MissedBlocks == 0 {
		return "excellent"
	} else if latest.NetworkLatency < 500*time.Millisecond && latest.MissedBlocks < 5 {
		return "good"
	} else if latest.NetworkLatency < 1*time.Second && latest.MissedBlocks < 10 {
		return "fair"
	} else {
		return "poor"
	}
}
