// consensus/performance_profiler.go

package consensus

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

// OperationType represents different types of operations that can be profiled
type OperationType string

const (
	// Event-related operations
	OpEventCreation     OperationType = "event_creation"
	OpEventFinalization OperationType = "event_finalization"
	OpEventValidation   OperationType = "event_validation"
	OpEventPropagation  OperationType = "event_propagation"
	OpEventBatchProcess OperationType = "event_batch_process"

	// Block-related operations
	OpBlockProduction   OperationType = "block_production"
	OpBlockValidation   OperationType = "block_validation"
	OpBlockFinalization OperationType = "block_finalization"

	// Consensus-related operations
	OpPoHTick            OperationType = "poh_tick"
	OpLachesisVoting     OperationType = "lachesis_voting"
	OpDPoSSelection      OperationType = "dpos_selection"
	OpCheckpointCreation OperationType = "checkpoint_creation"
	OpStateSync          OperationType = "state_sync"

	// Validator-related operations
	OpValidatorReward    OperationType = "validator_reward"
	OpStakeUpdate        OperationType = "stake_update"
	OpValidatorSelection OperationType = "validator_selection"
)

// OperationMetrics stores performance metrics for a specific operation
type OperationMetrics struct {
	Count           int64           // Number of times the operation was performed
	TotalDuration   time.Duration   // Total time spent on the operation
	MinDuration     time.Duration   // Minimum duration of the operation
	MaxDuration     time.Duration   // Maximum duration of the operation
	AvgDuration     time.Duration   // Average duration of the operation
	P50Duration     time.Duration   // 50th percentile (median) duration
	P90Duration     time.Duration   // 90th percentile duration
	P99Duration     time.Duration   // 99th percentile duration
	RecentDurations []time.Duration // Recent durations for percentile calculation
	LastUpdated     time.Time       // Last time the metrics were updated
}

// PerformanceProfiler provides utilities for profiling the performance of the consensus engine
type PerformanceProfiler struct {
	// Configuration
	enabled             bool
	cpuProfilingEnabled bool
	memProfilingEnabled bool
	traceEnabled        bool
	sampleSize          int           // Number of recent durations to keep for percentile calculation
	logFrequency        time.Duration // How often to log performance metrics

	// Metrics
	metrics   map[OperationType]*OperationMetrics
	metricsMu sync.RWMutex

	// CPU profiling
	cpuProfileFile *os.File

	// Memory profiling
	memProfileFile *os.File

	// Execution tracing
	traceFile *os.File

	// Bottleneck detection
	bottleneckThreshold time.Duration // Threshold for considering an operation a bottleneck

	// Logging
	logger *hybridConsensusLogger

	// Control
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewPerformanceProfiler creates a new PerformanceProfiler
func NewPerformanceProfiler(logger *hybridConsensusLogger) *PerformanceProfiler {
	return &PerformanceProfiler{
		enabled:             true,
		cpuProfilingEnabled: false,
		memProfilingEnabled: false,
		traceEnabled:        false,
		sampleSize:          100,
		logFrequency:        30 * time.Second,
		metrics:             make(map[OperationType]*OperationMetrics),
		bottleneckThreshold: 100 * time.Millisecond,
		logger:              logger,
		stopChan:            make(chan struct{}),
	}
}

// Start starts the performance profiler
func (p *PerformanceProfiler) Start() error {
	if !p.enabled {
		return nil
	}

	// Start CPU profiling if enabled
	if p.cpuProfilingEnabled {
		var err error
		p.cpuProfileFile, err = os.Create("cpu_profile.prof")
		if err != nil {
			return fmt.Errorf("could not create CPU profile: %v", err)
		}
		if err := pprof.StartCPUProfile(p.cpuProfileFile); err != nil {
			p.cpuProfileFile.Close()
			return fmt.Errorf("could not start CPU profile: %v", err)
		}
	}

	// Start periodic logging
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.logFrequency)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.logMetrics()
				p.detectBottlenecks()

				// Take memory profile if enabled
				if p.memProfilingEnabled {
					p.takeMemoryProfile()
				}
			case <-p.stopChan:
				return
			}
		}
	}()

	p.logger.Info("Performance profiler started")
	return nil
}

// Stop stops the performance profiler
func (p *PerformanceProfiler) Stop() error {
	if !p.enabled {
		return nil
	}

	close(p.stopChan)
	p.wg.Wait()

	// Stop CPU profiling if enabled
	if p.cpuProfilingEnabled && p.cpuProfileFile != nil {
		pprof.StopCPUProfile()
		p.cpuProfileFile.Close()
		p.cpuProfileFile = nil
	}

	// Take final memory profile if enabled
	if p.memProfilingEnabled {
		p.takeMemoryProfile()
	}

	p.logger.Info("Performance profiler stopped")
	return nil
}

// StartOperation starts timing an operation and returns a function to stop timing
func (p *PerformanceProfiler) StartOperation(opType OperationType) func() {
	if !p.enabled {
		return func() {}
	}

	startTime := ConsensusNow()

	return func() {
		duration := ConsensusSince(startTime)
		p.RecordOperation(opType, duration)
	}
}

// RecordOperation records the duration of an operation
func (p *PerformanceProfiler) RecordOperation(opType OperationType, duration time.Duration) {
	if !p.enabled {
		return
	}

	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()

	// Get or create metrics for this operation type
	metrics, exists := p.metrics[opType]
	if !exists {
		metrics = &OperationMetrics{
			MinDuration:     duration,
			MaxDuration:     duration,
			RecentDurations: make([]time.Duration, 0, p.sampleSize),
			LastUpdated:     ConsensusNow(),
		}
		p.metrics[opType] = metrics
	}

	// Update metrics
	metrics.Count++
	metrics.TotalDuration += duration
	metrics.AvgDuration = metrics.TotalDuration / time.Duration(metrics.Count)

	if duration < metrics.MinDuration {
		metrics.MinDuration = duration
	}
	if duration > metrics.MaxDuration {
		metrics.MaxDuration = duration
	}

	// Update recent durations for percentile calculation
	if len(metrics.RecentDurations) >= p.sampleSize {
		// Remove oldest duration
		metrics.RecentDurations = metrics.RecentDurations[1:]
	}
	metrics.RecentDurations = append(metrics.RecentDurations, duration)

	// Calculate percentiles
	if len(metrics.RecentDurations) > 0 {
		durations := make([]time.Duration, len(metrics.RecentDurations))
		copy(durations, metrics.RecentDurations)
		sort.Slice(durations, func(i, j int) bool {
			return durations[i] < durations[j]
		})

		p50Idx := int(float64(len(durations)) * 0.5)
		p90Idx := int(float64(len(durations)) * 0.9)
		p99Idx := int(float64(len(durations)) * 0.99)

		if p50Idx < len(durations) {
			metrics.P50Duration = durations[p50Idx]
		}
		if p90Idx < len(durations) {
			metrics.P90Duration = durations[p90Idx]
		}
		if p99Idx < len(durations) {
			metrics.P99Duration = durations[p99Idx]
		}
	}

	metrics.LastUpdated = ConsensusNow()
}

// GetMetrics returns a copy of the current metrics
func (p *PerformanceProfiler) GetMetrics() map[OperationType]OperationMetrics {
	if !p.enabled {
		return nil
	}

	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	result := make(map[OperationType]OperationMetrics)
	for opType, metrics := range p.metrics {
		result[opType] = *metrics
	}

	return result
}

// logMetrics logs the current performance metrics
func (p *PerformanceProfiler) logMetrics() {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	if len(p.metrics) == 0 {
		return
	}

	p.logger.Info("Performance metrics:")

	// Sort operation types for consistent logging
	opTypes := make([]OperationType, 0, len(p.metrics))
	for opType := range p.metrics {
		opTypes = append(opTypes, opType)
	}
	sort.Slice(opTypes, func(i, j int) bool {
		return string(opTypes[i]) < string(opTypes[j])
	})

	for _, opType := range opTypes {
		metrics := p.metrics[opType]
		p.logger.Info(fmt.Sprintf("  %s:", opType),
			LogKeyValue{Key: "count", Value: fmt.Sprintf("%d", metrics.Count)},
			LogKeyValue{Key: "avg", Value: metrics.AvgDuration.String()},
			LogKeyValue{Key: "min", Value: metrics.MinDuration.String()},
			LogKeyValue{Key: "max", Value: metrics.MaxDuration.String()},
			LogKeyValue{Key: "p50", Value: metrics.P50Duration.String()},
			LogKeyValue{Key: "p90", Value: metrics.P90Duration.String()},
			LogKeyValue{Key: "p99", Value: metrics.P99Duration.String()})
	}
}

// detectBottlenecks identifies potential performance bottlenecks
func (p *PerformanceProfiler) detectBottlenecks() {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	bottlenecks := make([]string, 0)

	for opType, metrics := range p.metrics {
		// Check if p90 duration exceeds threshold
		if metrics.P90Duration > p.bottleneckThreshold {
			bottlenecks = append(bottlenecks, fmt.Sprintf("%s (p90: %v)", opType, metrics.P90Duration))
		}
	}

	if len(bottlenecks) > 0 {
		p.logger.Warn("Potential performance bottlenecks detected", LogKeyValue{Key: "bottlenecks", Value: fmt.Sprintf("%v", bottlenecks)})
	}
}

// takeMemoryProfile takes a memory profile
func (p *PerformanceProfiler) takeMemoryProfile() {
	// Create a new file for the memory profile
	f, err := os.Create(fmt.Sprintf("mem_profile_%s.prof", ConsensusNow().Format("20060102_150405")))
	if err != nil {
		p.logger.Error("Could not create memory profile", LogKeyValue{Key: "error", Value: err.Error()})
		return
	}
	defer f.Close()

	// Force garbage collection before taking memory profile
	runtime.GC()

	// Write memory profile
	if err := pprof.WriteHeapProfile(f); err != nil {
		p.logger.Error("Could not write memory profile", LogKeyValue{Key: "error", Value: err.Error()})
	}
}

// EnableCPUProfiling enables or disables CPU profiling
func (p *PerformanceProfiler) EnableCPUProfiling(enable bool) {
	p.cpuProfilingEnabled = enable

	if enable && p.cpuProfileFile == nil && p.enabled {
		// Start CPU profiling
		var err error
		p.cpuProfileFile, err = os.Create("cpu_profile.prof")
		if err != nil {
			p.logger.Error("Could not create CPU profile", LogKeyValue{Key: "error", Value: err.Error()})
			return
		}
		if err := pprof.StartCPUProfile(p.cpuProfileFile); err != nil {
			p.logger.Error("Could not start CPU profile", LogKeyValue{Key: "error", Value: err.Error()})
			p.cpuProfileFile.Close()
			p.cpuProfileFile = nil
			return
		}
		p.logger.Info("CPU profiling enabled")
	} else if !enable && p.cpuProfileFile != nil {
		// Stop CPU profiling
		pprof.StopCPUProfile()
		p.cpuProfileFile.Close()
		p.cpuProfileFile = nil
		p.logger.Info("CPU profiling disabled")
	}
}

// EnableMemoryProfiling enables or disables memory profiling
func (p *PerformanceProfiler) EnableMemoryProfiling(enable bool) {
	p.memProfilingEnabled = enable
	p.logger.Info("Memory profiling", LogKeyValue{Key: "enabled", Value: fmt.Sprintf("%v", enable)})
}

// SetBottleneckThreshold sets the threshold for considering an operation a bottleneck
func (p *PerformanceProfiler) SetBottleneckThreshold(threshold time.Duration) {
	p.bottleneckThreshold = threshold
	p.logger.Info("Bottleneck threshold updated", LogKeyValue{Key: "threshold", Value: threshold.String()})
}

// SetLogFrequency sets how often to log performance metrics
func (p *PerformanceProfiler) SetLogFrequency(frequency time.Duration) {
	p.logFrequency = frequency
	p.logger.Info("Log frequency updated", LogKeyValue{Key: "frequency", Value: frequency.String()})
}

// SetSampleSize sets the number of recent durations to keep for percentile calculation
func (p *PerformanceProfiler) SetSampleSize(size int) {
	p.sampleSize = size
	p.logger.Info("Sample size updated", LogKeyValue{Key: "size", Value: fmt.Sprintf("%d", size)})
}

// ResetMetrics resets all performance metrics
func (p *PerformanceProfiler) ResetMetrics() {
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()

	p.metrics = make(map[OperationType]*OperationMetrics)
	p.logger.Info("Performance metrics reset")
}

// GetOperationCount returns the number of times an operation has been performed
func (p *PerformanceProfiler) GetOperationCount(opType OperationType) int64 {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	if metrics, exists := p.metrics[opType]; exists {
		return metrics.Count
	}
	return 0
}

// GetOperationAvgDuration returns the average duration of an operation
func (p *PerformanceProfiler) GetOperationAvgDuration(opType OperationType) time.Duration {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	if metrics, exists := p.metrics[opType]; exists {
		return metrics.AvgDuration
	}
	return 0
}

// GetOperationP90Duration returns the 90th percentile duration of an operation
func (p *PerformanceProfiler) GetOperationP90Duration(opType OperationType) time.Duration {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	if metrics, exists := p.metrics[opType]; exists {
		return metrics.P90Duration
	}
	return 0
}
