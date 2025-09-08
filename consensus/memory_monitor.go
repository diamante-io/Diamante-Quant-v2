// consensus/memory_monitor.go
package consensus

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	dtypes "diamante/types"
)

// MemoryMonitor monitors system memory usage and triggers optimizations
type MemoryMonitor struct {
	memoryThreshold float64 // Percentage of available memory
	gcThreshold     float64 // When to trigger GC
	logger          *hybridConsensusLogger

	// Metrics
	lastGCTime     time.Time
	gcCount        int64
	memoryPressure int32 // 0=low, 1=medium, 2=high, 3=critical

	// Configuration
	checkInterval  time.Duration
	forceGCEnabled bool
	memoryTarget   uint64 // Target memory usage in bytes

	// Callbacks
	onMemoryPressure func(level int)
	onGCTriggered    func()
}

// NewMemoryMonitor creates a new memory monitor
func NewMemoryMonitor(memoryThreshold, gcThreshold float64, logger *hybridConsensusLogger) *MemoryMonitor {
	return &MemoryMonitor{
		memoryThreshold: memoryThreshold,
		gcThreshold:     gcThreshold,
		logger:          logger,
		checkInterval:   5 * time.Second,
		forceGCEnabled:  true,
		lastGCTime:      ConsensusNow(),
	}
}

// MemoryStats contains memory usage statistics
type MemoryStats struct {
	// Go runtime memory stats
	Alloc        uint64 `json:"alloc"`       // Currently allocated memory
	TotalAlloc   uint64 `json:"total_alloc"` // Total allocated memory
	Sys          uint64 `json:"sys"`         // System memory
	NumGC        uint32 `json:"num_gc"`      // Number of GC cycles
	PauseTotalNs uint64 `json:"pause_total"` // Total GC pause time

	// Calculated metrics
	MemoryUsage   float64 `json:"memory_usage"`   // Memory usage percentage
	GCPressure    float64 `json:"gc_pressure"`    // GC pressure indicator
	PressureLevel int     `json:"pressure_level"` // 0=low, 1=medium, 2=high, 3=critical

	// System metrics
	SystemMemory    uint64 `json:"system_memory"`    // Total system memory
	AvailableMemory uint64 `json:"available_memory"` // Available system memory
}

// Check performs a memory usage check and triggers optimizations if needed
func (mm *MemoryMonitor) Check() {
	stats := mm.GetMemoryStats()

	// Update pressure level
	oldPressure := atomic.LoadInt32(&mm.memoryPressure)
	newPressure := mm.calculatePressureLevel(stats)
	atomic.StoreInt32(&mm.memoryPressure, int32(newPressure))

	// Log pressure changes
	if newPressure != int(oldPressure) {
		mm.logger.Info("Memory pressure changed",
			LogKeyValue{Key: "old_level", Value: fmt.Sprintf("%d", oldPressure)},
			LogKeyValue{Key: "new_level", Value: fmt.Sprintf("%d", newPressure)},
			LogKeyValue{Key: "memory_usage", Value: fmt.Sprintf("%.2f", stats.MemoryUsage)})
	}

	// Trigger callbacks if pressure increased
	if newPressure > int(oldPressure) && mm.onMemoryPressure != nil {
		mm.onMemoryPressure(newPressure)
	}

	// Trigger GC if needed
	if mm.shouldTriggerGC(stats) {
		mm.triggerGC()
	}
}

// GetMemoryStats returns current memory statistics
func (mm *MemoryMonitor) GetMemoryStats() MemoryStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// Calculate memory usage percentage
	var memoryUsage float64
	if ms.Sys > 0 {
		memoryUsage = float64(ms.Alloc) / float64(ms.Sys) * 100
	}

	// Calculate GC pressure (based on GC frequency and pause time)
	gcPressure := mm.calculateGCPressure(&ms)

	// Get system memory info (simplified - would need platform-specific code)
	systemMemory := uint64(0)    // Would be populated with actual system memory
	availableMemory := uint64(0) // Would be populated with available memory

	pressureLevel := mm.calculatePressureLevel(MemoryStats{
		MemoryUsage: memoryUsage,
		GCPressure:  gcPressure,
	})

	return MemoryStats{
		Alloc:           ms.Alloc,
		TotalAlloc:      ms.TotalAlloc,
		Sys:             ms.Sys,
		NumGC:           ms.NumGC,
		PauseTotalNs:    ms.PauseTotalNs,
		MemoryUsage:     memoryUsage,
		GCPressure:      gcPressure,
		PressureLevel:   pressureLevel,
		SystemMemory:    systemMemory,
		AvailableMemory: availableMemory,
	}
}

// calculatePressureLevel determines the memory pressure level
func (mm *MemoryMonitor) calculatePressureLevel(stats MemoryStats) int {
	usage := stats.MemoryUsage
	gcPressure := stats.GCPressure

	// Combine memory usage and GC pressure to determine overall pressure
	combinedPressure := (usage + gcPressure*50) / 2 // Weight GC pressure more heavily

	switch {
	case combinedPressure >= 90:
		return 3 // Critical
	case combinedPressure >= 75:
		return 2 // High
	case combinedPressure >= 50:
		return 1 // Medium
	default:
		return 0 // Low
	}
}

// calculateGCPressure calculates GC pressure based on frequency and pause times
func (mm *MemoryMonitor) calculateGCPressure(ms *runtime.MemStats) float64 {
	// Simple GC pressure calculation
	// In production, this would be more sophisticated

	if ms.NumGC == 0 {
		return 0
	}

	// Calculate average pause time
	avgPause := float64(ms.PauseTotalNs) / float64(ms.NumGC)

	// Normalize to percentage (assuming 1ms = 1% pressure)
	gcPressure := avgPause / 1000000 // Convert nanoseconds to milliseconds

	if gcPressure > 100 {
		gcPressure = 100
	}

	return gcPressure
}

// shouldTriggerGC determines if GC should be triggered
func (mm *MemoryMonitor) shouldTriggerGC(stats MemoryStats) bool {
	if !mm.forceGCEnabled {
		return false
	}

	// Don't trigger GC too frequently
	if ConsensusSince(mm.lastGCTime) < 30*time.Second {
		return false
	}

	// Trigger GC if memory usage is above threshold
	return stats.MemoryUsage >= mm.gcThreshold*100
}

// triggerGC manually triggers garbage collection
func (mm *MemoryMonitor) triggerGC() {
	mm.logger.Info("Triggering manual garbage collection")

	start := ConsensusNow()
	runtime.GC()
	duration := ConsensusSince(start)

	mm.lastGCTime = ConsensusNow()
	atomic.AddInt64(&mm.gcCount, 1)

	mm.logger.Info("Manual GC completed", LogKeyValue{Key: "duration", Value: duration.String()})

	if mm.onGCTriggered != nil {
		mm.onGCTriggered()
	}
}

// GetPressureLevel returns the current memory pressure level
func (mm *MemoryMonitor) GetPressureLevel() int {
	return int(atomic.LoadInt32(&mm.memoryPressure))
}

// SetMemoryPressureCallback sets a callback for memory pressure events
func (mm *MemoryMonitor) SetMemoryPressureCallback(callback func(level int)) {
	mm.onMemoryPressure = callback
}

// SetGCTriggeredCallback sets a callback for GC trigger events
func (mm *MemoryMonitor) SetGCTriggeredCallback(callback func()) {
	mm.onGCTriggered = callback
}

// SetMemoryTarget sets a target memory usage level
func (mm *MemoryMonitor) SetMemoryTarget(target uint64) {
	mm.memoryTarget = target
}

// GetMemoryTarget returns the current memory target
func (mm *MemoryMonitor) GetMemoryTarget() uint64 {
	return mm.memoryTarget
}

// OptimizeMemory performs memory optimization based on current pressure
func (mm *MemoryMonitor) OptimizeMemory() []string {
	stats := mm.GetMemoryStats()
	optimizations := make([]string, 0)

	switch stats.PressureLevel {
	case 3: // Critical
		optimizations = append(optimizations, "force_gc", "clear_caches", "reduce_batch_sizes")
		mm.triggerGC()

	case 2: // High
		optimizations = append(optimizations, "trigger_gc", "cleanup_expired", "reduce_workers")
		if mm.shouldTriggerGC(stats) {
			mm.triggerGC()
		}

	case 1: // Medium
		optimizations = append(optimizations, "cleanup_expired", "optimize_pools")

	default: // Low
		// No immediate action needed
	}

	if len(optimizations) > 0 {
		mm.logger.Info("Applied memory optimizations",
			LogKeyValue{Key: "optimizations", Value: fmt.Sprintf("%v", optimizations)},
			LogKeyValue{Key: "pressure_level", Value: fmt.Sprintf("%d", stats.PressureLevel)})
	}

	return optimizations
}

// GetOptimizationRecommendations returns optimization recommendations
func (mm *MemoryMonitor) GetOptimizationRecommendations() []OptimizationRecommendation {
	stats := mm.GetMemoryStats()
	recommendations := make([]OptimizationRecommendation, 0)

	if stats.MemoryUsage > 80 {
		recommendations = append(recommendations, OptimizationRecommendation{
			Type:         "memory",
			Priority:     "high",
			Description:  "High memory usage detected",
			Action:       "Consider increasing available memory or reducing cache sizes",
			CurrentValue: stats.MemoryUsage,
			TargetValue:  70.0,
		})
	}

	if stats.GCPressure > 50 {
		recommendations = append(recommendations, OptimizationRecommendation{
			Type:         "gc",
			Priority:     "medium",
			Description:  "High GC pressure detected",
			Action:       "Optimize object allocation patterns or increase heap size",
			CurrentValue: stats.GCPressure,
			TargetValue:  30.0,
		})
	}

	return recommendations
}

// OptimizationRecommendation represents a memory optimization recommendation
type OptimizationRecommendation struct {
	Type         string  `json:"type"`
	Priority     string  `json:"priority"`
	Description  string  `json:"description"`
	Action       string  `json:"action"`
	CurrentValue float64 `json:"current_value"`
	TargetValue  float64 `json:"target_value"`
}

// MemoryAlert represents a memory-related alert
type MemoryAlert struct {
	Level       int         `json:"level"`
	Message     string      `json:"message"`
	Timestamp   time.Time   `json:"timestamp"`
	Stats       MemoryStats `json:"stats"`
	Recommended []string    `json:"recommended_actions"`
}

// CheckAndAlert performs a check and returns alerts if needed
func (mm *MemoryMonitor) CheckAndAlert() *MemoryAlert {
	stats := mm.GetMemoryStats()

	if stats.PressureLevel >= 2 { // High or Critical pressure
		var message string
		var recommended []string

		switch stats.PressureLevel {
		case 3:
			message = "Critical memory pressure detected"
			recommended = []string{"immediate_gc", "clear_all_caches", "reduce_operations"}
		case 2:
			message = "High memory pressure detected"
			recommended = []string{"trigger_gc", "cleanup_expired", "monitor_closely"}
		}

		return &MemoryAlert{
			Level:       stats.PressureLevel,
			Message:     message,
			Timestamp:   ConsensusNow(),
			Stats:       stats,
			Recommended: recommended,
		}
	}

	return nil
}

// EnableForceGC enables or disables forced garbage collection
func (mm *MemoryMonitor) EnableForceGC(enabled bool) {
	mm.forceGCEnabled = enabled
	mm.logger.Info("Force GC setting changed", LogKeyValue{Key: "enabled", Value: fmt.Sprintf("%v", enabled)})
}

// GetGCStats returns garbage collection statistics
func (mm *MemoryMonitor) GetGCStats() *dtypes.GCStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return &dtypes.GCStats{
		NumGC:         ms.NumGC,
		PauseTotalNs:  ms.PauseTotalNs,
		LastGC:        time.Unix(0, int64(ms.LastGC)),
		GCCountManual: atomic.LoadInt64(&mm.gcCount),
		LastManualGC:  mm.lastGCTime,
	}
}
