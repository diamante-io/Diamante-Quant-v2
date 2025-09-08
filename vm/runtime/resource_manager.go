// Package runtime provides resource management for the hybrid VM
package runtime

import (
	"fmt"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// ResourceManager manages runtime resources and concurrency
type ResourceManager struct {
	maxConcurrency int
	semaphore      chan struct{}
	logger         *logrus.Logger
	mu             sync.RWMutex
	usage          map[string]ResourceUsage
	limits         map[string]ResourceLimit
}

// ResourceUsage tracks resource usage for a contract
type ResourceUsage struct {
	MemoryMB      uint64
	CPUMillicores uint64
	StorageMB     uint64
	StartTime     time.Time
}

// ResourceLimit defines resource limits for contracts
type ResourceLimit struct {
	MaxMemoryMB   uint64
	MaxCPUPercent float64
	MaxStorageMB  uint64
	MaxGasPerSec  uint64
}

// NewResourceManager creates a new resource manager
func NewResourceManager(maxConcurrency int, logger *logrus.Logger) *ResourceManager {
	if maxConcurrency <= 0 {
		maxConcurrency = 100 // Default
	}

	return &ResourceManager{
		maxConcurrency: maxConcurrency,
		semaphore:      make(chan struct{}, maxConcurrency),
		logger:         logger,
		usage:          make(map[string]ResourceUsage),
		limits:         make(map[string]ResourceLimit),
	}
}

// AcquireResources acquires resources for execution
func (rm *ResourceManager) AcquireResources(contractID string, required ResourceRequirements) error {
	// Try to acquire semaphore slot
	select {
	case rm.semaphore <- struct{}{}:
		// Track resource usage
		rm.mu.Lock()
		rm.usage[contractID] = ResourceUsage{
			MemoryMB:      uint64(required.MemoryMB),
			CPUMillicores: uint64(required.CPUCores * 1000),
			StorageMB:     uint64(required.StorageMB),
			StartTime:     consensus.ConsensusNow(),
		}
		rm.mu.Unlock()

		rm.logger.WithFields(logrus.Fields{
			"contractID": contractID,
			"memory":     required.MemoryMB,
			"cpu":        required.CPUCores,
			"storage":    required.StorageMB,
		}).Debug("Resources acquired")

		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("resource acquisition timeout for contract %s", contractID)
	}
}

// ReleaseResources releases resources after execution
func (rm *ResourceManager) ReleaseResources(contractID string) {
	// Release semaphore slot
	select {
	case <-rm.semaphore:
		// Remove resource tracking
		rm.mu.Lock()
		usage, exists := rm.usage[contractID]
		delete(rm.usage, contractID)
		rm.mu.Unlock()

		if exists {
			duration := time.Since(usage.StartTime)
			rm.logger.WithFields(logrus.Fields{
				"contractID": contractID,
				"duration":   duration,
				"memory":     usage.MemoryMB,
				"cpu":        usage.CPUMillicores,
			}).Debug("Resources released")
		}
	default:
		// This shouldn't happen, but log if it does
		rm.logger.Warn("Attempted to release resources that weren't acquired", "contractID", contractID)
	}
}

// CheckAndReserve checks if resources are available and reserves them
func (rm *ResourceManager) CheckAndReserve(contractID string, required ResourceRequirements) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check against limits if set
	if limit, exists := rm.limits[contractID]; exists {
		if uint64(required.MemoryMB) > limit.MaxMemoryMB {
			return fmt.Errorf("memory requirement %d MB exceeds limit %d MB", required.MemoryMB, limit.MaxMemoryMB)
		}
		if uint64(required.StorageMB) > limit.MaxStorageMB {
			return fmt.Errorf("storage requirement %d MB exceeds limit %d MB", required.StorageMB, limit.MaxStorageMB)
		}
	}

	// Check total system usage
	totalMemory := uint64(0)
	totalCPU := uint64(0)
	for _, usage := range rm.usage {
		totalMemory += usage.MemoryMB
		totalCPU += usage.CPUMillicores
	}

	// Simple check - in production, this would check against actual system limits
	const maxSystemMemoryMB = 32 * 1024      // 32GB
	const maxSystemCPUMillicores = 16 * 1000 // 16 cores

	if totalMemory+uint64(required.MemoryMB) > maxSystemMemoryMB {
		return fmt.Errorf("insufficient system memory: %d MB requested, %d MB available",
			required.MemoryMB, maxSystemMemoryMB-totalMemory)
	}

	if totalCPU+uint64(required.CPUCores*1000) > maxSystemCPUMillicores {
		return fmt.Errorf("insufficient system CPU: %.2f cores requested, %.2f cores available",
			required.CPUCores, float64(maxSystemCPUMillicores-totalCPU)/1000)
	}

	return nil
}

// SetContractLimits sets resource limits for a specific contract
func (rm *ResourceManager) SetContractLimits(contractID string, limit ResourceLimit) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.limits[contractID] = limit
	rm.logger.WithFields(logrus.Fields{
		"contractID":   contractID,
		"maxMemoryMB":  limit.MaxMemoryMB,
		"maxStorageMB": limit.MaxStorageMB,
	}).Info("Contract resource limits set")
}

// GetUsage returns current resource usage
func (rm *ResourceManager) GetUsage() map[string]ResourceUsage {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Return a copy
	usage := make(map[string]ResourceUsage)
	for k, v := range rm.usage {
		usage[k] = v
	}
	return usage
}

// GetMetrics returns resource manager metrics
func (rm *ResourceManager) GetMetrics() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	totalMemory := uint64(0)
	totalCPU := uint64(0)
	totalStorage := uint64(0)
	activeContracts := len(rm.usage)

	for _, usage := range rm.usage {
		totalMemory += usage.MemoryMB
		totalCPU += usage.CPUMillicores
		totalStorage += usage.StorageMB
	}

	return map[string]interface{}{
		"active_contracts":     activeContracts,
		"total_memory_mb":      totalMemory,
		"total_cpu_millicores": totalCPU,
		"total_storage_mb":     totalStorage,
		"max_concurrency":      rm.maxConcurrency,
		"available_slots":      rm.maxConcurrency - activeContracts,
		"utilization_percent":  float64(activeContracts) / float64(rm.maxConcurrency) * 100,
	}
}

// Stop stops the resource manager
func (rm *ResourceManager) Stop() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Wait for all resources to be released
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for len(rm.usage) > 0 {
		select {
		case <-timeout:
			rm.logger.Warn("Timeout waiting for resources to be released",
				"remaining", len(rm.usage))
			// Force release
			for contractID := range rm.usage {
				rm.logger.Warn("Force releasing resources", "contractID", contractID)
			}
			rm.usage = make(map[string]ResourceUsage)
			close(rm.semaphore)
			return fmt.Errorf("timeout waiting for resource release")
		case <-ticker.C:
			// Continue waiting
		}
	}

	close(rm.semaphore)
	rm.logger.Info("Resource manager stopped cleanly")
	return nil
}

// MonitorResources monitors resource usage and enforces limits
func (rm *ResourceManager) MonitorResources() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rm.mu.RLock()

		// Check for contracts exceeding time limits
		now := consensus.ConsensusNow()
		for contractID, usage := range rm.usage {
			duration := now.Sub(usage.StartTime)
			if duration > 5*time.Minute {
				rm.logger.Warn("Contract execution exceeding time limit",
					"contractID", contractID,
					"duration", duration)
			}
		}

		// Log current usage
		if len(rm.usage) > 0 {
			rm.logger.WithFields(logrus.Fields{
				"activeContracts": len(rm.usage),
				"utilization":     fmt.Sprintf("%.1f%%", float64(len(rm.usage))/float64(rm.maxConcurrency)*100),
			}).Debug("Resource usage status")
		}

		rm.mu.RUnlock()
	}
}
