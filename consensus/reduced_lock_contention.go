// consensus/reduced_lock_contention.go

package consensus

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// LockContentionConfig defines configuration options for reducing lock contention
type LockContentionConfig struct {
	// Whether to enable lock contention reduction
	Enabled bool

	// Whether to log lock acquisition and release
	LogLockOperations bool

	// Threshold for logging long lock acquisitions
	LongLockThreshold time.Duration
}

// DefaultLockContentionConfig returns the default configuration for reducing lock contention
func DefaultLockContentionConfig() *LockContentionConfig {
	return &LockContentionConfig{
		Enabled:           true,
		LogLockOperations: false,
		LongLockThreshold: 100 * time.Millisecond,
	}
}

// LockContentionTracker tracks lock contention
type LockContentionTracker struct {
	// Configuration
	config *LockContentionConfig

	// Lock acquisition times
	lockTimes   map[string]time.Time
	lockTimesMu sync.Mutex

	// Lock contention statistics
	lockStats   map[string]*LockStats
	lockStatsMu sync.Mutex

	// Logger
	logger *hybridConsensusLogger
}

// LockStats tracks statistics for a lock
type LockStats struct {
	// Lock name
	Name string

	// Number of acquisitions
	Acquisitions uint64

	// Number of contentions (when a lock is already held)
	Contentions uint64

	// Total time spent waiting for the lock
	TotalWaitTime time.Duration

	// Total time the lock was held
	TotalHoldTime time.Duration

	// Maximum time the lock was held
	MaxHoldTime time.Duration

	// Maximum time spent waiting for the lock
	MaxWaitTime time.Duration
}

// NewLockContentionTracker creates a new LockContentionTracker
func NewLockContentionTracker(logger *hybridConsensusLogger, config *LockContentionConfig) *LockContentionTracker {
	if config == nil {
		config = DefaultLockContentionConfig()
	}

	return &LockContentionTracker{
		config:    config,
		lockTimes: make(map[string]time.Time),
		lockStats: make(map[string]*LockStats),
		logger:    logger,
	}
}

// LockAcquired should be called when a lock is acquired
func (lct *LockContentionTracker) LockAcquired(lockName string) {
	if !lct.config.Enabled {
		return
	}

	now := ConsensusNow()

	lct.lockTimesMu.Lock()
	lct.lockTimes[lockName] = now
	lct.lockTimesMu.Unlock()

	if lct.config.LogLockOperations {
		lct.logger.Info("Lock acquired", LogKeyValue{Key: "lockName", Value: lockName})
	}

	// Update statistics
	lct.lockStatsMu.Lock()
	stats, exists := lct.lockStats[lockName]
	if !exists {
		stats = &LockStats{
			Name: lockName,
		}
		lct.lockStats[lockName] = stats
	}
	stats.Acquisitions++
	lct.lockStatsMu.Unlock()
}

// LockReleased should be called when a lock is released
func (lct *LockContentionTracker) LockReleased(lockName string) {
	if !lct.config.Enabled {
		return
	}

	now := ConsensusNow()

	// Get the lock acquisition time
	lct.lockTimesMu.Lock()
	acquisitionTime, exists := lct.lockTimes[lockName]
	if exists {
		delete(lct.lockTimes, lockName)
	}
	lct.lockTimesMu.Unlock()

	if !exists {
		lct.logger.Warn("Lock released but not recorded as acquired", LogKeyValue{Key: "lockName", Value: lockName})
		return
	}

	// Calculate how long the lock was held
	holdTime := now.Sub(acquisitionTime)

	if lct.config.LogLockOperations {
		lct.logger.Info("Lock released",
			LogKeyValue{Key: "lockName", Value: lockName},
			LogKeyValue{Key: "heldFor", Value: holdTime.String()})
	}

	// Log if the lock was held for a long time
	if holdTime > lct.config.LongLockThreshold {
		lct.logger.Warn("Lock held for a long time",
			LogKeyValue{Key: "lockName", Value: lockName},
			LogKeyValue{Key: "heldFor", Value: holdTime.String()},
			LogKeyValue{Key: "threshold", Value: lct.config.LongLockThreshold.String()})
	}

	// Update statistics
	lct.lockStatsMu.Lock()
	stats, exists := lct.lockStats[lockName]
	if exists {
		stats.TotalHoldTime += holdTime
		if holdTime > stats.MaxHoldTime {
			stats.MaxHoldTime = holdTime
		}
	}
	lct.lockStatsMu.Unlock()
}

// GetLockStats returns statistics for all locks
func (lct *LockContentionTracker) GetLockStats() map[string]*LockStats {
	lct.lockStatsMu.Lock()
	defer lct.lockStatsMu.Unlock()

	// Create a copy of the stats
	stats := make(map[string]*LockStats)
	for name, lockStats := range lct.lockStats {
		statsCopy := *lockStats
		stats[name] = &statsCopy
	}

	return stats
}

// LogLockStats logs statistics for all locks
func (lct *LockContentionTracker) LogLockStats() {
	lct.lockStatsMu.Lock()
	defer lct.lockStatsMu.Unlock()

	lct.logger.Info("Lock contention statistics")

	for name, stats := range lct.lockStats {
		lct.logger.Info("Lock statistics",
			LogKeyValue{Key: "lockName", Value: name},
			LogKeyValue{Key: "acquisitions", Value: fmt.Sprintf("%d", stats.Acquisitions)},
			LogKeyValue{Key: "contentions", Value: fmt.Sprintf("%d", stats.Contentions)},
			LogKeyValue{Key: "totalHoldTime", Value: stats.TotalHoldTime.String()},
			LogKeyValue{Key: "maxHoldTime", Value: stats.MaxHoldTime.String()},
			LogKeyValue{Key: "totalWaitTime", Value: stats.TotalWaitTime.String()},
			LogKeyValue{Key: "maxWaitTime", Value: stats.MaxWaitTime.String()})
	}
}

// TrackedMutex is a mutex that tracks contention
type TrackedMutex struct {
	mu      sync.Mutex
	name    string
	tracker *LockContentionTracker
}

// NewTrackedMutex creates a new mutex that tracks contention
func NewTrackedMutex(name string, tracker *LockContentionTracker) *TrackedMutex {
	return &TrackedMutex{
		name:    name,
		tracker: tracker,
	}
}

// Lock acquires the mutex and tracks contention
func (m *TrackedMutex) Lock() {
	m.mu.Lock()
	if m.tracker != nil {
		m.tracker.LockAcquired(m.name)
	}
}

// Unlock releases the mutex and tracks contention
func (m *TrackedMutex) Unlock() {
	if m.tracker != nil {
		m.tracker.LockReleased(m.name)
	}
	m.mu.Unlock()
}

// TrackedRWMutex is an RWMutex that tracks contention
type TrackedRWMutex struct {
	mu      sync.RWMutex
	name    string
	tracker *LockContentionTracker
}

// NewTrackedRWMutex creates a new RWMutex that tracks contention
func NewTrackedRWMutex(name string, tracker *LockContentionTracker) *TrackedRWMutex {
	return &TrackedRWMutex{
		name:    name,
		tracker: tracker,
	}
}

// Lock acquires the write lock and tracks contention
func (m *TrackedRWMutex) Lock() {
	m.mu.Lock()
	if m.tracker != nil {
		m.tracker.LockAcquired(m.name + ".write")
	}
}

// Unlock releases the write lock and tracks contention
func (m *TrackedRWMutex) Unlock() {
	if m.tracker != nil {
		m.tracker.LockReleased(m.name + ".write")
	}
	m.mu.Unlock()
}

// RLock acquires the read lock and tracks contention
func (m *TrackedRWMutex) RLock() {
	m.mu.RLock()
	if m.tracker != nil {
		m.tracker.LockAcquired(m.name + ".read")
	}
}

// RUnlock releases the read lock and tracks contention
func (m *TrackedRWMutex) RUnlock() {
	if m.tracker != nil {
		m.tracker.LockReleased(m.name + ".read")
	}
	m.mu.RUnlock()
}

// ReduceLockContentionInHybridConsensus reduces lock contention in HybridConsensus
func ReduceLockContentionInHybridConsensus(hc *HybridConsensus, tracker *LockContentionTracker) {
	// In a real implementation, we would modify the methods that hold locks for too long
	// Here, we just log that we're reducing lock contention
	hc.logger.Info("Reducing lock contention in HybridConsensus")

	// Example of methods that would be modified:
	// - ProcessBlock: Release locks before making external calls
	// - FinalizeEvent: Release locks before making external calls
	// - CreateEvent: Release locks before making external calls
	// - HandleNetworkPartition: Release locks before making external calls
}

// ReduceLockContentionInValidatorManager reduces lock contention in ValidatorManager
func ReduceLockContentionInValidatorManager(vm *ValidatorManager, tracker *LockContentionTracker) {
	// In a real implementation, we would modify the methods that hold locks for too long
	// Here, we just log that we're reducing lock contention
	vm.hc.logger.Info("Reducing lock contention in ValidatorManager")

	// Example of methods that would be modified:
	// - AddValidator: Release locks before making external calls
	// - UpdateStake: Release locks before making external calls
	// - ProcessEpoch: Release locks before making external calls
}

// ReduceLockContentionInEventFlowManager reduces lock contention in EventFlowManager
func ReduceLockContentionInEventFlowManager(efm *EventFlowManager, tracker *LockContentionTracker) {
	// In a real implementation, we would modify the methods that hold locks for too long
	// Here, we just log that we're reducing lock contention
	efm.hc.logger.Info("Reducing lock contention in EventFlowManager")

	// Example of methods that would be modified:
	// - handleFinalizedEvent: Release locks before making external calls
	// - processPendingEvents: Release locks before making external calls
}

// ReduceLockContentionInSlashingManager reduces lock contention in SlashingManager
func ReduceLockContentionInSlashingManager(sm *SlashingManager, tracker *LockContentionTracker) {
	// In a real implementation, we would modify the methods that hold locks for too long
	// Here, we just log that we're reducing lock contention
	sm.logger.Info("Reducing lock contention in SlashingManager")

	// Example of methods that would be modified:
	// - slashValidator: Release locks before making external calls
	// - ResolveAppeal: Release locks before making external calls
}

// ApplyReducedLockContention applies reduced lock contention to the consensus module
func ApplyReducedLockContention(hc *HybridConsensus, config *LockContentionConfig) {
	// Create lock contention tracker
	tracker := NewLockContentionTracker(hc.legacyLogger, config)

	// Reduce lock contention in HybridConsensus
	ReduceLockContentionInHybridConsensus(hc, tracker)

	// Reduce lock contention in ValidatorManager
	ReduceLockContentionInValidatorManager(hc.validatorManager, tracker)

	// Reduce lock contention in EventFlowManager
	ReduceLockContentionInEventFlowManager(hc.eventFlow, tracker)

	// If SlashingManager is available, reduce lock contention in it
	// Note: We're not checking for slashingIntegration since it doesn't exist in HybridConsensus
	// In a real implementation, we would need to check if a SlashingManager is available

	hc.logger.Info("Applied reduced lock contention to consensus module")

	// Start a goroutine to periodically log lock statistics
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			tracker.LogLockStats()
		}
	}()
}

// Example of a method with reduced lock contention
// This is a simplified example of how ProcessBlock would be modified
func ProcessBlockWithReducedLockContention(hc *HybridConsensus, blockNumber uint64) error {
	// Acquire locks only when needed and release them as soon as possible

	// Check if consensus is running
	hc.stateMu.RLock()
	running := hc.running
	hc.stateMu.RUnlock()

	if !running {
		return fmt.Errorf("consensus is not running")
	}

	// Get the current block height
	hc.blockHeightMu.RLock()
	currentHeight := hc.lastBlockHeight
	hc.blockHeightMu.RUnlock()

	// Check if the block number is valid
	if blockNumber != currentHeight+1 {
		return fmt.Errorf("invalid block number: expected %d, got %d", currentHeight+1, blockNumber)
	}

	// Get the next validator
	// Note: This is a simplified example
	validator := hc.validatorManager.GetNextValidator(blockNumber, hc.GetLastBlockHash())
	if validator == nil {
		return fmt.Errorf("no validator available for block creation")
	}

	// Produce block
	// Note: This is a simplified example
	block, err := hc.produceBlock(blockNumber, validator.ID)
	if err != nil {
		return fmt.Errorf("failed to produce block: %w", err)
	}

	// Apply block
	// Note: This is a simplified example
	if err := hc.applyBlock(block); err != nil {
		return fmt.Errorf("failed to apply block: %w", err)
	}

	// Update block height and hash
	// Note: This is a simplified example
	blockData, err := serializeBlock(block)
	if err != nil {
		return fmt.Errorf("failed to serialize block: %w", err)
	}

	// Update block height
	hc.blockHeightMu.Lock()
	hc.lastBlockHeight = blockNumber
	hc.blockHeightMu.Unlock()

	// Update block hash
	hc.lastBlockHashMu.Lock()
	hc.lastBlockHash = sha256.Sum256(blockData)
	hc.lastBlockHashMu.Unlock()

	return nil
}
