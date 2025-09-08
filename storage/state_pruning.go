// storage/state_pruning.go

package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// PruningPolicy defines how state pruning should be performed
type PruningPolicy int

const (
	// NoPruning keeps all historical state
	NoPruning PruningPolicy = iota

	// ArchivePruning keeps full state at checkpoint intervals, prunes in between
	ArchivePruning

	// FullPruning keeps only recent state plus periodic checkpoints
	FullPruning
)

// PruningConfig holds configuration for the state pruning mechanism
type PruningConfig struct {
	// Policy determines the pruning strategy
	Policy PruningPolicy

	// BlocksToKeep is the number of recent blocks for which to keep full state
	BlocksToKeep uint64

	// CheckpointInterval defines how often to create state checkpoints
	CheckpointInterval uint64

	// PruningInterval defines how often to run the pruning process (in blocks)
	PruningInterval uint64

	// MaxPruningBatchSize is the maximum number of entries to prune in a single batch
	MaxPruningBatchSize int

	// EnableCompaction determines whether to compact the database after pruning
	EnableCompaction bool

	// CompactionThreshold is the minimum number of pruned entries to trigger compaction
	CompactionThreshold int

	// RetentionPolicy allows keeping specific types of data longer
	RetentionPolicy map[string]uint64
}

// DefaultPruningConfig returns a default configuration for state pruning
func DefaultPruningConfig() *PruningConfig {
	return &PruningConfig{
		Policy:              ArchivePruning,
		BlocksToKeep:        10000,
		CheckpointInterval:  1000,
		PruningInterval:     100,
		MaxPruningBatchSize: 1000,
		EnableCompaction:    true,
		CompactionThreshold: 10000,
		RetentionPolicy: map[string]uint64{
			"accounts":     50000,  // Keep account data for 50000 blocks
			"transactions": 100000, // Keep transaction data for 100000 blocks
		},
	}
}

// PruningMetrics tracks statistics about the pruning process
type PruningMetrics struct {
	TotalPruningRuns       uint64
	TotalEntriesPruned     uint64
	TotalBytesReclaimed    uint64
	LastPruningTime        time.Time
	LastPruningDuration    time.Duration
	LastCompactionTime     time.Time
	LastCompactionBytes    uint64
	LastCompactionDuration time.Duration
	AvgPruningDuration     time.Duration
	PruningErrors          uint64
}

// StatePruningManager handles the pruning of historical state data
type StatePruningManager struct {
	config  *PruningConfig
	logger  *logrus.Logger
	metrics PruningMetrics
	db      StateDB

	// Current state tracking
	currentBlockHeight uint64
	lastPrunedBlock    uint64
	lastCheckpoint     uint64

	// Mutex for thread safety
	mu sync.RWMutex

	// Control channels
	stopChan chan struct{}
	running  bool
}

// StateDB defines the interface for database operations needed by the pruning manager
type StateDB interface {
	// Get retrieves a value from the database
	Get(key []byte) ([]byte, error)

	// Put stores a key-value pair in the database
	Put(key, value []byte) error

	// Delete removes a key-value pair from the database
	Delete(key []byte) error

	// NewIterator creates an iterator over a key range
	NewIterator(start, end []byte) StateIterator

	// Compact runs database compaction on a key range
	Compact(start, end []byte) error

	// GetStats returns database statistics
	GetStats() *StorageStats
}

// StateIterator defines the interface for iterating over database entries
type StateIterator interface {
	// Next advances the iterator to the next key
	Next() bool

	// Key returns the current key
	Key() []byte

	// Value returns the current value
	Value() []byte

	// Error returns any accumulated error
	Error() error

	// Close releases resources associated with the iterator
	Close()
}

// NewStatePruningManager creates a new state pruning manager
func NewStatePruningManager(db StateDB, config *PruningConfig, logger *logrus.Logger) *StatePruningManager {
	if config == nil {
		config = DefaultPruningConfig()
	}

	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &StatePruningManager{
		config:             config,
		logger:             logger,
		db:                 db,
		currentBlockHeight: 0,
		lastPrunedBlock:    0,
		lastCheckpoint:     0,
		stopChan:           make(chan struct{}),
		running:            false,
	}
}

// Start begins the background pruning process
func (pm *StatePruningManager) Start() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.running {
		return errors.New("pruning manager is already running")
	}

	pm.running = true
	pm.stopChan = make(chan struct{})

	// Start background pruning goroutine
	go pm.pruningLoop()

	pm.logger.Info("State pruning manager started",
		"policy", pm.config.Policy,
		"blocksToKeep", pm.config.BlocksToKeep,
		"checkpointInterval", pm.config.CheckpointInterval)

	return nil
}

// Stop halts the background pruning process
func (pm *StatePruningManager) Stop() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.running {
		return errors.New("pruning manager is not running")
	}

	close(pm.stopChan)
	pm.running = false

	pm.logger.Info("State pruning manager stopped")
	return nil
}

// UpdateBlockHeight updates the current block height and triggers pruning if needed
func (pm *StatePruningManager) UpdateBlockHeight(height uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.currentBlockHeight = height

	// Check if we need to create a checkpoint
	if pm.config.CheckpointInterval > 0 && height%pm.config.CheckpointInterval == 0 {
		pm.lastCheckpoint = height
		pm.logger.Info("Created state checkpoint", "height", height)
	}

	// Check if we need to trigger pruning
	if pm.config.Policy != NoPruning &&
		pm.config.PruningInterval > 0 &&
		height%pm.config.PruningInterval == 0 &&
		height > pm.config.BlocksToKeep {
		go pm.triggerPruning(height)
	}
}

// triggerPruning initiates a pruning operation
func (pm *StatePruningManager) triggerPruning(height uint64) {
	pm.mu.RLock()
	if !pm.running || height <= pm.lastPrunedBlock {
		pm.mu.RUnlock()
		return
	}
	pm.mu.RUnlock()

	pm.logger.Info("Triggering state pruning", "currentHeight", height)

	// Calculate the target height to prune up to
	targetHeight := height - pm.config.BlocksToKeep

	// Don't prune beyond the last checkpoint
	if targetHeight > pm.lastCheckpoint {
		targetHeight = pm.lastCheckpoint
	}

	// Don't prune if there's nothing to prune
	if targetHeight <= pm.lastPrunedBlock {
		pm.logger.Info("No state to prune",
			"targetHeight", targetHeight,
			"lastPrunedBlock", pm.lastPrunedBlock)
		return
	}

	// Perform the pruning
	err := pm.pruneStateToHeight(targetHeight)
	if err != nil {
		pm.logger.Error("State pruning failed", "error", err)
		pm.mu.Lock()
		pm.metrics.PruningErrors++
		pm.mu.Unlock()
		return
	}

	// Update the last pruned block
	pm.mu.Lock()
	pm.lastPrunedBlock = targetHeight
	pm.mu.Unlock()

	pm.logger.Info("State pruning completed",
		"prunedToHeight", targetHeight,
		"currentHeight", height)
}

// pruningLoop runs periodic pruning operations
func (pm *StatePruningManager) pruningLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if we need to run pruning
			pm.mu.RLock()
			currentHeight := pm.currentBlockHeight
			lastPruned := pm.lastPrunedBlock
			shouldPrune := pm.running &&
				pm.config.Policy != NoPruning &&
				currentHeight > pm.config.BlocksToKeep &&
				currentHeight > lastPruned+pm.config.PruningInterval
			pm.mu.RUnlock()

			if shouldPrune {
				pm.triggerPruning(currentHeight)
			}

			// Check if we need to run compaction
			if pm.config.EnableCompaction {
				pm.checkAndTriggerCompaction()
			}

		case <-pm.stopChan:
			return
		}
	}
}

// pruneStateToHeight prunes state data up to the specified height
func (pm *StatePruningManager) pruneStateToHeight(height uint64) error {
	startTime := time.Now()

	pm.logger.Info("Starting state pruning", "targetHeight", height)

	// Use the concrete pruning implementation to remove data
	impl := NewStatePruningImpl(pm, pm.logger)

	config := pm.GetConfig()

	totalPruned := 0
	bytesReclaimed := uint64(0)

	// Iterate through each configured state type and prune accordingly
	for stateType, retention := range config.RetentionPolicy {
		effectiveHeight := height
		if retention > 0 {
			retentionHeight := pm.currentBlockHeight - retention
			if retentionHeight < effectiveHeight {
				effectiveHeight = retentionHeight
			}
		}

		if effectiveHeight == 0 {
			continue
		}

		pruned, bytes, err := impl.pruneStateType(StateType(stateType), effectiveHeight)
		if err != nil {
			pm.logger.Error("Failed to prune state type",
				"stateType", stateType,
				"error", err)
			pm.mu.Lock()
			pm.metrics.PruningErrors++
			pm.mu.Unlock()
			continue
		}

		totalPruned += pruned
		bytesReclaimed += bytes
	}

	// Update metrics
	pm.mu.Lock()
	pm.metrics.TotalPruningRuns++
	pm.metrics.TotalEntriesPruned += uint64(totalPruned)
	pm.metrics.TotalBytesReclaimed += bytesReclaimed
	pm.metrics.LastPruningTime = time.Now()
	pm.metrics.LastPruningDuration = time.Since(startTime)

	if pm.metrics.AvgPruningDuration == 0 {
		pm.metrics.AvgPruningDuration = pm.metrics.LastPruningDuration
	} else {
		pm.metrics.AvgPruningDuration = (pm.metrics.AvgPruningDuration*9 + pm.metrics.LastPruningDuration) / 10
	}
	pm.mu.Unlock()

	pm.logger.Info("State pruning completed",
		"entriesPruned", totalPruned,
		"bytesReclaimed", bytesReclaimed,
		"duration", time.Since(startTime))

	return nil
}

// checkAndTriggerCompaction checks if database compaction is needed and triggers it
func (pm *StatePruningManager) checkAndTriggerCompaction() {
	pm.mu.RLock()
	shouldCompact := pm.metrics.TotalEntriesPruned > uint64(pm.config.CompactionThreshold)
	pm.mu.RUnlock()

	if !shouldCompact {
		return
	}

	pm.logger.Info("Starting database compaction")
	startTime := time.Now()

	// Get initial database stats
	initialStats := pm.db.GetStats()
	initialSize := uint64(0)
	if initialStats != nil {
		// Use memory usage as a proxy for disk size
		initialSize = uint64(initialStats.MemoryUsageMB * 1024 * 1024)
	}

	// Compact each state type's key range
	stateTypes := []struct {
		name   string
		prefix []byte
	}{
		{"accounts", []byte("acc:")},
		{"transactions", []byte("tx:")},
		{"blocks", []byte("blk:")},
		{"contracts", []byte("con:")},
		{"contract_storage", []byte("cst:")},
		{"state", []byte("state:")},
	}

	compactionErrors := 0
	for _, stateType := range stateTypes {
		// Create key range for this state type
		startKey := stateType.prefix
		endKey := append([]byte{}, stateType.prefix...)
		if len(endKey) > 0 {
			endKey[len(endKey)-1]++ // Increment last byte to create exclusive end range
		}

		// Perform compaction for this range
		if err := pm.db.Compact(startKey, endKey); err != nil {
			pm.logger.WithError(err).WithField("stateType", stateType.name).Error("Failed to compact state type")
			compactionErrors++
		} else {
			pm.logger.WithField("stateType", stateType.name).Debug("Compacted state type")
		}
	}

	// Get final database stats
	finalStats := pm.db.GetStats()
	finalSize := uint64(0)
	if finalStats != nil {
		// Use memory usage as a proxy for disk size
		finalSize = uint64(finalStats.MemoryUsageMB * 1024 * 1024)
	}

	// Calculate bytes reclaimed (if initial size was larger)
	compactedBytes := uint64(0)
	if initialSize > finalSize {
		compactedBytes = initialSize - finalSize
	}

	// Update metrics
	pm.mu.Lock()
	pm.metrics.LastCompactionTime = time.Now()
	pm.metrics.LastCompactionDuration = time.Since(startTime)
	pm.metrics.LastCompactionBytes = compactedBytes
	// Reset pruned entries counter after compaction
	pm.metrics.TotalEntriesPruned = 0
	pm.mu.Unlock()

	if compactionErrors > 0 {
		pm.logger.WithFields(logrus.Fields{
			"bytesCompacted":   compactedBytes,
			"duration":         time.Since(startTime),
			"compactionErrors": compactionErrors,
		}).Warn("Database compaction completed with errors")
	} else {
		pm.logger.WithFields(logrus.Fields{
			"bytesCompacted": compactedBytes,
			"duration":       time.Since(startTime),
		}).Info("Database compaction completed successfully")
	}
}

// GetMetrics returns the current pruning metrics
func (pm *StatePruningManager) GetMetrics() PruningMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Return a copy to avoid race conditions
	return PruningMetrics{
		TotalPruningRuns:       pm.metrics.TotalPruningRuns,
		TotalEntriesPruned:     pm.metrics.TotalEntriesPruned,
		TotalBytesReclaimed:    pm.metrics.TotalBytesReclaimed,
		LastPruningTime:        pm.metrics.LastPruningTime,
		LastPruningDuration:    pm.metrics.LastPruningDuration,
		LastCompactionTime:     pm.metrics.LastCompactionTime,
		LastCompactionBytes:    pm.metrics.LastCompactionBytes,
		LastCompactionDuration: pm.metrics.LastCompactionDuration,
		AvgPruningDuration:     pm.metrics.AvgPruningDuration,
		PruningErrors:          pm.metrics.PruningErrors,
	}
}

// GetConfig returns the current pruning configuration
func (pm *StatePruningManager) GetConfig() *PruningConfig {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Return a copy to avoid race conditions
	config := *pm.config

	// Deep copy the retention policy map
	config.RetentionPolicy = make(map[string]uint64)
	for k, v := range pm.config.RetentionPolicy {
		config.RetentionPolicy[k] = v
	}

	return &config
}

// UpdateConfig updates the pruning configuration
func (pm *StatePruningManager) UpdateConfig(config *PruningConfig) error {
	if config == nil {
		return errors.New("config cannot be nil")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Validate the new configuration
	if config.BlocksToKeep < config.CheckpointInterval {
		return fmt.Errorf("blocksToKeep (%d) must be >= checkpointInterval (%d)",
			config.BlocksToKeep, config.CheckpointInterval)
	}

	// Apply the new configuration
	pm.config = config

	pm.logger.Info("Pruning configuration updated",
		"policy", config.Policy,
		"blocksToKeep", config.BlocksToKeep,
		"checkpointInterval", config.CheckpointInterval)

	return nil
}

// ForceCompaction triggers an immediate database compaction
func (pm *StatePruningManager) ForceCompaction() error {
	pm.logger.Info("Forcing database compaction")
	startTime := time.Now()

	// Get initial database stats
	initialStats := pm.db.GetStats()
	initialSize := uint64(0)
	if initialStats != nil {
		// Use memory usage as a proxy for disk size
		initialSize = uint64(initialStats.MemoryUsageMB * 1024 * 1024)
	}

	// Define all key ranges to compact
	keyRanges := []struct {
		name  string
		start []byte
		end   []byte
	}{
		// State type prefixes
		{"accounts", []byte("acc:"), []byte("acd:")},
		{"transactions", []byte("tx:"), []byte("ty:")},
		{"blocks", []byte("blk:"), []byte("bll:")},
		{"contracts", []byte("con:"), []byte("coo:")},
		{"contract_storage", []byte("cst:"), []byte("csu:")},
		{"state", []byte("state:"), []byte("statf:")},
		// Owner index
		{"owner_index", []byte("owner:index:"), []byte("owner:indey:")},
		// Generic contract data
		{"contract_data", []byte("contract:"), []byte("contracu:")},
	}

	var lastError error
	successfulCompactions := 0

	// Compact each key range
	for _, kr := range keyRanges {
		pm.logger.WithField("keyRange", kr.name).Debug("Compacting key range")

		if err := pm.db.Compact(kr.start, kr.end); err != nil {
			pm.logger.WithError(err).WithField("keyRange", kr.name).Error("Failed to compact key range")
			lastError = fmt.Errorf("failed to compact %s: %w", kr.name, err)
		} else {
			successfulCompactions++
		}
	}

	// Try a full database compaction if supported
	// Using nil start and end keys typically means compact everything
	if err := pm.db.Compact(nil, nil); err != nil {
		pm.logger.WithError(err).Debug("Full database compaction not supported or failed")
	} else {
		pm.logger.Info("Full database compaction completed")
	}

	// Get final database stats
	finalStats := pm.db.GetStats()
	finalSize := uint64(0)
	if finalStats != nil {
		// Use memory usage as a proxy for disk size
		finalSize = uint64(finalStats.MemoryUsageMB * 1024 * 1024)
	}

	// Calculate bytes reclaimed
	compactedBytes := uint64(0)
	if initialSize > finalSize {
		compactedBytes = initialSize - finalSize
	} else if successfulCompactions > 0 {
		// Estimate if we can't get actual size difference
		compactedBytes = uint64(successfulCompactions) * 100 * 1024 // 100KB per range as estimate
	}

	// Update metrics
	pm.mu.Lock()
	pm.metrics.LastCompactionTime = time.Now()
	pm.metrics.LastCompactionDuration = time.Since(startTime)
	pm.metrics.LastCompactionBytes = compactedBytes
	// Reset pruned entries counter after compaction
	pm.metrics.TotalEntriesPruned = 0
	pm.mu.Unlock()

	pm.logger.WithFields(logrus.Fields{
		"bytesCompacted":        compactedBytes,
		"duration":              time.Since(startTime),
		"successfulCompactions": successfulCompactions,
		"totalRanges":           len(keyRanges),
	}).Info("Forced database compaction completed")

	if lastError != nil && successfulCompactions == 0 {
		return fmt.Errorf("all compactions failed: %w", lastError)
	}

	return nil
}

// ForcePruning triggers an immediate pruning operation
func (pm *StatePruningManager) ForcePruning(targetHeight uint64) error {
	pm.mu.RLock()
	currentHeight := pm.currentBlockHeight
	pm.mu.RUnlock()

	if targetHeight >= currentHeight {
		return fmt.Errorf("target height (%d) must be less than current height (%d)",
			targetHeight, currentHeight)
	}

	if targetHeight <= pm.lastPrunedBlock {
		return fmt.Errorf("target height (%d) must be greater than last pruned block (%d)",
			targetHeight, pm.lastPrunedBlock)
	}

	pm.logger.Info("Forcing state pruning", "targetHeight", targetHeight)

	err := pm.pruneStateToHeight(targetHeight)
	if err != nil {
		return fmt.Errorf("forced pruning failed: %w", err)
	}

	pm.mu.Lock()
	pm.lastPrunedBlock = targetHeight
	pm.mu.Unlock()

	return nil
}
