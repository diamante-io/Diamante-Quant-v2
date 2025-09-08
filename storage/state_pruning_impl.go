// storage/state_pruning_impl.go

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// StateType represents different types of state data
type StateType string

const (
	// StateTypeAccount represents account state data
	StateTypeAccount StateType = "account"

	// StateTypeTransaction represents transaction data
	StateTypeTransaction StateType = "transaction"

	// StateTypeBlock represents block data
	StateTypeBlock StateType = "block"

	// StateTypeContract represents smart contract data
	StateTypeContract StateType = "contract"

	// StateTypeContractStorage represents smart contract storage data
	StateTypeContractStorage StateType = "contract_storage"
)

// KeyPrefix constants for different state types
var (
	AccountPrefix         = []byte("acc:")
	TransactionPrefix     = []byte("tx:")
	BlockPrefix           = []byte("blk:")
	ContractPrefix        = []byte("con:")
	ContractStoragePrefix = []byte("cst:")
)

// StateTypeStats represents statistics for a state type
type StateTypeStats struct {
	Type       string    `json:"type"`
	Count      int       `json:"count"`
	TotalSize  uint64    `json:"totalSize"`
	MinHeight  uint64    `json:"minHeight"`
	MaxHeight  uint64    `json:"maxHeight"`
	AvgSize    float64   `json:"avgSize"`
	LastPruned time.Time `json:"lastPruned"`
}

// StatePruningImpl provides a concrete implementation of state pruning
type StatePruningImpl struct {
	manager *StatePruningManager
	logger  *logrus.Logger
}

// NewStatePruningImpl creates a new state pruning implementation
func NewStatePruningImpl(manager *StatePruningManager, logger *logrus.Logger) *StatePruningImpl {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &StatePruningImpl{
		manager: manager,
		logger:  logger,
	}
}

// PruneState prunes all state data up to the specified height
func (sp *StatePruningImpl) PruneState(targetHeight uint64) error {
	startTime := time.Now()
	sp.logger.Info("Starting full state pruning", "targetHeight", targetHeight)

	// Get the pruning configuration
	config := sp.manager.GetConfig()

	// Track pruning statistics
	totalPruned := 0
	totalBytes := uint64(0)

	// Prune each state type according to its retention policy
	for stateType, retention := range config.RetentionPolicy {
		// Calculate the effective target height for this state type
		effectiveHeight := targetHeight
		if retention > 0 {
			// If this state type has a longer retention period, adjust the target height
			retentionHeight := sp.manager.currentBlockHeight - retention
			if retentionHeight < effectiveHeight {
				effectiveHeight = retentionHeight
			}
		}

		// Skip pruning if the effective height is negative or zero
		if effectiveHeight <= 0 {
			continue
		}

		// Prune this state type
		pruned, bytes, err := sp.pruneStateType(StateType(stateType), effectiveHeight)
		if err != nil {
			sp.logger.Error("Failed to prune state type",
				"stateType", stateType,
				"targetHeight", effectiveHeight,
				"error", err)
			continue
		}

		totalPruned += pruned
		totalBytes += bytes

		sp.logger.Info("Pruned state type",
			"stateType", stateType,
			"targetHeight", effectiveHeight,
			"entriesPruned", pruned,
			"bytesReclaimed", bytes)
	}

	// Log overall pruning results
	duration := time.Since(startTime)
	sp.logger.Info("Full state pruning completed",
		"targetHeight", targetHeight,
		"totalEntriesPruned", totalPruned,
		"totalBytesReclaimed", totalBytes,
		"duration", duration)

	return nil
}

// pruneStateType prunes a specific type of state data up to the specified height
func (sp *StatePruningImpl) pruneStateType(stateType StateType, targetHeight uint64) (int, uint64, error) {
	startTime := time.Now()
	sp.logger.Info("Pruning state type", "stateType", stateType, "targetHeight", targetHeight)

	// Get the prefix for this state type
	prefix := sp.getStateTypePrefix(stateType)
	if prefix == nil {
		return 0, 0, fmt.Errorf("unknown state type: %s", stateType)
	}

	// Create a key range for iteration
	// The range is from the prefix to the prefix + targetHeight (exclusive)
	startKey := prefix
	endKey := sp.createHeightKey(prefix, targetHeight)

	// Get the pruning configuration
	config := sp.manager.GetConfig()

	// Initialize counters
	pruned := 0
	bytesReclaimed := uint64(0)
	batchSize := 0

	// Create an iterator over the key range
	iter := sp.manager.db.NewIterator(startKey, endKey)
	defer iter.Close()

	// Iterate over all keys in the range
	for iter.Next() {
		// Check if we should stop
		if !sp.manager.running {
			return pruned, bytesReclaimed, fmt.Errorf("pruning stopped")
		}

		// Get the current key and value
		key := iter.Key()
		value := iter.Value()

		// Extract the height from the key
		height, err := sp.extractHeightFromKey(key, prefix)
		if err != nil {
			sp.logger.Warn("Failed to extract height from key",
				"key", key,
				"error", err)
			continue
		}

		// Skip keys that are at checkpoints
		if sp.isCheckpointHeight(height) {
			continue
		}

		// Delete the key
		if err := sp.manager.db.Delete(key); err != nil {
			sp.logger.Error("Failed to delete key",
				"key", key,
				"error", err)
			continue
		}

		// Update counters
		pruned++
		bytesReclaimed += uint64(len(key) + len(value))
		batchSize++

		// If we've reached the batch size limit, log progress and reset batch counter
		if batchSize >= config.MaxPruningBatchSize {
			sp.logger.Info("Pruning batch completed",
				"stateType", stateType,
				"batchSize", batchSize,
				"totalPruned", pruned,
				"totalBytes", bytesReclaimed)
			batchSize = 0

			// Small pause to avoid overwhelming the database - use context-based timing
			select {
			case <-time.After(10 * time.Millisecond):
				// Pause completed normally
			default:
				// Non-blocking check if manager is still running
				if !sp.manager.running {
					return pruned, bytesReclaimed, fmt.Errorf("pruning stopped")
				}
			}
		}
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		return pruned, bytesReclaimed, fmt.Errorf("iterator error: %w", err)
	}

	// Log completion
	duration := time.Since(startTime)
	sp.logger.Info("State type pruning completed",
		"stateType", stateType,
		"targetHeight", targetHeight,
		"entriesPruned", pruned,
		"bytesReclaimed", bytesReclaimed,
		"duration", duration)

	return pruned, bytesReclaimed, nil
}

// getStateTypePrefix returns the key prefix for a given state type
func (sp *StatePruningImpl) getStateTypePrefix(stateType StateType) []byte {
	switch stateType {
	case StateTypeAccount:
		return AccountPrefix
	case StateTypeTransaction:
		return TransactionPrefix
	case StateTypeBlock:
		return BlockPrefix
	case StateTypeContract:
		return ContractPrefix
	case StateTypeContractStorage:
		return ContractStoragePrefix
	default:
		return nil
	}
}

// createHeightKey creates a key with the given prefix and height
func (sp *StatePruningImpl) createHeightKey(prefix []byte, height uint64) []byte {
	// Allocate a buffer for the key
	key := make([]byte, len(prefix)+8)

	// Copy the prefix
	copy(key, prefix)

	// Encode the height as big-endian uint64
	binary.BigEndian.PutUint64(key[len(prefix):], height)

	return key
}

// extractHeightFromKey extracts the height from a key
func (sp *StatePruningImpl) extractHeightFromKey(key, prefix []byte) (uint64, error) {
	// Check if the key has the correct prefix
	if !bytes.HasPrefix(key, prefix) {
		return 0, fmt.Errorf("key does not have the expected prefix")
	}

	// Check if the key has enough bytes for the height
	if len(key) < len(prefix)+8 {
		return 0, fmt.Errorf("key is too short to contain a height")
	}

	// Extract the height
	height := binary.BigEndian.Uint64(key[len(prefix):])

	return height, nil
}

// isCheckpointHeight checks if a height is a checkpoint height
func (sp *StatePruningImpl) isCheckpointHeight(height uint64) bool {
	config := sp.manager.GetConfig()
	return config.CheckpointInterval > 0 && height%config.CheckpointInterval == 0
}

// PruneAccountState prunes account state data up to the specified height
func (sp *StatePruningImpl) PruneAccountState(targetHeight uint64) (int, uint64, error) {
	return sp.pruneStateType(StateTypeAccount, targetHeight)
}

// PruneTransactionState prunes transaction data up to the specified height
func (sp *StatePruningImpl) PruneTransactionState(targetHeight uint64) (int, uint64, error) {
	return sp.pruneStateType(StateTypeTransaction, targetHeight)
}

// PruneBlockState prunes block data up to the specified height
func (sp *StatePruningImpl) PruneBlockState(targetHeight uint64) (int, uint64, error) {
	return sp.pruneStateType(StateTypeBlock, targetHeight)
}

// PruneContractState prunes smart contract data up to the specified height
func (sp *StatePruningImpl) PruneContractState(targetHeight uint64) (int, uint64, error) {
	return sp.pruneStateType(StateTypeContract, targetHeight)
}

// PruneContractStorageState prunes smart contract storage data up to the specified height
func (sp *StatePruningImpl) PruneContractStorageState(targetHeight uint64) (int, uint64, error) {
	return sp.pruneStateType(StateTypeContractStorage, targetHeight)
}

// CompactStateRange compacts the database for a specific state type
func (sp *StatePruningImpl) CompactStateRange(stateType StateType) error {
	prefix := sp.getStateTypePrefix(stateType)
	if prefix == nil {
		return fmt.Errorf("unknown state type: %s", stateType)
	}

	// Create a key range for compaction
	// The range is from the prefix to the prefix + 0xFF (inclusive)
	startKey := prefix
	endKey := make([]byte, len(prefix)+1)
	copy(endKey, prefix)
	endKey[len(prefix)] = 0xFF

	// Log the compaction operation
	sp.logger.Info("Compacting state range",
		"stateType", stateType,
		"startKey", startKey,
		"endKey", endKey)

	// Perform the compaction
	startTime := time.Now()
	err := sp.manager.db.Compact(startKey, endKey)
	duration := time.Since(startTime)

	if err != nil {
		sp.logger.Error("Failed to compact state range",
			"stateType", stateType,
			"error", err,
			"duration", duration)
		return fmt.Errorf("compaction failed: %w", err)
	}

	sp.logger.Info("State range compaction completed",
		"stateType", stateType,
		"duration", duration)

	return nil
}

// CompactAllStateRanges compacts the database for all state types
func (sp *StatePruningImpl) CompactAllStateRanges() error {
	stateTypes := []StateType{
		StateTypeAccount,
		StateTypeTransaction,
		StateTypeBlock,
		StateTypeContract,
		StateTypeContractStorage,
	}

	for _, stateType := range stateTypes {
		if err := sp.CompactStateRange(stateType); err != nil {
			sp.logger.Error("Failed to compact state range",
				"stateType", stateType,
				"error", err)
			// Continue with other state types even if one fails
		}
	}

	return nil
}

// VerifyPruning verifies that pruning was successful by checking that no data exists below the pruned height
func (sp *StatePruningImpl) VerifyPruning(stateType StateType, prunedHeight uint64) (bool, error) {
	prefix := sp.getStateTypePrefix(stateType)
	if prefix == nil {
		return false, fmt.Errorf("unknown state type: %s", stateType)
	}

	// Create a key range for verification
	// The range is from the prefix to the prefix + prunedHeight (exclusive)
	startKey := prefix
	endKey := sp.createHeightKey(prefix, prunedHeight)

	// Create an iterator over the key range
	iter := sp.manager.db.NewIterator(startKey, endKey)
	defer iter.Close()

	// If the iterator has any entries, pruning was not successful
	if iter.Next() {
		key := iter.Key()
		height, _ := sp.extractHeightFromKey(key, prefix)

		sp.logger.Warn("Found unpruned data",
			"stateType", stateType,
			"prunedHeight", prunedHeight,
			"foundKey", key,
			"foundHeight", height)

		return false, nil
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		return false, fmt.Errorf("iterator error: %w", err)
	}

	return true, nil
}

// VerifyAllPruning verifies that pruning was successful for all state types
func (sp *StatePruningImpl) VerifyAllPruning(prunedHeight uint64) (bool, map[StateType]bool, error) {
	stateTypes := []StateType{
		StateTypeAccount,
		StateTypeTransaction,
		StateTypeBlock,
		StateTypeContract,
		StateTypeContractStorage,
	}

	allSuccess := true
	results := make(map[StateType]bool)

	for _, stateType := range stateTypes {
		success, err := sp.VerifyPruning(stateType, prunedHeight)
		if err != nil {
			sp.logger.Error("Failed to verify pruning",
				"stateType", stateType,
				"error", err)
			return false, results, err
		}

		results[stateType] = success
		if !success {
			allSuccess = false
		}
	}

	return allSuccess, results, nil
}

// GetStateTypeStats returns statistics about a specific state type
func (sp *StatePruningImpl) GetStateTypeStats(stateType StateType) (*StateTypeStats, error) {
	prefix := sp.getStateTypePrefix(stateType)
	if prefix == nil {
		return nil, fmt.Errorf("unknown state type: %s", stateType)
	}

	// Create a key range for counting
	// The range is from the prefix to the prefix + 0xFF (inclusive)
	startKey := prefix
	endKey := make([]byte, len(prefix)+1)
	copy(endKey, prefix)
	endKey[len(prefix)] = 0xFF

	// Initialize counters
	count := 0
	totalSize := uint64(0)
	minHeight := uint64(0xFFFFFFFFFFFFFFFF)
	maxHeight := uint64(0)

	// Create an iterator over the key range
	iter := sp.manager.db.NewIterator(startKey, endKey)
	defer iter.Close()

	// Iterate over all keys in the range
	for iter.Next() {
		// Get the current key and value
		key := iter.Key()
		value := iter.Value()

		// Extract the height from the key
		height, err := sp.extractHeightFromKey(key, prefix)
		if err != nil {
			continue
		}

		// Update counters
		count++
		totalSize += uint64(len(key) + len(value))

		// Update min/max height
		if height < minHeight {
			minHeight = height
		}
		if height > maxHeight {
			maxHeight = height
		}
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// If no entries were found, reset min height
	if count == 0 {
		minHeight = 0
	}

	// Calculate average size
	avgSize := float64(0)
	if count > 0 {
		avgSize = float64(totalSize) / float64(count)
	}

	// Return statistics
	return &StateTypeStats{
		Type:       string(stateType),
		Count:      count,
		TotalSize:  totalSize,
		MinHeight:  minHeight,
		MaxHeight:  maxHeight,
		AvgSize:    avgSize,
		LastPruned: sp.manager.metrics.LastPruningTime,
	}, nil
}

// GetAllStateTypeStats returns statistics about all state types
func (sp *StatePruningImpl) GetAllStateTypeStats() (map[StateType]*StateTypeStats, error) {
	stateTypes := []StateType{
		StateTypeAccount,
		StateTypeTransaction,
		StateTypeBlock,
		StateTypeContract,
		StateTypeContractStorage,
	}

	results := make(map[StateType]*StateTypeStats)

	for _, stateType := range stateTypes {
		stats, err := sp.GetStateTypeStats(stateType)
		if err != nil {
			sp.logger.Error("Failed to get state type stats",
				"stateType", stateType,
				"error", err)
			continue
		}

		results[stateType] = stats
	}

	return results, nil
}
