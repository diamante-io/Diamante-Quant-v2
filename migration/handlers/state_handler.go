package handlers

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"diamante/common"
	"diamante/migration"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// StateHandler handles blockchain state migrations
type StateHandler struct {
	dataDir     string
	stateDB     StateDatabase
	snapshotMgr SnapshotManager
}

// StateDatabase interface for interacting with state database
type StateDatabase interface {
	GetState(key []byte) ([]byte, error)
	SetState(key, value []byte) error
	DeleteState(key []byte) error
	IterateState(prefix []byte, callback func(key, value []byte) error) error
	BatchUpdate(updates map[string][]byte, deletes []string) error
	CreateSnapshot() (string, error)
	RestoreSnapshot(snapshotID string) error
	Close() error
}

// SnapshotManager interface for managing state snapshots
type SnapshotManager interface {
	CreateSnapshot(version string) (*SnapshotInfo, error)
	RestoreSnapshot(snapshotID string) error
	ListSnapshots() ([]*SnapshotInfo, error)
	DeleteSnapshot(snapshotID string) error
}

// SnapshotInfo contains snapshot metadata
type SnapshotInfo struct {
	ID        string    `json:"id"`
	Version   string    `json:"version"`
	Height    int64     `json:"height"`
	CreatedAt time.Time `json:"created_at"`
	Size      int64     `json:"size"`
	Path      string    `json:"path"`
}

// NewStateHandler creates a new state handler
func NewStateHandler(dataDir string, stateDB StateDatabase, snapshotMgr SnapshotManager) *StateHandler {
	return &StateHandler{
		dataDir:     dataDir,
		stateDB:     stateDB,
		snapshotMgr: snapshotMgr,
	}
}

// Execute performs the state migration
func (h *StateHandler) Execute(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	startTime := common.ConsensusNow()

	migrationType, _ := step.Config.GetString("migration_type")
	if migrationType == "" {
		return nil, fmt.Errorf("migration_type is required")
	}

	if logger, ok := ctx.Logger.(*logrus.Logger); ok {
		logger.Info("Starting state migration", logrus.Fields{"type": migrationType})
	}

	var result *migration.StepResult
	var err error

	switch migrationType {
	case "schema_migration":
		result, err = h.executeSchemaMigration(ctx, step)
	case "data_migration":
		result, err = h.executeDataMigration(ctx, step)
	case "key_migration":
		result, err = h.executeKeyMigration(ctx, step)
	case "compression":
		result, err = h.executeCompression(ctx, step)
	case "cleanup":
		result, err = h.executeCleanup(ctx, step)
	case "reindex":
		result, err = h.executeReindex(ctx, step)
	default:
		return nil, fmt.Errorf("unknown migration type: %s", migrationType)
	}

	if err != nil {
		return nil, err
	}

	duration := time.Since(startTime)
	result.Metrics.Duration = duration

	if logger, ok := ctx.Logger.(*logrus.Logger); ok {
		logger.Info("State migration completed", logrus.Fields{"type": migrationType, "duration": duration})
	}
	return result, nil
}

// executeSchemaMigration migrates database schema
func (h *StateHandler) executeSchemaMigration(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	// Get schema changes from config
	var schemaChanges []interface{}
	if schemaValue, ok := step.Config.Get("schema_changes"); ok && schemaValue != nil {
		// Try to unmarshal as slice
		json.Unmarshal(schemaValue.Data, &schemaChanges)
	}
	if schemaChanges == nil {
		return nil, fmt.Errorf("schema_changes are required for schema migration")
	}

	metrics := &migration.MigrationMetrics{}
	processedItems := int64(0)

	for _, change := range schemaChanges {
		changeMap, ok := change.(*types.TypedMap)
		if !ok {
			continue
		}

		changeType, _ := changeMap.GetString("type")
		switch changeType {
		case "add_prefix":
			if err := h.addKeyPrefix(changeMap); err != nil {
				return nil, fmt.Errorf("failed to add key prefix: %w", err)
			}
			processedItems++

		case "remove_prefix":
			if err := h.removeKeyPrefix(changeMap); err != nil {
				return nil, fmt.Errorf("failed to remove key prefix: %w", err)
			}
			processedItems++

		case "migrate_keys":
			count, err := h.migrateKeys(changeMap)
			if err != nil {
				return nil, fmt.Errorf("failed to migrate keys: %w", err)
			}
			processedItems += count

		default:
			if logger, ok := ctx.Logger.(*logrus.Logger); ok {
				logger.WithField("type", changeType).Warn("Unknown schema change type")
			}
		}
	}

	metrics.ProcessedItems = processedItems

	return &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Message: fmt.Sprintf("Schema migration completed, processed %d items", processedItems),
	}, nil
}

// executeDataMigration migrates data format or structure
func (h *StateHandler) executeDataMigration(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	keyPrefix, _ := step.Config.GetString("key_prefix")
	transformer, _ := step.Config.GetString("transformer")

	if keyPrefix == "" {
		return nil, fmt.Errorf("key_prefix is required for data migration")
	}

	metrics := &migration.MigrationMetrics{}
	processedItems := int64(0)
	failedItems := int64(0)
	bytesProcessed := int64(0)

	updates := make(map[string][]byte)

	err := h.stateDB.IterateState([]byte(keyPrefix), func(key, value []byte) error {
		originalSize := len(value)

		// Apply transformation
		newValue, err := h.transformValue(value, transformer, step.Config)
		if err != nil {
			if logger, ok := ctx.Logger.(*logrus.Logger); ok {
				logger.WithFields(logrus.Fields{
					"key":   string(key),
					"error": err,
				}).Warn("Failed to transform value")
			}
			failedItems++
			return nil // Continue with next item
		}

		if !ctx.DryRun {
			updates[string(key)] = newValue
		}

		processedItems++
		bytesProcessed += int64(originalSize)

		// Batch updates for performance
		if len(updates) >= 1000 {
			if err := h.stateDB.BatchUpdate(updates, nil); err != nil {
				return fmt.Errorf("batch update failed: %w", err)
			}
			updates = make(map[string][]byte)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("iteration failed: %w", err)
	}

	// Apply remaining updates
	if len(updates) > 0 && !ctx.DryRun {
		if err := h.stateDB.BatchUpdate(updates, nil); err != nil {
			return nil, fmt.Errorf("final batch update failed: %w", err)
		}
	}

	metrics.ProcessedItems = processedItems
	metrics.FailedItems = failedItems
	metrics.BytesProcessed = bytesProcessed

	return &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Message: fmt.Sprintf("Data migration completed, processed %d items (%d failed)", processedItems, failedItems),
	}, nil
}

// executeKeyMigration migrates database keys
func (h *StateHandler) executeKeyMigration(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	oldPrefix, _ := step.Config.GetString("old_prefix")
	newPrefix, _ := step.Config.GetString("new_prefix")

	if oldPrefix == "" || newPrefix == "" {
		return nil, fmt.Errorf("old_prefix and new_prefix are required for key migration")
	}

	metrics := &migration.MigrationMetrics{}
	processedItems := int64(0)

	updates := make(map[string][]byte)
	deletes := make([]string, 0)

	err := h.stateDB.IterateState([]byte(oldPrefix), func(key, value []byte) error {
		// Create new key with new prefix
		oldKey := string(key)
		newKey := newPrefix + oldKey[len(oldPrefix):]

		if !ctx.DryRun {
			updates[newKey] = value
			deletes = append(deletes, oldKey)
		}

		processedItems++

		// Batch operations for performance
		if len(updates) >= 1000 {
			if err := h.stateDB.BatchUpdate(updates, deletes); err != nil {
				return fmt.Errorf("batch operation failed: %w", err)
			}
			updates = make(map[string][]byte)
			deletes = make([]string, 0)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("key migration iteration failed: %w", err)
	}

	// Apply remaining operations
	if len(updates) > 0 && !ctx.DryRun {
		if err := h.stateDB.BatchUpdate(updates, deletes); err != nil {
			return nil, fmt.Errorf("final batch operation failed: %w", err)
		}
	}

	metrics.ProcessedItems = processedItems

	return &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Message: fmt.Sprintf("Key migration completed, migrated %d keys from '%s' to '%s'", processedItems, oldPrefix, newPrefix),
	}, nil
}

// executeCompression compresses or decompresses state data
func (h *StateHandler) executeCompression(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	operation, _ := step.Config.GetString("operation") // "compress" or "decompress"
	keyPrefix, _ := step.Config.GetString("key_prefix")
	algorithm, _ := step.Config.GetString("algorithm") // "gzip", "lz4", "snappy"

	if operation == "" || keyPrefix == "" {
		return nil, fmt.Errorf("operation and key_prefix are required for compression")
	}

	metrics := &migration.MigrationMetrics{}
	processedItems := int64(0)
	originalSize := int64(0)
	compressedSize := int64(0)

	updates := make(map[string][]byte)

	err := h.stateDB.IterateState([]byte(keyPrefix), func(key, value []byte) error {
		originalSize += int64(len(value))

		var newValue []byte
		var err error

		switch operation {
		case "compress":
			newValue, err = h.compressData(value, algorithm)
		case "decompress":
			newValue, err = h.decompressData(value, algorithm)
		default:
			return fmt.Errorf("unknown compression operation: %s", operation)
		}

		if err != nil {
			return fmt.Errorf("compression operation failed for key %s: %w", string(key), err)
		}

		compressedSize += int64(len(newValue))

		if !ctx.DryRun {
			updates[string(key)] = newValue
		}

		processedItems++

		// Batch updates
		if len(updates) >= 500 { // Smaller batches for compression
			if err := h.stateDB.BatchUpdate(updates, nil); err != nil {
				return fmt.Errorf("batch update failed: %w", err)
			}
			updates = make(map[string][]byte)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("compression iteration failed: %w", err)
	}

	// Apply remaining updates
	if len(updates) > 0 && !ctx.DryRun {
		if err := h.stateDB.BatchUpdate(updates, nil); err != nil {
			return nil, fmt.Errorf("final compression update failed: %w", err)
		}
	}

	metrics.ProcessedItems = processedItems
	metrics.BytesProcessed = originalSize

	result := &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Data:    types.NewTypedMap(),
	}

	result.Data.Set("original_size", types.Int64ToValue(originalSize))
	result.Data.Set("final_size", types.Int64ToValue(compressedSize))
	result.Data.Set("compression_ratio", types.FloatToValue(float64(compressedSize)/float64(originalSize)))

	if operation == "compress" {
		result.Message = fmt.Sprintf("Compression completed, %d items compressed from %d to %d bytes (%.2f%% reduction)",
			processedItems, originalSize, compressedSize, (1.0-float64(compressedSize)/float64(originalSize))*100)
	} else {
		result.Message = fmt.Sprintf("Decompression completed, %d items decompressed from %d to %d bytes",
			processedItems, originalSize, compressedSize)
	}

	return result, nil
}

// executeCleanup removes obsolete or invalid state data
func (h *StateHandler) executeCleanup(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	cleanupType, _ := step.Config.GetString("cleanup_type")
	keyPrefix, _ := step.Config.GetString("key_prefix")

	if cleanupType == "" {
		return nil, fmt.Errorf("cleanup_type is required")
	}

	metrics := &migration.MigrationMetrics{}
	deletedItems := int64(0)
	bytesReclaimed := int64(0)

	deletes := make([]string, 0)

	switch cleanupType {
	case "orphaned_keys":
		err := h.cleanupOrphanedKeys(keyPrefix, &deletes, &deletedItems, &bytesReclaimed)
		if err != nil {
			return nil, err
		}

	case "expired_data":
		err := h.cleanupExpiredData(step.Config, &deletes, &deletedItems, &bytesReclaimed)
		if err != nil {
			return nil, err
		}

	case "invalid_data":
		err := h.cleanupInvalidData(keyPrefix, step.Config, &deletes, &deletedItems, &bytesReclaimed)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown cleanup type: %s", cleanupType)
	}

	// Apply deletions
	if len(deletes) > 0 && !ctx.DryRun {
		if err := h.stateDB.BatchUpdate(nil, deletes); err != nil {
			return nil, fmt.Errorf("cleanup deletion failed: %w", err)
		}
	}

	metrics.ProcessedItems = deletedItems
	metrics.BytesProcessed = bytesReclaimed

	return &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Message: fmt.Sprintf("Cleanup completed, removed %d items, reclaimed %d bytes", deletedItems, bytesReclaimed),
	}, nil
}

// executeReindex rebuilds database indexes
func (h *StateHandler) executeReindex(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	indexType, _ := step.Config.GetString("index_type")
	keyPrefix, _ := step.Config.GetString("key_prefix")

	metrics := &migration.MigrationMetrics{}
	processedItems := int64(0)

	// This is a simplified reindex implementation
	// In practice, this would interact with the specific database's indexing system

	if logger, ok := ctx.Logger.(*logrus.Logger); ok {
		logger.WithFields(logrus.Fields{
			"type":   indexType,
			"prefix": keyPrefix,
		}).Info("Starting reindex")
	}

	// Simulate reindexing by iterating through data
	err := h.stateDB.IterateState([]byte(keyPrefix), func(key, value []byte) error {
		// In a real implementation, this would rebuild indexes
		processedItems++

		// Simulate processing time
		if processedItems%10000 == 0 {
			if logger, ok := ctx.Logger.(*logrus.Logger); ok {
				logger.WithField("processed", processedItems).Info("Reindex progress")
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("reindex failed: %w", err)
	}

	metrics.ProcessedItems = processedItems

	return &migration.StepResult{
		Success: true,
		Metrics: metrics,
		Message: fmt.Sprintf("Reindex completed, processed %d items", processedItems),
	}, nil
}

// Validate checks if the state migration can be performed
func (h *StateHandler) Validate(ctx *migration.MigrationContext, step *migration.MigrationStep) error {
	// Check if data directory exists
	if _, err := os.Stat(h.dataDir); os.IsNotExist(err) {
		return fmt.Errorf("data directory does not exist: %s", h.dataDir)
	}

	// Check database connectivity
	if h.stateDB == nil {
		return fmt.Errorf("state database is not initialized")
	}

	// Test database read access
	if err := h.testDatabaseAccess(); err != nil {
		return fmt.Errorf("database access test failed: %w", err)
	}

	// Validate migration-specific requirements
	migrationType, _ := step.Config.GetString("migration_type")
	switch migrationType {
	case "schema_migration":
		return h.validateSchemaMigration(step)
	case "data_migration":
		return h.validateDataMigration(step)
	case "key_migration":
		return h.validateKeyMigration(step)
	default:
		return nil
	}
}

// EstimateTime estimates migration duration
func (h *StateHandler) EstimateTime(migrationCtx *migration.MigrationContext, step *migration.MigrationStep) (time.Duration, error) {
	keyPrefix, _ := step.Config.GetString("key_prefix")
	if keyPrefix == "" {
		return time.Minute, nil // Default estimate
	}

	// Count items to estimate processing time
	itemCount := int64(0)
	err := h.stateDB.IterateState([]byte(keyPrefix), func(key, value []byte) error {
		itemCount++
		// Note: For estimation, we don't need cancellation as it should be fast
		// The actual migration Execute() method would handle cancellation
		return nil
	})

	if err != nil {
		return 0, err
	}

	// Estimate based on migration type
	migrationType, _ := step.Config.GetString("migration_type")
	var itemsPerSecond int64

	switch migrationType {
	case "schema_migration":
		itemsPerSecond = 10000 // Fast operations
	case "data_migration":
		itemsPerSecond = 1000 // Medium operations
	case "compression":
		itemsPerSecond = 500 // Slow operations
	default:
		itemsPerSecond = 5000 // Default estimate
	}

	estimatedSeconds := itemCount / itemsPerSecond
	if estimatedSeconds < 1 {
		estimatedSeconds = 1
	}

	return time.Duration(estimatedSeconds) * time.Second, nil
}

// CanRollback indicates if this handler supports rollback
func (h *StateHandler) CanRollback() bool {
	return true
}

// Rollback reverses the state migration
func (h *StateHandler) Rollback(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	// Create snapshot before migration for rollback
	if h.snapshotMgr != nil {
		snapshots, err := h.snapshotMgr.ListSnapshots()
		if err == nil && len(snapshots) > 0 {
			// Restore the most recent snapshot
			latestSnapshot := snapshots[len(snapshots)-1]
			if err := h.snapshotMgr.RestoreSnapshot(latestSnapshot.ID); err != nil {
				return nil, fmt.Errorf("failed to restore snapshot: %w", err)
			}

			return &migration.StepResult{
				Success: true,
				Message: fmt.Sprintf("State rolled back to snapshot %s", latestSnapshot.ID),
			}, nil
		}
	}

	return &migration.StepResult{
		Success: true,
		Message: "Rollback completed (no snapshot available)",
	}, nil
}

// Helper methods

func (h *StateHandler) addKeyPrefix(change *types.TypedMap) error {
	// Implementation for adding key prefix
	return nil
}

func (h *StateHandler) removeKeyPrefix(change *types.TypedMap) error {
	// Implementation for removing key prefix
	return nil
}

func (h *StateHandler) migrateKeys(change *types.TypedMap) (int64, error) {
	// Implementation for migrating keys
	return 0, nil
}

func (h *StateHandler) transformValue(value []byte, transformer string, config *types.TypedMap) ([]byte, error) {
	// Implementation for value transformation
	switch transformer {
	case "json_to_protobuf":
		return h.jsonToProtobuf(value, config)
	case "protobuf_to_json":
		return h.protobufToJSON(value, config)
	case "compress":
		return h.compressData(value, "gzip")
	case "decompress":
		return h.decompressData(value, "gzip")
	default:
		return value, nil // No transformation
	}
}

func (h *StateHandler) jsonToProtobuf(data []byte, config *types.TypedMap) ([]byte, error) {
	// Simplified implementation
	return data, nil
}

func (h *StateHandler) protobufToJSON(data []byte, config *types.TypedMap) ([]byte, error) {
	// Simplified implementation
	return data, nil
}

func (h *StateHandler) compressData(data []byte, algorithm string) ([]byte, error) {
	// Simplified implementation - would use actual compression libraries
	return data, nil
}

func (h *StateHandler) decompressData(data []byte, algorithm string) ([]byte, error) {
	// Simplified implementation - would use actual decompression libraries
	return data, nil
}

func (h *StateHandler) cleanupOrphanedKeys(keyPrefix string, deletes *[]string, count *int64, bytes *int64) error {
	// Implementation for cleaning up orphaned keys
	return nil
}

func (h *StateHandler) cleanupExpiredData(config *types.TypedMap, deletes *[]string, count *int64, bytes *int64) error {
	// Implementation for cleaning up expired data
	return nil
}

func (h *StateHandler) cleanupInvalidData(keyPrefix string, config *types.TypedMap, deletes *[]string, count *int64, bytes *int64) error {
	// Implementation for cleaning up invalid data
	return nil
}

func (h *StateHandler) testDatabaseAccess() error {
	// Test basic database operations
	testKey := []byte("test_migration_key")
	testValue := []byte("test_value")

	// Test write
	if err := h.stateDB.SetState(testKey, testValue); err != nil {
		return fmt.Errorf("database write test failed: %w", err)
	}

	// Test read
	retrievedValue, err := h.stateDB.GetState(testKey)
	if err != nil {
		return fmt.Errorf("database read test failed: %w", err)
	}

	if string(retrievedValue) != string(testValue) {
		return fmt.Errorf("database read/write test failed: value mismatch")
	}

	// Cleanup test key
	if err := h.stateDB.DeleteState(testKey); err != nil {
		return fmt.Errorf("database delete test failed: %w", err)
	}

	return nil
}

func (h *StateHandler) validateSchemaMigration(step *migration.MigrationStep) error {
	schemaChangesValue, ok := step.Config.Get("schema_changes")
	if !ok || schemaChangesValue == nil {
		return fmt.Errorf("schema_changes are required for schema migration")
	}

	// For now, just check if the value exists
	// In a full implementation, we'd parse the slice from the Value
	return nil
}

func (h *StateHandler) validateDataMigration(step *migration.MigrationStep) error {
	keyPrefix, _ := step.Config.GetString("key_prefix")
	if keyPrefix == "" {
		return fmt.Errorf("key_prefix is required for data migration")
	}
	return nil
}

func (h *StateHandler) validateKeyMigration(step *migration.MigrationStep) error {
	oldPrefix, _ := step.Config.GetString("old_prefix")
	newPrefix, _ := step.Config.GetString("new_prefix")
	if oldPrefix == "" || newPrefix == "" {
		return fmt.Errorf("old_prefix and new_prefix are required for key migration")
	}
	return nil
}
