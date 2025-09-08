package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/mobile"
	"diamante/storage/cache"
	dtypes "diamante/types"

	"github.com/sirupsen/logrus"
)

// TieredStorageConfig holds configuration for the tiered storage system
type TieredStorageConfig struct {
	// Primary storage (LMDB)
	PrimaryConfig AdapterConfig

	// Redis cache configuration
	CacheEnabled bool
	CacheConfig  *cache.RedisCacheConfig

	// MongoDB archive configuration
	ArchiveEnabled   bool
	ArchiveThreshold uint64 // Blocks older than this height go to archive
	ArchiveHost      string
	ArchiveDatabase  string

	// SQLite light node configuration
	LightNodeMode   bool
	LightNodeDBPath string
	MaxHeaderCount  int

	// Performance tuning
	AsyncArchival      bool
	ArchivalBatchSize  int
	CacheWarmupEnabled bool
	MetricsEnabled     bool
}

// DefaultTieredStorageConfig returns a production-ready configuration
func DefaultTieredStorageConfig() *TieredStorageConfig {
	return &TieredStorageConfig{
		PrimaryConfig: AdapterConfig{
			CacheSize:           10000,
			CacheTTL:            5 * time.Minute,
			MetricsEnabled:      true,
			HealthCheckInterval: 30 * time.Second,
			MaxConcurrentOps:    100,
			EnableCompression:   true,
		},
		CacheEnabled:       true,
		CacheConfig:        cache.DefaultRedisCacheConfig(),
		ArchiveEnabled:     true,
		ArchiveThreshold:   7 * 24 * 60 * 60 / 2, // 1 week worth of blocks (2s block time)
		ArchiveHost:        "mongodb://localhost:27017",
		ArchiveDatabase:    "diamante_archive",
		LightNodeMode:      false,
		LightNodeDBPath:    "lightnode.db",
		MaxHeaderCount:     10000,
		AsyncArchival:      true,
		ArchivalBatchSize:  100,
		CacheWarmupEnabled: true,
		MetricsEnabled:     true,
	}
}

// TieredStorageManager manages multi-tiered storage architecture
type TieredStorageManager struct {
	// Storage layers
	primary   LedgerStore             // LMDB - primary state storage
	cache     *cache.RedisCache       // Redis - hot data cache
	archive   *MongoAdapter           // MongoDB - historical archive
	lightNode *mobile.SQLiteLightNode // SQLite - light node storage

	// Configuration
	config *TieredStorageConfig
	logger *logrus.Logger

	// State management
	mu             sync.RWMutex
	isOpen         atomic.Bool
	currentHeight  atomic.Uint64
	archivalHeight atomic.Uint64

	// Archival management
	archivalQueue chan *common.Block
	archivalStop  chan struct{}
	archivalWg    sync.WaitGroup

	// Cache failure tracking
	cacheFailures    atomic.Uint32
	cacheDisabled    atomic.Bool
	maxCacheFailures uint32

	// Metrics
	metrics struct {
		cacheHits      atomic.Uint64
		cacheMisses    atomic.Uint64
		primaryReads   atomic.Uint64
		archiveReads   atomic.Uint64
		archivalBlocks atomic.Uint64
		errors         atomic.Uint64
	}
}

// NewTieredStorageManager creates a new tiered storage manager
func NewTieredStorageManager(config *TieredStorageConfig, primary LedgerStore, logger *logrus.Logger) (*TieredStorageManager, error) {
	if config == nil {
		config = DefaultTieredStorageConfig()
	}

	if primary == nil {
		return nil, errors.New("primary storage cannot be nil")
	}

	if logger == nil {
		logger = logrus.New()
	}

	tsm := &TieredStorageManager{
		primary:          primary,
		config:           config,
		logger:           logger,
		archivalQueue:    make(chan *common.Block, config.ArchivalBatchSize),
		archivalStop:     make(chan struct{}),
		maxCacheFailures: 3, // After 3 consecutive failures, disable cache
	}

	return tsm, nil
}

// Open initializes all storage tiers
func (tsm *TieredStorageManager) Open() error {
	if tsm.isOpen.Load() {
		return nil
	}

	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	// Open primary storage if not already open
	if err := tsm.primary.Open(); err != nil {
		return fmt.Errorf("failed to open primary storage: %w", err)
	}

	// Initialize Redis cache if enabled
	if tsm.config.CacheEnabled {
		cache, err := cache.NewRedisCache(tsm.config.CacheConfig)
		if err != nil {
			tsm.logger.WithError(err).Warn("Failed to initialize Redis cache, continuing without cache")
			tsm.config.CacheEnabled = false
		} else {
			tsm.cache = cache
			tsm.logger.Info("Redis cache initialized successfully")

			// Warm up cache if enabled
			if tsm.config.CacheWarmupEnabled {
				go tsm.warmupCache()
			}
		}
	}

	// Initialize MongoDB archive if enabled
	if tsm.config.ArchiveEnabled {
		archive, err := NewMongoAdapter(
			tsm.config.ArchiveHost,
			tsm.config.ArchiveDatabase,
			tsm.logger,
			1000, // Small cache for archive
		)
		if err != nil {
			tsm.logger.WithError(err).Warn("Failed to initialize MongoDB archive, continuing without archive")
			tsm.config.ArchiveEnabled = false
		} else {
			if err := archive.Open(); err != nil {
				tsm.logger.WithError(err).Warn("Failed to open MongoDB archive")
				tsm.config.ArchiveEnabled = false
			} else {
				tsm.archive = archive
				tsm.logger.Info("MongoDB archive initialized successfully")

				// Start archival worker if async archival is enabled
				if tsm.config.AsyncArchival {
					tsm.startArchivalWorker()
				}
			}
		}
	}

	// Initialize SQLite light node if in light node mode
	if tsm.config.LightNodeMode {
		lightConfig := &mobile.Config{
			DatabasePath:       tsm.config.LightNodeDBPath,
			ConnectionTimeout:  30 * time.Second,
			MaxOpenConnections: 10,
			MaxIdleConnections: 5,
			EnableWALMode:      true,
		}

		lightNode, err := mobile.NewSQLiteLightNode(lightConfig)
		if err != nil {
			tsm.logger.WithError(err).Warn("Failed to initialize SQLite light node")
			tsm.config.LightNodeMode = false
		} else {
			tsm.lightNode = lightNode
			tsm.logger.Info("SQLite light node initialized successfully")
		}
	}

	// Get current height
	latestBlock, err := tsm.primary.GetLatestBlock()
	if err == nil {
		tsm.currentHeight.Store(uint64(latestBlock.Number))
		// Prevent underflow when height is less than archive threshold
		if uint64(latestBlock.Number) > tsm.config.ArchiveThreshold {
			tsm.archivalHeight.Store(uint64(latestBlock.Number) - tsm.config.ArchiveThreshold)
		} else {
			tsm.archivalHeight.Store(0)
		}
	}

	tsm.isOpen.Store(true)
	tsm.logger.Info("Tiered storage manager opened successfully")
	return nil
}

// Close cleanly shuts down all storage tiers
func (tsm *TieredStorageManager) Close() error {
	if !tsm.isOpen.Load() {
		return nil
	}

	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	var errs []error

	// Stop archival worker
	if tsm.config.AsyncArchival && tsm.archivalStop != nil {
		close(tsm.archivalStop)
		tsm.archivalWg.Wait()
	}

	// Close cache
	if tsm.cache != nil {
		if err := tsm.cache.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Redis cache: %w", err))
		}
	}

	// Close archive
	if tsm.archive != nil {
		if err := tsm.archive.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close MongoDB archive: %w", err))
		}
	}

	// Close light node
	if tsm.lightNode != nil {
		if err := tsm.lightNode.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close SQLite light node: %w", err))
		}
	}

	// Close primary storage
	if err := tsm.primary.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close primary storage: %w", err))
	}

	tsm.isOpen.Store(false)

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// IsOpen returns whether the storage manager is open
func (tsm *TieredStorageManager) IsOpen() bool {
	return tsm.isOpen.Load()
}

// isCacheAvailable checks if cache is enabled and not disabled due to failures
func (tsm *TieredStorageManager) isCacheAvailable() bool {
	return tsm.config.CacheEnabled && tsm.cache != nil && !tsm.cacheDisabled.Load()
}

// recordCacheFailure tracks cache failures and disables cache after threshold
func (tsm *TieredStorageManager) recordCacheFailure() {
	failures := tsm.cacheFailures.Add(1)
	if failures >= tsm.maxCacheFailures && !tsm.cacheDisabled.Load() {
		tsm.cacheDisabled.Store(true)
		tsm.logger.Warn("Redis cache disabled after repeated failures",
			"failures", failures)
	}
}

// resetCacheFailures resets the failure counter on successful operation
func (tsm *TieredStorageManager) resetCacheFailures() {
	tsm.cacheFailures.Store(0)
}

// GetBlock retrieves a block from the appropriate tier
func (tsm *TieredStorageManager) GetBlock(height uint64) (*common.Block, error) {
	// For light nodes, only return headers
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		header, err := tsm.lightNode.GetLatestBlockHeader()
		if err != nil {
			return nil, err
		}
		return &header, nil
	}

	// Check cache first
	if tsm.isCacheAvailable() {
		cacheKey := &dtypes.CacheKey{
			Namespace: "block",
			Key:       fmt.Sprintf("%d", height),
		}

		if value, ok := tsm.cache.GetTyped(cacheKey); ok {
			tsm.metrics.cacheHits.Add(1)
			tsm.resetCacheFailures() // Reset on successful operation
			// Deserialize block from cache
			if value.Data != nil {
				var block common.Block
				if err := json.Unmarshal(value.Data, &block); err == nil {
					return &block, nil
				}
			}
		} else {
			// Only record failure if it was an error, not a miss
			stats := tsm.cache.GetStats()
			if stats.Errors > 0 {
				tsm.recordCacheFailure()
			}
		}
		tsm.metrics.cacheMisses.Add(1)
	}

	// Check if block should be in archive
	currentHeight := tsm.currentHeight.Load()
	if uint64(height) < currentHeight-tsm.config.ArchiveThreshold {
		// Try archive
		if tsm.config.ArchiveEnabled && tsm.archive != nil {
			block, err := tsm.archive.GetBlock(height)
			if err == nil {
				tsm.metrics.archiveReads.Add(1)
				// Cache the block for future reads
				tsm.cacheBlock(block)
				return block, nil
			}
		}
	}

	// Get from primary storage
	tsm.metrics.primaryReads.Add(1)
	block, err := tsm.primary.GetBlock(height)
	if err != nil {
		return nil, err
	}

	// Cache the block
	tsm.cacheBlock(block)

	return block, nil
}

// SaveBlock saves a block to primary storage and cache
func (tsm *TieredStorageManager) SaveBlock(block *common.Block) error {
	// For light nodes, only save headers
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		return tsm.lightNode.SaveBlockHeader(*block)
	}

	// Save to primary storage
	if err := tsm.primary.SaveBlock(block); err != nil {
		return err
	}

	// Update current height
	tsm.currentHeight.Store(uint64(block.Number))

	// Cache the block
	tsm.cacheBlock(block)

	// Queue for archival if old enough
	if tsm.shouldArchive(block) {
		select {
		case tsm.archivalQueue <- block:
		default:
			// Queue is full, log warning
			tsm.logger.Warn("Archival queue is full, skipping block", "height", block.Number)
		}
	}

	return nil
}

// GetTransaction retrieves a transaction from any tier
func (tsm *TieredStorageManager) GetTransaction(id string) (*common.Transaction, error) {
	// Check cache first
	if tsm.config.CacheEnabled && tsm.cache != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "tx",
			Key:       id,
		}

		if value, ok := tsm.cache.GetTyped(cacheKey); ok {
			tsm.metrics.cacheHits.Add(1)
			// Deserialize transaction from cache
			if value.Data != nil {
				var tx common.Transaction
				if err := json.Unmarshal(value.Data, &tx); err == nil {
					return &tx, nil
				}
			}
		}
		tsm.metrics.cacheMisses.Add(1)
	}

	// Try primary storage
	tx, err := tsm.primary.GetTransaction(id)
	if err == nil {
		tsm.metrics.primaryReads.Add(1)
		tsm.cacheTransaction(tx)
		return tx, nil
	}

	// Try archive
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		tx, err := tsm.archive.GetTransaction(id)
		if err == nil {
			tsm.metrics.archiveReads.Add(1)
			tsm.cacheTransaction(tx)
			return tx, nil
		}
	}

	return nil, ErrNotFound
}

// SaveTransaction saves a transaction to primary storage and cache
func (tsm *TieredStorageManager) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	// For light nodes, save with pending status
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		return tsm.lightNode.SaveTransaction(tx, mobile.StatusPending)
	}

	// Save to primary storage
	if err := tsm.primary.SaveTransaction(tx, blockHeight); err != nil {
		return err
	}

	// Cache the transaction
	tsm.cacheTransaction(tx)

	return nil
}

// GetAccount retrieves an account state
func (tsm *TieredStorageManager) GetAccount(address string) (*common.Account, error) {
	// For light nodes, use light node storage
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		return tsm.lightNode.GetAccount(address)
	}

	// Check cache first
	if tsm.config.CacheEnabled && tsm.cache != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "account",
			Key:       address,
		}

		if value, ok := tsm.cache.GetTyped(cacheKey); ok {
			tsm.metrics.cacheHits.Add(1)
			// Deserialize account from cache
			if value.Data != nil {
				var account common.Account
				if err := json.Unmarshal(value.Data, &account); err == nil {
					return &account, nil
				}
			}
		}
		tsm.metrics.cacheMisses.Add(1)
	}

	// Get from primary storage
	tsm.metrics.primaryReads.Add(1)
	account, err := tsm.primary.GetAccount(address)
	if err != nil {
		return nil, err
	}

	// Cache the account
	tsm.cacheAccount(account)

	return account, nil
}

// SaveAccount saves an account to primary storage and cache
func (tsm *TieredStorageManager) SaveAccount(account *common.Account) error {
	// For light nodes, use light node storage
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		return tsm.lightNode.SaveAccount(account)
	}

	// Save to primary storage
	if err := tsm.primary.SaveAccount(account); err != nil {
		return err
	}

	// Cache the account
	tsm.cacheAccount(account)

	return nil
}

// GetState retrieves a state value
func (tsm *TieredStorageManager) GetState(key []byte) ([]byte, error) {
	// Check cache first
	if tsm.config.CacheEnabled && tsm.cache != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "state",
			Key:       string(key),
		}

		if value, ok := tsm.cache.GetTyped(cacheKey); ok {
			tsm.metrics.cacheHits.Add(1)
			return value.Data, nil
		}
		tsm.metrics.cacheMisses.Add(1)
	}

	// Get from primary storage
	tsm.metrics.primaryReads.Add(1)
	state, err := tsm.primary.GetState(key)
	if err != nil {
		return nil, err
	}

	// Cache the state
	if tsm.config.CacheEnabled && tsm.cache != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "state",
			Key:       string(key),
		}
		tsm.cache.SetTyped(cacheKey, &dtypes.Value{
			Type: dtypes.ValueTypeBytes,
			Data: state,
		})
	}

	return state, nil
}

// SaveState saves a state value
func (tsm *TieredStorageManager) SaveState(key []byte, value []byte) error {
	// Save to primary storage
	if err := tsm.primary.SaveState(key, value); err != nil {
		return err
	}

	// Cache the state
	if tsm.config.CacheEnabled && tsm.cache != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "state",
			Key:       string(key),
		}
		tsm.cache.SetTyped(cacheKey, &dtypes.Value{
			Type: dtypes.ValueTypeBytes,
			Data: value,
		})
	}

	return nil
}

// GetLatestBlock returns the latest block
func (tsm *TieredStorageManager) GetLatestBlock() (*common.Block, error) {
	// For light nodes, use light node storage
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		header, err := tsm.lightNode.GetLatestBlockHeader()
		if err != nil {
			return nil, err
		}
		return &header, nil
	}

	return tsm.primary.GetLatestBlock()
}

// GetReceipt retrieves a transaction receipt
func (tsm *TieredStorageManager) GetReceipt(txID string) (*Receipt, error) {
	// Try primary storage first
	receipt, err := tsm.primary.GetReceipt(txID)
	if err == nil {
		return receipt, nil
	}

	// Try archive
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		return tsm.archive.GetReceipt(txID)
	}

	return nil, ErrNotFound
}

// SaveReceipt saves a transaction receipt
func (tsm *TieredStorageManager) SaveReceipt(receipt *Receipt) error {
	return tsm.primary.SaveReceipt(receipt)
}

// WriteBatch executes a batch of write operations atomically
func (tsm *TieredStorageManager) WriteBatch(batch WriteBatch) error {
	// Execute batch on primary storage
	if err := tsm.primary.WriteBatch(batch); err != nil {
		return err
	}

	// Update cache with batch operations
	// This is best-effort, failures are logged but not returned
	go tsm.updateCacheFromBatch(batch)

	return nil
}

// HealthCheck performs health checks on all tiers
func (tsm *TieredStorageManager) HealthCheck(ctx context.Context) error {
	// Primary storage is critical - if it's healthy, storage is healthy
	if err := tsm.primary.HealthCheck(ctx); err != nil {
		return fmt.Errorf("primary storage unhealthy: %w", err)
	}

	// Optional components don't affect overall health
	// Log warnings but don't fail the health check

	// Check cache (optional)
	if tsm.config.CacheEnabled && tsm.cache != nil && !tsm.cacheDisabled.Load() {
		stats := tsm.cache.GetStats()
		if !stats.Healthy {
			tsm.logger.Debug("Redis cache unhealthy but not critical")
		}
	}

	// Check archive (optional)
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		if err := tsm.archive.HealthCheck(ctx); err != nil {
			tsm.logger.Debug("MongoDB archive unhealthy but not critical", "error", err)
		}
	}

	// Check light node (optional)
	if tsm.config.LightNodeMode && tsm.lightNode != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tsm.lightNode.HealthCheck(ctx); err != nil {
			tsm.logger.Debug("SQLite light node unhealthy but not critical", "error", err)
		}
	}

	// As long as primary storage is healthy, overall storage is healthy
	return nil
}

// GetStats returns storage statistics
func (tsm *TieredStorageManager) GetStats() (*StoreStats, error) {
	stats, err := tsm.primary.GetStats()
	if err != nil {
		return nil, err
	}

	// Add tiered storage specific stats
	stats.CacheHits = tsm.metrics.cacheHits.Load()
	stats.CacheMisses = tsm.metrics.cacheMisses.Load()
	stats.PrimaryReads = tsm.metrics.primaryReads.Load()
	stats.ArchiveReads = tsm.metrics.archiveReads.Load()
	stats.ArchivedBlocks = tsm.metrics.archivalBlocks.Load()

	// Add cache stats if available
	if tsm.cache != nil {
		cacheStats := tsm.cache.GetStats()
		stats.CacheSize = int64(cacheStats.Size)
		stats.CacheErrors = cacheStats.Errors
	}

	return stats, nil
}

// Helper methods

func (tsm *TieredStorageManager) cacheBlock(block *common.Block) {
	if tsm.isCacheAvailable() && block != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "block",
			Key:       fmt.Sprintf("%d", block.Number),
		}

		if blockData, err := json.Marshal(block); err == nil {
			tsm.cache.SetTyped(cacheKey, &dtypes.Value{
				Type: dtypes.ValueTypeBytes,
				Data: blockData,
			})
			tsm.resetCacheFailures() // Reset on successful operation
		}
	}
}

func (tsm *TieredStorageManager) cacheTransaction(tx *common.Transaction) {
	if tsm.isCacheAvailable() && tx != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "tx",
			Key:       tx.ID,
		}

		if txData, err := json.Marshal(tx); err == nil {
			tsm.cache.SetTyped(cacheKey, &dtypes.Value{
				Type: dtypes.ValueTypeBytes,
				Data: txData,
			})
			tsm.resetCacheFailures() // Reset on successful operation
		}
	}
}

func (tsm *TieredStorageManager) cacheAccount(account *common.Account) {
	if tsm.isCacheAvailable() && account != nil {
		cacheKey := &dtypes.CacheKey{
			Namespace: "account",
			Key:       account.ID,
		}

		if accData, err := json.Marshal(account); err == nil {
			tsm.cache.SetTyped(cacheKey, &dtypes.Value{
				Type: dtypes.ValueTypeBytes,
				Data: accData,
			})
			tsm.resetCacheFailures() // Reset on successful operation
		}
	}
}

func (tsm *TieredStorageManager) shouldArchive(block *common.Block) bool {
	if !tsm.config.ArchiveEnabled || tsm.archive == nil {
		return false
	}

	currentHeight := tsm.currentHeight.Load()
	return uint64(block.Number) < currentHeight-tsm.config.ArchiveThreshold
}

func (tsm *TieredStorageManager) warmupCache() {
	tsm.logger.Info("Starting cache warmup")

	// Get latest block
	latestBlock, err := tsm.primary.GetLatestBlock()
	if err != nil {
		tsm.logger.WithError(err).Warn("Failed to get latest block for cache warmup")
		return
	}

	// Cache recent blocks
	recentBlocks := 100
	for i := 0; i < recentBlocks && latestBlock.Number-i >= 0; i++ {
		block, err := tsm.primary.GetBlock(uint64(latestBlock.Number - i))
		if err != nil {
			continue
		}
		tsm.cacheBlock(block)
	}

	tsm.logger.Info("Cache warmup completed", "blocks", recentBlocks)
}

func (tsm *TieredStorageManager) startArchivalWorker() {
	tsm.archivalWg.Add(1)
	go func() {
		defer tsm.archivalWg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		batch := make([]*common.Block, 0, tsm.config.ArchivalBatchSize)

		for {
			select {
			case <-tsm.archivalStop:
				// Process remaining blocks
				if len(batch) > 0 {
					tsm.archiveBatch(batch)
				}
				return

			case block := <-tsm.archivalQueue:
				batch = append(batch, block)
				if len(batch) >= tsm.config.ArchivalBatchSize {
					tsm.archiveBatch(batch)
					batch = batch[:0]
				}

			case <-ticker.C:
				// Periodic archival of accumulated blocks
				if len(batch) > 0 {
					tsm.archiveBatch(batch)
					batch = batch[:0]
				}
				// Also check for old blocks to archive
				tsm.archiveOldBlocks()
			}
		}
	}()
}

func (tsm *TieredStorageManager) archiveBatch(blocks []*common.Block) {
	if len(blocks) == 0 || tsm.archive == nil {
		return
	}

	tsm.logger.Debug("Archiving block batch", "count", len(blocks))

	for _, block := range blocks {
		if err := tsm.archive.SaveBlock(block); err != nil {
			tsm.logger.WithError(err).Error("Failed to archive block", "height", block.Number)
			tsm.metrics.errors.Add(1)
			continue
		}

		// Archive transactions
		for _, tx := range block.Transactions {
			if err := tsm.archive.SaveTransaction(&tx, int(block.Number)); err != nil {
				tsm.logger.WithError(err).Error("Failed to archive transaction", "id", tx.ID)
				tsm.metrics.errors.Add(1)
			}
		}

		tsm.metrics.archivalBlocks.Add(1)
	}

	// Update archival height (with underflow protection)
	if len(blocks) > 0 {
		currentHeight := tsm.currentHeight.Load()
		// Only update archival height if it makes sense
		if currentHeight > tsm.config.ArchiveThreshold {
			newArchivalHeight := currentHeight - tsm.config.ArchiveThreshold
			tsm.archivalHeight.Store(newArchivalHeight)
		} else {
			tsm.archivalHeight.Store(0)
		}
	}

	tsm.logger.Info("Archived block batch", "count", len(blocks))
}

func (tsm *TieredStorageManager) archiveOldBlocks() {
	currentHeight := tsm.currentHeight.Load()
	archivalHeight := tsm.archivalHeight.Load()

	// Prevent underflow
	if currentHeight <= tsm.config.ArchiveThreshold {
		return // Not enough blocks to archive
	}

	targetHeight := currentHeight - tsm.config.ArchiveThreshold

	if archivalHeight >= targetHeight {
		return // Nothing to archive
	}

	tsm.logger.Debug("Checking for old blocks to archive",
		"current", currentHeight,
		"archival", archivalHeight,
		"target", targetHeight)

	batch := make([]*common.Block, 0, tsm.config.ArchivalBatchSize)

	for height := archivalHeight + 1; height <= targetHeight && len(batch) < tsm.config.ArchivalBatchSize; height++ {
		block, err := tsm.primary.GetBlock(height)
		if err != nil {
			tsm.logger.WithError(err).Warn("Failed to get block for archival", "height", height)
			continue
		}
		batch = append(batch, block)
	}

	if len(batch) > 0 {
		tsm.archiveBatch(batch)
	}
}

func (tsm *TieredStorageManager) updateCacheFromBatch(batch WriteBatch) {
	// This is a simplified implementation
	// In production, you would parse the batch operations and update cache accordingly
	tsm.logger.Debug("Updating cache from batch operations")
}

// Delegated methods to maintain LedgerStore interface compatibility

func (tsm *TieredStorageManager) SaveContract(contract *Contract) error {
	return tsm.primary.SaveContract(contract)
}

func (tsm *TieredStorageManager) GetContract(address string) (*Contract, error) {
	return tsm.primary.GetContract(address)
}

func (tsm *TieredStorageManager) Snapshot(path string) error {
	return tsm.primary.Snapshot(path)
}

func (tsm *TieredStorageManager) Restore(path string) error {
	return tsm.primary.Restore(path)
}

// GetBlockByHash retrieves a block by its hash
func (tsm *TieredStorageManager) GetBlockByHash(hash string) (*common.Block, error) {
	// Check primary first
	block, err := tsm.primary.GetBlockByHash(hash)
	if err == nil {
		return block, nil
	}

	// Check archive if enabled
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		return tsm.archive.GetBlockByHash(hash)
	}

	return nil, err
}

// GetBlockRange retrieves blocks within a range
func (tsm *TieredStorageManager) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	// For block range, we may need to query both primary and archive
	return tsm.primary.GetBlockRange(startHeight, endHeight)
}

// GetTransactionsByAddress retrieves transactions for an address
func (tsm *TieredStorageManager) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	// For now, just query primary
	return tsm.primary.GetTransactionsByAddress(address, limit, offset)
}

// Compact performs compaction on all storage tiers
func (tsm *TieredStorageManager) Compact() error {
	// Compact primary storage
	if err := tsm.primary.Compact(); err != nil {
		return fmt.Errorf("failed to compact primary storage: %w", err)
	}

	// Compact archive storage if enabled
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		if err := tsm.archive.Compact(); err != nil {
			tsm.logger.WithError(err).Warn("Failed to compact archive storage")
		}
	}

	return nil
}

// PruneData prunes data older than the specified time
func (tsm *TieredStorageManager) PruneData(olderThan time.Time) error {
	// Prune primary storage
	if err := tsm.primary.PruneData(olderThan); err != nil {
		return fmt.Errorf("failed to prune primary storage: %w", err)
	}

	// Prune archive storage if enabled
	if tsm.config.ArchiveEnabled && tsm.archive != nil {
		if err := tsm.archive.PruneData(olderThan); err != nil {
			tsm.logger.WithError(err).Warn("Failed to prune archive storage")
		}
	}

	return nil
}

// ReplaceBlockSameHeight atomically replaces a block at the same height (testnet-only conflict repair)
func (tsm *TieredStorageManager) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	// Replace in primary storage
	if err := tsm.primary.ReplaceBlockSameHeight(height, newBlock); err != nil {
		return fmt.Errorf("failed to replace block in primary storage: %w", err)
	}

	// Update current height if needed
	tsm.currentHeight.Store(uint64(newBlock.Number))

	// Update cache with new block
	tsm.cacheBlock(newBlock)

	// If archive is enabled and block should be archived, replace there too
	if tsm.config.ArchiveEnabled && tsm.archive != nil && tsm.shouldArchive(newBlock) {
		if err := tsm.archive.ReplaceBlockSameHeight(height, newBlock); err != nil {
			tsm.logger.WithError(err).Warn("Failed to replace block in archive storage")
		}
	}

	return nil
}
