//go:build !windows
// +build !windows

// storage/lmdb_adapter.go

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"diamante/common"

	"github.com/bmatsuo/lmdb-go/lmdb"
	//"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/sirupsen/logrus"
)

const (
	// Database names
	dbBlocks    = "blocks"
	dbTxs       = "transactions"
	dbAccounts  = "accounts"
	dbState     = "state"
	dbContracts = "contracts"
	dbReceipts  = "receipts"
	dbSnapshots = "snapshots"
	dbMetadata  = "metadata"

	// Default LMDB settings
	defaultMapSize    = 10 * 1024 * 1024 * 1024 // 10GB
	defaultMaxReaders = 126                     // LMDB default
	defaultMaxDBs     = 8                       // Number of named DBs we use
)

// LMDBConfig holds configuration for the LMDB adapter
type LMDBConfig struct {
	Path           string        // Directory path for LMDB files
	MapSize        int64         // Size of mmap region
	MaxReaders     int           // Maximum number of readers
	MaxDBs         int           // Maximum number of named databases
	SyncWrites     bool          // Whether to sync writes to disk
	ReadOnly       bool          // Whether to open in read-only mode
	NoMetaSync     bool          // Whether to disable meta page sync
	NoMemInit      bool          // Whether to disable memory initialization
	DirectIO       bool          // Whether to use direct I/O
	AutoResize     bool          // Whether to automatically resize the map
	ResizeFactor   float64       // Factor to resize by when needed (e.g., 1.5 = 150%)
	BackupInterval time.Duration // Interval for automatic backups (0 to disable)
	BackupPath     string        // Path where backups are stored
	StatsInterval  time.Duration // Interval for periodic stats logging (0 to disable)
}

// DefaultLMDBConfig returns the default LMDB configuration
func DefaultLMDBConfig() *LMDBConfig {
	return &LMDBConfig{
		Path:           "data/lmdb",
		MapSize:        defaultMapSize,
		MaxReaders:     defaultMaxReaders,
		MaxDBs:         defaultMaxDBs,
		SyncWrites:     true,
		ReadOnly:       false,
		NoMetaSync:     false,
		NoMemInit:      false,
		DirectIO:       false,
		AutoResize:     true,
		ResizeFactor:   1.5,
		BackupInterval: 0,
		BackupPath:     "",
		StatsInterval:  0,
	}
}

// LMDBAdapter implements the LedgerStore interface using LMDB
type LMDBAdapter struct {
	*BaseAdapter
	config       *LMDBConfig
	env          *lmdb.Env
	dbi          map[string]lmdb.DBI
	path         string
	mapSize      int64
	resizeMutex  sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	backupTicker *time.Ticker
	statsTicker  *time.Ticker
	wg           sync.WaitGroup
}

// NewLMDBAdapter creates a new LMDBAdapter
func NewLMDBAdapter(config *LMDBConfig, logger *logrus.Logger, cacheSize int) (*LMDBAdapter, error) {
	if config == nil {
		config = DefaultLMDBConfig()
	}

	adapterConfig := &AdapterConfig{
		CacheSize:           cacheSize,
		CacheTTL:            5 * time.Minute,
		MetricsEnabled:      true,
		HealthCheckInterval: 30 * time.Second,
		MaxConcurrentOps:    100,
		EnableCompression:   true,
		EnableEncryption:    false,
	}

	baseAdapter, err := NewBaseAdapter(logger, adapterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create base adapter: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &LMDBAdapter{
		BaseAdapter: baseAdapter,
		config:      config,
		path:        config.Path,
		mapSize:     config.MapSize,
		dbi:         make(map[string]lmdb.DBI),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Open opens the LMDB environment and initializes the databases
func (la *LMDBAdapter) Open() error {
	if la.IsOpen() {
		return nil // Already open
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(la.path, 0755); err != nil {
		return fmt.Errorf("failed to create LMDB directory: %w", err)
	}

	// Create backup directory if configured
	if la.config.BackupPath != "" {
		if err := os.MkdirAll(la.config.BackupPath, 0755); err != nil {
			return fmt.Errorf("failed to create backup directory: %w", err)
		}
	}

	// Create and configure the LMDB environment
	env, err := lmdb.NewEnv()
	if err != nil {
		return fmt.Errorf("failed to create LMDB environment: %w", err)
	}

	// Set maximum number of named databases
	if err := env.SetMaxDBs(la.config.MaxDBs); err != nil {
		return fmt.Errorf("failed to set max DBs: %w", err)
	}

	// Set maximum number of readers
	if err := env.SetMaxReaders(la.config.MaxReaders); err != nil {
		return fmt.Errorf("failed to set max readers: %w", err)
	}

	// Set map size
	if err := env.SetMapSize(la.mapSize); err != nil {
		return fmt.Errorf("failed to set map size: %w", err)
	}

	// Configure environment flags
	var flags uint
	if la.config.ReadOnly {
		flags |= lmdb.Readonly
	}
	if la.config.NoMetaSync {
		flags |= lmdb.NoMetaSync
	}
	if la.config.NoMemInit {
		flags |= lmdb.NoMemInit
	}
	if !la.config.SyncWrites {
		flags |= lmdb.NoSync
	}

	// Open the environment
	if err := env.Open(la.path, flags, 0644); err != nil {
		return fmt.Errorf("failed to open LMDB environment: %w", err)
	}

	la.env = env

	// Initialize databases
	if err := la.initDatabases(); err != nil {
		la.env.Close()
		return fmt.Errorf("failed to initialize databases: %w", err)
	}

	la.startBackgroundProcesses()

	la.SetOpen(true)
	la.logger.Info("LMDB adapter opened successfully")
	return nil
}

// initDatabases initializes the named databases
func (la *LMDBAdapter) initDatabases() error {
	// Define the databases to create
	databases := []string{
		dbBlocks,
		dbTxs,
		dbAccounts,
		dbState,
		dbContracts,
		dbReceipts,
		dbSnapshots,
		dbMetadata,
	}

	// Create each database
	err := la.env.Update(func(txn *lmdb.Txn) error {
		for _, dbName := range databases {
			dbi, err := txn.CreateDBI(dbName)
			if err != nil {
				return fmt.Errorf("failed to create database %s: %w", dbName, err)
			}
			// Don't store DBI here - it's transaction specific
			_ = dbi
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Now open DBIs for use - these can be cached
	err = la.env.View(func(txn *lmdb.Txn) error {
		for _, dbName := range databases {
			dbi, err := txn.OpenDBI(dbName, 0)
			if err != nil {
				return fmt.Errorf("failed to open database %s: %w", dbName, err)
			}
			la.dbi[dbName] = dbi
		}
		return nil
	})

	return err
}

// startBackgroundProcesses starts backup and stats routines based on configuration
func (la *LMDBAdapter) startBackgroundProcesses() {
	if la.config.BackupInterval > 0 && la.config.BackupPath != "" {
		la.backupTicker = time.NewTicker(la.config.BackupInterval)
		la.wg.Add(1)
		go la.backupRoutine()
	}
	if la.config.StatsInterval > 0 {
		la.statsTicker = time.NewTicker(la.config.StatsInterval)
		la.wg.Add(1)
		go la.statsRoutine()
	}
}

// stopBackgroundProcesses stops all background routines
func (la *LMDBAdapter) stopBackgroundProcesses() {
	if la.cancel != nil {
		la.cancel()
	}
	if la.backupTicker != nil {
		la.backupTicker.Stop()
	}
	if la.statsTicker != nil {
		la.statsTicker.Stop()
	}
	la.wg.Wait()
}

func (la *LMDBAdapter) backupRoutine() {
	defer la.wg.Done()
	for {
		select {
		case <-la.ctx.Done():
			return
		case <-la.backupTicker.C:
			timestamp := time.Now().Format("20060102_150405")
			backupPath := filepath.Join(la.config.BackupPath, "lmdb_backup_"+timestamp)
			if err := la.Backup(backupPath); err != nil {
				la.logger.Errorf("LMDB backup failed: %v", err)
				la.LogOperation("backup_error", time.Now(), err)
			} else {
				la.LogOperation("backup_success", time.Now(), nil)
			}
		}
	}
}

func (la *LMDBAdapter) statsRoutine() {
	defer la.wg.Done()
	for {
		select {
		case <-la.ctx.Done():
			return
		case <-la.statsTicker.C:
			stats, err := la.GetStats()
			if err != nil {
				la.logger.Errorf("LMDB stats error: %v", err)
				la.LogOperation("stats_error", time.Now(), err)
				continue
			}
			la.logger.WithFields(logrus.Fields(stats)).Debug("lmdb stats")
		}
	}
}

// Close closes the LMDB environment
func (la *LMDBAdapter) Close() error {
	if !la.IsOpen() {
		return nil // Already closed
	}

	la.stopBackgroundProcesses()

	// Close the environment
	la.env.Close()
	la.dbi = make(map[string]lmdb.DBI)

	la.SetOpen(false)
	la.logger.Info("LMDB adapter closed successfully")
	return nil
}

// GetDBPath returns the database file path
func (la *LMDBAdapter) GetDBPath() string {
	return la.path
}

// checkMapSize checks if the map size needs to be increased
func (la *LMDBAdapter) checkMapSize() error {
	if !la.config.AutoResize {
		return nil
	}

	la.resizeMutex.Lock()
	defer la.resizeMutex.Unlock()

	// Get current info
	info, err := la.env.Info()
	if err != nil {
		return fmt.Errorf("failed to get environment info: %w", err)
	}

	// Check if we need to resize
	pageSize := 4096 // Default page size for most systems
	if float64(info.MapSize-int64(info.LastPNO)*int64(pageSize))/float64(info.MapSize) < 0.2 {
		// Less than 20% free space, resize
		newSize := int64(float64(info.MapSize) * la.config.ResizeFactor)
		la.logger.Infof("Resizing LMDB map from %d to %d bytes", info.MapSize, newSize)

		if err := la.env.SetMapSize(newSize); err != nil {
			return fmt.Errorf("failed to resize map: %w", err)
		}

		la.mapSize = newSize
	}

	return nil
}

// SaveBlock saves a block to the database
func (la *LMDBAdapter) SaveBlock(block *common.Block) error {
	if err := la.CheckOpen(); err != nil {
		la.logger.Error("LMDB SaveBlock: not open", err)
		return err
	}

	la.logger.Debug("LMDB SaveBlock: starting", "blockNumber", block.Number, "hash", block.Hash)

	startTime := time.Now()
	defer func() {
		la.LogOperation("save_block", startTime, nil)
	}()

	// Check if block already exists
	la.logger.Debug("LMDB SaveBlock: checking if block exists", "blockNumber", block.Number)
	exists, err := la.blockExists(uint64(block.Number))
	if err != nil {
		la.logger.Error("LMDB SaveBlock: error checking block existence", err, "blockNumber", block.Number)
		return err
	}
	if exists {
		la.logger.Debug("LMDB SaveBlock: block already exists", "blockNumber", block.Number)
		return ErrAlreadyExists
	}

	// Serialize the block
	la.logger.Debug("LMDB SaveBlock: marshaling block", "blockNumber", block.Number)
	blockData, err := json.Marshal(block)
	if err != nil {
		la.logger.Error("LMDB SaveBlock: failed to marshal block", err, "blockNumber", block.Number)
		return fmt.Errorf("failed to marshal block: %w", err)
	}
	la.logger.Debug("LMDB SaveBlock: block marshaled", "blockNumber", block.Number, "dataSize", len(blockData))

	// Check map size before writing
	la.logger.Debug("LMDB SaveBlock: checking map size", "blockNumber", block.Number)
	if err := la.checkMapSize(); err != nil {
		la.logger.Error("LMDB SaveBlock: map size check failed", err, "blockNumber", block.Number)
		return err
	}

	// Write to database
	la.logger.Info("LMDB SaveBlock: starting update transaction", "blockNumber", block.Number)
	err = la.env.Update(func(txn *lmdb.Txn) error {
		la.logger.Info("LMDB SaveBlock: inside transaction", "blockNumber", block.Number)
		dbi := la.dbi[dbBlocks]
		la.logger.Info("LMDB SaveBlock: got DBI handle", "blockNumber", block.Number, "dbiNil", dbi == 0)

		// Key is block number as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(block.Number))

		// Put block data
		la.logger.Info("LMDB SaveBlock: putting block data", "blockNumber", block.Number, "key", fmt.Sprintf("%x", key), "dataLen", len(blockData))
		if err := txn.Put(dbi, key, blockData, 0); err != nil {
			la.logger.Error("LMDB SaveBlock: failed to put block", err, "blockNumber", block.Number)
			return fmt.Errorf("failed to put block: %w", err)
		}
		la.logger.Info("LMDB SaveBlock: block data put successfully", "blockNumber", block.Number)

		// Also index by hash
		hashKey := []byte("h:" + block.Hash)
		la.logger.Debug("LMDB SaveBlock: putting hash index", "blockNumber", block.Number, "hashKey", string(hashKey))
		if err := txn.Put(dbi, hashKey, key, 0); err != nil {
			la.logger.Error("LMDB SaveBlock: failed to put hash index", err, "blockNumber", block.Number)
			return fmt.Errorf("failed to put block hash index: %w", err)
		}

		// Update latest height key for O(1) retrieval
		// Only update if this block is higher than the current latest
		latestHeightKey := []byte("latest_height")
		currentLatestBytes, err := txn.Get(dbi, latestHeightKey)
		updateLatest := true
		if err == nil && len(currentLatestBytes) == 8 {
			currentLatest := binary.BigEndian.Uint64(currentLatestBytes)
			if uint64(block.Number) <= currentLatest {
				updateLatest = false
				la.logger.Debug("LMDB SaveBlock: not updating latest height, current is higher",
					"blockNumber", block.Number,
					"currentLatest", currentLatest)
			}
		}

		if updateLatest {
			heightBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(heightBytes, uint64(block.Number))
			if err := txn.Put(dbi, latestHeightKey, heightBytes, 0); err != nil {
				la.logger.Error("LMDB SaveBlock: failed to update latest height", err, "blockNumber", block.Number)
				return fmt.Errorf("failed to update latest height: %w", err)
			}
			la.logger.Info("LMDB SaveBlock: updated latest height",
				"event", "UpdateLatestHeight",
				"height", block.Number,
				"result", "success")
		}

		la.logger.Info("LMDB SaveBlock: transaction complete", "blockNumber", block.Number)
		return nil
	})

	if err != nil {
		la.logger.Error("LMDB SaveBlock: update transaction failed", err, "blockNumber", block.Number)
		return err
	}

	// Add to cache
	la.logger.Debug("LMDB SaveBlock: caching block", "blockNumber", block.Number)
	la.CacheBlock(block)

	la.logger.Info("LMDB SaveBlock: block saved successfully", "blockNumber", block.Number, "hash", block.Hash)
	return nil
}

// ReplaceBlockSameHeight atomically replaces a block at the same height (testnet-only conflict repair)
func (la *LMDBAdapter) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	if err := la.CheckOpen(); err != nil {
		la.logger.Error("LMDB ReplaceBlockSameHeight: not open", err)
		return err
	}

	// Verify the new block is for the correct height
	if uint64(newBlock.Number) != height {
		return fmt.Errorf("block height mismatch: expected %d, got %d", height, newBlock.Number)
	}

	la.logger.Info("LMDB ReplaceBlockSameHeight: starting atomic replace",
		"event", "ReplaceConflictingBlock",
		"height", height,
		"newHash8", newBlock.Hash[:8])

	startTime := time.Now()
	defer func() {
		la.LogOperation("replace_block_same_height", startTime, nil)
	}()

	// Single atomic transaction
	err := la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Key is block number as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		// Verify existing block exists
		oldBlockData, err := txn.Get(dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return fmt.Errorf("no existing block at height %d", height)
			}
			return fmt.Errorf("failed to get existing block: %w", err)
		}

		// Parse old block for logging
		var oldBlock common.Block
		if err := json.Unmarshal(oldBlockData, &oldBlock); err != nil {
			la.logger.Warn("Failed to unmarshal old block for logging", "error", err)
		}

		// Serialize new block
		newBlockData, err := json.Marshal(newBlock)
		if err != nil {
			return fmt.Errorf("failed to marshal new block: %w", err)
		}

		// Delete old block entry (not strictly necessary with Put overwrite, but explicit)
		if err := txn.Del(dbi, key, nil); err != nil && !lmdb.IsNotFound(err) {
			return fmt.Errorf("failed to delete old block: %w", err)
		}

		// Write new block
		if err := txn.Put(dbi, key, newBlockData, 0); err != nil {
			return fmt.Errorf("failed to write new block: %w", err)
		}

		// Update hash index if it exists
		hashIndexDB := la.dbi[dbMetadata]
		oldHashKey := []byte("hash:" + oldBlock.Hash)
		newHashKey := []byte("hash:" + newBlock.Hash)

		// Delete old hash index
		if err := txn.Del(hashIndexDB, oldHashKey, nil); err != nil && !lmdb.IsNotFound(err) {
			la.logger.Warn("Failed to delete old hash index", "error", err)
		}

		// Create new hash index
		if err := txn.Put(hashIndexDB, newHashKey, key, 0); err != nil {
			la.logger.Warn("Failed to create new hash index", "error", err)
		}

		// Do NOT update latest height - we're replacing at same height

		la.logger.Info("LMDB ReplaceBlockSameHeight: atomic replace complete",
			"event", "ReplaceConflictingBlock",
			"height", height,
			"oldHash8", oldBlock.Hash[:8],
			"newHash8", newBlock.Hash[:8],
			"proposerOK", true,
			"prevOK", true)

		return nil
	})

	if err != nil {
		la.logger.Error("LMDB ReplaceBlockSameHeight: transaction failed", err, "height", height)
		return err
	}

	// Update cache with new block
	la.CacheBlock(newBlock)

	la.logger.Info("LMDB ReplaceBlockSameHeight: replacement successful",
		"height", height,
		"newHash", newBlock.Hash)
	return nil
}

// blockExists checks if a block exists
func (la *LMDBAdapter) blockExists(height uint64) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Key is block number as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		_, err := txn.Get(dbi, key)
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// GetBlock retrieves a block by its height
func (la *LMDBAdapter) GetBlock(height uint64) (*common.Block, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_block", startTime, nil)
	}()

	// Check cache first
	if block, found := la.GetCachedBlock(height); found {
		return block, nil
	}

	var block common.Block

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Key is block number as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		// Get block data
		blockData, err := txn.Get(dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get block: %w", err)
		}

		// Unmarshal block
		if err := json.Unmarshal(blockData, &block); err != nil {
			return fmt.Errorf("failed to unmarshal block: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Add to cache
	la.CacheBlock(&block)

	return &block, nil
}

// GetBlockByHash retrieves a block by its hash
func (la *LMDBAdapter) GetBlockByHash(hash string) (*common.Block, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_block_by_hash", startTime, nil)
	}()

	var block common.Block

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Get block number from hash index
		heightBytes, err := txn.Get(dbi, []byte("h:"+hash))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get block hash index: %w", err)
		}

		// Get block data
		blockData, err := txn.Get(dbi, heightBytes)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get block: %w", err)
		}

		// Unmarshal block
		if err := json.Unmarshal(blockData, &block); err != nil {
			return fmt.Errorf("failed to unmarshal block: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Add to cache
	la.CacheBlock(&block)

	return &block, nil
}

// GetBlockRange retrieves blocks within a range of heights
func (la *LMDBAdapter) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_block_range", startTime, nil)
	}()

	var blocks []*common.Block

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Start key
		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, startHeight)

		// End key
		endKey := make([]byte, 8)
		binary.BigEndian.PutUint64(endKey, endHeight)

		// Iterate through range
		for key, blockData, err := cursor.Get(startKey, nil, lmdb.SetRange); err == nil; key, blockData, err = cursor.Get(nil, nil, lmdb.Next) {

			// Check if key is a hash index
			if len(key) > 0 && key[0] == 'h' {
				continue
			}

			// Check if we've gone past the end key
			if len(key) == 8 && binary.BigEndian.Uint64(key) > endHeight {
				break
			}

			// Unmarshal block
			var block common.Block
			if err := json.Unmarshal(blockData, &block); err != nil {
				return fmt.Errorf("failed to unmarshal block: %w", err)
			}

			blocks = append(blocks, &block)

			// Add to cache
			la.CacheBlock(&block)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// GetLatestBlock retrieves the latest block
func (la *LMDBAdapter) GetLatestBlock() (*common.Block, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_latest_block", startTime, nil)
	}()

	var latestHeight uint64
	var latestBlock *common.Block

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// First try to get the latest height from our optimized key
		latestHeightKey := []byte("latest_height")
		heightBytes, err := txn.Get(dbi, latestHeightKey)
		if err == nil && len(heightBytes) == 8 {
			// We have a cached latest height
			latestHeight = binary.BigEndian.Uint64(heightBytes)
			la.logger.Debug("LMDB GetLatestBlock: found cached latest height", "height", latestHeight)

			// Now get the block at this height
			blockKey := make([]byte, 8)
			binary.BigEndian.PutUint64(blockKey, latestHeight)
			blockData, err := txn.Get(dbi, blockKey)
			if err == nil {
				var block common.Block
				if err := json.Unmarshal(blockData, &block); err == nil {
					latestBlock = &block
					la.logger.Info("LMDB GetLatestBlock: found block via cached height",
						"event", "GetLatestBlock",
						"source", "meta",
						"blockNumber", block.Number,
						"hash", block.Hash)
					return nil
				}
			}
		}

		// Fall back to scanning if we don't have cached latest height
		la.logger.Debug("LMDB GetLatestBlock: falling back to scan method")

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Get first entry
		key, blockData, err := cursor.Get(nil, nil, lmdb.First)
		if err != nil {
			if lmdb.IsNotFound(err) {
				la.logger.Debug("LMDB GetLatestBlock: database is empty")
				return nil
			}
			return fmt.Errorf("failed to get first entry: %w", err)
		}

		// Iterate through all entries to find the highest block number
		highestNumber := uint64(0)
		for {
			// Skip non-block entries (like hash indexes and latest_height)
			if len(key) == 8 && !bytes.Equal(key, latestHeightKey) {
				// This looks like a block key
				blockNum := binary.BigEndian.Uint64(key)

				// If this is the highest block we've seen, try to parse it
				if blockNum > highestNumber {
					var block common.Block
					if err := json.Unmarshal(blockData, &block); err != nil {
						la.logger.Error("Failed to unmarshal block", err, "blockNumber", blockNum)
					} else {
						highestNumber = blockNum
						// Create a new block instance to avoid pointer issues
						newBlock := block
						latestBlock = &newBlock
						la.logger.Debug("LMDB GetLatestBlock: found higher block", "blockNumber", blockNum, "hash", block.Hash)
					}
				}
			}

			// Get next entry
			key, blockData, err = cursor.Get(nil, nil, lmdb.Next)
			if err != nil {
				if lmdb.IsNotFound(err) {
					break // End of database
				}
				return fmt.Errorf("cursor get error: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if latestBlock == nil {
		la.logger.Debug("LMDB GetLatestBlock: no blocks found",
			"event", "GetLatestBlock",
			"source", "scan",
			"result", "not_found")
		return nil, ErrNotFound
	}

	la.logger.Info("LMDB GetLatestBlock: returning latest block",
		"event", "GetLatestBlock",
		"source", "scan",
		"blockNumber", latestBlock.Number,
		"hash", latestBlock.Hash)

	// Add to cache
	la.CacheBlock(latestBlock)

	return latestBlock, nil
}

// SaveTransaction saves a transaction to the database
func (la *LMDBAdapter) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("save_transaction", startTime, nil)
	}()

	// Check if transaction already exists
	exists, err := la.transactionExists(tx.ID)
	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}

	// Set block height
	tx.BlockHeight = blockHeight

	// Serialize the transaction
	txData, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbTxs]

		// Put transaction data
		if err := txn.Put(dbi, []byte(tx.ID), txData, 0); err != nil {
			return fmt.Errorf("failed to put transaction: %w", err)
		}

		// Index by sender
		if err := txn.Put(dbi, []byte("s:"+tx.Sender+":"+tx.ID), []byte(tx.ID), 0); err != nil {
			return fmt.Errorf("failed to put sender index: %w", err)
		}

		// Index by receiver
		if err := txn.Put(dbi, []byte("r:"+tx.Receiver+":"+tx.ID), []byte(tx.ID), 0); err != nil {
			return fmt.Errorf("failed to put receiver index: %w", err)
		}

		// Index by block height
		blockKey := make([]byte, 8)
		binary.BigEndian.PutUint64(blockKey, uint64(blockHeight))
		if err := txn.Put(dbi, append([]byte("b:"), append(blockKey, []byte(":"+tx.ID)...)...), []byte(tx.ID), 0); err != nil {
			return fmt.Errorf("failed to put block index: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Add to cache
	la.CacheTransaction(tx)

	return nil
}

// transactionExists checks if a transaction exists
func (la *LMDBAdapter) transactionExists(txID string) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbTxs]

		_, err := txn.Get(dbi, []byte(txID))
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// GetTransaction retrieves a transaction by its ID
func (la *LMDBAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_transaction", startTime, nil)
	}()

	// Check cache first
	if tx, found := la.GetCachedTransaction(txID); found {
		return tx, nil
	}

	var transaction common.Transaction

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbTxs]

		// Get transaction data
		txData, err := txn.Get(dbi, []byte(txID))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get transaction: %w", err)
		}

		// Unmarshal transaction
		if err := json.Unmarshal(txData, &transaction); err != nil {
			return fmt.Errorf("failed to unmarshal transaction: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Add to cache
	la.CacheTransaction(&transaction)

	return &transaction, nil
}

// GetTransactionsByAddress retrieves transactions for a given address
func (la *LMDBAdapter) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_transactions_by_address", startTime, nil)
	}()

	var transactions []*common.Transaction
	var txIDs []string

	// First, collect all transaction IDs for the address
	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbTxs]

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Collect sender transactions
		senderPrefix := []byte("s:" + address + ":")
		for key, val, err := cursor.Get(senderPrefix, nil, lmdb.SetRange); err == nil; key, val, err = cursor.Get(nil, nil, lmdb.Next) {
			// Check if we've gone past the prefix
			if !bytes.HasPrefix(key, senderPrefix) {
				break
			}

			txIDs = append(txIDs, string(val))
		}

		// Collect receiver transactions
		receiverPrefix := []byte("r:" + address + ":")

		for key, val, err := cursor.Get(receiverPrefix, nil, lmdb.SetRange); err == nil; key, val, err = cursor.Get(nil, nil, lmdb.Next) {
			// Check if we've gone past the prefix
			if !bytes.HasPrefix(key, receiverPrefix) {
				break
			}

			txIDs = append(txIDs, string(val))
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Apply pagination
	if offset >= len(txIDs) {
		return []*common.Transaction{}, nil
	}

	end := offset + limit
	if end > len(txIDs) {
		end = len(txIDs)
	}

	txIDs = txIDs[offset:end]

	// Now fetch the actual transactions
	for _, txID := range txIDs {
		tx, err := la.GetTransaction(txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Skip not found transactions
				continue
			}
			return nil, err
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// GetTransactionsByBlock retrieves transactions for a given block
func (la *LMDBAdapter) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_transactions_by_block", startTime, nil)
	}()

	var transactions []*common.Transaction
	var txIDs []string

	// First, collect all transaction IDs for the block
	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbTxs]

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Block height prefix
		blockKey := make([]byte, 8)
		binary.BigEndian.PutUint64(blockKey, blockHeight)
		blockPrefix := append([]byte("b:"), blockKey...)

		for key, val, err := cursor.Get(blockPrefix, nil, lmdb.SetRange); err == nil; key, val, err = cursor.Get(nil, nil, lmdb.Next) {

			// Check if we've gone past the prefix
			if !bytes.HasPrefix(key, blockPrefix) {
				break
			}

			txIDs = append(txIDs, string(val))
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Now fetch the actual transactions
	for _, txID := range txIDs {
		tx, err := la.GetTransaction(txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Skip not found transactions
				continue
			}
			return nil, err
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// SaveAccount saves an account to the database
func (la *LMDBAdapter) SaveAccount(account *common.Account) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("save_account", startTime, nil)
	}()

	// Debug logging
	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"balance":   account.Balance,
		"nonce":     account.Nonce,
	}).Debug("Attempting to save account")

	// Check if account already exists
	exists, err := la.accountExists(account.ID)
	if err != nil {
		return err
	}
	if exists {
		// If account exists, update it instead
		la.logger.WithFields(logrus.Fields{
			"accountID": account.ID,
		}).Debug("Account already exists, updating instead")
		return la.UpdateAccount(account)
	}

	// Serialize the account
	accountData, err := json.Marshal(account)
	if err != nil {
		return fmt.Errorf("failed to marshal account: %w", err)
	}

	// Debug: Log the serialized JSON
	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"jsonData":  string(accountData),
	}).Debug("Serialized account JSON")

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbAccounts]

		// Put account data
		if err := txn.Put(dbi, []byte(account.ID), accountData, 0); err != nil {
			return fmt.Errorf("failed to put account: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Add to cache
	la.CacheAccount(account)

	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"balance":   account.Balance,
	}).Info("Account saved successfully")

	return nil
}

// accountExists checks if an account exists
func (la *LMDBAdapter) accountExists(accountID string) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbAccounts]

		_, err := txn.Get(dbi, []byte(accountID))
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// GetAccount retrieves an account by its ID
func (la *LMDBAdapter) GetAccount(accountID string) (*common.Account, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_account", startTime, nil)
	}()

	la.logger.WithFields(logrus.Fields{
		"accountID": accountID,
	}).Debug("Attempting to get account")

	// Check cache first
	if account, found := la.GetCachedAccount(accountID); found {
		la.logger.WithFields(logrus.Fields{
			"accountID": accountID,
			"balance":   account.Balance,
		}).Debug("Account found in cache")
		return account, nil
	}

	var account common.Account

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbAccounts]

		// Get account data
		accountData, err := txn.Get(dbi, []byte(accountID))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get account: %w", err)
		}

		// Debug: Log raw account data
		la.logger.WithFields(logrus.Fields{
			"accountID":  accountID,
			"dataLength": len(accountData),
		}).Debug("Raw account data retrieved from LMDB")

		// Unmarshal account
		if err := json.Unmarshal(accountData, &account); err != nil {
			la.logger.WithFields(logrus.Fields{
				"accountID": accountID,
				"rawData":   string(accountData),
				"error":     err,
			}).Error("Failed to unmarshal account data")
			return fmt.Errorf("failed to unmarshal account: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Add to cache
	la.CacheAccount(&account)

	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"balance":   account.Balance,
		"nonce":     account.Nonce,
	}).Info("Account retrieved successfully")

	return &account, nil
}

// UpdateAccount updates an account in the database
func (la *LMDBAdapter) UpdateAccount(account *common.Account) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("update_account", startTime, nil)
	}()

	// Debug logging
	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"balance":   account.Balance,
		"nonce":     account.Nonce,
	}).Debug("Attempting to update account")

	// Check if account exists
	exists, err := la.accountExists(account.ID)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	// Serialize the account
	accountData, err := json.Marshal(account)
	if err != nil {
		return fmt.Errorf("failed to marshal account: %w", err)
	}

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbAccounts]

		// Put account data
		if err := txn.Put(dbi, []byte(account.ID), accountData, 0); err != nil {
			return fmt.Errorf("failed to put account: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Update cache
	la.CacheAccount(account)

	la.logger.WithFields(logrus.Fields{
		"accountID": account.ID,
		"balance":   account.Balance,
	}).Info("Account updated successfully")

	return nil
}

// GetState retrieves state data by key
func (la *LMDBAdapter) GetState(key []byte) ([]byte, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_state", startTime, nil)
	}()

	// Check cache first
	if v, ok := la.GetCachedState(string(key)); ok {
		return v, nil
	}

	var value []byte

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbState]

		// Get state data
		val, err := txn.Get(dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get state: %w", err)
		}

		// Copy the value since it's only valid during the transaction
		value = make([]byte, len(val))
		copy(value, val)

		return nil
	})

	if err != nil {
		return nil, err
	}

	la.CacheState(string(key), value)
	return value, nil
}

// SetState sets state data by key
func (la *LMDBAdapter) SetState(key, value []byte) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("set_state", startTime, nil)
	}()

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err := la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbState]

		// Put state data
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return fmt.Errorf("failed to put state: %w", err)
		}

		return nil
	})
	if err == nil {
		la.CacheState(string(key), value)
	}
	return err
}

// DeleteState deletes state data by key
func (la *LMDBAdapter) DeleteState(key []byte) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("delete_state", startTime, nil)
	}()

	// Check if state exists
	exists, err := la.stateExists(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	// Delete from database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbState]

		// Delete state data
		if err := txn.Del(dbi, key, nil); err != nil {
			return fmt.Errorf("failed to delete state: %w", err)
		}

		return nil
	})

	if err == nil {
		la.stateCache.Delete(string(key))
	}
	return err
}

// stateExists checks if a state key exists
func (la *LMDBAdapter) stateExists(key []byte) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbState]

		_, err := txn.Get(dbi, key)
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// SaveContract saves a smart contract to the database
func (la *LMDBAdapter) SaveContract(contract *common.SmartContract) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("save_contract", startTime, nil)
	}()

	// Check if contract already exists
	exists, err := la.contractExists(contract.ID)
	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}

	// Convert State to map[string]interface{}
	var stateMap map[string]interface{}
	if contract.State != nil {
		stateBytes, _ := json.Marshal(contract.State)
		json.Unmarshal(stateBytes, &stateMap)
	}

	// Convert Metadata to map[string]interface{}
	var metadataMap map[string]interface{}
	if contract.Metadata != nil {
		metadataBytes, _ := json.Marshal(contract.Metadata)
		json.Unmarshal(metadataBytes, &metadataMap)
	}

	// Serialize the contract (excluding function types that can't be marshalled)
	serializableContract := struct {
		ID        string                      `json:"id"`
		Code      string                      `json:"code"`
		CodeHash  string                      `json:"codeHash"`
		Owner     string                      `json:"owner"`
		Version   string                      `json:"version"`
		State     map[string]interface{}      `json:"state"`
		ABI       string                      `json:"abi"`
		Language  string                      `json:"language"`
		Events    []common.SmartContractEvent `json:"events"`
		GasUsage  float64                     `json:"gasUsage"`
		Metadata  map[string]interface{}      `json:"metadata"`
		CreatedAt time.Time                   `json:"createdAt"`
		UpdatedAt time.Time                   `json:"updatedAt"`
	}{
		ID:        contract.ID,
		Code:      contract.Code,
		CodeHash:  contract.CodeHash,
		Owner:     contract.Owner,
		Version:   contract.Version,
		State:     stateMap,
		ABI:       contract.ABI,
		Language:  contract.Language,
		Events:    contract.Events,
		GasUsage:  contract.GasUsage,
		Metadata:  metadataMap,
		CreatedAt: contract.CreatedAt,
		UpdatedAt: contract.UpdatedAt,
	}

	contractData, err := json.Marshal(serializableContract)
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %w", err)
	}

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbContracts]

		// Put contract data
		if err := txn.Put(dbi, []byte(contract.ID), contractData, 0); err != nil {
			return fmt.Errorf("failed to put contract: %w", err)
		}

		return nil
	})

	return err
}

// contractExists checks if a contract exists
func (la *LMDBAdapter) contractExists(contractID string) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbContracts]

		_, err := txn.Get(dbi, []byte(contractID))
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// GetContract retrieves a smart contract by its ID
func (la *LMDBAdapter) GetContract(contractID string) (*common.SmartContract, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_contract", startTime, nil)
	}()

	var contract common.SmartContract

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbContracts]

		// Get contract data
		contractData, err := txn.Get(dbi, []byte(contractID))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get contract: %w", err)
		}

		// Unmarshal contract
		if err := json.Unmarshal(contractData, &contract); err != nil {
			return fmt.Errorf("failed to unmarshal contract: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &contract, nil
}

// UpdateContract updates a smart contract in the database
func (la *LMDBAdapter) UpdateContract(contract *common.SmartContract) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("update_contract", startTime, nil)
	}()

	// Check if contract exists
	exists, err := la.contractExists(contract.ID)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	// Convert State to map[string]interface{}
	var stateMap map[string]interface{}
	if contract.State != nil {
		stateBytes, _ := json.Marshal(contract.State)
		json.Unmarshal(stateBytes, &stateMap)
	}

	// Convert Metadata to map[string]interface{}
	var metadataMap map[string]interface{}
	if contract.Metadata != nil {
		metadataBytes, _ := json.Marshal(contract.Metadata)
		json.Unmarshal(metadataBytes, &metadataMap)
	}

	// Serialize the contract (excluding function types that can't be marshalled)
	serializableContract := struct {
		ID        string                      `json:"id"`
		Code      string                      `json:"code"`
		CodeHash  string                      `json:"codeHash"`
		Owner     string                      `json:"owner"`
		Version   string                      `json:"version"`
		State     map[string]interface{}      `json:"state"`
		ABI       string                      `json:"abi"`
		Language  string                      `json:"language"`
		Events    []common.SmartContractEvent `json:"events"`
		GasUsage  float64                     `json:"gasUsage"`
		Metadata  map[string]interface{}      `json:"metadata"`
		CreatedAt time.Time                   `json:"createdAt"`
		UpdatedAt time.Time                   `json:"updatedAt"`
	}{
		ID:        contract.ID,
		Code:      contract.Code,
		CodeHash:  contract.CodeHash,
		Owner:     contract.Owner,
		Version:   contract.Version,
		State:     stateMap,
		ABI:       contract.ABI,
		Language:  contract.Language,
		Events:    contract.Events,
		GasUsage:  contract.GasUsage,
		Metadata:  metadataMap,
		CreatedAt: contract.CreatedAt,
		UpdatedAt: contract.UpdatedAt,
	}

	contractData, err := json.Marshal(serializableContract)
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %w", err)
	}

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbContracts]

		// Put contract data
		if err := txn.Put(dbi, []byte(contract.ID), contractData, 0); err != nil {
			return fmt.Errorf("failed to put contract: %w", err)
		}

		return nil
	})

	return err
}

// DeleteContract deletes a smart contract from the database
func (la *LMDBAdapter) DeleteContract(contractID string) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("delete_contract", startTime, nil)
	}()

	// Check if contract exists
	exists, err := la.contractExists(contractID)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	// Delete from database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbContracts]

		// Delete contract data
		if err := txn.Del(dbi, []byte(contractID), nil); err != nil {
			return fmt.Errorf("failed to delete contract: %w", err)
		}

		return nil
	})

	return err
}

// SaveReceipt saves a transaction receipt to the database
func (la *LMDBAdapter) SaveReceipt(receipt *Receipt) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("save_receipt", startTime, nil)
	}()

	// Check if receipt already exists
	exists, err := la.receiptExists(receipt.TxID)
	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}

	// Serialize the receipt
	receiptData, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("failed to marshal receipt: %w", err)
	}

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write to database
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbReceipts]

		// Put receipt data
		if err := txn.Put(dbi, []byte(receipt.TxID), receiptData, 0); err != nil {
			return fmt.Errorf("failed to put receipt: %w", err)
		}

		// Index by block height
		blockKey := make([]byte, 8)
		binary.BigEndian.PutUint64(blockKey, receipt.BlockHeight)
		if err := txn.Put(dbi, append([]byte("b:"), append(blockKey, []byte(":"+receipt.TxID)...)...), []byte(receipt.TxID), 0); err != nil {
			return fmt.Errorf("failed to put block index: %w", err)
		}

		return nil
	})

	return err
}

// receiptExists checks if a receipt exists
func (la *LMDBAdapter) receiptExists(txID string) (bool, error) {
	var exists bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbReceipts]

		_, err := txn.Get(dbi, []byte(txID))
		if err == nil {
			exists = true
		} else if lmdb.IsNotFound(err) {
			exists = false
		} else {
			return err
		}

		return nil
	})

	return exists, err
}

// GetReceipt retrieves a transaction receipt by its transaction ID
func (la *LMDBAdapter) GetReceipt(txID string) (*Receipt, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_receipt", startTime, nil)
	}()

	var receipt Receipt

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbReceipts]

		// Get receipt data
		receiptData, err := txn.Get(dbi, []byte(txID))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get receipt: %w", err)
		}

		// Unmarshal receipt
		if err := json.Unmarshal(receiptData, &receipt); err != nil {
			return fmt.Errorf("failed to unmarshal receipt: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &receipt, nil
}

// BatchWrite performs a batch of write operations atomically
func (la *LMDBAdapter) BatchWrite(batch *WriteBatch) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	if batch.IsEmpty() {
		return nil // Nothing to do
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("batch_write", startTime, nil)
	}()

	// Check map size before writing
	if err := la.checkMapSize(); err != nil {
		return err
	}

	// Write all operations in a single transaction
	err := la.env.Update(func(txn *lmdb.Txn) error {
		// Write blocks
		for _, block := range batch.Blocks {
			blockData, err := json.Marshal(block)
			if err != nil {
				return fmt.Errorf("failed to marshal block: %w", err)
			}

			// Key is block number as big-endian uint64
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(block.Number))

			// Put block data
			if err := txn.Put(la.dbi[dbBlocks], key, blockData, 0); err != nil {
				return fmt.Errorf("failed to put block: %w", err)
			}

			// Also index by hash
			if err := txn.Put(la.dbi[dbBlocks], []byte("h:"+block.Hash), key, 0); err != nil {
				return fmt.Errorf("failed to put block hash index: %w", err)
			}

			// Add to cache
			la.CacheBlock(block)
		}

		// Write transactions
		for _, tx := range batch.Transactions {
			txData, err := json.Marshal(tx)
			if err != nil {
				return fmt.Errorf("failed to marshal transaction: %w", err)
			}

			// Put transaction data
			if err := txn.Put(la.dbi[dbTxs], []byte(tx.ID), txData, 0); err != nil {
				return fmt.Errorf("failed to put transaction: %w", err)
			}

			// Index by sender
			if err := txn.Put(la.dbi[dbTxs], []byte("s:"+tx.Sender+":"+tx.ID), []byte(tx.ID), 0); err != nil {
				return fmt.Errorf("failed to put sender index: %w", err)
			}

			// Index by receiver
			if err := txn.Put(la.dbi[dbTxs], []byte("r:"+tx.Receiver+":"+tx.ID), []byte(tx.ID), 0); err != nil {
				return fmt.Errorf("failed to put receiver index: %w", err)
			}

			// Index by block height
			blockKey := make([]byte, 8)
			binary.BigEndian.PutUint64(blockKey, uint64(tx.BlockHeight))
			if err := txn.Put(la.dbi[dbTxs], append([]byte("b:"), append(blockKey, []byte(":"+tx.ID)...)...), []byte(tx.ID), 0); err != nil {
				return fmt.Errorf("failed to put block index: %w", err)
			}

			// Add to cache
			la.CacheTransaction(tx)
		}

		// Write accounts
		for _, account := range batch.Accounts {
			accountData, err := json.Marshal(account)
			if err != nil {
				return fmt.Errorf("failed to marshal account: %w", err)
			}

			// Put account data
			if err := txn.Put(la.dbi[dbAccounts], []byte(account.ID), accountData, 0); err != nil {
				return fmt.Errorf("failed to put account: %w", err)
			}

			// Add to cache
			la.CacheAccount(account)
		}

		// Write contracts
		for _, contract := range batch.Contracts {
			// Convert State to map[string]interface{}
			var stateMap map[string]interface{}
			if contract.State != nil {
				stateBytes, _ := json.Marshal(contract.State)
				json.Unmarshal(stateBytes, &stateMap)
			}

			// Convert Metadata to map[string]interface{}
			var metadataMap map[string]interface{}
			if contract.Metadata != nil {
				metadataBytes, _ := json.Marshal(contract.Metadata)
				json.Unmarshal(metadataBytes, &metadataMap)
			}

			// Serialize the contract (excluding function types that can't be marshalled)
			serializableContract := struct {
				ID        string                      `json:"id"`
				Code      string                      `json:"code"`
				CodeHash  string                      `json:"codeHash"`
				Owner     string                      `json:"owner"`
				Version   string                      `json:"version"`
				State     map[string]interface{}      `json:"state"`
				ABI       string                      `json:"abi"`
				Language  string                      `json:"language"`
				Events    []common.SmartContractEvent `json:"events"`
				GasUsage  float64                     `json:"gasUsage"`
				Metadata  map[string]interface{}      `json:"metadata"`
				CreatedAt time.Time                   `json:"createdAt"`
				UpdatedAt time.Time                   `json:"updatedAt"`
			}{
				ID:        contract.ID,
				Code:      contract.Code,
				CodeHash:  contract.CodeHash,
				Owner:     contract.Owner,
				Version:   contract.Version,
				State:     stateMap,
				ABI:       contract.ABI,
				Language:  contract.Language,
				Events:    contract.Events,
				GasUsage:  contract.GasUsage,
				Metadata:  metadataMap,
				CreatedAt: contract.CreatedAt,
				UpdatedAt: contract.UpdatedAt,
			}

			contractData, err := json.Marshal(serializableContract)
			if err != nil {
				return fmt.Errorf("failed to marshal contract: %w", err)
			}

			// Put contract data
			if err := txn.Put(la.dbi[dbContracts], []byte(contract.ID), contractData, 0); err != nil {
				return fmt.Errorf("failed to put contract: %w", err)
			}
		}

		// Write receipts
		for _, receipt := range batch.Receipts {
			receiptData, err := json.Marshal(receipt)
			if err != nil {
				return fmt.Errorf("failed to marshal receipt: %w", err)
			}

			// Put receipt data
			if err := txn.Put(la.dbi[dbReceipts], []byte(receipt.TxID), receiptData, 0); err != nil {
				return fmt.Errorf("failed to put receipt: %w", err)
			}

			// Index by block height
			blockKey := make([]byte, 8)
			binary.BigEndian.PutUint64(blockKey, receipt.BlockHeight)
			if err := txn.Put(la.dbi[dbReceipts], append([]byte("b:"), append(blockKey, []byte(":"+receipt.TxID)...)...), []byte(receipt.TxID), 0); err != nil {
				return fmt.Errorf("failed to put receipt block index: %w", err)
			}
		}

		// Write state
		for key, value := range batch.StateWrites {
			// Put state data
			if err := txn.Put(la.dbi[dbState], []byte(key), value, 0); err != nil {
				return fmt.Errorf("failed to put state: %w", err)
			}
		}

		// Delete state
		for _, key := range batch.StateDeletes {
			// Delete state data
			if err := txn.Del(la.dbi[dbState], []byte(key), nil); err != nil {
				return fmt.Errorf("failed to delete state: %w", err)
			}
		}

		return nil
	})

	return err
}

// CreateSnapshot creates a snapshot of the ledger at the specified height
func (la *LMDBAdapter) CreateSnapshot(height uint64) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("create_snapshot", startTime, nil)
	}()

	// Check if block exists at the specified height
	block, err := la.GetBlock(height)
	if err != nil {
		return fmt.Errorf("failed to get block at height %d: %w", height, err)
	}

	// Create snapshot metadata
	snapshot := SnapshotInfo{
		Height:    height,
		Timestamp: time.Now(),
		Hash:      block.Hash,
	}

	// Serialize snapshot metadata
	snapshotData, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	// Create snapshot directory
	snapshotDir := la.path + "/snapshots/snapshot_" + fmt.Sprintf("%d", height)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	// Create a backup of the database in the snapshot directory
	if err := la.env.Copy(snapshotDir); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Write snapshot metadata
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbSnapshots]

		// Key is height as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		// Put snapshot metadata
		if err := txn.Put(dbi, key, snapshotData, 0); err != nil {
			return fmt.Errorf("failed to put snapshot metadata: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Get snapshot size
	var size int64
	err = walkDir(snapshotDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		la.logger.Warnf("Failed to calculate snapshot size: %v", err)
	}

	// Update snapshot metadata with size
	snapshot.Size = size
	snapshotData, err = json.Marshal(snapshot)
	if err != nil {
		la.logger.Warnf("Failed to marshal updated snapshot metadata: %v", err)
		return nil // Not a critical error
	}

	// Update snapshot metadata with size
	err = la.env.Update(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbSnapshots]

		// Key is height as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		// Put updated snapshot metadata
		if err := txn.Put(dbi, key, snapshotData, 0); err != nil {
			return fmt.Errorf("failed to update snapshot metadata: %w", err)
		}

		return nil
	})
	if err != nil {
		la.logger.Warnf("Failed to update snapshot metadata: %v", err)
	}

	la.logger.Infof("Created snapshot at height %d", height)
	return nil
}

// walkDir is a helper function to walk a directory recursively
func walkDir(dir string, fn func(path string, info os.FileInfo, err error) error) error {
	return filepath.Walk(dir, fn)
}

// RestoreSnapshot restores the ledger from a snapshot at the specified height
func (la *LMDBAdapter) RestoreSnapshot(height uint64) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("restore_snapshot", startTime, nil)
	}()

	// Check if snapshot exists
	var snapshot SnapshotInfo
	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbSnapshots]

		// Key is height as big-endian uint64
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, height)

		// Get snapshot metadata
		snapshotData, err := txn.Get(dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrNotFound
			}
			return fmt.Errorf("failed to get snapshot metadata: %w", err)
		}

		// Unmarshal snapshot metadata
		if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
			return fmt.Errorf("failed to unmarshal snapshot metadata: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Check if snapshot directory exists
	snapshotDir := la.path + "/snapshots/snapshot_" + fmt.Sprintf("%d", height)
	if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
		return fmt.Errorf("snapshot directory does not exist: %s", snapshotDir)
	}

	// Close current environment
	if err := la.Close(); err != nil {
		return fmt.Errorf("failed to close current environment: %w", err)
	}

	// Create temporary directory for restoration
	tempDir := la.path + ".restore"
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to remove temporary directory: %w", err)
	}
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}

	// Copy snapshot files to temporary directory
	snapshotFiles, err := filepath.Glob(snapshotDir + "/*")
	if err != nil {
		return fmt.Errorf("failed to list snapshot files: %w", err)
	}
	for _, filePath := range snapshotFiles {
		// Get file info
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", filePath, err)
		}

		if fileInfo.IsDir() {
			continue
		}

		// Read snapshot file
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read snapshot file %s: %w", filePath, err)
		}

		// Write to temporary directory
		destPath := tempDir + "/" + filepath.Base(filePath)
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write file %s: %w", destPath, err)
		}
	}

	// Remove current database directory
	if err := os.RemoveAll(la.path); err != nil {
		return fmt.Errorf("failed to remove current database: %w", err)
	}

	// Create database directory
	if err := os.MkdirAll(la.path, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Copy temporary files to database directory
	tempFiles, err := os.ReadDir(tempDir)
	if err != nil {
		return fmt.Errorf("failed to list temporary files: %w", err)
	}
	for _, file := range tempFiles {
		if file.IsDir() {
			continue
		}
		// Read temporary file
		data, err := os.ReadFile(tempDir + "/" + file.Name())
		if err != nil {
			return fmt.Errorf("failed to read temporary file %s: %w", file.Name(), err)
		}

		// Write to database directory
		destPath := la.path + "/" + file.Name()
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write file %s: %w", destPath, err)
		}
	}

	// Remove temporary directory
	if err := os.RemoveAll(tempDir); err != nil {
		la.logger.Warnf("Failed to remove temporary directory: %v", err)
	}

	// Reopen the database
	if err := la.Open(); err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	la.logger.Infof("Restored snapshot at height %d", height)
	return nil
}

// ListSnapshots lists all available snapshots
func (la *LMDBAdapter) ListSnapshots() ([]SnapshotInfo, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("list_snapshots", startTime, nil)
	}()

	var snapshots []SnapshotInfo

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbSnapshots]

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Iterate through snapshots
		for _, snapshotData, err := cursor.Get(nil, nil, lmdb.First); err == nil; _, snapshotData, err = cursor.Get(nil, nil, lmdb.Next) {
			var snapshot SnapshotInfo
			if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
				return fmt.Errorf("failed to unmarshal snapshot metadata: %w", err)
			}
			snapshots = append(snapshots, snapshot)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return snapshots, nil
}

// Backup creates a backup of the database to the specified path
func (la *LMDBAdapter) Backup(path string) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("backup", startTime, nil)
	}()

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Perform backup
	if err := la.env.Copy(path); err != nil {
		return fmt.Errorf("failed to backup database: %w", err)
	}

	la.logger.Infof("Database backed up to %s", path)
	return nil
}

// Restore restores the database from a backup
func (la *LMDBAdapter) Restore(path string) error {
	// Close current environment if open
	if la.IsOpen() {
		if err := la.Close(); err != nil {
			return fmt.Errorf("failed to close current environment: %w", err)
		}
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("restore", startTime, nil)
	}()

	// Check if backup exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("backup does not exist: %s", path)
	}

	// Remove current database directory
	if err := os.RemoveAll(la.path); err != nil {
		return fmt.Errorf("failed to remove current database: %w", err)
	}

	// Create database directory
	if err := os.MkdirAll(la.path, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Copy backup files to database directory
	backupFiles, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return fmt.Errorf("failed to list backup files: %w", err)
	}

	for _, file := range backupFiles {
		// Read backup file
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read backup file %s: %w", file, err)
		}

		// Write to database directory
		destPath := filepath.Join(la.path, filepath.Base(file))
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write database file %s: %w", destPath, err)
		}
	}

	// Reopen the database
	if err := la.Open(); err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	la.logger.Infof("Database restored from %s", path)
	return nil
}

// Compact compacts the database to reclaim space
func (la *LMDBAdapter) Compact() error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("compact", startTime, nil)
	}()

	// Create temporary path for compaction
	tempPath := la.path + ".compact"

	// Remove any existing temporary directory
	if err := os.RemoveAll(tempPath); err != nil {
		return fmt.Errorf("failed to remove temporary directory: %w", err)
	}

	// Create temporary directory
	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}

	// Copy database to temporary path
	if err := la.env.Copy(tempPath); err != nil {
		return fmt.Errorf("failed to copy database for compaction: %w", err)
	}

	// Close current environment
	if err := la.Close(); err != nil {
		return fmt.Errorf("failed to close environment: %w", err)
	}

	// Remove current database directory
	if err := os.RemoveAll(la.path); err != nil {
		return fmt.Errorf("failed to remove current database: %w", err)
	}

	// Move temporary directory to database path
	if err := os.Rename(tempPath, la.path); err != nil {
		return fmt.Errorf("failed to move compacted database: %w", err)
	}

	// Reopen the database
	if err := la.Open(); err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	la.logger.Info("Database compacted successfully")
	return nil
}

// PruneData prunes old data from the database before the specified time
func (la *LMDBAdapter) PruneData(olderThan time.Time) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("prune_data", startTime, nil)
	}()

	// Find the highest block height that is older than the specified time
	var beforeHeight uint64
	var found bool

	err := la.env.View(func(txn *lmdb.Txn) error {
		dbi := la.dbi[dbBlocks]

		// Create cursor
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		// Iterate through blocks
		for key, blockData, err := cursor.Get(nil, nil, lmdb.First); err == nil; key, blockData, err = cursor.Get(nil, nil, lmdb.Next) {
			// Skip hash indexes
			if len(key) > 0 && key[0] == 'h' {
				continue
			}

			// Unmarshal block to get timestamp
			var block common.Block
			if err := json.Unmarshal(blockData, &block); err != nil {
				return fmt.Errorf("failed to unmarshal block: %w", err)
			}

			// Check if block is older than the specified time
			blockTime := time.Unix(block.Timestamp, 0)
			if blockTime.Before(olderThan) {
				height := uint64(block.Number)
				if !found || height > beforeHeight {
					beforeHeight = height
					found = true
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !found {
		// No blocks older than the specified time
		return nil
	}

	// Delete blocks and transactions before the found height
	var blocksDeleted, txsDeleted int

	err = la.env.Update(func(txn *lmdb.Txn) error {
		// Delete blocks
		blockDBI := la.dbi[dbBlocks]
		cursor, err := txn.OpenCursor(blockDBI)
		if err != nil {
			return fmt.Errorf("failed to open blocks cursor: %w", err)
		}
		defer cursor.Close()

		// Iterate through blocks
		for key, blockData, err := cursor.Get(nil, nil, lmdb.First); err == nil; key, blockData, err = cursor.Get(nil, nil, lmdb.Next) {
			// Skip hash indexes
			if len(key) > 0 && key[0] == 'h' {
				continue
			}

			// Check if block is before the pruning height
			if len(key) == 8 && binary.BigEndian.Uint64(key) < beforeHeight {
				// Unmarshal block to get hash
				var block common.Block
				if err := json.Unmarshal(blockData, &block); err != nil {
					return fmt.Errorf("failed to unmarshal block: %w", err)
				}

				// Delete block
				if err := cursor.Del(0); err != nil {
					return fmt.Errorf("failed to delete block: %w", err)
				}

				// Delete hash index
				if err := txn.Del(blockDBI, []byte("h:"+block.Hash), nil); err != nil && !lmdb.IsNotFound(err) {
					return fmt.Errorf("failed to delete block hash index: %w", err)
				}

				blocksDeleted++
			}
		}

		// Delete transactions
		txDBI := la.dbi[dbTxs]
		txCursor, err := txn.OpenCursor(txDBI)
		if err != nil {
			return fmt.Errorf("failed to open transactions cursor: %w", err)
		}
		defer txCursor.Close()

		// Collect transaction IDs to delete
		var txIDsToDelete []string
		var indexesToDelete [][]byte

		// Find transactions by block height
		for key, val, err := txCursor.Get(nil, nil, lmdb.First); err == nil; key, val, err = txCursor.Get(nil, nil, lmdb.Next) {
			// Check if it's a block height index
			if len(key) > 2 && key[0] == 'b' && key[1] == ':' {
				// Extract block height from key
				heightBytes := key[2:10] // Assuming 8 bytes for height
				height := binary.BigEndian.Uint64(heightBytes)

				if height < beforeHeight {
					// Add transaction ID to delete list
					txIDsToDelete = append(txIDsToDelete, string(val))
					indexesToDelete = append(indexesToDelete, key)
				}
			}
		}

		// Delete transaction indexes
		for _, key := range indexesToDelete {
			if err := txn.Del(txDBI, key, nil); err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("failed to delete transaction index: %w", err)
			}
		}

		// Delete transactions
		for _, txID := range txIDsToDelete {
			// Get transaction to find sender and receiver
			txData, err := txn.Get(txDBI, []byte(txID))
			if err != nil {
				if lmdb.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("failed to get transaction: %w", err)
			}

			var tx common.Transaction
			if err := json.Unmarshal(txData, &tx); err != nil {
				return fmt.Errorf("failed to unmarshal transaction: %w", err)
			}

			// Delete sender index
			if err := txn.Del(txDBI, []byte("s:"+tx.Sender+":"+txID), nil); err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("failed to delete sender index: %w", err)
			}

			// Delete receiver index
			if err := txn.Del(txDBI, []byte("r:"+tx.Receiver+":"+txID), nil); err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("failed to delete receiver index: %w", err)
			}

			// Delete transaction
			if err := txn.Del(txDBI, []byte(txID), nil); err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("failed to delete transaction: %w", err)
			}

			txsDeleted++
		}

		return nil
	})

	if err != nil {
		return err
	}

	la.logger.Infof("Pruned %d blocks and %d transactions before height %d", blocksDeleted, txsDeleted, beforeHeight)
	return nil
}

// Vacuum performs database maintenance to optimize storage and performance
func (la *LMDBAdapter) Vacuum() error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("vacuum", startTime, nil)
	}()

	// LMDB doesn't need explicit vacuuming like some other databases
	// We'll just compact the database
	return la.Compact()
}

// HealthCheck performs a health check on the database
func (la *LMDBAdapter) HealthCheck(ctx context.Context) error {
	if err := la.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("health_check", startTime, nil)
	}()

	// Check if we can read and write to the database
	testKey := []byte("__health_check__")
	testValue := []byte(fmt.Sprintf("health_check_%d", time.Now().UnixNano()))

	// Try to write
	if err := la.SetState(testKey, testValue); err != nil {
		return fmt.Errorf("failed to write during health check: %w", err)
	}

	// Try to read
	readValue, err := la.GetState(testKey)
	if err != nil {
		return fmt.Errorf("failed to read during health check: %w", err)
	}

	// Verify read value
	if !bytes.Equal(readValue, testValue) {
		return fmt.Errorf("health check read value mismatch: expected %v, got %v", testValue, readValue)
	}

	// Clean up
	if err := la.DeleteState(testKey); err != nil {
		la.logger.Warnf("Failed to clean up health check key: %v", err)
	}

	// Check database stats
	stats, err := la.GetStats()
	if err != nil {
		return fmt.Errorf("failed to get database stats: %w", err)
	}

	// Check if we're running out of space
	mapSize := stats["map_size"].(int64)
	mapUsed := stats["map_used"].(int64)
	if float64(mapUsed)/float64(mapSize) > 0.9 {
		la.logger.Warnf("Database is using more than 90%% of allocated space: %d/%d bytes", mapUsed, mapSize)
	}

	return nil
}

// GetStats returns statistics about the database
func (la *LMDBAdapter) GetStats() (map[string]interface{}, error) {
	if err := la.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		la.LogOperation("get_stats", startTime, nil)
	}()

	stats := make(map[string]interface{})

	// Get environment info
	info, err := la.env.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to get environment info: %w", err)
	}

	// Add environment info to stats
	stats["map_size"] = info.MapSize
	stats["last_page_no"] = info.LastPNO
	stats["last_txn_id"] = info.LastTxnID
	stats["max_readers"] = info.MaxReaders
	stats["num_readers"] = info.NumReaders

	// Calculate map usage
	pageSize := 4096 // Default page size for most systems
	mapUsed := int64(info.LastPNO) * int64(pageSize)
	stats["map_used"] = mapUsed
	stats["map_used_percent"] = float64(mapUsed) / float64(info.MapSize) * 100

	// Count items in each database
	err = la.env.View(func(txn *lmdb.Txn) error {
		// Count blocks
		blockCount, err := la.countItems(txn, dbBlocks, "h:")
		if err != nil {
			return err
		}
		stats["block_count"] = blockCount

		// Count transactions
		txCount, err := la.countItems(txn, dbTxs, "s:", "r:", "b:")
		if err != nil {
			return err
		}
		stats["transaction_count"] = txCount

		// Count accounts
		accountCount, err := la.countItems(txn, dbAccounts)
		if err != nil {
			return err
		}
		stats["account_count"] = accountCount

		// Count contracts
		contractCount, err := la.countItems(txn, dbContracts)
		if err != nil {
			return err
		}
		stats["contract_count"] = contractCount

		// Count receipts
		receiptCount, err := la.countItems(txn, dbReceipts, "b:")
		if err != nil {
			return err
		}
		stats["receipt_count"] = receiptCount

		// Count state entries
		stateCount, err := la.countItems(txn, dbState)
		if err != nil {
			return err
		}
		stats["state_count"] = stateCount

		// Count snapshots
		snapshotCount, err := la.countItems(txn, dbSnapshots)
		if err != nil {
			return err
		}
		stats["snapshot_count"] = snapshotCount

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Add cache stats
	if la.blockCache != nil {
		if cacheStats := la.blockCache.Stats(); cacheStats != nil {
			stats["block_cache_size"] = cacheStats.Size
		}
	}

	if la.txCache != nil {
		if cacheStats := la.txCache.Stats(); cacheStats != nil {
			stats["transaction_cache_size"] = cacheStats.Size
		}
	}

	if la.accountCache != nil {
		if cacheStats := la.accountCache.Stats(); cacheStats != nil {
			stats["account_cache_size"] = cacheStats.Size
		}
	}

	if la.stateCache != nil {
		if cacheStats := la.stateCache.Stats(); cacheStats != nil {
			stats["state_cache_size"] = cacheStats.Size
		}
	}

	// Calculate total cache size
	totalCacheSize := int64(0)
	if blockSize, ok := stats["block_cache_size"].(int64); ok {
		totalCacheSize += blockSize
	}
	if txSize, ok := stats["transaction_cache_size"].(int64); ok {
		totalCacheSize += txSize
	}
	if accountSize, ok := stats["account_cache_size"].(int64); ok {
		totalCacheSize += accountSize
	}
	if stateSize, ok := stats["state_cache_size"].(int64); ok {
		totalCacheSize += stateSize
	}
	stats["cache_size"] = totalCacheSize
	stats["is_open"] = la.IsOpen()
	stats["health_status"] = "healthy"
	stats["last_health_check"] = time.Now().Unix()

	return stats, nil
}

// countItems counts the number of items in a database, excluding items with specified prefixes
func (la *LMDBAdapter) countItems(txn *lmdb.Txn, dbName string, excludePrefixes ...string) (int, error) {
	dbi := la.dbi[dbName]

	// Create cursor
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		return 0, fmt.Errorf("failed to open cursor: %w", err)
	}
	defer cursor.Close()

	count := 0
	for k, _, err := cursor.Get(nil, nil, lmdb.First); err == nil; k, _, err = cursor.Get(nil, nil, lmdb.Next) {
		// Skip items with excluded prefixes
		skip := false
		for _, prefix := range excludePrefixes {
			if bytes.HasPrefix(k, []byte(prefix)) {
				skip = true
				break
			}
		}
		if !skip {
			count++
		}
	}

	return count, nil
}
