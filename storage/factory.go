package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"diamante/common"
	"github.com/sirupsen/logrus"
)

// StorageType represents the type of storage backend
type StorageType string

const (
	StorageTypeLMDB   StorageType = "lmdb"
	StorageTypeTiered StorageType = "tiered"
	StorageTypeLight  StorageType = "light"
	StorageTypeMongo  StorageType = "mongo"
)

// StorageFactoryConfig holds configuration for creating storage instances
type StorageFactoryConfig struct {
	Type StorageType

	// Primary storage config (for LMDB)
	LMDBPath      string
	LMDBSize      int64
	LMDBCacheSize int

	// Tiered storage config
	EnableRedis      bool
	RedisHost        string
	EnableMongoDB    bool
	MongoHost        string
	MongoDatabase    string
	ArchiveThreshold uint64

	// Light node config
	LightNodeDBPath string
	MaxHeaders      int

	// Common settings
	Logger         *logrus.Logger
	MetricsEnabled bool
}

// DefaultStorageFactoryConfig returns default configuration
func DefaultStorageFactoryConfig() *StorageFactoryConfig {
	return &StorageFactoryConfig{
		Type:             StorageTypeLMDB,
		LMDBPath:         "diamante.db",
		LMDBSize:         10 * 1024 * 1024 * 1024, // 10GB
		LMDBCacheSize:    10000,
		EnableRedis:      false,
		RedisHost:        "127.0.0.1:6379",
		EnableMongoDB:    false,
		MongoHost:        "mongodb://localhost:27017",
		MongoDatabase:    "diamante",
		ArchiveThreshold: 7 * 24 * 60 * 60 / 2, // 1 week
		LightNodeDBPath:  "lightnode.db",
		MaxHeaders:       10000,
		MetricsEnabled:   true,
	}
}

// NewStorageFromConfig creates a storage instance based on configuration
func NewStorageFromConfig(config *StorageFactoryConfig) (LedgerStore, error) {
	if config == nil {
		config = DefaultStorageFactoryConfig()
	}

	if config.Logger == nil {
		config.Logger = logrus.New()
	}

	switch config.Type {
	case StorageTypeLMDB:
		return createLMDBStorage(config)

	case StorageTypeTiered:
		return createTieredStorage(config)

	case StorageTypeLight:
		return createLightNodeStorage(config)

	case StorageTypeMongo:
		return createMongoStorage(config)

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}

// createLMDBStorage creates a basic LMDB storage instance
func createLMDBStorage(config *StorageFactoryConfig) (LedgerStore, error) {
	lmdbConfig := DefaultLMDBConfig()
	lmdbConfig.Path = config.LMDBPath
	lmdbConfig.MapSize = config.LMDBSize

	adapter, err := NewLMDBAdapter(lmdbConfig, config.Logger, config.LMDBCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LMDB adapter: %w", err)
	}

	// Wrap adapter to fix interface compatibility
	return &lmdbStoreWrapper{adapter: adapter}, nil
}

// createTieredStorage creates a tiered storage instance with all layers
func createTieredStorage(config *StorageFactoryConfig) (LedgerStore, error) {
	// Create primary LMDB storage
	primary, err := createLMDBStorage(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary storage: %w", err)
	}

	// Create tiered storage configuration
	tieredConfig := DefaultTieredStorageConfig()
	tieredConfig.CacheEnabled = config.EnableRedis
	tieredConfig.ArchiveEnabled = config.EnableMongoDB
	tieredConfig.ArchiveHost = config.MongoHost
	tieredConfig.ArchiveDatabase = config.MongoDatabase
	tieredConfig.ArchiveThreshold = config.ArchiveThreshold
	tieredConfig.MetricsEnabled = config.MetricsEnabled

	// Override Redis host if provided
	if config.RedisHost != "" {
		tieredConfig.CacheConfig.Addr = config.RedisHost
	}

	// Create tiered storage manager
	tiered, err := NewTieredStorageManager(tieredConfig, primary, config.Logger)
	if err != nil {
		primary.Close()
		return nil, fmt.Errorf("failed to create tiered storage: %w", err)
	}

	return tiered, nil
}

// createLightNodeStorage creates a light node storage instance
func createLightNodeStorage(config *StorageFactoryConfig) (LedgerStore, error) {
	// For light nodes, we create a tiered storage in light node mode
	tieredConfig := DefaultTieredStorageConfig()
	tieredConfig.LightNodeMode = true
	tieredConfig.LightNodeDBPath = config.LightNodeDBPath
	tieredConfig.MaxHeaderCount = config.MaxHeaders
	tieredConfig.CacheEnabled = false   // Light nodes don't need Redis
	tieredConfig.ArchiveEnabled = false // Light nodes don't archive

	// Create a minimal LMDB for primary storage
	lmdbConfig := DefaultLMDBConfig()
	lmdbConfig.Path = config.LightNodeDBPath + ".lmdb"
	lmdbConfig.MapSize = 100 * 1024 * 1024 // 100MB for light node

	adapter, err := NewLMDBAdapter(lmdbConfig, config.Logger, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create light node primary storage: %w", err)
	}

	// Wrap adapter for interface compatibility
	primary := &lmdbStoreWrapper{adapter: adapter}

	// Create tiered storage in light node mode
	tiered, err := NewTieredStorageManager(tieredConfig, primary, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create light node storage: %w", err)
	}

	return tiered, nil
}

// createMongoStorage creates a MongoDB storage instance
func createMongoStorage(config *StorageFactoryConfig) (LedgerStore, error) {
	adapter, err := NewMongoAdapter(
		config.MongoHost,
		config.MongoDatabase,
		config.Logger,
		config.LMDBCacheSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB adapter: %w", err)
	}

	// Wrap adapter to fix interface compatibility
	return &mongoStoreWrapper{adapter: adapter}, nil
}

// DetectOptimalStorage detects and returns the optimal storage configuration
// based on system resources and requirements
func DetectOptimalStorage(logger *logrus.Logger) *StorageFactoryConfig {
	config := DefaultStorageFactoryConfig()

	// Check if Redis is available
	if isRedisAvailable() {
		config.EnableRedis = true
		logger.Info("Redis detected, enabling cache layer")
	}

	// Check if MongoDB is available
	if isMongoDBAvailable() {
		config.EnableMongoDB = true
		logger.Info("MongoDB detected, enabling archive layer")
	}

	// If both are available, use tiered storage
	if config.EnableRedis || config.EnableMongoDB {
		config.Type = StorageTypeTiered
		logger.Info("Using tiered storage architecture")
	}

	// Check available disk space
	if diskSpace := getAvailableDiskSpace(); diskSpace < 1024*1024*1024 {
		// Less than 1GB available, use light node mode
		config.Type = StorageTypeLight
		logger.Info("Low disk space detected, using light node storage")
	}

	return config
}

// Helper functions

func isRedisAvailable() bool {
	// Simple check - try to connect to default Redis port
	// In production, use actual Redis client to test connection
	return os.Getenv("REDIS_ADDR") != ""
}

func isMongoDBAvailable() bool {
	// Simple check - look for MongoDB environment variable
	// In production, use actual MongoDB client to test connection
	return os.Getenv("MONGODB_URI") != ""
}

func getAvailableDiskSpace() int64 {
	// This is a placeholder implementation
	// In production, use syscall to get actual disk space
	return 10 * 1024 * 1024 * 1024 // 10GB
}

// CreateOptimalStorage creates the optimal storage configuration for the system
func CreateOptimalStorage(logger *logrus.Logger) (LedgerStore, error) {
	config := DetectOptimalStorage(logger)
	return NewStorageFromConfig(config)
}

// lmdbStoreWrapper wraps LMDBAdapter to provide interface compatibility
type lmdbStoreWrapper struct {
	adapter *LMDBAdapter
}

// Delegate all methods to the adapter
func (w *lmdbStoreWrapper) SaveBlock(block *common.Block) error {
	return w.adapter.SaveBlock(block)
}

func (w *lmdbStoreWrapper) GetBlock(height uint64) (*common.Block, error) {
	return w.adapter.GetBlock(height)
}

func (w *lmdbStoreWrapper) GetLatestBlock() (*common.Block, error) {
	return w.adapter.GetLatestBlock()
}

func (w *lmdbStoreWrapper) GetBlockByHash(hash string) (*common.Block, error) {
	return w.adapter.GetBlockByHash(hash)
}

func (w *lmdbStoreWrapper) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	return w.adapter.GetBlockRange(startHeight, endHeight)
}

func (w *lmdbStoreWrapper) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	return w.adapter.SaveTransaction(tx, blockHeight)
}

func (w *lmdbStoreWrapper) GetTransaction(txID string) (*common.Transaction, error) {
	return w.adapter.GetTransaction(txID)
}

func (w *lmdbStoreWrapper) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	return w.adapter.GetTransactionsByAddress(address, limit, offset)
}

func (w *lmdbStoreWrapper) SaveAccount(account *common.Account) error {
	return w.adapter.SaveAccount(account)
}

func (w *lmdbStoreWrapper) GetAccount(accountID string) (*common.Account, error) {
	return w.adapter.GetAccount(accountID)
}

func (w *lmdbStoreWrapper) GetState(key []byte) ([]byte, error) {
	return w.adapter.GetState(key)
}

func (w *lmdbStoreWrapper) SaveState(key []byte, value []byte) error {
	return w.adapter.SetState(key, value)
}

func (w *lmdbStoreWrapper) GetContract(address string) (*Contract, error) {
	return w.adapter.GetContract(address)
}

func (w *lmdbStoreWrapper) SaveContract(contract *Contract) error {
	return w.adapter.SaveContract(contract)
}

func (w *lmdbStoreWrapper) SaveReceipt(receipt *Receipt) error {
	return w.adapter.SaveReceipt(receipt)
}

func (w *lmdbStoreWrapper) GetReceipt(txID string) (*Receipt, error) {
	return w.adapter.GetReceipt(txID)
}

func (w *lmdbStoreWrapper) WriteBatch(batch WriteBatch) error {
	return w.adapter.BatchWrite(&batch)
}

func (w *lmdbStoreWrapper) Snapshot(path string) error {
	// For now, create a snapshot at the latest height
	latestBlock, err := w.adapter.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block for snapshot: %w", err)
	}
	return w.adapter.CreateSnapshot(uint64(latestBlock.Number))
}

func (w *lmdbStoreWrapper) Restore(path string) error {
	// LMDB restore implementation
	// 1. Validate backup file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("backup file not found: %s", path)
	}

	// 2. Close current database
	if err := w.adapter.Close(); err != nil {
		return fmt.Errorf("failed to close current database: %w", err)
	}

	// 3. Get current database path from config
	dbPath := w.adapter.GetDBPath()
	if dbPath == "" {
		return fmt.Errorf("cannot determine database path")
	}

	// 4. Backup current database (safety measure)
	backupPath := dbPath + ".backup." + time.Now().Format("20060102-150405")
	if err := copyFile(dbPath, backupPath); err != nil {
		// Log but don't fail - backup is optional
		if w.adapter.logger != nil {
			w.adapter.logger.WithError(err).Warn("Failed to backup current database")
		}
	}

	// 5. Replace database file with backup
	if err := copyFile(path, dbPath); err != nil {
		// Try to restore from backup
		if backupErr := copyFile(backupPath, dbPath); backupErr != nil {
			return fmt.Errorf("critical: restore failed and cannot rollback: %w", err)
		}
		return fmt.Errorf("failed to restore database: %w", err)
	}

	// 6. Sync to disk
	if err := syncFile(dbPath); err != nil {
		if w.adapter.logger != nil {
			w.adapter.logger.WithError(err).Warn("Failed to sync restored database")
		}
	}

	// 7. Reopen database
	if err := w.adapter.Open(); err != nil {
		// Try to restore from backup
		if backupErr := copyFile(backupPath, dbPath); backupErr == nil {
			w.adapter.Open() // Try to open backup
		}
		return fmt.Errorf("failed to open restored database: %w", err)
	}

	// 8. Validate restored data
	if _, err := w.adapter.GetLatestBlock(); err != nil {
		return fmt.Errorf("restored database validation failed: %w", err)
	}

	if w.adapter.logger != nil {
		w.adapter.logger.WithField("path", path).Info("Successfully restored LMDB database")
	}
	return nil
}

func (w *lmdbStoreWrapper) Open() error {
	return w.adapter.Open()
}

func (w *lmdbStoreWrapper) Close() error {
	return w.adapter.Close()
}

func (w *lmdbStoreWrapper) IsOpen() bool {
	return w.adapter.IsOpen()
}

func (w *lmdbStoreWrapper) HealthCheck(ctx context.Context) error {
	return w.adapter.HealthCheck(ctx)
}

// GetStats converts the map[string]interface{} to StoreStats
func (w *lmdbStoreWrapper) GetStats() (*StoreStats, error) {
	statsMap, err := w.adapter.GetStats()
	if err != nil {
		return nil, err
	}

	// Convert map to StoreStats
	stats := &StoreStats{
		DatabaseType: "LMDB",
	}

	// Extract values from map
	if v, ok := statsMap["blockCount"].(int64); ok {
		stats.BlockCount = v
	}
	if v, ok := statsMap["transactionCount"].(int64); ok {
		stats.TransactionCount = v
	}
	if v, ok := statsMap["accountCount"].(int64); ok {
		stats.AccountCount = v
	}
	if v, ok := statsMap["contractCount"].(int64); ok {
		stats.ContractCount = v
	}
	if v, ok := statsMap["dataSize"].(int64); ok {
		stats.DataSize = v
	}
	if v, ok := statsMap["cacheSize"].(int64); ok {
		stats.CacheSize = v
	}
	if v, ok := statsMap["cacheHitRate"].(float64); ok {
		stats.CacheHitRate = v
	}

	return stats, nil
}

func (w *lmdbStoreWrapper) Compact() error {
	return w.adapter.Compact()
}

func (w *lmdbStoreWrapper) PruneData(olderThan time.Time) error {
	return w.adapter.PruneData(olderThan)
}

func (w *lmdbStoreWrapper) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return w.adapter.ReplaceBlockSameHeight(height, newBlock)
}

// mongoStoreWrapper wraps MongoAdapter to provide interface compatibility
type mongoStoreWrapper struct {
	adapter *MongoAdapter
}

// Delegate all methods to the adapter
func (w *mongoStoreWrapper) SaveBlock(block *common.Block) error {
	return w.adapter.SaveBlock(block)
}

func (w *mongoStoreWrapper) GetBlock(height uint64) (*common.Block, error) {
	return w.adapter.GetBlock(height)
}

func (w *mongoStoreWrapper) GetLatestBlock() (*common.Block, error) {
	return w.adapter.GetLatestBlock()
}

func (w *mongoStoreWrapper) GetBlockByHash(hash string) (*common.Block, error) {
	return w.adapter.GetBlockByHash(hash)
}

func (w *mongoStoreWrapper) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	return w.adapter.GetBlockRange(startHeight, endHeight)
}

func (w *mongoStoreWrapper) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	return w.adapter.SaveTransaction(tx, blockHeight)
}

func (w *mongoStoreWrapper) GetTransaction(txID string) (*common.Transaction, error) {
	return w.adapter.GetTransaction(txID)
}

func (w *mongoStoreWrapper) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	return w.adapter.GetTransactionsByAddress(address, limit, offset)
}

func (w *mongoStoreWrapper) SaveAccount(account *common.Account) error {
	return w.adapter.SaveAccount(account)
}

func (w *mongoStoreWrapper) GetAccount(accountID string) (*common.Account, error) {
	return w.adapter.GetAccount(accountID)
}

func (w *mongoStoreWrapper) GetState(key []byte) ([]byte, error) {
	return w.adapter.GetState(key)
}

func (w *mongoStoreWrapper) SaveState(key []byte, value []byte) error {
	return w.adapter.SetState(key, value)
}

func (w *mongoStoreWrapper) GetContract(address string) (*Contract, error) {
	return w.adapter.GetContract(address)
}

func (w *mongoStoreWrapper) SaveContract(contract *Contract) error {
	return w.adapter.SaveContract(contract)
}

func (w *mongoStoreWrapper) SaveReceipt(receipt *Receipt) error {
	return w.adapter.SaveReceipt(receipt)
}

func (w *mongoStoreWrapper) GetReceipt(txID string) (*Receipt, error) {
	return w.adapter.GetReceipt(txID)
}

func (w *mongoStoreWrapper) WriteBatch(batch WriteBatch) error {
	return w.adapter.BatchWrite(&batch)
}

func (w *mongoStoreWrapper) Snapshot(path string) error {
	// For now, create a snapshot at the latest height
	latestBlock, err := w.adapter.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block for snapshot: %w", err)
	}
	return w.adapter.CreateSnapshot(uint64(latestBlock.Number))
}

func (w *mongoStoreWrapper) Restore(path string) error {
	// MongoDB restore implementation using mongorestore
	// 1. Validate backup directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", path)
	}

	// 2. Check if mongorestore is available
	mongorestorePath, err := exec.LookPath("mongorestore")
	if err != nil {
		return fmt.Errorf("mongorestore tool not found in PATH. Please install MongoDB tools: apt-get install mongodb-database-tools")
	}

	// 3. Get MongoDB connection info
	connStr := w.adapter.GetConnectionString()
	dbName := w.adapter.GetDatabaseName()

	if connStr == "" || dbName == "" {
		return fmt.Errorf("cannot determine MongoDB connection details")
	}

	// 4. Build mongorestore command
	args := []string{
		"--uri", connStr,
		"--db", dbName,
		"--drop", // Drop existing collections before restore
		"--dir", path,
	}

	// Add authentication if present in connection string
	if strings.Contains(connStr, "@") {
		args = append(args, "--authenticationDatabase", "admin")
	}

	// 5. Execute mongorestore
	cmd := exec.Command(mongorestorePath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongorestore failed: %w\nOutput: %s", err, string(output))
	}

	// 6. Validate restored data
	if _, err := w.adapter.GetLatestBlock(); err != nil {
		return fmt.Errorf("restored database validation failed: %w", err)
	}

	if w.adapter.logger != nil {
		w.adapter.logger.WithField("path", path).Info("Successfully restored MongoDB database")
	}
	return nil
}

func (w *mongoStoreWrapper) Open() error {
	return w.adapter.Open()
}

func (w *mongoStoreWrapper) Close() error {
	return w.adapter.Close()
}

func (w *mongoStoreWrapper) IsOpen() bool {
	return w.adapter.IsOpen()
}

func (w *mongoStoreWrapper) HealthCheck(ctx context.Context) error {
	return w.adapter.HealthCheck(ctx)
}

// GetStats converts the map[string]interface{} to StoreStats
func (w *mongoStoreWrapper) GetStats() (*StoreStats, error) {
	statsMap, err := w.adapter.GetStats()
	if err != nil {
		return nil, err
	}

	// Convert map to StoreStats
	stats := &StoreStats{
		DatabaseType: "MongoDB",
	}

	// Extract values from map
	if v, ok := statsMap["blockCount"].(int64); ok {
		stats.BlockCount = v
	}
	if v, ok := statsMap["transactionCount"].(int64); ok {
		stats.TransactionCount = v
	}
	if v, ok := statsMap["accountCount"].(int64); ok {
		stats.AccountCount = v
	}
	if v, ok := statsMap["contractCount"].(int64); ok {
		stats.ContractCount = v
	}
	if v, ok := statsMap["dataSize"].(int64); ok {
		stats.DataSize = v
	}
	if v, ok := statsMap["cacheSize"].(int64); ok {
		stats.CacheSize = v
	}
	if v, ok := statsMap["cacheHitRate"].(float64); ok {
		stats.CacheHitRate = v
	}

	return stats, nil
}

func (w *mongoStoreWrapper) Compact() error {
	return w.adapter.Compact()
}

func (w *mongoStoreWrapper) PruneData(olderThan time.Time) error {
	return w.adapter.PruneData(olderThan)
}

func (w *mongoStoreWrapper) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return w.adapter.ReplaceBlockSameHeight(height, newBlock)
}

// Helper functions for backup/restore

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return err
	}

	// Copy file permissions
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	return os.Chmod(dst, info.Mode())
}

// syncFile ensures file is written to disk
func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Sync()
}
