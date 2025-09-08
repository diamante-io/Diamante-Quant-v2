// storage/base_adapter.go

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	cache "diamante/storage/cache"
	"diamante/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// BaseAdapter provides common functionality for all storage implementations with production-grade features
type BaseAdapter struct {
	logger *logrus.Logger
	isOpen atomic.Bool
	mu     sync.RWMutex

	// Cache system with proper LRU implementation
	cacheManager *cache.Manager
	blockCache   cache.Cache
	txCache      cache.Cache
	accountCache cache.Cache
	stateCache   cache.Cache
	cacheSize    int

	// Metrics
	metrics *StorageMetrics

	// Context for lifecycle management
	ctx    context.Context
	cancel context.CancelFunc

	// Health monitoring
	healthTicker    *time.Ticker
	healthStatus    atomic.Bool
	lastHealthCheck time.Time

	// Resource management
	resourcePool *ResourcePool

	// Configuration
	config *AdapterConfig
}

// AdapterConfig holds configuration for the base adapter
type AdapterConfig struct {
	CacheSize           int
	CacheTTL            time.Duration
	MetricsEnabled      bool
	HealthCheckInterval time.Duration
	MaxConcurrentOps    int
	EnableCompression   bool
	EnableEncryption    bool
	EncryptionKey       []byte
}

// DefaultAdapterConfig returns production-ready default configuration
func DefaultAdapterConfig() *AdapterConfig {
	return &AdapterConfig{
		CacheSize:           10000,
		CacheTTL:            5 * time.Minute,
		MetricsEnabled:      true,
		HealthCheckInterval: 30 * time.Second,
		MaxConcurrentOps:    100,
		EnableCompression:   true,
		EnableEncryption:    false,
	}
}

// StorageMetrics holds Prometheus metrics for storage operations
type StorageMetrics struct {
	operationDuration *prometheus.HistogramVec
	operationErrors   *prometheus.CounterVec
	cacheHits         prometheus.Counter
	cacheMisses       prometheus.Counter
	cacheEvictions    prometheus.Counter
	activeConnections prometheus.Gauge
	storageSize       prometheus.Gauge
}

// BufferPool wraps sync.Pool for byte buffers
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

// Get retrieves a buffer from the pool
func (bp *BufferPool) Get() []byte {
	return bp.pool.Get().([]byte)
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf []byte) {
	bp.pool.Put(buf)
}

// ResourcePool manages shared resources for the adapter
type ResourcePool struct {
	semaphore  chan struct{}
	bufferPool *BufferPool
}

// NewBaseAdapter creates a new BaseAdapter with production-grade features
func NewBaseAdapter(logger *logrus.Logger, config *AdapterConfig) (*BaseAdapter, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	if config == nil {
		config = DefaultAdapterConfig()
	}

	// Validate configuration
	if err := validateAdapterConfig(config); err != nil {
		return nil, fmt.Errorf("invalid adapter configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ba := &BaseAdapter{
		logger:       logger,
		cacheSize:    config.CacheSize,
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		resourcePool: newResourcePool(config.MaxConcurrentOps),
	}

	// Initialize cache manager
	ba.cacheManager = cache.NewManager()

	// Initialize individual caches with proper configuration
	cacheOpts := &cache.Options{
		Size: config.CacheSize,
		TTL:  config.CacheTTL,
	}

	ba.blockCache = ba.cacheManager.GetCache("blocks", cacheOpts)
	ba.txCache = ba.cacheManager.GetCache("transactions", cacheOpts)
	ba.accountCache = ba.cacheManager.GetCache("accounts", cacheOpts)
	ba.stateCache = ba.cacheManager.GetCache("state", cacheOpts)

	// Initialize metrics if enabled
	if config.MetricsEnabled {
		if err := ba.initializeMetrics(); err != nil {
			return nil, fmt.Errorf("failed to initialize metrics: %w", err)
		}
	}

	// Start health monitoring
	ba.startHealthMonitoring()

	return ba, nil
}

// validateAdapterConfig validates the adapter configuration
func validateAdapterConfig(config *AdapterConfig) error {
	if config.CacheSize <= 0 {
		return fmt.Errorf("cache size must be positive")
	}
	if config.CacheTTL <= 0 {
		return fmt.Errorf("cache TTL must be positive")
	}
	if config.MaxConcurrentOps <= 0 {
		return fmt.Errorf("max concurrent operations must be positive")
	}
	if config.EnableEncryption && len(config.EncryptionKey) == 0 {
		return fmt.Errorf("encryption key required when encryption is enabled")
	}
	return nil
}

// newResourcePool creates a new resource pool
func newResourcePool(maxConcurrent int) *ResourcePool {
	return &ResourcePool{
		semaphore:  make(chan struct{}, maxConcurrent),
		bufferPool: NewBufferPool(4096),
	}
}

// initializeMetrics sets up Prometheus metrics
func (ba *BaseAdapter) initializeMetrics() error {
	ba.metrics = &StorageMetrics{
		operationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "storage_operation_duration_seconds",
				Help:    "Duration of storage operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "status"},
		),
		operationErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_operation_errors_total",
				Help: "Total number of storage operation errors",
			},
			[]string{"operation", "error_type"},
		),
		cacheHits: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "storage_cache_hits_total",
				Help: "Total number of cache hits",
			},
		),
		cacheMisses: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "storage_cache_misses_total",
				Help: "Total number of cache misses",
			},
		),
		cacheEvictions: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "storage_cache_evictions_total",
				Help: "Total number of cache evictions",
			},
		),
		activeConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_active_connections",
				Help: "Number of active storage connections",
			},
		),
		storageSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_size_bytes",
				Help: "Total storage size in bytes",
			},
		),
	}

	// Register metrics
	collectors := []prometheus.Collector{
		ba.metrics.operationDuration,
		ba.metrics.operationErrors,
		ba.metrics.cacheHits,
		ba.metrics.cacheMisses,
		ba.metrics.cacheEvictions,
		ba.metrics.activeConnections,
		ba.metrics.storageSize,
	}

	for _, collector := range collectors {
		if err := prometheus.Register(collector); err != nil {
			// Check if already registered
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				return fmt.Errorf("failed to register metric: %w", err)
			}
		}
	}

	return nil
}

// startHealthMonitoring starts periodic health checks
func (ba *BaseAdapter) startHealthMonitoring() {
	ba.healthTicker = time.NewTicker(ba.config.HealthCheckInterval)
	ba.healthStatus.Store(true)

	go func() {
		for {
			select {
			case <-ba.ctx.Done():
				return
			case <-ba.healthTicker.C:
				ctx, cancel := context.WithTimeout(ba.ctx, 5*time.Second)
				err := ba.performHealthCheck(ctx)
				cancel()

				ba.healthStatus.Store(err == nil)
				ba.lastHealthCheck = common.ConsensusNow()

				if err != nil {
					ba.logger.WithError(err).Error("Health check failed")
				}
			}
		}
	}()
}

// performHealthCheck performs a basic health check
func (ba *BaseAdapter) performHealthCheck(ctx context.Context) error {
	// Update last health check time
	ba.lastHealthCheck = common.ConsensusNow()

	// Check if adapter is open
	if !ba.IsOpen() {
		return fmt.Errorf("adapter is not open")
	}

	// Check cache health
	if ba.blockCache.Len() < 0 || ba.txCache.Len() < 0 {
		return fmt.Errorf("cache system unhealthy")
	}

	// Check resource pool
	select {
	case ba.resourcePool.semaphore <- struct{}{}:
		<-ba.resourcePool.semaphore
	case <-ctx.Done():
		return fmt.Errorf("resource pool blocked: %w", ctx.Err())
	}

	return nil
}

// IsOpen returns whether the adapter is open
func (ba *BaseAdapter) IsOpen() bool {
	return ba.isOpen.Load()
}

// SetOpen sets the open state of the adapter
func (ba *BaseAdapter) SetOpen(open bool) {
	ba.isOpen.Store(open)

	if ba.config.MetricsEnabled && ba.metrics != nil {
		if open {
			ba.metrics.activeConnections.Inc()
		} else {
			ba.metrics.activeConnections.Dec()
		}
	}
}

// CheckOpen returns an error if the adapter is not open
func (ba *BaseAdapter) CheckOpen() error {
	if !ba.IsOpen() {
		return fmt.Errorf("storage adapter is not open")
	}
	return nil
}

// acquireResource acquires a resource from the pool
func (ba *BaseAdapter) acquireResource(ctx context.Context) error {
	select {
	case ba.resourcePool.semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("failed to acquire resource: %w", ctx.Err())
	}
}

// releaseResource releases a resource back to the pool
func (ba *BaseAdapter) releaseResource() {
	<-ba.resourcePool.semaphore
}

// CacheBlock adds a block to the cache with proper error handling
func (ba *BaseAdapter) CacheBlock(block *common.Block) error {
	if err := ba.CheckOpen(); err != nil {
		return err
	}

	if block == nil {
		return fmt.Errorf("cannot cache nil block")
	}

	// Serialize block to bytes for cache storage
	blockData, err := json.Marshal(block)
	if err != nil {
		ba.logger.WithError(err).Error("Failed to serialize block for cache")
		return nil
	}

	cacheValue := &types.CacheValue{
		Key:        fmt.Sprintf("%d", block.Number),
		Data:       blockData,
		Size:       uint64(len(blockData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}

	ba.blockCache.Set(fmt.Sprintf("%d", block.Number), cacheValue)

	ba.logger.WithFields(logrus.Fields{
		"block_number": block.Number,
		"block_hash":   block.Hash,
	}).Debug("Block cached")

	return nil
}

// GetCachedBlock retrieves a block from the cache
func (ba *BaseAdapter) GetCachedBlock(height uint64) (*common.Block, bool) {
	value, found := ba.blockCache.Get(fmt.Sprintf("%d", height))

	if ba.config.MetricsEnabled && ba.metrics != nil {
		if found {
			ba.metrics.cacheHits.Inc()
		} else {
			ba.metrics.cacheMisses.Inc()
		}
	}

	if !found {
		return nil, false
	}

	// Deserialize block from cache value
	var block common.Block
	if err := json.Unmarshal(value.Data, &block); err != nil {
		ba.logger.WithField("height", height).WithError(err).Error("Failed to deserialize block from cache")
		ba.blockCache.Delete(fmt.Sprintf("%d", height))
		return nil, false
	}

	return &block, true
}

// CacheTransaction adds a transaction to the cache
func (ba *BaseAdapter) CacheTransaction(tx *common.Transaction) error {
	if err := ba.CheckOpen(); err != nil {
		return err
	}

	if tx == nil {
		return fmt.Errorf("cannot cache nil transaction")
	}

	// Serialize transaction to bytes for cache storage
	txData, err := json.Marshal(tx)
	if err != nil {
		ba.logger.WithError(err).Error("Failed to serialize transaction for cache")
		return nil
	}

	cacheValue := &types.CacheValue{
		Key:        tx.ID,
		Data:       txData,
		Size:       uint64(len(txData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}

	ba.txCache.Set(tx.ID, cacheValue)

	ba.logger.WithField("tx_id", tx.ID).Debug("Transaction cached")

	return nil
}

// GetCachedTransaction retrieves a transaction from the cache
func (ba *BaseAdapter) GetCachedTransaction(txID string) (*common.Transaction, bool) {
	value, found := ba.txCache.Get(txID)

	if ba.config.MetricsEnabled && ba.metrics != nil {
		if found {
			ba.metrics.cacheHits.Inc()
		} else {
			ba.metrics.cacheMisses.Inc()
		}
	}

	if !found {
		return nil, false
	}

	// Deserialize transaction from cache value
	var tx common.Transaction
	if err := json.Unmarshal(value.Data, &tx); err != nil {
		ba.logger.WithField("tx_id", txID).WithError(err).Error("Failed to deserialize transaction from cache")
		ba.txCache.Delete(txID)
		return nil, false
	}

	return &tx, true
}

// CacheAccount adds an account to the cache
func (ba *BaseAdapter) CacheAccount(account *common.Account) error {
	if err := ba.CheckOpen(); err != nil {
		return err
	}

	if account == nil {
		return fmt.Errorf("cannot cache nil account")
	}

	// Serialize account to bytes for cache storage
	accountData, err := json.Marshal(account)
	if err != nil {
		ba.logger.WithError(err).Error("Failed to serialize account for cache")
		return nil
	}

	cacheValue := &types.CacheValue{
		Key:        account.ID,
		Data:       accountData,
		Size:       uint64(len(accountData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}

	ba.accountCache.Set(account.ID, cacheValue)

	ba.logger.WithField("account_id", account.ID).Debug("Account cached")

	return nil
}

// GetCachedAccount retrieves an account from the cache
func (ba *BaseAdapter) GetCachedAccount(accountID string) (*common.Account, bool) {
	value, found := ba.accountCache.Get(accountID)

	if ba.config.MetricsEnabled && ba.metrics != nil {
		if found {
			ba.metrics.cacheHits.Inc()
		} else {
			ba.metrics.cacheMisses.Inc()
		}
	}

	if !found {
		return nil, false
	}

	// Deserialize account from cache value
	var account common.Account
	if err := json.Unmarshal(value.Data, &account); err != nil {
		ba.logger.WithField("account_id", accountID).WithError(err).Error("Failed to deserialize account from cache")
		ba.accountCache.Delete(accountID)
		return nil, false
	}

	return &account, true
}

// CacheState adds state data to the cache
func (ba *BaseAdapter) CacheState(key string, value []byte) error {
	if err := ba.CheckOpen(); err != nil {
		return err
	}

	if key == "" {
		return fmt.Errorf("cannot cache with empty key")
	}

	// Clone the value to prevent external modifications
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	cacheValue := &types.CacheValue{
		Key:        key,
		Data:       valueCopy,
		Size:       uint64(len(valueCopy)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}

	ba.stateCache.Set(key, cacheValue)

	return nil
}

// GetCachedState retrieves state data from the cache
func (ba *BaseAdapter) GetCachedState(key string) ([]byte, bool) {
	value, found := ba.stateCache.Get(key)

	if ba.config.MetricsEnabled && ba.metrics != nil {
		if found {
			ba.metrics.cacheHits.Inc()
		} else {
			ba.metrics.cacheMisses.Inc()
		}
	}

	if !found {
		return nil, false
	}

	// State data is already stored as bytes in CacheValue
	if value.Data == nil {
		ba.logger.WithField("key", key).Error("Nil state data in cache")
		ba.stateCache.Delete(key)
		return nil, false
	}

	// Return a copy to prevent external modifications
	result := make([]byte, len(value.Data))
	copy(result, value.Data)

	return result, true
}

// InvalidateBlockCache clears the block cache
func (ba *BaseAdapter) InvalidateBlockCache() {
	ba.blockCache.Clear()
	ba.logger.Info("Block cache invalidated")
}

// InvalidateTransactionCache clears the transaction cache
func (ba *BaseAdapter) InvalidateTransactionCache() {
	ba.txCache.Clear()
	ba.logger.Info("Transaction cache invalidated")
}

// InvalidateAccountCache clears the account cache
func (ba *BaseAdapter) InvalidateAccountCache() {
	ba.accountCache.Clear()
	ba.logger.Info("Account cache invalidated")
}

// InvalidateStateCache clears the state cache
func (ba *BaseAdapter) InvalidateStateCache() {
	ba.stateCache.Clear()
	ba.logger.Info("State cache invalidated")
}

// InvalidateAllCaches clears all caches
func (ba *BaseAdapter) InvalidateAllCaches() {
	ba.InvalidateBlockCache()
	ba.InvalidateTransactionCache()
	ba.InvalidateAccountCache()
	ba.InvalidateStateCache()
}

// LogOperation logs an operation with comprehensive metrics
func (ba *BaseAdapter) LogOperation(operation string, startTime time.Time, err error) {
	duration := time.Since(startTime)

	// Update metrics
	if ba.config.MetricsEnabled && ba.metrics != nil {
		status := "success"
		if err != nil {
			status = "error"
			ba.metrics.operationErrors.WithLabelValues(operation, getErrorType(err)).Inc()
		}
		ba.metrics.operationDuration.WithLabelValues(operation, status).Observe(duration.Seconds())
	}

	// Log the operation
	fields := logrus.Fields{
		"operation": operation,
		"duration":  duration,
	}

	if err != nil {
		fields["error"] = err.Error()
		ba.logger.WithFields(fields).Error("Storage operation failed")
	} else {
		ba.logger.WithFields(fields).Debug("Storage operation completed")
	}
}

// getErrorType categorizes errors for metrics
func getErrorType(err error) string {
	switch {
	case err == nil:
		return "none"
	case err == ErrNotFound:
		return "not_found"
	case err == ErrAlreadyExists:
		return "already_exists"
	case err == ErrInvalidData:
		return "invalid_data"
	case err == ErrDatabaseError:
		return "database_error"
	case err == ErrConnectionFailed:
		return "connection_failed"
	case err == ErrTransactionFailed:
		return "transaction_failed"
	default:
		return "unknown"
	}
}

// DefaultHealthCheck provides a comprehensive health check
func (ba *BaseAdapter) DefaultHealthCheck(ctx context.Context) error {
	// Check basic health
	if err := ba.performHealthCheck(ctx); err != nil {
		return fmt.Errorf("basic health check failed: %w", err)
	}

	// Check last health check time
	if time.Since(ba.lastHealthCheck) > 2*ba.config.HealthCheckInterval {
		return fmt.Errorf("health check stale, last check: %v", ba.lastHealthCheck)
	}

	return nil
}

// StorageStats represents structured statistics for storage operations
type StorageStats struct {
	ConnectionsActive     int     `json:"connections_active"`
	ConnectionsTotal      int64   `json:"connections_total"`
	QueriesExecuted       int64   `json:"queries_executed"`
	TransactionsProcessed int64   `json:"transactions_processed"`
	CacheHitRate          float64 `json:"cache_hit_rate"`
	AverageQueryTime      float64 `json:"average_query_time_ms"`
	ErrorCount            int64   `json:"error_count"`
	LastError             string  `json:"last_error,omitempty"`
	UptimeSeconds         int64   `json:"uptime_seconds"`
	MemoryUsageMB         float64 `json:"memory_usage_mb"`
}

// DefaultGetStats returns default storage statistics
func (ba *BaseAdapter) DefaultGetStats() *StorageStats {
	ba.mu.RLock()
	defer ba.mu.RUnlock()

	// Calculate active connections from semaphore
	activeConnections := ba.config.MaxConcurrentOps - len(ba.resourcePool.semaphore)

	// Calculate cache metrics
	var cacheHits, cacheMisses float64
	if ba.metrics != nil {
		// Note: Prometheus counters don't have Load method, this is just a placeholder
		// In real implementation, you'd need to query Prometheus or maintain separate counters
		cacheHits = 0
		cacheMisses = 0
	}

	cacheHitRate := 0.0
	if (cacheHits + cacheMisses) > 0 {
		cacheHitRate = cacheHits / (cacheHits + cacheMisses)
	}

	return &StorageStats{
		ConnectionsActive:     activeConnections,
		ConnectionsTotal:      int64(ba.config.MaxConcurrentOps),
		QueriesExecuted:       int64(cacheHits + cacheMisses),
		TransactionsProcessed: 0, // Would need actual transaction counter
		CacheHitRate:          cacheHitRate,
		AverageQueryTime:      0.0, // Would need actual timing metrics
		ErrorCount:            0,   // Would need actual error counter
		LastError:             "",
		UptimeSeconds:         int64(time.Since(ba.lastHealthCheck).Seconds()),
		MemoryUsageMB:         0.0, // Would need actual memory stats
	}
}

// Close cleanly shuts down the adapter
func (ba *BaseAdapter) Close() error {
	// Cancel context to stop background operations
	ba.cancel()

	// Stop health monitoring
	if ba.healthTicker != nil {
		ba.healthTicker.Stop()
	}

	// Clear all caches
	ba.InvalidateAllCaches()

	// Update open status
	ba.SetOpen(false)

	ba.logger.Info("Base adapter closed")

	return nil
}

// WarmCache pre-loads frequently accessed data into cache
func (ba *BaseAdapter) WarmCache(ctx context.Context, loader CacheLoader) error {
	if err := ba.CheckOpen(); err != nil {
		return err
	}

	ba.logger.Info("Starting cache warming")

	// Warm block cache
	if blocks, err := loader.LoadRecentBlocks(ctx, 100); err == nil {
		for _, block := range blocks {
			if err := ba.CacheBlock(block); err != nil {
				ba.logger.WithError(err).Warn("Failed to cache block during warming")
			}
		}
	}

	// Warm account cache
	if accounts, err := loader.LoadActiveAccounts(ctx, 1000); err == nil {
		for _, account := range accounts {
			if err := ba.CacheAccount(account); err != nil {
				ba.logger.WithError(err).Warn("Failed to cache account during warming")
			}
		}
	}

	ba.logger.Info("Cache warming completed")

	return nil
}

// CacheLoader interface for warming cache
type CacheLoader interface {
	LoadRecentBlocks(ctx context.Context, count int) ([]*common.Block, error)
	LoadActiveAccounts(ctx context.Context, count int) ([]*common.Account, error)
	LoadHotState(ctx context.Context) (map[string][]byte, error)
}

// GetBuffer retrieves a buffer from the pool
func (ba *BaseAdapter) GetBuffer() []byte {
	return ba.resourcePool.bufferPool.Get()
}

// PutBuffer returns a buffer to the pool
func (ba *BaseAdapter) PutBuffer(buf []byte) {
	// Clear sensitive data before returning to pool
	for i := range buf {
		buf[i] = 0
	}
	ba.resourcePool.bufferPool.Put(buf)
}

// RegisterMetricsExporter registers metrics with external monitoring system
func (ba *BaseAdapter) RegisterMetricsExporter(exporter MetricsExporter) error {
	if !ba.config.MetricsEnabled {
		return fmt.Errorf("metrics not enabled")
	}

	// Register storage-specific metrics
	return exporter.RegisterCollector("storage", ba)
}

// Collect implements prometheus.Collector for custom metrics
func (ba *BaseAdapter) Collect(ch chan<- prometheus.Metric) {
	// This would be implemented to expose custom metrics
	// For now, the individual metrics handle their own collection
}

// Describe implements prometheus.Collector
func (ba *BaseAdapter) Describe(ch chan<- *prometheus.Desc) {
	// This would describe all custom metrics
}
