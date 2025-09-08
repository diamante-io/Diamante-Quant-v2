// storage/storage_integration.go

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/storage/cache"
	"diamante/storage/optimization"
	"diamante/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// StorageIntegration provides production-grade storage integration with other modules
type StorageIntegration struct {
	store  LedgerStore
	config *IntegrationConfig
	logger *logrus.Logger

	// Module integrations
	consensus ConsensusEngine
	network   NetworkManager
	vmManager VMManager
	metrics   MetricsReporter

	// Cache system
	cacheManager *cache.Manager
	blockCache   cache.Cache
	txCache      cache.Cache
	stateCache   cache.Cache

	// Optimization
	connPool   *optimization.DBPool
	blockPool  *optimization.Pool
	bufferPool *optimization.BufferPool

	// Synchronization
	syncManager *SyncManager

	// Event handling
	eventBus    *EventBus
	subscribers map[string][]EventHandler
	subMu       sync.RWMutex

	// Metrics
	intMetrics *IntegrationMetrics

	// Health monitoring
	healthMon *HealthMonitor
	startTime time.Time

	// Context for lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// IntegrationConfig holds configuration for storage integration
type IntegrationConfig struct {
	// Cache configuration
	CacheSize    int
	CacheTTL     time.Duration
	RedisEnabled bool
	RedisAddress string

	// Performance tuning
	BatchSize       int
	SyncInterval    time.Duration
	PruneInterval   time.Duration
	CompactInterval time.Duration

	// Integration settings
	EnableConsensusSync bool
	EnableNetworkSync   bool
	EnableVMIntegration bool
	EnableMetrics       bool

	// Health monitoring
	HealthCheckInterval time.Duration

	// Event handling
	EventBufferSize int
	EventWorkers    int
}

// DefaultIntegrationConfig returns production-ready default configuration
func DefaultIntegrationConfig() *IntegrationConfig {
	return &IntegrationConfig{
		CacheSize:           100000,
		CacheTTL:            5 * time.Minute,
		RedisEnabled:        false,
		BatchSize:           1000,
		SyncInterval:        100 * time.Millisecond,
		PruneInterval:       24 * time.Hour,
		CompactInterval:     6 * time.Hour,
		EnableConsensusSync: true,
		EnableNetworkSync:   true,
		EnableVMIntegration: true,
		EnableMetrics:       true,
		HealthCheckInterval: 30 * time.Second,
		EventBufferSize:     10000,
		EventWorkers:        10,
	}
}

// IntegrationMetrics holds metrics for storage integration
type IntegrationMetrics struct {
	consensusSyncs     *prometheus.CounterVec
	networkSyncs       *prometheus.CounterVec
	vmOperations       *prometheus.CounterVec
	eventProcessed     *prometheus.CounterVec
	syncDuration       *prometheus.HistogramVec
	cacheHitRate       prometheus.Gauge
	storageUtilization prometheus.Gauge
}

// Event handling types are defined in integration_interfaces.go

// EventBus manages event distribution
type EventBus struct {
	events   chan StorageEvent
	workers  int
	handlers map[EventType][]EventHandler
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	metrics  *prometheus.CounterVec
}

// SyncManager handles synchronization with other nodes
type SyncManager struct {
	store     LedgerStore
	network   NetworkManager
	logger    *logrus.Logger
	syncState atomic.Int32 // 0: idle, 1: syncing, 2: synchronized
	lastSync  atomic.Int64
	errors    atomic.Int64
	syncMutex sync.Mutex
}

// HealthMonitor monitors storage health
type HealthMonitor struct {
	integration *StorageIntegration
	ticker      *time.Ticker
	lastCheck   atomic.Int64
	healthy     atomic.Bool
	alerts      chan HealthAlert
}

// HealthAlert is defined in integration_interfaces.go

// NewStorageIntegration creates a new storage integration instance
func NewStorageIntegration(
	store LedgerStore,
	config *IntegrationConfig,
	logger *logrus.Logger,
) (*StorageIntegration, error) {
	if config == nil {
		config = DefaultIntegrationConfig()
	}

	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	ctx, cancel := context.WithCancel(context.Background())

	si := &StorageIntegration{
		store:       store,
		config:      config,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		subscribers: make(map[string][]EventHandler),
		startTime:   time.Now(),
	}

	// Initialize components
	if err := si.initialize(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize storage integration: %w", err)
	}

	return si, nil
}

// initialize sets up all integration components
func (si *StorageIntegration) initialize() error {
	// Initialize cache system
	si.cacheManager = cache.NewManager()

	cacheOpts := &cache.Options{
		Size: si.config.CacheSize,
		TTL:  si.config.CacheTTL,
	}

	if si.config.RedisEnabled {
		cacheOpts.RedisAddress = si.config.RedisAddress
		cacheOpts.RedisDB = 0
	}

	si.blockCache = si.cacheManager.GetCache("blocks", cacheOpts)
	si.txCache = si.cacheManager.GetCache("transactions", cacheOpts)
	si.stateCache = si.cacheManager.GetCache("state", cacheOpts)

	// Initialize memory pools
	// Create block pool with proper factory function
	blockFactory := func() interface{} {
		return &common.Block{}
	}
	poolConfig := &optimization.PoolConfig{
		MaxSize:     1000,
		InitialSize: 100,
		Name:        "block-pool",
	}
	si.blockPool = optimization.NewPool(blockFactory, poolConfig)

	si.bufferPool = optimization.NewBufferPool(4096, 1048576, nil)

	// Initialize event bus
	si.eventBus = NewEventBus(si.config.EventBufferSize, si.config.EventWorkers, si.ctx)

	// Initialize sync manager
	if si.network != nil && si.config.EnableNetworkSync {
		si.syncManager = &SyncManager{
			store:   si.store,
			network: si.network,
			logger:  si.logger,
		}
	}

	// Initialize metrics
	if si.config.EnableMetrics {
		if err := si.initializeMetrics(); err != nil {
			return fmt.Errorf("failed to initialize metrics: %w", err)
		}
	}

	// Initialize health monitor
	si.healthMon = &HealthMonitor{
		integration: si,
		alerts:      make(chan HealthAlert, 100),
	}

	// Start background processes
	si.startBackgroundProcesses()

	return nil
}

// initializeMetrics sets up Prometheus metrics
func (si *StorageIntegration) initializeMetrics() error {
	si.intMetrics = &IntegrationMetrics{
		consensusSyncs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_consensus_syncs_total",
				Help: "Total number of consensus synchronizations",
			},
			[]string{"type", "status"},
		),
		networkSyncs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_network_syncs_total",
				Help: "Total number of network synchronizations",
			},
			[]string{"type", "status"},
		),
		vmOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_vm_operations_total",
				Help: "Total number of VM operations",
			},
			[]string{"operation", "status"},
		),
		eventProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_events_processed_total",
				Help: "Total number of processed storage events",
			},
			[]string{"type", "status"},
		),
		syncDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "storage_sync_duration_seconds",
				Help:    "Duration of storage synchronization operations",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		cacheHitRate: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_cache_hit_rate",
				Help: "Cache hit rate percentage",
			},
		),
		storageUtilization: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_utilization_bytes",
				Help: "Storage space utilization in bytes",
			},
		),
	}

	// Register metrics
	collectors := []prometheus.Collector{
		si.intMetrics.consensusSyncs,
		si.intMetrics.networkSyncs,
		si.intMetrics.vmOperations,
		si.intMetrics.eventProcessed,
		si.intMetrics.syncDuration,
		si.intMetrics.cacheHitRate,
		si.intMetrics.storageUtilization,
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

// startBackgroundProcesses starts all background routines
func (si *StorageIntegration) startBackgroundProcesses() {
	// Start event processing
	si.eventBus.Start()

	// Start sync routine
	if si.config.EnableNetworkSync && si.syncManager != nil {
		si.wg.Add(1)
		go si.syncRoutine()
	}

	// Start pruning routine
	if si.config.PruneInterval > 0 {
		si.wg.Add(1)
		go si.pruneRoutine()
	}

	// Start compaction routine
	if si.config.CompactInterval > 0 {
		si.wg.Add(1)
		go si.compactRoutine()
	}

	// Start health monitoring
	si.healthMon.Start()
}

// SetConsensus sets the consensus engine integration
func (si *StorageIntegration) SetConsensus(consensus ConsensusEngine) {
	si.consensus = consensus

	if si.config.EnableConsensusSync {
		// Subscribe to consensus events
		si.subscribeToConsensusEvents()
	}
}

// SetNetwork sets the network manager integration
func (si *StorageIntegration) SetNetwork(network NetworkManager) {
	si.network = network

	if si.syncManager != nil {
		si.syncManager.network = network
	}

	if si.config.EnableNetworkSync {
		// Subscribe to network events
		si.subscribeToNetworkEvents()
	}
}

// SetVMManager sets the VM manager integration
func (si *StorageIntegration) SetVMManager(vmManager VMManager) {
	si.vmManager = vmManager

	if si.config.EnableVMIntegration {
		// Set up VM state callbacks
		si.setupVMCallbacks()
	}
}

// SetMetricsReporter sets the metrics reporter
func (si *StorageIntegration) SetMetricsReporter(reporter MetricsReporter) {
	si.metrics = reporter
}

// subscribeToConsensusEvents subscribes to consensus events
func (si *StorageIntegration) subscribeToConsensusEvents() {
	if si.consensus == nil {
		return
	}

	// Subscribe to block finalized events
	si.consensus.Subscribe("block_finalized", func(event *ConsensusEvent) {
		if event != nil && event.BlockData != nil {
			si.handleBlockFinalized(event.BlockData)
		}
	})
}

// subscribeToNetworkEvents subscribes to network events
func (si *StorageIntegration) subscribeToNetworkEvents() {
	if si.network == nil {
		return
	}

	// Subscribe to sync request events
	si.network.Subscribe("sync_request", func(event *NetworkEvent) {
		if event != nil {
			si.handleSyncRequest(event)
		}
	})
}

// setupVMCallbacks sets up VM state callbacks
func (si *StorageIntegration) setupVMCallbacks() {
	if si.vmManager == nil {
		return
	}

	// Set state read callback
	si.vmManager.SetStateReadCallback(func(key []byte) ([]byte, error) {
		// Check cache first
		if cacheVal, ok := si.stateCache.Get(string(key)); ok && cacheVal != nil {
			return cacheVal.Data, nil
		}

		// Read from store
		data, err := si.store.GetState(key)
		if err != nil {
			return nil, err
		}

		return data, nil
	})

	// Set state write callback
	si.vmManager.SetStateWriteCallback(func(key, value []byte) error {
		// Create cache value
		cacheValue := &types.CacheValue{
			Key:        string(key),
			Data:       value,
			Size:       uint64(len(value)),
			CreatedAt:  time.Now(),
			AccessedAt: time.Now(),
		}

		// Update cache
		si.stateCache.Set(string(key), cacheValue)

		// Write to store
		return si.store.SaveState(key, value)
	})
}

// handleBlockFinalized handles finalized blocks from consensus
func (si *StorageIntegration) handleBlockFinalized(block *common.Block) {
	startTime := time.Now()

	// Create batch for atomic operations
	batch := NewWriteBatch()
	batch.AddBlock(block)

	// Add transactions
	for _, tx := range block.Transactions {
		batch.AddTransaction(&tx)
	}

	// Execute batch
	if err := si.store.WriteBatch(*batch); err != nil {
		si.logger.WithError(err).Error("Failed to write finalized block")
		if si.intMetrics != nil {
			si.intMetrics.consensusSyncs.WithLabelValues("block", "error").Inc()
		}
		return
	}

	// Update cache
	blockData, _ := json.Marshal(block)
	cacheValue := &types.CacheValue{
		Key:        fmt.Sprintf("%d", block.Number),
		Data:       blockData,
		Size:       uint64(len(blockData)),
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
	}
	si.blockCache.Set(fmt.Sprintf("%d", block.Number), cacheValue)

	// Emit event
	si.eventBus.Publish(StorageEvent{
		Type: EventBlockAdded,
		Data: &EventData{
			Block:     block,
			EventType: string(EventBlockAdded),
			Timestamp: time.Now(),
		},
		Timestamp: time.Now(),
	})

	// Update metrics
	if si.intMetrics != nil {
		si.intMetrics.consensusSyncs.WithLabelValues("block", "success").Inc()
		si.intMetrics.syncDuration.WithLabelValues("block_finalize").Observe(time.Since(startTime).Seconds())
	}
}

// handleSyncRequest handles sync requests from network
func (si *StorageIntegration) handleSyncRequest(event *NetworkEvent) {
	// Handle different types of sync requests based on NetworkEvent
	// This would be implemented based on the network protocol
	if event.EventType == "sync_request" && event.Data != nil {
		// Process sync request data
		si.logger.WithFields(logrus.Fields{
			"peer_id":      event.PeerID,
			"message_type": event.MessageType,
		}).Debug("Handling sync request")
	}
}

// syncRoutine handles periodic synchronization
func (si *StorageIntegration) syncRoutine() {
	defer si.wg.Done()

	ticker := time.NewTicker(si.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			return
		case <-ticker.C:
			if si.syncManager != nil {
				si.syncManager.PerformSync()
			}
		}
	}
}

// pruneRoutine handles periodic data pruning
func (si *StorageIntegration) pruneRoutine() {
	defer si.wg.Done()

	ticker := time.NewTicker(si.config.PruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			return
		case <-ticker.C:
			olderThan := time.Now().Add(-30 * 24 * time.Hour) // 30 days
			if err := si.store.PruneData(olderThan); err != nil {
				si.logger.WithError(err).Error("Failed to prune data")
			}
		}
	}
}

// compactRoutine handles periodic storage compaction
func (si *StorageIntegration) compactRoutine() {
	defer si.wg.Done()

	ticker := time.NewTicker(si.config.CompactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			return
		case <-ticker.C:
			if err := si.store.Compact(); err != nil {
				si.logger.WithError(err).Error("Failed to compact storage")
			}
		}
	}
}

// Subscribe subscribes to storage events
func (si *StorageIntegration) Subscribe(eventType EventType, handler EventHandler) string {
	return si.eventBus.Subscribe(eventType, handler)
}

// Unsubscribe removes an event subscription
func (si *StorageIntegration) Unsubscribe(id string) {
	si.eventBus.Unsubscribe(id)
}

// GetBlock retrieves a block with caching
func (si *StorageIntegration) GetBlock(height uint64) (*common.Block, error) {
	// Check cache first
	key := fmt.Sprintf("%d", height)
	if val, ok := si.blockCache.Get(key); ok && val != nil {
		// Deserialize block from cache
		var block common.Block
		if err := json.Unmarshal(val.Data, &block); err == nil {
			return &block, nil
		}
	}

	// Get from store
	block, err := si.store.GetBlock(height)
	if err != nil {
		return nil, err
	}

	// Update cache
	blockData, _ := json.Marshal(block)
	cacheValue := &types.CacheValue{
		Key:        key,
		Data:       blockData,
		Size:       uint64(len(blockData)),
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
	}
	si.blockCache.Set(key, cacheValue)

	return block, nil
}

// GetTransaction retrieves a transaction with caching
func (si *StorageIntegration) GetTransaction(txID string) (*common.Transaction, error) {
	// Check cache first
	if val, ok := si.txCache.Get(txID); ok && val != nil {
		// Deserialize transaction from cache
		var tx common.Transaction
		if err := json.Unmarshal(val.Data, &tx); err == nil {
			return &tx, nil
		}
	}

	// Get from store
	tx, err := si.store.GetTransaction(txID)
	if err != nil {
		return nil, err
	}

	// Update cache
	txData, _ := json.Marshal(tx)
	cacheValue := &types.CacheValue{
		Key:        txID,
		Data:       txData,
		Size:       uint64(len(txData)),
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
	}
	si.txCache.Set(txID, cacheValue)

	return tx, nil
}

// GetState retrieves state with caching
func (si *StorageIntegration) GetState(key []byte) ([]byte, error) {
	// Check cache first
	cacheKey := string(key)
	if val, ok := si.stateCache.Get(cacheKey); ok && val != nil {
		return val.Data, nil
	}

	// Get from store
	value, err := si.store.GetState(key)
	if err != nil {
		return nil, err
	}

	// Update cache
	cacheValue := &types.CacheValue{
		Key:        cacheKey,
		Data:       value,
		Size:       uint64(len(value)),
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
	}
	si.stateCache.Set(cacheKey, cacheValue)

	return value, nil
}

// SaveBlock saves a block with integration hooks
func (si *StorageIntegration) SaveBlock(block *common.Block) error {
	// Save to store
	if err := si.store.SaveBlock(block); err != nil {
		return err
	}

	// Update cache
	blockData, _ := json.Marshal(block)
	cacheValue := &types.CacheValue{
		Key:        fmt.Sprintf("%d", block.Number),
		Data:       blockData,
		Size:       uint64(len(blockData)),
		CreatedAt:  time.Now(),
		AccessedAt: time.Now(),
	}
	si.blockCache.Set(fmt.Sprintf("%d", block.Number), cacheValue)

	// Emit event
	si.eventBus.Publish(StorageEvent{
		Type: EventBlockAdded,
		Data: &EventData{
			Block:     block,
			EventType: string(EventBlockAdded),
			Timestamp: time.Now(),
		},
		Timestamp: time.Now(),
	})

	return nil
}

// GetHealthAlerts returns a channel for health alerts
func (si *StorageIntegration) GetHealthAlerts() <-chan HealthAlert {
	return si.healthMon.alerts
}

// GetStats returns integration statistics
func (si *StorageIntegration) GetStats() *StorageStats {
	// Calculate cache hit rate
	cacheHits := 0.0
	cacheMisses := 0.0
	cacheHitRate := 0.0
	if (cacheHits + cacheMisses) > 0 {
		cacheHitRate = cacheHits / (cacheHits + cacheMisses)
	}

	// Return basic storage stats for now
	stats := &StorageStats{
		ConnectionsActive:     1,
		ConnectionsTotal:      1,
		QueriesExecuted:       int64(si.blockCache.Len() + si.txCache.Len() + si.stateCache.Len()),
		TransactionsProcessed: 0,
		CacheHitRate:          cacheHitRate,
		AverageQueryTime:      0.0,
		ErrorCount:            0,
		LastError:             "",
		UptimeSeconds:         int64(time.Since(si.startTime).Seconds()),
		MemoryUsageMB:         0.0,
	}

	return stats
}

// Close shuts down the storage integration
func (si *StorageIntegration) Close() error {
	// Cancel context
	si.cancel()

	// Stop health monitor
	if si.healthMon != nil {
		si.healthMon.Stop()
	}

	// Stop event bus
	if si.eventBus != nil {
		si.eventBus.Stop()
	}

	// Wait for background routines
	si.wg.Wait()

	// Close pools
	if si.blockPool != nil {
		// blockPool doesn't have Close method in the current implementation
	}

	if si.bufferPool != nil {
		// bufferPool doesn't have Close method in the current implementation
	}

	if si.connPool != nil {
		si.connPool.Close()
	}

	si.logger.Info("Storage integration closed")
	return nil
}

// EventBus implementation

// NewEventBus creates a new event bus
func NewEventBus(bufferSize, workers int, ctx context.Context) *EventBus {
	busCtx, cancel := context.WithCancel(ctx)

	return &EventBus{
		events:   make(chan StorageEvent, bufferSize),
		workers:  workers,
		handlers: make(map[EventType][]EventHandler),
		ctx:      busCtx,
		cancel:   cancel,
	}
}

// Start starts the event bus workers
func (eb *EventBus) Start() {
	for i := 0; i < eb.workers; i++ {
		eb.wg.Add(1)
		go eb.worker()
	}
}

// Stop stops the event bus
func (eb *EventBus) Stop() {
	eb.cancel()
	close(eb.events)
	eb.wg.Wait()
}

// Publish publishes an event
func (eb *EventBus) Publish(event StorageEvent) {
	select {
	case eb.events <- event:
	case <-eb.ctx.Done():
	default:
		// Event buffer full, drop event
		if eb.metrics != nil {
			eb.metrics.WithLabelValues(string(event.Type), "dropped").Inc()
		}
	}
}

// Subscribe subscribes to an event type
func (eb *EventBus) Subscribe(eventType EventType, handler EventHandler) string {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	id := fmt.Sprintf("%s_%d", eventType, time.Now().UnixNano())
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)

	return id
}

// Unsubscribe removes a subscription
func (eb *EventBus) Unsubscribe(id string) {
	// Implementation would track subscription IDs
}

// worker processes events
func (eb *EventBus) worker() {
	defer eb.wg.Done()

	for {
		select {
		case event, ok := <-eb.events:
			if !ok {
				return
			}
			eb.processEvent(event)
		case <-eb.ctx.Done():
			return
		}
	}
}

// processEvent processes a single event
func (eb *EventBus) processEvent(event StorageEvent) {
	eb.mu.RLock()
	handlers := eb.handlers[event.Type]
	eb.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler(eb.ctx, event); err != nil {
			if eb.metrics != nil {
				eb.metrics.WithLabelValues(string(event.Type), "error").Inc()
			}
		} else {
			if eb.metrics != nil {
				eb.metrics.WithLabelValues(string(event.Type), "success").Inc()
			}
		}
	}
}

// SyncManager implementation

// PerformSync performs synchronization
func (sm *SyncManager) PerformSync() {
	if !sm.syncMutex.TryLock() {
		return // Already syncing
	}
	defer sm.syncMutex.Unlock()

	sm.syncState.Store(1) // Syncing
	defer func() {
		sm.syncState.Store(2) // Synchronized
		sm.lastSync.Store(time.Now().Unix())
	}()

	// Implementation would handle actual synchronization logic
}

// IsActive returns if sync is active
func (sm *SyncManager) IsActive() bool {
	state := sm.syncState.Load()
	return state == 1 // syncing
}

// HealthMonitor implementation

// Start starts health monitoring
func (hm *HealthMonitor) Start() {
	hm.ticker = time.NewTicker(hm.integration.config.HealthCheckInterval)
	hm.healthy.Store(true)

	go func() {
		for range hm.ticker.C {
			hm.performCheck()
		}
	}()
}

// Stop stops health monitoring
func (hm *HealthMonitor) Stop() {
	if hm.ticker != nil {
		hm.ticker.Stop()
	}
	close(hm.alerts)
}

// performCheck performs a health check
func (hm *HealthMonitor) performCheck() {
	hm.lastCheck.Store(time.Now().Unix())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check store health
	if err := hm.integration.store.HealthCheck(ctx); err != nil {
		hm.healthy.Store(false)
		hm.sendAlert(HealthAlert{
			Level:     "error",
			Component: "store",
			Message:   fmt.Sprintf("Store health check failed: %v", err),
			Timestamp: time.Now(),
		})
		return
	}

	// Check cache health
	if hm.integration.blockCache.Len() < 0 {
		hm.healthy.Store(false)
		hm.sendAlert(HealthAlert{
			Level:     "warning",
			Component: "cache",
			Message:   "Cache system unhealthy",
			Timestamp: time.Now(),
		})
		return
	}

	hm.healthy.Store(true)
}

// sendAlert sends a health alert
func (hm *HealthMonitor) sendAlert(alert HealthAlert) {
	select {
	case hm.alerts <- alert:
	default:
		// Alert channel full
	}
}
