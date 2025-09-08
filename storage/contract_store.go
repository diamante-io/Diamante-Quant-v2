// Package storage provides contract storage functionality for the blockchain
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/types"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ContractStore manages smart contract storage with caching and persistence
type ContractStore struct {
	db          LedgerStore
	cache       map[string]*contractCacheEntry
	cacheMu     sync.RWMutex
	maxCache    int
	cacheHits   uint64
	cacheMisses uint64
	logger      *logrus.Logger

	// LRU tracking
	accessList     *accessListNode
	accessListTail *accessListNode
	accessMap      map[string]*accessListNode

	// Owner index
	ownerIndex   map[string][]string // owner -> contract IDs
	ownerIndexMu sync.RWMutex

	// Metrics
	metrics *contractStoreMetrics
}

// contractCacheEntry represents a cached contract
type contractCacheEntry struct {
	contract    *common.SmartContract
	lastAccess  time.Time
	accessCount uint64
}

// accessListNode for LRU implementation
type accessListNode struct {
	contractID string
	prev       *accessListNode
	next       *accessListNode
}

// contractStoreMetrics holds Prometheus metrics
type contractStoreMetrics struct {
	contractsStored    prometheus.Counter
	contractsRetrieved prometheus.Counter
	contractsUpdated   prometheus.Counter
	contractsDeleted   prometheus.Counter
	cacheHitRate       prometheus.Gauge
	cacheSizeGauge     prometheus.Gauge
	storeLatency       prometheus.Histogram
}

// NewContractStore creates a new contract store with caching
func NewContractStore(db LedgerStore, maxCache int, logger *logrus.Logger) *ContractStore {
	if logger == nil {
		logger = logrus.New()
	}

	if maxCache <= 0 {
		maxCache = 1000 // Default cache size
	}

	cs := &ContractStore{
		db:         db,
		cache:      make(map[string]*contractCacheEntry),
		maxCache:   maxCache,
		logger:     logger,
		accessMap:  make(map[string]*accessListNode),
		ownerIndex: make(map[string][]string),
	}

	// Initialize metrics
	cs.initMetrics()

	return cs
}

// initMetrics initializes Prometheus metrics
func (cs *ContractStore) initMetrics() {
	cs.metrics = &contractStoreMetrics{
		contractsStored: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_store_contracts_stored_total",
			Help: "Total number of contracts stored",
		}),
		contractsRetrieved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_store_contracts_retrieved_total",
			Help: "Total number of contracts retrieved",
		}),
		contractsUpdated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_store_contracts_updated_total",
			Help: "Total number of contracts updated",
		}),
		contractsDeleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_store_contracts_deleted_total",
			Help: "Total number of contracts deleted",
		}),
		cacheHitRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contract_store_cache_hit_rate",
			Help: "Cache hit rate percentage",
		}),
		cacheSizeGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contract_store_cache_size",
			Help: "Current number of contracts in cache",
		}),
		storeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "contract_store_operation_latency_ms",
			Help:    "Contract store operation latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		}),
	}

	// Register metrics
	prometheus.MustRegister(
		cs.metrics.contractsStored,
		cs.metrics.contractsRetrieved,
		cs.metrics.contractsUpdated,
		cs.metrics.contractsDeleted,
		cs.metrics.cacheHitRate,
		cs.metrics.cacheSizeGauge,
		cs.metrics.storeLatency,
	)
}

// StoreContract stores a smart contract with validation and caching
func (cs *ContractStore) StoreContract(contract *common.SmartContract) error {
	start := time.Now()
	defer func() {
		cs.metrics.storeLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Validate contract
	if err := cs.validateContract(contract); err != nil {
		return err
	}

	// Initialize state if nil
	if contract.State == nil {
		contract.State = &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   time.Now().Unix(),
		}
	}

	// Initialize metadata if nil
	if contract.Metadata == nil {
		contract.Metadata = &common.SmartContractMetadata{}
	}

	// Update timestamps
	if contract.CreatedAt.IsZero() {
		contract.CreatedAt = time.Now()
	}
	contract.UpdatedAt = time.Now()
	contract.State.LastUpdated = time.Now().Unix()

	// Serialize contract
	data, err := json.Marshal(contract)
	if err != nil {
		return fmt.Errorf("failed to serialize contract: %w", err)
	}

	// Store in database using SaveState method
	key := cs.contractKey(contract.ID)
	if err := cs.db.SaveState(key, data); err != nil {
		return fmt.Errorf("failed to store contract: %w", err)
	}

	// Update owner index
	if err := cs.updateOwnerIndex(contract.Owner, contract.ID); err != nil {
		// Log error but don't fail the operation
		cs.logger.WithError(err).WithFields(logrus.Fields{
			"contractID": contract.ID,
			"owner":      contract.Owner,
		}).Error("Failed to update owner index")
	}

	// Update cache
	cs.updateCache(contract)

	// Update metrics
	cs.metrics.contractsStored.Inc()

	cs.logger.WithFields(logrus.Fields{
		"contractID": contract.ID,
		"language":   contract.Language,
		"codeSize":   len(contract.Code),
		"owner":      contract.Owner,
	}).Debug("Contract stored successfully")

	return nil
}

// GetContract retrieves a smart contract with caching
func (cs *ContractStore) GetContract(contractID string) (*common.SmartContract, error) {
	start := time.Now()
	defer func() {
		cs.metrics.storeLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Validate contract ID
	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	// Check cache first
	cs.cacheMu.RLock()
	entry, exists := cs.cache[contractID]
	if exists {
		atomic.AddUint64(&entry.accessCount, 1)
		entry.lastAccess = time.Now()
		atomic.AddUint64(&cs.cacheHits, 1)
		cs.cacheMu.RUnlock()

		// Update LRU
		cs.updateLRU(contractID)

		// Update metrics
		cs.metrics.contractsRetrieved.Inc()
		cs.updateCacheMetrics()

		// Return copy to prevent external modifications
		return cs.copyContract(entry.contract), nil
	}
	cs.cacheMu.RUnlock()

	atomic.AddUint64(&cs.cacheMisses, 1)

	// Get from database
	key := cs.contractKey(contractID)
	data, err := cs.db.GetState(key)
	if err != nil {
		return nil, fmt.Errorf("contract not found: %w", err)
	}

	// Deserialize contract
	var contract common.SmartContract
	if err := json.Unmarshal(data, &contract); err != nil {
		return nil, fmt.Errorf("failed to deserialize contract: %w", err)
	}

	// Update cache
	cs.updateCache(&contract)

	// Update metrics
	cs.metrics.contractsRetrieved.Inc()
	cs.updateCacheMetrics()

	cs.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"fromCache":  false,
	}).Debug("Contract retrieved from storage")

	// Return copy
	return cs.copyContract(&contract), nil
}

// GetContractCode retrieves just the code with decoding
func (cs *ContractStore) GetContractCode(contractID string) ([]byte, error) {
	contract, err := cs.GetContract(contractID)
	if err != nil {
		return nil, err
	}

	// Decode hex code
	code, err := hexutil.Decode(contract.Code)
	if err != nil {
		return nil, fmt.Errorf("failed to decode contract code: %w", err)
	}

	return code, nil
}

// GetContractCodeHash retrieves the code hash
func (cs *ContractStore) GetContractCodeHash(contractID string) (string, error) {
	contract, err := cs.GetContract(contractID)
	if err != nil {
		return "", err
	}

	if contract.CodeHash == "" {
		// Calculate if not stored
		code, err := hexutil.Decode(contract.Code)
		if err != nil {
			return "", fmt.Errorf("failed to decode contract code: %w", err)
		}
		contract.CodeHash = hexutil.Encode(crypto.Keccak256(code))

		// Update stored contract with hash
		cs.UpdateContract(contractID, &types.ContractUpdate{
			CodeHash: contract.CodeHash,
		})
	}

	return contract.CodeHash, nil
}

// UpdateContract updates contract metadata
func (cs *ContractStore) UpdateContract(contractID string, updates *types.ContractUpdate) error {
	start := time.Now()
	defer func() {
		cs.metrics.storeLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Get existing contract
	contract, err := cs.GetContract(contractID)
	if err != nil {
		return err
	}

	// Apply updates
	if updates.Version != "" {
		contract.Version = updates.Version
	}

	if updates.CodeHash != "" {
		contract.CodeHash = updates.CodeHash
	}

	if updates.Owner != "" {
		contract.Owner = updates.Owner
	}

	if updates.Code != "" {
		contract.Code = updates.Code
	}

	if updates.Language != "" {
		contract.Language = updates.Language
	}

	// Description field can be stored in metadata if needed

	// Handle state updates
	if updates.State != nil && len(updates.State) > 0 {
		if contract.State == nil {
			contract.State = &common.SmartContractState{
				Variables:     make(map[string]string),
				Balances:      make(map[string]float64),
				Permissions:   make(map[string]bool),
				Configuration: make(map[string]string),
				Counters:      make(map[string]int64),
			}
		}

		// Update state fields - for now just store all as variables
		// A more sophisticated implementation would parse the Value types
		for k, v := range updates.State {
			if v != nil && v.Data != nil {
				// Store as string in variables map
				contract.State.Variables[k] = string(v.Data)
			}
		}
		contract.State.LastUpdated = common.ConsensusUnix()
	}

	// Handle metadata updates
	if updates.Metadata != nil && len(updates.Metadata) > 0 {
		if contract.Metadata == nil {
			contract.Metadata = &common.SmartContractMetadata{}
		}

		// Update metadata fields from typed values
		for k, v := range updates.Metadata {
			switch k {
			case "author":
				if val, err := v.String(); err == nil {
					contract.Metadata.Author = val
				}
			case "license":
				if val, err := v.String(); err == nil {
					contract.Metadata.License = val
				}
			case "description":
				if val, err := v.String(); err == nil {
					contract.Metadata.Description = val
				}
			case "documentation":
				if val, err := v.String(); err == nil {
					contract.Metadata.Documentation = val
				}
			case "version":
				if val, err := v.String(); err == nil {
					contract.Metadata.Version = val
				}
			case "audited":
				if val, err := v.Bool(); err == nil {
					contract.Metadata.Audited = val
				}
			}
		}
	}

	// Store updated contract
	if err := cs.StoreContract(contract); err != nil {
		return err
	}

	// Update metrics
	cs.metrics.contractsUpdated.Inc()

	cs.logger.WithFields(logrus.Fields{
		"contractID": contractID,
	}).Debug("Contract updated successfully")

	return nil
}

// DeleteContract marks a contract as deleted (soft delete)
func (cs *ContractStore) DeleteContract(contractID string) error {
	start := time.Now()
	defer func() {
		cs.metrics.storeLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Get contract to update status
	contract, err := cs.GetContract(contractID)
	if err != nil {
		return err
	}

	// Mark as deleted in configuration (soft delete)
	if contract.State == nil {
		contract.State = &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
		}
	}
	contract.State.Configuration["deleted"] = "true"
	contract.State.Configuration["deletedAt"] = time.Now().Format(time.RFC3339)
	contract.State.LastUpdated = time.Now().Unix()

	// Store updated contract
	if err := cs.StoreContract(contract); err != nil {
		return err
	}

	// Remove from cache
	cs.removeFromCache(contractID)

	// Update metrics
	cs.metrics.contractsDeleted.Inc()

	cs.logger.WithFields(logrus.Fields{
		"contractID": contractID,
	}).Info("Contract marked as deleted")

	return nil
}

// ContractExists checks if a contract exists
func (cs *ContractStore) ContractExists(contractID string) (bool, error) {
	// Check cache first
	cs.cacheMu.RLock()
	if _, exists := cs.cache[contractID]; exists {
		cs.cacheMu.RUnlock()
		return true, nil
	}
	cs.cacheMu.RUnlock()

	// Check database
	key := cs.contractKey(contractID)
	_, err := cs.db.GetState(key)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// GetContractsByOwner retrieves all contracts owned by an address
func (cs *ContractStore) GetContractsByOwner(owner string) ([]*common.SmartContract, error) {
	start := time.Now()
	defer func() {
		cs.metrics.storeLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	if owner == "" {
		return nil, errors.New("owner address cannot be empty")
	}

	// First try to get from in-memory index
	cs.ownerIndexMu.RLock()
	contractIDs, hasInMemory := cs.ownerIndex[owner]
	cs.ownerIndexMu.RUnlock()

	// If not in memory, try to load from database
	if !hasInMemory {
		var err error
		contractIDs, err = cs.loadOwnerIndex(owner)
		if err != nil {
			cs.logger.WithError(err).WithField("owner", owner).Error("Failed to load owner index")
			// Continue with empty list instead of failing
			contractIDs = []string{}
		}
	}

	// Retrieve contracts
	contracts := make([]*common.SmartContract, 0, len(contractIDs))
	for _, contractID := range contractIDs {
		contract, err := cs.GetContract(contractID)
		if err != nil {
			cs.logger.WithError(err).WithFields(logrus.Fields{
				"contractID": contractID,
				"owner":      owner,
			}).Warn("Failed to retrieve contract for owner")
			continue
		}

		// Verify owner matches (in case of index corruption)
		if contract.Owner == owner {
			contracts = append(contracts, contract)
		}
	}

	cs.logger.WithFields(logrus.Fields{
		"owner":         owner,
		"contractCount": len(contracts),
	}).Debug("Retrieved contracts by owner")

	return contracts, nil
}

// updateOwnerIndex updates the owner index for a contract
func (cs *ContractStore) updateOwnerIndex(owner, contractID string) error {
	if owner == "" || contractID == "" {
		return errors.New("owner and contract ID must not be empty")
	}

	// Update in-memory index
	cs.ownerIndexMu.Lock()
	cs.ownerIndex[owner] = append(cs.ownerIndex[owner], contractID)
	cs.ownerIndexMu.Unlock()

	// Persist owner index to database
	indexKey := cs.ownerIndexKey(owner)

	// Get existing index from database
	existingData, err := cs.db.GetState([]byte(indexKey))
	var contractIDs []string
	if err == nil && existingData != nil && len(existingData) > 0 {
		if err := json.Unmarshal(existingData, &contractIDs); err != nil {
			cs.logger.WithError(err).Warn("Failed to unmarshal existing owner index")
			contractIDs = []string{}
		}
	}

	// Add new contract ID if not already present
	found := false
	for _, id := range contractIDs {
		if id == contractID {
			found = true
			break
		}
	}
	if !found {
		contractIDs = append(contractIDs, contractID)
	}

	// Serialize updated index
	data, err := json.Marshal(contractIDs)
	if err != nil {
		return fmt.Errorf("failed to serialize owner index: %w", err)
	}

	// Store updated index
	if err := cs.db.SaveState([]byte(indexKey), data); err != nil {
		return fmt.Errorf("failed to store owner index: %w", err)
	}

	return nil
}

// loadOwnerIndex loads the owner index from database
func (cs *ContractStore) loadOwnerIndex(owner string) ([]string, error) {
	indexKey := cs.ownerIndexKey(owner)

	data, err := cs.db.GetState([]byte(indexKey))
	if err != nil {
		// Not found is not an error, just return empty list
		return []string{}, nil
	}

	var contractIDs []string
	if err := json.Unmarshal(data, &contractIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal owner index: %w", err)
	}

	// Update in-memory index
	cs.ownerIndexMu.Lock()
	cs.ownerIndex[owner] = contractIDs
	cs.ownerIndexMu.Unlock()

	return contractIDs, nil
}

// RebuildOwnerIndex rebuilds the owner index by scanning all contracts
func (cs *ContractStore) RebuildOwnerIndex() error {
	cs.logger.Info("Starting owner index rebuild")
	start := time.Now()

	// Clear existing index
	cs.ownerIndexMu.Lock()
	cs.ownerIndex = make(map[string][]string)
	cs.ownerIndexMu.Unlock()

	// Track progress using atomic integers
	var processedCount int32
	var errorCount int32
	batchSize := 100
	lastLogTime := time.Now()

	// Create temporary index to build
	tempIndex := make(map[string][]string)
	tempIndexMu := sync.Mutex{}

	// Use a worker pool for parallel processing
	workerCount := 4
	contractChan := make(chan string, batchSize)
	errorChan := make(chan error, workerCount)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for contractID := range contractChan {
				// Get contract from database
				contract, err := cs.GetContract(contractID)
				if err != nil {
					cs.logger.WithError(err).WithFields(logrus.Fields{
						"contractID": contractID,
						"workerID":   workerID,
					}).Warn("Failed to load contract during index rebuild")
					errorChan <- err
					continue
				}

				// Skip deleted contracts
				if contract.State != nil && contract.State.Configuration != nil {
					if deleted, exists := contract.State.Configuration["deleted"]; exists && deleted == "true" {
						continue
					}
				}

				// Update temporary index
				tempIndexMu.Lock()
				tempIndex[contract.Owner] = append(tempIndex[contract.Owner], contract.ID)
				tempIndexMu.Unlock()

				// Log progress periodically
				if time.Since(lastLogTime) > 10*time.Second {
					cs.logger.WithFields(logrus.Fields{
						"processed": atomic.LoadInt32(&processedCount),
						"errors":    atomic.LoadInt32(&errorCount),
						"elapsed":   time.Since(start),
					}).Info("Owner index rebuild progress")
					lastLogTime = time.Now()
				}
			}
		}(i)
	}

	// Monitor errors in a separate goroutine
	go func() {
		for err := range errorChan {
			if err != nil {
				atomic.AddInt32(&errorCount, 1)
			}
		}
	}()

	// Scan all contract keys using MongoDB cursor for efficient iteration
	go func() {
		defer close(contractChan)

		// Check if database is MongoDB-based (supports cursor iteration)
		if mongoStore, ok := cs.db.(interface {
			GetDatabase() interface {
				Collection(name string) interface {
					Find(ctx context.Context, filter interface{}, opts ...interface{}) (interface {
						Next(ctx context.Context) bool
						Decode(v interface{}) error
						Close(ctx context.Context) error
						Err() error
					}, error)
				}
			}
		}); ok {
			// Use MongoDB cursor for efficient key scanning
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			// Get state collection from MongoDB
			collection := mongoStore.GetDatabase().Collection("state")

			// Create filter for contract keys
			filter := bson.M{
				"key": bson.M{
					"$regex": "^contract:.*:data$",
				},
			}

			// Use cursor with batch size for memory efficiency
			opts := options.Find().SetBatchSize(1000).SetProjection(bson.M{"key": 1})

			cursor, err := collection.Find(ctx, filter, opts)
			if err != nil {
				cs.logger.WithError(err).Error("Failed to create cursor for contract scanning")
				return
			}
			defer cursor.Close(ctx)

			// Process results
			for cursor.Next(ctx) {
				var result struct {
					Key string `bson:"key"`
				}

				if err := cursor.Decode(&result); err != nil {
					cs.logger.WithError(err).Warn("Failed to decode key during contract scan")
					continue
				}

				// Extract contract ID from key
				if strings.HasPrefix(result.Key, "contract:") && strings.HasSuffix(result.Key, ":data") {
					contractID := strings.TrimPrefix(result.Key, "contract:")
					contractID = strings.TrimSuffix(contractID, ":data")

					select {
					case contractChan <- contractID:
						atomic.AddInt32(&processedCount, 1)
					case <-ctx.Done():
						cs.logger.Warn("Context cancelled during contract scanning")
						return
					}
				}
			}

			if err := cursor.Err(); err != nil {
				cs.logger.WithError(err).Error("Cursor error during contract scanning")
			}
		} else if lmdbStore, ok := cs.db.(interface {
			IterateKeys(prefix []byte, handler func(key, value []byte) error) error
		}); ok {
			// LMDB implementation with key iteration
			contractPrefix := []byte("contract:")
			err := lmdbStore.IterateKeys(contractPrefix, func(key, value []byte) error {
				keyStr := string(key)
				if strings.HasPrefix(keyStr, "contract:") && strings.HasSuffix(keyStr, ":data") {
					contractID := strings.TrimPrefix(keyStr, "contract:")
					contractID = strings.TrimSuffix(contractID, ":data")

					select {
					case contractChan <- contractID:
						atomic.AddInt32(&processedCount, 1)
					case <-time.After(30 * time.Second):
						return fmt.Errorf("timeout sending contract ID to worker")
					}
				}
				return nil
			})

			if err != nil {
				cs.logger.WithError(err).Error("Error iterating LMDB keys")
			}
		} else {
			// Production fallback using contract registry
			cs.logger.Info("Using contract registry for index rebuild")

			// Maintain a registry of all contracts for databases without native iteration
			if err := cs.scanUsingRegistry(contractChan, &processedCount); err != nil {
				cs.logger.WithError(err).Error("Failed to scan using registry")
			}
		}
	}()

	// Wait for key scanning to complete - use context-aware timing
	select {
	case <-time.After(100 * time.Millisecond):
		// Scanner initialization delay completed
	default:
		// Non-blocking, continue immediately if no delay needed
	}

	// Close contract channel when done
	go func() {
		// Wait for a reasonable time for scanning to complete
		maxScanTime := 5 * time.Minute
		scanTimeout := time.After(maxScanTime)

		select {
		case <-scanTimeout:
			cs.logger.Warn("Contract scanning timed out")
		case <-time.After(100 * time.Millisecond):
			// Check periodically if scanning is done using ticker
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if len(contractChan) == 0 {
						// Double-check after brief delay
						select {
						case <-time.After(1 * time.Second):
							if len(contractChan) == 0 {
								return // Exit the goroutine
							}
						}
					}
				case <-scanTimeout:
					return // Exit on timeout
				}
			}
		}

		close(contractChan)
	}()

	// Wait for all workers to complete
	wg.Wait()
	close(errorChan)

	// Persist the new index
	cs.ownerIndexMu.Lock()
	cs.ownerIndex = tempIndex
	cs.ownerIndexMu.Unlock()

	// Persist each owner's index to database
	finalErrorCount := int(atomic.LoadInt32(&errorCount))
	for owner, contractIDs := range tempIndex {
		indexKey := cs.ownerIndexKey(owner)
		data, err := json.Marshal(contractIDs)
		if err != nil {
			cs.logger.WithError(err).WithField("owner", owner).Error("Failed to marshal owner index")
			continue
		}

		if err := cs.db.SaveState([]byte(indexKey), data); err != nil {
			cs.logger.WithError(err).WithField("owner", owner).Error("Failed to persist owner index")
			finalErrorCount++
		}
	}

	cs.logger.WithFields(logrus.Fields{
		"duration":       time.Since(start),
		"processed":      atomic.LoadInt32(&processedCount),
		"errors":         finalErrorCount,
		"uniqueOwners":   len(tempIndex),
		"totalContracts": cs.countTotalContracts(tempIndex),
	}).Info("Owner index rebuild completed")

	if finalErrorCount > 0 {
		return fmt.Errorf("owner index rebuild completed with %d errors", finalErrorCount)
	}

	return nil
}

// getContractList retrieves a list of all contract IDs (fallback method)
func (cs *ContractStore) getContractList() ([]string, error) {
	// Check if we maintain a contract registry
	registryKey := "contract:registry:list"
	data, err := cs.db.GetState([]byte(registryKey))
	if err != nil {
		return nil, fmt.Errorf("contract registry not found: %w", err)
	}

	var contractIDs []string
	if err := json.Unmarshal(data, &contractIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal contract registry: %w", err)
	}

	return contractIDs, nil
}

// countTotalContracts counts total contracts across all owners
func (cs *ContractStore) countTotalContracts(index map[string][]string) int {
	total := 0
	for _, contracts := range index {
		total += len(contracts)
	}
	return total
}

// maintainContractRegistry adds a contract ID to the registry (for fallback scanning)
func (cs *ContractStore) maintainContractRegistry(contractID string, remove bool) error {
	registryKey := "contract:registry:list"

	// Get existing registry
	var contractIDs []string
	data, err := cs.db.GetState([]byte(registryKey))
	if err == nil && data != nil && len(data) > 0 {
		if err := json.Unmarshal(data, &contractIDs); err != nil {
			cs.logger.WithError(err).Warn("Failed to unmarshal contract registry")
			contractIDs = []string{}
		}
	}

	if remove {
		// Remove contract ID
		newIDs := make([]string, 0, len(contractIDs))
		for _, id := range contractIDs {
			if id != contractID {
				newIDs = append(newIDs, id)
			}
		}
		contractIDs = newIDs
	} else {
		// Add contract ID if not present
		found := false
		for _, id := range contractIDs {
			if id == contractID {
				found = true
				break
			}
		}
		if !found {
			contractIDs = append(contractIDs, contractID)
		}
	}

	// Save updated registry
	data, err = json.Marshal(contractIDs)
	if err != nil {
		return fmt.Errorf("failed to marshal contract registry: %w", err)
	}

	return cs.db.SaveState([]byte(registryKey), data)
}

// Helper methods

func (cs *ContractStore) contractKey(contractID string) []byte {
	return []byte(fmt.Sprintf("contract:%s:data", contractID))
}

func (cs *ContractStore) validateContract(contract *common.SmartContract) error {
	if contract.ID == "" {
		return errors.New("contract ID required")
	}
	if contract.Code == "" {
		return errors.New("contract code required")
	}
	if contract.Language == "" {
		return errors.New("contract language required")
	}
	if contract.Owner == "" {
		return errors.New("contract owner required")
	}

	// Validate code format
	if _, err := hexutil.Decode(contract.Code); err != nil {
		return fmt.Errorf("invalid contract code format: %w", err)
	}

	return nil
}

func (cs *ContractStore) updateCache(contract *common.SmartContract) {
	cs.cacheMu.Lock()
	defer cs.cacheMu.Unlock()

	// Check cache size and evict if necessary
	if len(cs.cache) >= cs.maxCache {
		cs.evictLRU()
	}

	// Add to cache
	entry := &contractCacheEntry{
		contract:    cs.copyContract(contract),
		lastAccess:  time.Now(),
		accessCount: 1,
	}
	cs.cache[contract.ID] = entry

	// Update LRU list
	cs.addToLRU(contract.ID)

	// Update metrics
	cs.metrics.cacheSizeGauge.Set(float64(len(cs.cache)))
}

func (cs *ContractStore) removeFromCache(contractID string) {
	cs.cacheMu.Lock()
	defer cs.cacheMu.Unlock()

	delete(cs.cache, contractID)
	cs.removeFromLRU(contractID)

	// Update metrics
	cs.metrics.cacheSizeGauge.Set(float64(len(cs.cache)))
}

func (cs *ContractStore) addToLRU(contractID string) {
	// Remove if already exists
	if node, exists := cs.accessMap[contractID]; exists {
		cs.removeLRUNode(node)
	}

	// Add to front
	node := &accessListNode{contractID: contractID}
	cs.accessMap[contractID] = node

	if cs.accessList == nil {
		cs.accessList = node
		cs.accessListTail = node
	} else {
		node.next = cs.accessList
		cs.accessList.prev = node
		cs.accessList = node
	}
}

func (cs *ContractStore) updateLRU(contractID string) {
	cs.cacheMu.Lock()
	defer cs.cacheMu.Unlock()

	if node, exists := cs.accessMap[contractID]; exists {
		// Move to front
		cs.removeLRUNode(node)
		cs.addToLRU(contractID)
	}
}

func (cs *ContractStore) removeLRUNode(node *accessListNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		cs.accessList = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		cs.accessListTail = node.prev
	}

	delete(cs.accessMap, node.contractID)
}

func (cs *ContractStore) removeFromLRU(contractID string) {
	if node, exists := cs.accessMap[contractID]; exists {
		cs.removeLRUNode(node)
	}
}

func (cs *ContractStore) evictLRU() {
	if cs.accessListTail != nil {
		contractID := cs.accessListTail.contractID
		delete(cs.cache, contractID)
		cs.removeLRUNode(cs.accessListTail)

		cs.logger.WithField("contractID", contractID).Debug("Evicted contract from cache")
	}
}

// copyContract creates a deep copy of a contract
func (cs *ContractStore) copyContract(contract *common.SmartContract) *common.SmartContract {
	if contract == nil {
		return nil
	}

	// Create a new contract with copied values
	contractCopy := &common.SmartContract{
		ID:        contract.ID,
		Code:      contract.Code,
		CodeHash:  contract.CodeHash,
		Owner:     contract.Owner,
		Version:   contract.Version,
		ABI:       contract.ABI,
		Language:  contract.Language,
		GasUsage:  contract.GasUsage,
		CreatedAt: contract.CreatedAt,
		UpdatedAt: contract.UpdatedAt,
	}

	// Deep copy state
	if contract.State != nil {
		contractCopy.State = &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   contract.State.LastUpdated,
		}

		// Copy maps
		for k, v := range contract.State.Variables {
			contractCopy.State.Variables[k] = v
		}
		for k, v := range contract.State.Balances {
			contractCopy.State.Balances[k] = v
		}
		for k, v := range contract.State.Permissions {
			contractCopy.State.Permissions[k] = v
		}
		for k, v := range contract.State.Configuration {
			contractCopy.State.Configuration[k] = v
		}
		for k, v := range contract.State.Counters {
			contractCopy.State.Counters[k] = v
		}
	}

	// Deep copy metadata
	if contract.Metadata != nil {
		contractCopy.Metadata = &common.SmartContractMetadata{
			Author:        contract.Metadata.Author,
			License:       contract.Metadata.License,
			Description:   contract.Metadata.Description,
			Documentation: contract.Metadata.Documentation,
			Version:       contract.Metadata.Version,
			Audited:       contract.Metadata.Audited,
		}

		// Copy slices
		if len(contract.Metadata.Tags) > 0 {
			contractCopy.Metadata.Tags = make([]string, len(contract.Metadata.Tags))
			copy(contractCopy.Metadata.Tags, contract.Metadata.Tags)
		}
		if len(contract.Metadata.Dependencies) > 0 {
			contractCopy.Metadata.Dependencies = make([]string, len(contract.Metadata.Dependencies))
			copy(contractCopy.Metadata.Dependencies, contract.Metadata.Dependencies)
		}
		if len(contract.Metadata.AuditReports) > 0 {
			contractCopy.Metadata.AuditReports = make([]string, len(contract.Metadata.AuditReports))
			copy(contractCopy.Metadata.AuditReports, contract.Metadata.AuditReports)
		}
	}

	// Copy events
	if len(contract.Events) > 0 {
		contractCopy.Events = make([]common.SmartContractEvent, len(contract.Events))
		copy(contractCopy.Events, contract.Events)
	}

	// Copy functions map (shallow copy as functions are not serializable)
	if len(contract.Functions) > 0 {
		contractCopy.Functions = make(map[string]common.SmartContractFunction)
		for k, v := range contract.Functions {
			contractCopy.Functions[k] = v
		}
	}

	return contractCopy
}

func (cs *ContractStore) updateCacheMetrics() {
	hits := atomic.LoadUint64(&cs.cacheHits)
	misses := atomic.LoadUint64(&cs.cacheMisses)
	total := hits + misses

	if total > 0 {
		hitRate := float64(hits) / float64(total) * 100
		cs.metrics.cacheHitRate.Set(hitRate)
	}
}

// GetMetrics returns contract store metrics
func (cs *ContractStore) GetMetrics() ContractStoreMetrics {
	cs.cacheMu.RLock()
	cacheSize := len(cs.cache)
	cs.cacheMu.RUnlock()

	hits := atomic.LoadUint64(&cs.cacheHits)
	misses := atomic.LoadUint64(&cs.cacheMisses)
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return ContractStoreMetrics{
		CacheSize:    cacheSize,
		MaxCacheSize: cs.maxCache,
		CacheHits:    hits,
		CacheMisses:  misses,
		CacheHitRate: hitRate,
	}
}

// scanUsingRegistry scans contracts using a registry for databases without native iteration
func (cs *ContractStore) scanUsingRegistry(contractChan chan<- string, processedCount *int32) error {
	contractList, err := cs.getContractList()
	if err != nil {
		return fmt.Errorf("failed to get contract list: %w", err)
	}

	for _, contractID := range contractList {
		select {
		case contractChan <- contractID:
			atomic.AddInt32(processedCount, 1)
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout sending contract ID")
		}
	}

	return nil
}

// ContractStoreMetrics contains metrics about the contract store
type ContractStoreMetrics struct {
	CacheSize    int
	MaxCacheSize int
	CacheHits    uint64
	CacheMisses  uint64
	CacheHitRate float64
}

// ownerIndexKey generates the storage key for an owner index
func (cs *ContractStore) ownerIndexKey(owner string) string {
	return fmt.Sprintf("owner:index:%s", owner)
}

// tagIndexKey generates the storage key for a tag index
func (cs *ContractStore) tagIndexKey(tag string) []byte {
	return []byte(fmt.Sprintf("contract:tag:%s", tag))
}
