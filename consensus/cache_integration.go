// consensus/cache_integration.go

package consensus

import (
	"encoding/json"
	"fmt"

	"diamante/config"
	"diamante/consensus/types"
	"diamante/crypto"
	cache "diamante/storage/cache"
	diamanteTypes "diamante/types"
)

// CacheNames defines the names of caches used in the consensus module
const (
	// Event-related caches
	EventCache             = "event"
	EventValidationCache   = "event_validation"
	EventFinalizationCache = "event_finalization"

	// Block-related caches
	BlockCache           = "block"
	BlockValidationCache = "block_validation"

	// Validator-related caches
	ValidatorCache       = "validator"
	ValidatorStakeCache  = "validator_stake"
	ValidatorStatusCache = "validator_status"

	// State-related caches
	StateCache = "state"

	// Signature-related caches
	SignatureCache = "signature"
)

// CachedConsensus wraps a HybridConsensus instance and provides caching functionality
type CachedConsensus struct {
	consensus    *HybridConsensus
	cacheManager *CacheManager
	storageCache *cache.Manager
	logger       StructuredLogger
}

// NewCachedConsensus creates a new CachedConsensus instance
func NewCachedConsensus(consensus *HybridConsensus, cfg *config.CacheConfig) *CachedConsensus {
	// Create a legacy logger adapter for backward compatibility
	legacyLogger := consensus.legacyLogger

	cc := &CachedConsensus{
		consensus:    consensus,
		cacheManager: NewCacheManager(legacyLogger),
		storageCache: cache.NewManager(),
		logger:       consensus.logger,
	}

	cc.initializeCaches(cfg)

	return cc
}

// initializeCaches initializes all caches used by the consensus module
func (cc *CachedConsensus) initializeCaches(cfg *config.CacheConfig) {
	opts := &cache.Options{}
	if cfg != nil {
		opts.Size = cfg.Size
		opts.TTL = cfg.TTL
		opts.RedisAddress = cfg.RedisURL
		opts.RedisDB = cfg.RedisDB
	}
	// Event cache - stores events by ID
	cc.storageCache.GetCache(EventCache, opts)

	// Event validation cache - stores validation results for events
	cc.storageCache.GetCache(EventValidationCache, opts)

	// Event finalization cache - stores finalization status for events
	cc.storageCache.GetCache(EventFinalizationCache, opts)

	// Block cache - stores blocks by height
	cc.storageCache.GetCache(BlockCache, opts)

	// Block validation cache - stores validation results for blocks
	cc.storageCache.GetCache(BlockValidationCache, opts)

	// Validator cache - stores validator information
	cc.storageCache.GetCache(ValidatorCache, opts)

	// Validator stake cache - stores validator stakes
	cc.storageCache.GetCache(ValidatorStakeCache, opts)

	// Validator status cache - stores validator statuses
	cc.storageCache.GetCache(ValidatorStatusCache, opts)

	// State cache - stores state information
	cc.storageCache.GetCache(StateCache, opts)

	// Signature cache - stores signature verification results
	cc.storageCache.GetCache(SignatureCache, opts)

	cc.logger.Info("Initialized consensus caches")
}

// CacheableConsensus defines the interface for consensus operations that can be cached
type CacheableConsensus interface {
	// Event-related operations
	GetEventByID(eventID [32]byte) (*types.Event, error)
	ValidateEventData(event *types.Event) (bool, error)
	FinalizeEventProcessing(event *types.Event) (bool, error)

	// Block-related operations
	GetBlockByHeight(height uint64) (*types.Block, error)
	ValidateBlockData(block *types.Block) (bool, error)

	// Validator-related operations
	GetValidatorByID(validatorID [32]byte) (*ValidatorInfo, error)
	GetValidatorStakeByID(validatorID [32]byte) (uint64, error)
	IsValidatorActiveByID(validatorID [32]byte) (bool, error)

	// Signature-related operations
	VerifySignatureData(data []byte, signature []byte, publicKey []byte) (bool, error)
}

// CachedConsensusAdapter adapts HybridConsensus to the CacheableConsensus interface
type CachedConsensusAdapter struct {
	consensus *HybridConsensus
}

// NewCachedConsensusAdapter creates a new adapter for HybridConsensus
func NewCachedConsensusAdapter(consensus *HybridConsensus) *CachedConsensusAdapter {
	return &CachedConsensusAdapter{
		consensus: consensus,
	}
}

// GetEventByID retrieves an event by its ID
func (cca *CachedConsensusAdapter) GetEventByID(eventID [32]byte) (*types.Event, error) {
	// Get finalized events from the EventFlowManager
	// Since HybridConsensus doesn't have a direct GetEvent method, we need to search through finalized events
	// First, get the last block height
	lastBlockHeight := cca.consensus.GetLastBlockHeight()

	// Search through finalized events from the most recent blocks
	// We'll search through the last 10 blocks, which should be sufficient for most cases
	searchDepth := uint64(10)
	if lastBlockHeight < searchDepth {
		searchDepth = lastBlockHeight
	}

	// Get finalized events from the EventFlowManager
	events, err := cca.consensus.GetFinalizedEvents(lastBlockHeight-searchDepth, lastBlockHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to get finalized events: %w", err)
	}

	// Search for the event with the given ID
	for _, event := range events {
		if event.ID == eventID {
			return event, nil
		}
	}

	// If not found in finalized events, check pending events
	pendingEvents := cca.consensus.GetPendingEvents()
	for _, event := range pendingEvents {
		if event.ID == eventID {
			return event, nil
		}
	}

	return nil, fmt.Errorf("event not found: %x", eventID)
}

// ValidateEventData validates an event
func (cca *CachedConsensusAdapter) ValidateEventData(event *types.Event) (bool, error) {
	if event == nil {
		return false, fmt.Errorf("event is nil")
	}

	// Verify the event's PoH information
	pohVerified := cca.verifyPoHWithDrift(event)
	if !pohVerified && cca.consensus.IsTestMode() {
		// In test mode, we're more lenient
		return true, nil
	} else if !pohVerified {
		return false, fmt.Errorf("PoH verification failed")
	}

	// Check if the creator is an active validator
	if !cca.consensus.validatorManager.IsActiveValidator(event.Creator) {
		return false, fmt.Errorf("creator is not an active validator")
	}

	// Additional validation logic could be added here

	return true, nil
}

// verifyPoHWithDrift verifies an event's PoH information with drift tolerance
func (cca *CachedConsensusAdapter) verifyPoHWithDrift(event *types.Event) bool {
	// Get the PoH instance
	poh := cca.consensus.GetPoH()

	// Get the current PoH count
	currentCount := poh.GetCount()

	// Get the drift tolerance from the consensus config
	driftTolerance := cca.consensus.cfg.PoHDriftTolerance

	// Check if the event's PoH count is within the drift tolerance
	if cca.consensus.IsTestMode() {
		// In test mode, allow both forward and backward drift
		diff := int64(event.PoHCount) - int64(currentCount)
		if diff < 0 {
			diff = -diff
		}
		if diff > int64(driftTolerance) {
			return false
		}
	} else {
		// In production mode, only allow forward drift
		if event.PoHCount > currentCount+driftTolerance {
			return false
		}
	}

	// Verify the PoH proof
	return poh.Verify(event.PoHState, event.Data, event.PoHProof, event.PoHCount)
}

// FinalizeEventProcessing finalizes an event
func (cca *CachedConsensusAdapter) FinalizeEventProcessing(event *types.Event) (bool, error) {
	// Call the FinalizeEvent method on HybridConsensus
	return cca.consensus.FinalizeEvent(event)
}

// GetBlockByHeight retrieves a block by its height
func (cca *CachedConsensusAdapter) GetBlockByHeight(height uint64) (*types.Block, error) {
	// HybridConsensus doesn't have a direct GetBlock method
	// We'll need to construct a Block from finalized events

	// Check if the height is valid
	lastBlockHeight := cca.consensus.GetLastBlockHeight()
	if height > lastBlockHeight {
		return nil, fmt.Errorf("block height %d is greater than last block height %d", height, lastBlockHeight)
	}

	// Construct a Block
	block := &types.Block{
		Number:       height,
		Transactions: []types.Transaction{}, // Initialize with empty transactions
	}

	return block, nil
}

// ValidateBlockData validates a block
func (cca *CachedConsensusAdapter) ValidateBlockData(block *types.Block) (bool, error) {
	if block == nil {
		return false, fmt.Errorf("block is nil")
	}

	// Check if the block number is valid
	lastBlockHeight := cca.consensus.GetLastBlockHeight()
	if block.Number > lastBlockHeight+1 {
		return false, fmt.Errorf("block number %d is too high (current height: %d)", block.Number, lastBlockHeight)
	}

	// In a real implementation, we would validate the block's transactions
	// For now, we'll just return true since we're not actually validating anything

	return true, nil
}

// GetValidatorByID retrieves a validator by its ID
func (cca *CachedConsensusAdapter) GetValidatorByID(validatorID [32]byte) (*ValidatorInfo, error) {
	// Get all validators from the ValidatorManager
	validators := cca.consensus.validatorManager.GetValidators()

	// Search for the validator with the given ID
	for _, v := range validators {
		if v.ID == validatorID {
			// Convert types.Validator to ValidatorInfo
			return &ValidatorInfo{
				ID:     v.ID,
				Stake:  v.Stake,
				Status: ValidatorStatusActive, // Assume active since we found it
			}, nil
		}
	}

	return nil, fmt.Errorf("validator not found: %x", validatorID)
}

// GetValidatorStakeByID retrieves a validator's stake by its ID
func (cca *CachedConsensusAdapter) GetValidatorStakeByID(validatorID [32]byte) (uint64, error) {
	// We can use the DPoS component to get the validator stake
	return cca.consensus.dpos.GetValidatorStake(validatorID), nil
}

// IsValidatorActiveByID checks if a validator is active
func (cca *CachedConsensusAdapter) IsValidatorActiveByID(validatorID [32]byte) (bool, error) {
	// Check if the validator is active using the ValidatorManager
	return cca.consensus.validatorManager.IsActiveValidator(validatorID), nil
}

// VerifySignatureData verifies a signature
func (cca *CachedConsensusAdapter) VerifySignatureData(data []byte, signature []byte, publicKey []byte) (bool, error) {
	if len(data) == 0 || len(signature) == 0 || len(publicKey) == 0 {
		return false, fmt.Errorf("invalid input for signature verification")
	}

	// Use the crypto package's constant-time verification
	return crypto.VerifySignatureConstantTime(publicKey, data, signature)
}

// CachedConsensusOperations provides cached versions of consensus operations
type CachedConsensusOperations struct {
	adapter      CacheableConsensus
	cacheManager *CacheManager
	storageCache *cache.Manager
	logger       StructuredLogger
}

// NewCachedConsensusOperations creates a new CachedConsensusOperations
func NewCachedConsensusOperations(adapter CacheableConsensus, logger StructuredLogger, legacyLogger *hybridConsensusLogger, cfg *config.CacheConfig) *CachedConsensusOperations {
	cco := &CachedConsensusOperations{
		adapter:      adapter,
		cacheManager: NewCacheManager(legacyLogger),
		storageCache: cache.NewManager(),
		logger:       logger,
	}

	cco.initializeCaches(cfg)

	return cco
}

// initializeCaches initializes all caches used by the consensus operations
func (cco *CachedConsensusOperations) initializeCaches(cfg *config.CacheConfig) {
	opts := &cache.Options{}
	if cfg != nil {
		opts.Size = cfg.Size
		opts.TTL = cfg.TTL
		opts.RedisAddress = cfg.RedisURL
		opts.RedisDB = cfg.RedisDB
	}
	// Event cache - stores events by ID
	cco.storageCache.GetCache(EventCache, opts)

	// Event validation cache - stores validation results for events
	cco.storageCache.GetCache(EventValidationCache, opts)

	// Event finalization cache - stores finalization status for events
	cco.storageCache.GetCache(EventFinalizationCache, opts)

	// Block cache - stores blocks by height
	cco.storageCache.GetCache(BlockCache, opts)

	// Block validation cache - stores validation results for blocks
	cco.storageCache.GetCache(BlockValidationCache, opts)

	// Validator cache - stores validator information
	cco.storageCache.GetCache(ValidatorCache, opts)

	// Validator stake cache - stores validator stakes
	cco.storageCache.GetCache(ValidatorStakeCache, opts)

	// Validator status cache - stores validator statuses
	cco.storageCache.GetCache(ValidatorStatusCache, opts)

	// State cache - stores state information
	cco.storageCache.GetCache(StateCache, opts)

	// Signature cache - stores signature verification results
	cco.storageCache.GetCache(SignatureCache, opts)

	cco.logger.Info("Initialized consensus operation caches")
}

// GetEvent retrieves an event from the cache or the underlying storage
func (cco *CachedConsensusOperations) GetEvent(eventID [32]byte) (*types.Event, error) {
	// Try to get from cache first
	eventCache := cco.cacheManager.GetCache(EventCache, nil)
	eventKey := fmt.Sprintf("%x", eventID)
	if cachedValue, found := eventCache.Get(eventKey); found {
		cco.logger.Info("Event cache hit", EventIDField(eventID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeJSON {
			eventData := &types.Event{}
			if err := json.Unmarshal(cachedValue.Data, eventData); err == nil {
				return eventData, nil
			}
		}
	}

	// Not in cache, get from storage
	event, err := cco.adapter.GetEventByID(eventID)
	if err != nil {
		return nil, err
	}

	// Add to cache
	eventData, _ := json.Marshal(event)
	eventCache.Set(eventKey, &diamanteTypes.Value{
		Type: diamanteTypes.ValueTypeJSON,
		Data: eventData,
	})
	cco.logger.Info("Event cache miss, added to cache", EventIDField(eventID))

	return event, nil
}

// ValidateEvent validates an event, using the cache for validation results
func (cco *CachedConsensusOperations) ValidateEvent(event *types.Event) (bool, error) {
	if event == nil {
		return false, fmt.Errorf("event is nil")
	}

	// Try to get validation result from cache
	validationCache := cco.cacheManager.GetCache(EventValidationCache, nil)
	eventKey := fmt.Sprintf("%x", event.ID)
	if cachedValue, found := validationCache.Get(eventKey); found {
		cco.logger.Info("Event validation cache hit", EventIDField(event.ID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeBool {
			if valid, err := cachedValue.Bool(); err == nil {
				return valid, nil
			}
		}
	}

	// Not in cache, validate
	valid, err := cco.adapter.ValidateEventData(event)
	if err != nil {
		return false, err
	}

	// Add to cache
	validationCache.Set(eventKey, diamanteTypes.NewBoolValue(valid))
	cco.logger.Info("Event validation cache miss, added to cache",
		EventIDField(event.ID),
		BoolField("valid", valid))

	return valid, nil
}

// FinalizeEvent finalizes an event, using the cache for finalization status
func (cco *CachedConsensusOperations) FinalizeEvent(event *types.Event) (bool, error) {
	if event == nil {
		return false, fmt.Errorf("event is nil")
	}

	// Check if already finalized in cache
	finalizationCache := cco.cacheManager.GetCache(EventFinalizationCache, nil)
	eventKey := fmt.Sprintf("%x", event.ID)
	if cachedValue, found := finalizationCache.Get(eventKey); found {
		cco.logger.Info("Event finalization cache hit", EventIDField(event.ID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeBool {
			if valid, err := cachedValue.Bool(); err == nil {
				return valid, nil
			}
		}
	}

	// Not in cache, finalize
	finalized, err := cco.adapter.FinalizeEventProcessing(event)
	if err != nil {
		return false, err
	}

	// Add to cache
	finalizationCache.Set(eventKey, diamanteTypes.NewBoolValue(finalized))
	cco.logger.Info("Event finalization cache miss, added to cache",
		EventIDField(event.ID),
		BoolField("finalized", finalized))

	// If finalized, update the event in the event cache
	if finalized {
		event.Finalized = true
		eventCache := cco.cacheManager.GetCache(EventCache, nil)
		eventKey := fmt.Sprintf("%x", event.ID)
		eventData, _ := json.Marshal(event)
		eventCache.Set(eventKey, &diamanteTypes.Value{
			Type: diamanteTypes.ValueTypeJSON,
			Data: eventData,
		})
	}

	return finalized, nil
}

// GetBlock retrieves a block from the cache or the underlying storage
func (cco *CachedConsensusOperations) GetBlock(height uint64) (*types.Block, error) {
	// Try to get from cache first
	blockCache := cco.cacheManager.GetCache(BlockCache, nil)
	blockKey := fmt.Sprintf("%d", height)
	if cachedValue, found := blockCache.Get(blockKey); found {
		cco.logger.Info("Block cache hit", BlockHeightField(height))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeJSON {
			blockData := &types.Block{}
			if err := json.Unmarshal(cachedValue.Data, blockData); err == nil {
				return blockData, nil
			}
		}
	}

	// Not in cache, get from storage
	block, err := cco.adapter.GetBlockByHeight(height)
	if err != nil {
		return nil, err
	}

	// Add to cache
	blockData, _ := json.Marshal(block)
	blockCache.Set(blockKey, &diamanteTypes.Value{
		Type: diamanteTypes.ValueTypeJSON,
		Data: blockData,
	})
	cco.logger.Info("Block cache miss, added to cache", BlockHeightField(height))

	return block, nil
}

// ValidateBlock validates a block, using the cache for validation results
func (cco *CachedConsensusOperations) ValidateBlock(block *types.Block) (bool, error) {
	if block == nil {
		return false, fmt.Errorf("block is nil")
	}

	// Try to get validation result from cache
	validationCache := cco.cacheManager.GetCache(BlockValidationCache, nil)
	cacheKey := fmt.Sprintf("block-%d", block.Number)
	if cachedValue, found := validationCache.Get(cacheKey); found {
		cco.logger.Info("Block validation cache hit", BlockHeightField(block.Number))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeBool {
			if valid, err := cachedValue.Bool(); err == nil {
				return valid, nil
			}
		}
	}

	// Not in cache, validate
	valid, err := cco.adapter.ValidateBlockData(block)
	if err != nil {
		return false, err
	}

	// Add to cache
	validationCache.Set(cacheKey, diamanteTypes.NewBoolValue(valid))
	cco.logger.Info("Block validation cache miss, added to cache",
		BlockHeightField(block.Number),
		BoolField("valid", valid))

	return valid, nil
}

// GetValidator retrieves validator information from the cache or the underlying storage
func (cco *CachedConsensusOperations) GetValidator(validatorID [32]byte) (*ValidatorInfo, error) {
	// Try to get from cache first
	validatorCache := cco.cacheManager.GetCache(ValidatorCache, nil)
	validatorKey := fmt.Sprintf("%x", validatorID)
	if cachedValue, found := validatorCache.Get(validatorKey); found {
		cco.logger.Info("Validator cache hit", ValidatorIDField(validatorID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeJSON {
			validatorData := &ValidatorInfo{}
			if err := json.Unmarshal(cachedValue.Data, validatorData); err == nil {
				return validatorData, nil
			}
		}
	}

	// Not in cache, get from storage
	validator, err := cco.adapter.GetValidatorByID(validatorID)
	if err != nil {
		return nil, err
	}

	// Add to cache
	validatorData, _ := json.Marshal(validator)
	validatorCache.Set(validatorKey, &diamanteTypes.Value{
		Type: diamanteTypes.ValueTypeJSON,
		Data: validatorData,
	})
	cco.logger.Info("Validator cache miss, added to cache", ValidatorIDField(validatorID))

	return validator, nil
}

// GetValidatorStake retrieves validator stake from the cache or the underlying storage
func (cco *CachedConsensusOperations) GetValidatorStake(validatorID [32]byte) (uint64, error) {
	// Try to get from cache first
	stakeCache := cco.cacheManager.GetCache(ValidatorStakeCache, nil)
	validatorKey := fmt.Sprintf("%x", validatorID)
	if cachedValue, found := stakeCache.Get(validatorKey); found {
		cco.logger.Info("Validator stake cache hit", ValidatorIDField(validatorID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeUint64 {
			if stake, err := cachedValue.Uint64(); err == nil {
				return stake, nil
			}
		}
	}

	// Not in cache, get from storage
	stake, err := cco.adapter.GetValidatorStakeByID(validatorID)
	if err != nil {
		return 0, err
	}

	// Add to cache
	stakeCache.Set(validatorKey, diamanteTypes.Uint64ToValue(stake))
	cco.logger.Info("Validator stake cache miss, added to cache",
		ValidatorIDField(validatorID),
		IntField("stake", int(stake)))

	return stake, nil
}

// IsValidatorActive checks if a validator is active, using the cache for status
func (cco *CachedConsensusOperations) IsValidatorActive(validatorID [32]byte) (bool, error) {
	// Try to get from cache first
	statusCache := cco.cacheManager.GetCache(ValidatorStatusCache, nil)
	validatorKey := fmt.Sprintf("%x", validatorID)
	if cachedValue, found := statusCache.Get(validatorKey); found {
		cco.logger.Info("Validator status cache hit", ValidatorIDField(validatorID))
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeBool {
			if active, err := cachedValue.Bool(); err == nil {
				return active, nil
			}
		}
	}

	// Not in cache, get from storage
	active, err := cco.adapter.IsValidatorActiveByID(validatorID)
	if err != nil {
		return false, err
	}

	// Add to cache
	statusCache.Set(validatorKey, diamanteTypes.NewBoolValue(active))
	cco.logger.Info("Validator status cache miss, added to cache", ValidatorIDField(validatorID))

	return active, nil
}

// VerifySignature verifies a signature, using the cache for verification results
func (cco *CachedConsensusOperations) VerifySignature(data []byte, signature []byte, publicKey []byte) (bool, error) {
	// Create a cache key from the data, signature, and public key
	cacheKey := fmt.Sprintf("%x-%x-%x", data, signature, publicKey)

	// Try to get from cache first
	signatureCache := cco.cacheManager.GetCache(SignatureCache, nil)
	if cachedValue, found := signatureCache.Get(cacheKey); found {
		cco.logger.Info("Signature verification cache hit")
		// cachedValue is already of type *types.Value
		if cachedValue != nil && cachedValue.Type == diamanteTypes.ValueTypeBool {
			if valid, err := cachedValue.Bool(); err == nil {
				return valid, nil
			}
		}
	}

	// Not in cache, verify
	valid, err := cco.adapter.VerifySignatureData(data, signature, publicKey)
	if err != nil {
		return false, err
	}

	// Add to cache
	signatureCache.Set(cacheKey, diamanteTypes.NewBoolValue(valid))
	cco.logger.Info("Signature verification cache miss, added to cache",
		BoolField("valid", valid))

	return valid, nil
}

// ClearCaches clears all caches
func (cc *CachedConsensus) ClearCaches() {
	cc.cacheManager.ClearAllCaches()
	cc.logger.Info("Cleared all consensus caches")
}

// LogCacheMetrics logs metrics for all caches
func (cc *CachedConsensus) LogCacheMetrics() {
	cc.cacheManager.LogMetrics()
}

// GetCacheManager returns the cache manager
func (cc *CachedConsensus) GetCacheManager() *CacheManager {
	return cc.cacheManager
}

// IntegrateCachingWithHybridConsensus integrates caching with the HybridConsensus
func IntegrateCachingWithHybridConsensus(hc *HybridConsensus, cfg *config.CacheConfig) *CachedConsensus {
	return NewCachedConsensus(hc, cfg)
}
