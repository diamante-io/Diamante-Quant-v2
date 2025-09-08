// consensus/state_manager.go
package consensus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/storage"
)

// PeerInfo tracks information about a peer's blockchain state
type PeerInfo struct {
	PeerID      string
	Height      uint64
	LastHash    string
	LastSeen    time.Time
	IsValidator bool
}

// BlockchainState represents the current state of the blockchain
type BlockchainState struct {
	ChainID        string
	CurrentHeight  uint64
	CurrentHash    string
	PreviousHash   string
	BlockTime      time.Time
	ValidatorCount int
	PeerCount      int
	Syncing        bool
	SyncProgress   float64
	TPS            float64
	PendingTxCount int
	NetworkHeight  uint64 // Highest known height from peers
	LastBlockTime  time.Time
}

// ValidatorStats holds statistics for a validator
type ValidatorStats struct {
	BlocksProduced uint64
	BlocksMissed   uint64
	LastActiveTime time.Time
}

// BlockchainStateManager provides a centralized, thread-safe source of truth for blockchain state
type BlockchainStateManager struct {
	mu sync.RWMutex

	// Core state - use atomic operations where possible
	chainID       string
	currentHeight atomic.Uint64
	currentHash   atomic.Value // string
	previousHash  atomic.Value // string

	// Complex state requiring mutex protection
	peers          map[string]*PeerInfo
	validatorCount int
	syncing        bool
	syncProgress   float64

	// Block production tracking
	blockTimes     []time.Time // Ring buffer of last N block times
	blockIndex     int
	blockTimesSize int

	// Transaction tracking for TPS
	txCounts     []int // Transaction counts for last N blocks
	txIndex      int
	txCountsSize int

	// Validator statistics tracking
	validatorStats map[[32]byte]*ValidatorStats

	// Dependencies
	storage storage.LedgerStore
	logger  *logrus.Logger

	// Transaction pool accessor for pending tx count
	txPoolAccessor func() int

	// Configuration
	syncThreshold    float64       // Percentage of peers we need to be within to consider synced
	maxBlockTimeDiff time.Duration // Max time difference to consider node synced
	tpsWindowSize    int           // Number of blocks to calculate TPS over
}

// NewBlockchainStateManager creates a new state manager
func NewBlockchainStateManager(chainID string, storage storage.LedgerStore, logger *logrus.Logger) *BlockchainStateManager {
	bsm := &BlockchainStateManager{
		chainID:          chainID,
		peers:            make(map[string]*PeerInfo),
		validatorStats:   make(map[[32]byte]*ValidatorStats),
		storage:          storage,
		logger:           logger,
		syncThreshold:    0.67, // Need to be within 67% of peers
		maxBlockTimeDiff: 30 * time.Second,
		tpsWindowSize:    10,
		blockTimesSize:   100,
		txCountsSize:     100,
		blockTimes:       make([]time.Time, 100),
		txCounts:         make([]int, 100),
	}

	// Initialize atomic values
	bsm.currentHash.Store("")
	bsm.previousHash.Store("")

	// Initialize from storage
	bsm.initializeFromStorage()

	// Start background tasks
	go bsm.peerCleanupRoutine()

	return bsm
}

// initializeFromStorage loads initial state from storage
func (bsm *BlockchainStateManager) initializeFromStorage() {
	if bsm.storage == nil {
		bsm.logger.Warn("Storage not available, starting with genesis state")
		return
	}

	// Get latest block from storage
	block, err := bsm.storage.GetLatestBlock()
	if err != nil {
		bsm.logger.WithError(err).Warn("Failed to get latest block from storage")
		return
	}

	if block != nil {
		bsm.currentHeight.Store(uint64(block.Number))
		bsm.currentHash.Store(block.Hash)
		if block.PreviousHash != "" {
			bsm.previousHash.Store(block.PreviousHash)
		}

		// Initialize block time tracking
		bsm.blockTimes[0] = time.Unix(block.Timestamp, 0)
		bsm.txCounts[0] = len(block.Transactions)

		bsm.logger.WithFields(logrus.Fields{
			"height":       block.Number,
			"hash":         block.Hash,
			"transactions": len(block.Transactions),
		}).Info("Initialized state from storage")
	}
}

// UpdateBlockHeight atomically updates the block height and related information
// This is called when a new block is produced or received
func (bsm *BlockchainStateManager) UpdateBlockHeight(height uint64, hash string, prevHash string, txCount int) error {
	// Ensure monotonic increase - this is critical for blockchain immutability
	currentHeight := bsm.currentHeight.Load()
	if height < currentHeight {
		bsm.logger.WithFields(logrus.Fields{
			"event":          "StateManagerHeightRejected",
			"current_height": currentHeight,
			"new_height":     height,
			"hash":           hash,
		}).Warn("Attempted to update with decreasing height")
		return nil // Not an error, just ignore
	}

	// For equal heights, only update if hash is different (reorg scenario)
	if height == currentHeight {
		currentHash := bsm.currentHash.Load().(string)
		if currentHash == hash {
			// Same height and hash, nothing to do
			return nil
		}
		// Different hash at same height - this would be a reorg, log it
		bsm.logger.WithFields(logrus.Fields{
			"event":    "StateManagerHeightReorg",
			"height":   height,
			"old_hash": currentHash,
			"new_hash": hash,
		}).Warn("Updating same height with different hash")
	}

	// Update atomic values
	bsm.currentHeight.Store(height)
	bsm.currentHash.Store(hash)
	bsm.previousHash.Store(prevHash)

	// Update tracking data
	bsm.mu.Lock()
	now := time.Now()
	bsm.blockIndex = (bsm.blockIndex + 1) % bsm.blockTimesSize
	bsm.blockTimes[bsm.blockIndex] = now

	bsm.txIndex = (bsm.txIndex + 1) % bsm.txCountsSize
	bsm.txCounts[bsm.txIndex] = txCount

	// Update sync status
	bsm.updateSyncStatus()
	bsm.mu.Unlock()

	bsm.logger.WithFields(logrus.Fields{
		"event":      "StateManagerHeightUpdated",
		"height":     height,
		"hash":       hash,
		"prev_hash":  prevHash,
		"tx_count":   txCount,
		"syncing":    bsm.syncing,
		"old_height": currentHeight,
	}).Info("Updated blockchain height")

	return nil
}

// UpdatePeerInfo updates information about a peer's state
func (bsm *BlockchainStateManager) UpdatePeerInfo(peerID string, height uint64, hash string, isValidator bool) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	peer, exists := bsm.peers[peerID]
	if !exists {
		peer = &PeerInfo{PeerID: peerID}
		bsm.peers[peerID] = peer
	}

	peer.Height = height
	peer.LastHash = hash
	peer.LastSeen = time.Now()
	peer.IsValidator = isValidator

	// Update network height
	bsm.updateNetworkHeight()

	// Check if we need to update sync status
	bsm.updateSyncStatus()
}

// RemovePeer removes a peer from tracking
func (bsm *BlockchainStateManager) RemovePeer(peerID string) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	delete(bsm.peers, peerID)
	bsm.updateNetworkHeight()
	bsm.updateSyncStatus()
}

// GetCurrentState returns the current blockchain state atomically
func (bsm *BlockchainStateManager) GetCurrentState() BlockchainState {
	// Read atomic values first
	height := bsm.currentHeight.Load()
	currentHash, _ := bsm.currentHash.Load().(string)
	previousHash, _ := bsm.previousHash.Load().(string)

	// Read complex state under lock
	bsm.mu.RLock()
	defer bsm.mu.RUnlock()

	state := BlockchainState{
		ChainID:        bsm.chainID,
		CurrentHeight:  height,
		CurrentHash:    currentHash,
		PreviousHash:   previousHash,
		ValidatorCount: bsm.validatorCount,
		PeerCount:      len(bsm.peers),
		Syncing:        bsm.syncing,
		SyncProgress:   bsm.syncProgress,
		NetworkHeight:  bsm.getNetworkHeightLocked(),
		TPS:            bsm.calculateTPSLocked(),
	}

	// Get last block time
	if bsm.blockIndex >= 0 && !bsm.blockTimes[bsm.blockIndex].IsZero() {
		state.LastBlockTime = bsm.blockTimes[bsm.blockIndex]
		state.BlockTime = state.LastBlockTime
	}

	// Get pending transaction count from transaction pool if available
	if bsm.txPoolAccessor != nil {
		state.PendingTxCount = bsm.txPoolAccessor()
	} else {
		state.PendingTxCount = 0
	}

	return state
}

// SetValidatorCount updates the validator count
func (bsm *BlockchainStateManager) SetValidatorCount(count int) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()
	bsm.validatorCount = count
}

// updateNetworkHeight calculates the highest known height from peers
func (bsm *BlockchainStateManager) updateNetworkHeight() {
	maxHeight := bsm.currentHeight.Load()

	for _, peer := range bsm.peers {
		if peer.Height > maxHeight {
			maxHeight = peer.Height
		}
	}

	// Network height is tracked in the state
}

// getNetworkHeightLocked returns the highest known height (must be called with lock held)
func (bsm *BlockchainStateManager) getNetworkHeightLocked() uint64 {
	maxHeight := bsm.currentHeight.Load()

	for _, peer := range bsm.peers {
		if peer.Height > maxHeight {
			maxHeight = peer.Height
		}
	}

	return maxHeight
}

// updateSyncStatus determines if the node is syncing (must be called with lock held)
func (bsm *BlockchainStateManager) updateSyncStatus() {
	currentHeight := bsm.currentHeight.Load()
	networkHeight := bsm.getNetworkHeightLocked()

	// Calculate sync progress
	if networkHeight > 0 {
		bsm.syncProgress = float64(currentHeight) / float64(networkHeight)
	} else {
		bsm.syncProgress = 1.0
	}

	// Determine if we're syncing
	if currentHeight == 0 && len(bsm.peers) > 0 {
		// Just started, definitely syncing
		bsm.syncing = true
		return
	}

	// Check if we're significantly behind the network
	// Use hysteresis to avoid flip-flopping between syncing states
	const syncStartGap = 5 // Start syncing if peer is 5+ blocks ahead
	const syncEndGap = 1   // Stop syncing when within 1 block

	// Start syncing only if peer tip >= currentHeight + 5
	if networkHeight >= currentHeight+syncStartGap {
		bsm.syncing = true
		return
	}

	// Stop syncing when currentHeight >= peerTip - 1
	if bsm.syncing && currentHeight >= networkHeight-syncEndGap {
		bsm.syncing = false
		return
	}

	// Check if we haven't produced a block recently
	if bsm.blockIndex >= 0 && !bsm.blockTimes[bsm.blockIndex].IsZero() {
		timeSinceLastBlock := time.Since(bsm.blockTimes[bsm.blockIndex])
		if timeSinceLastBlock > bsm.maxBlockTimeDiff && len(bsm.peers) > 0 {
			bsm.syncing = true
			return
		}
	}

	// Check if majority of peers are ahead
	if len(bsm.peers) >= 3 {
		aheadCount := 0
		for _, peer := range bsm.peers {
			if peer.Height > currentHeight {
				aheadCount++
			}
		}

		if float64(aheadCount)/float64(len(bsm.peers)) > (1.0 - bsm.syncThreshold) {
			bsm.syncing = true
			return
		}
	}

	// We're synced!
	bsm.syncing = false
}

// calculateTPSLocked calculates transactions per second (must be called with lock held)
func (bsm *BlockchainStateManager) calculateTPSLocked() float64 {
	// Need at least 2 blocks to calculate TPS
	validBlocks := 0
	totalTx := 0
	var oldestTime, newestTime time.Time

	// Find valid blocks in our circular buffer
	for i := 0; i < bsm.blockTimesSize && validBlocks < bsm.tpsWindowSize; i++ {
		idx := (bsm.blockIndex - i + bsm.blockTimesSize) % bsm.blockTimesSize
		if !bsm.blockTimes[idx].IsZero() {
			if oldestTime.IsZero() {
				oldestTime = bsm.blockTimes[idx]
				newestTime = bsm.blockTimes[idx]
			} else {
				if bsm.blockTimes[idx].Before(oldestTime) {
					oldestTime = bsm.blockTimes[idx]
				}
				if bsm.blockTimes[idx].After(newestTime) {
					newestTime = bsm.blockTimes[idx]
				}
			}

			totalTx += bsm.txCounts[idx]
			validBlocks++
		}
	}

	if validBlocks < 2 {
		return 0.0
	}

	duration := newestTime.Sub(oldestTime).Seconds()
	if duration <= 0 {
		return 0.0
	}

	return float64(totalTx) / duration
}

// peerCleanupRoutine removes stale peers
func (bsm *BlockchainStateManager) peerCleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bsm.mu.Lock()

		// Remove peers we haven't heard from in 2 minutes
		cutoff := time.Now().Add(-2 * time.Minute)
		for peerID, peer := range bsm.peers {
			if peer.LastSeen.Before(cutoff) {
				delete(bsm.peers, peerID)
				bsm.logger.WithField("peer_id", peerID).Debug("Removed stale peer")
			}
		}

		// Update sync status after cleanup
		bsm.updateSyncStatus()

		bsm.mu.Unlock()
	}
}

// GetPeerCount returns the current number of connected peers
func (bsm *BlockchainStateManager) GetPeerCount() int {
	bsm.mu.RLock()
	defer bsm.mu.RUnlock()
	return len(bsm.peers)
}

// SetPeerCount directly sets the peer count (used for network manager sync)
func (bsm *BlockchainStateManager) SetPeerCount(count int) {
	// Since we can't directly set peer count without peer info,
	// we'll use a special approach by updating a dummy peer entry
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	// Clear all peers first
	bsm.peers = make(map[string]*PeerInfo)

	// Add dummy peers to match the count
	for i := 0; i < count; i++ {
		peerID := fmt.Sprintf("network-peer-%d", i)
		bsm.peers[peerID] = &PeerInfo{
			PeerID:      peerID,
			Height:      bsm.currentHeight.Load(),
			LastHash:    "",
			LastSeen:    time.Now(),
			IsValidator: false,
		}
	}
}

// OnExternalBlockAccepted notifies the state manager that an external block was accepted
// This should clear the syncing state as we're making progress
func (bsm *BlockchainStateManager) OnExternalBlockAccepted(height uint64) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	// Clear syncing state when we accept an external block
	if bsm.syncing {
		bsm.syncing = false
		bsm.logger.WithFields(logrus.Fields{
			"height": height,
		}).Info("Cleared syncing state after accepting external block")
	}

	// Update sync status
	bsm.updateSyncStatus()
}

// GetSyncingStatus returns detailed sync information
func (bsm *BlockchainStateManager) GetSyncingStatus() (bool, float64, uint64, uint64) {
	bsm.mu.RLock()
	defer bsm.mu.RUnlock()

	currentHeight := bsm.currentHeight.Load()
	networkHeight := bsm.getNetworkHeightLocked()

	return bsm.syncing, bsm.syncProgress, currentHeight, networkHeight
}

// GetPeerHeights returns a map of peer heights for debugging
func (bsm *BlockchainStateManager) GetPeerHeights() map[string]uint64 {
	bsm.mu.RLock()
	defer bsm.mu.RUnlock()

	heights := make(map[string]uint64)
	for peerID, peer := range bsm.peers {
		heights[peerID] = peer.Height
	}

	return heights
}

// GetValidatorStats returns the statistics for a specific validator
func (bsm *BlockchainStateManager) GetValidatorStats(validatorID [32]byte) ValidatorStats {
	bsm.mu.RLock()
	defer bsm.mu.RUnlock()

	if stats, exists := bsm.validatorStats[validatorID]; exists {
		return *stats
	}

	// Return empty stats if validator not found
	return ValidatorStats{
		BlocksProduced: 0,
		BlocksMissed:   0,
		LastActiveTime: time.Time{},
	}
}

// RecordBlockProduced updates validator stats when they produce a block
func (bsm *BlockchainStateManager) RecordBlockProduced(validatorID [32]byte) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	if stats, exists := bsm.validatorStats[validatorID]; exists {
		stats.BlocksProduced++
		stats.LastActiveTime = time.Now()
	} else {
		bsm.validatorStats[validatorID] = &ValidatorStats{
			BlocksProduced: 1,
			BlocksMissed:   0,
			LastActiveTime: time.Now(),
		}
	}
}

// RecordBlockMissed updates validator stats when they miss a block
func (bsm *BlockchainStateManager) RecordBlockMissed(validatorID [32]byte) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	if stats, exists := bsm.validatorStats[validatorID]; exists {
		stats.BlocksMissed++
	} else {
		bsm.validatorStats[validatorID] = &ValidatorStats{
			BlocksProduced: 0,
			BlocksMissed:   1,
			LastActiveTime: time.Time{},
		}
	}
}

// UpdateBlockHeightWithValidator updates the blockchain height and records validator stats
func (bsm *BlockchainStateManager) UpdateBlockHeightWithValidator(height uint64, hash string, prevHash string, txCount int, validatorID [32]byte) error {
	// First update the block height
	if err := bsm.UpdateBlockHeight(height, hash, prevHash, txCount); err != nil {
		return err
	}

	// Record that this validator produced a block
	bsm.RecordBlockProduced(validatorID)

	return nil
}

// SetTransactionPoolAccessor sets the function to get pending transaction count
func (bsm *BlockchainStateManager) SetTransactionPoolAccessor(accessor func() int) {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()
	bsm.txPoolAccessor = accessor
}
