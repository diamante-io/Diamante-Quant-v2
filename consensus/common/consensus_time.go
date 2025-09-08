// consensus/common/consensus_time.go

package common

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// ConsensusTime provides deterministic time for consensus operations
type ConsensusTime interface {
	// Now returns the current consensus time based on block height
	Now() Time
	// ValidateTimestamp validates a timestamp against consensus time
	ValidateTimestamp(t time.Time) error
	// SetBlockTime updates the consensus time for a new block
	SetBlockTime(blockHeight uint64, blockTime time.Time) error
	// GetBlockTime returns the time for a specific block height
	GetBlockTime(blockHeight uint64) (time.Time, error)
}

// Time represents a deterministic consensus time
type Time struct {
	BlockHeight uint64
	Nanos       int64 // Nanoseconds since genesis
}

// ToTime converts consensus time to standard time.Time
func (t Time) ToTime() time.Time {
	return time.Unix(0, t.Nanos)
}

// String returns a string representation of consensus time
func (t Time) String() string {
	return fmt.Sprintf("Block:%d Time:%s", t.BlockHeight, t.ToTime().Format(time.RFC3339Nano))
}

// Before returns true if t is before other
func (t Time) Before(other Time) bool {
	return t.BlockHeight < other.BlockHeight ||
		(t.BlockHeight == other.BlockHeight && t.Nanos < other.Nanos)
}

// After returns true if t is after other
func (t Time) After(other Time) bool {
	return t.BlockHeight > other.BlockHeight ||
		(t.BlockHeight == other.BlockHeight && t.Nanos > other.Nanos)
}

// Equal returns true if t equals other
func (t Time) Equal(other Time) bool {
	return t.BlockHeight == other.BlockHeight && t.Nanos == other.Nanos
}

// Add adds a duration to the time
func (t Time) Add(d time.Duration) Time {
	return Time{
		BlockHeight: t.BlockHeight,
		Nanos:       t.Nanos + d.Nanoseconds(),
	}
}

// Sub returns the duration between two times
func (t Time) Sub(other Time) time.Duration {
	return time.Duration(t.Nanos - other.Nanos)
}

// DeterministicConsensusTime implements ConsensusTime with deterministic behavior
type DeterministicConsensusTime struct {
	mu            sync.RWMutex
	genesisTime   time.Time
	blockTimes    map[uint64]time.Time
	currentBlock  uint64
	blockInterval time.Duration
	maxTimeDrift  time.Duration
	minBlockTime  time.Duration
}

// NewDeterministicConsensusTime creates a new deterministic consensus time
func NewDeterministicConsensusTime(genesisTime time.Time, blockInterval time.Duration) *DeterministicConsensusTime {
	return &DeterministicConsensusTime{
		genesisTime:   genesisTime,
		blockTimes:    make(map[uint64]time.Time),
		blockInterval: blockInterval,
		maxTimeDrift:  5 * time.Second,        // Maximum allowed drift from expected time
		minBlockTime:  100 * time.Millisecond, // Minimum time between blocks
	}
}

// Now returns the current consensus time
func (dct *DeterministicConsensusTime) Now() Time {
	dct.mu.RLock()
	defer dct.mu.RUnlock()

	// Calculate expected time based on block height
	expectedTime := dct.genesisTime.Add(time.Duration(dct.currentBlock) * dct.blockInterval)

	// Get actual block time if available
	if blockTime, exists := dct.blockTimes[dct.currentBlock]; exists {
		return Time{
			BlockHeight: dct.currentBlock,
			Nanos:       blockTime.UnixNano(),
		}
	}

	// Return expected time if actual not available
	return Time{
		BlockHeight: dct.currentBlock,
		Nanos:       expectedTime.UnixNano(),
	}
}

// ValidateTimestamp validates a timestamp against consensus rules
func (dct *DeterministicConsensusTime) ValidateTimestamp(t time.Time) error {
	dct.mu.RLock()
	defer dct.mu.RUnlock()

	// Get current consensus time
	currentTime := dct.Now().ToTime()

	// Check if timestamp is too far in the future
	if t.After(currentTime.Add(dct.maxTimeDrift)) {
		return fmt.Errorf("timestamp %v is too far in the future (current: %v, max drift: %v)",
			t, currentTime, dct.maxTimeDrift)
	}

	// Check if timestamp is before previous block
	if dct.currentBlock > 0 {
		prevBlockTime, exists := dct.blockTimes[dct.currentBlock-1]
		if exists && t.Before(prevBlockTime) {
			return fmt.Errorf("timestamp %v is before previous block time %v",
				t, prevBlockTime)
		}
	}

	// Check minimum block time
	if dct.currentBlock > 0 {
		prevBlockTime, exists := dct.blockTimes[dct.currentBlock-1]
		if exists && t.Sub(prevBlockTime) < dct.minBlockTime {
			return fmt.Errorf("timestamp %v is too close to previous block (min interval: %v)",
				t, dct.minBlockTime)
		}
	}

	return nil
}

// SetBlockTime updates the consensus time for a new block
func (dct *DeterministicConsensusTime) SetBlockTime(blockHeight uint64, blockTime time.Time) error {
	dct.mu.Lock()
	defer dct.mu.Unlock()

	// Validate block height is sequential
	if blockHeight != dct.currentBlock+1 {
		return fmt.Errorf("invalid block height: expected %d, got %d",
			dct.currentBlock+1, blockHeight)
	}

	// Validate timestamp
	if err := dct.validateBlockTime(blockHeight, blockTime); err != nil {
		return fmt.Errorf("invalid block time: %w", err)
	}

	// Update state
	dct.blockTimes[blockHeight] = blockTime
	dct.currentBlock = blockHeight

	// Clean old block times to prevent unbounded growth
	dct.cleanOldBlockTimes()

	return nil
}

// GetBlockTime returns the time for a specific block height
func (dct *DeterministicConsensusTime) GetBlockTime(blockHeight uint64) (time.Time, error) {
	dct.mu.RLock()
	defer dct.mu.RUnlock()

	if blockTime, exists := dct.blockTimes[blockHeight]; exists {
		return blockTime, nil
	}

	// If block time not recorded, calculate expected time
	if blockHeight <= dct.currentBlock {
		expectedTime := dct.genesisTime.Add(time.Duration(blockHeight) * dct.blockInterval)
		return expectedTime, nil
	}

	return time.Time{}, fmt.Errorf("block height %d not yet reached (current: %d)",
		blockHeight, dct.currentBlock)
}

// validateBlockTime validates a block time against consensus rules
func (dct *DeterministicConsensusTime) validateBlockTime(blockHeight uint64, blockTime time.Time) error {
	// Check against genesis time
	if blockTime.Before(dct.genesisTime) {
		return fmt.Errorf("block time %v is before genesis time %v",
			blockTime, dct.genesisTime)
	}

	// Check against previous block
	if blockHeight > 0 {
		prevTime, exists := dct.blockTimes[blockHeight-1]
		if exists {
			if blockTime.Before(prevTime) {
				return fmt.Errorf("block time %v is before previous block time %v",
					blockTime, prevTime)
			}
			if blockTime.Sub(prevTime) < dct.minBlockTime {
				return fmt.Errorf("block time too close to previous block (interval: %v, min: %v)",
					blockTime.Sub(prevTime), dct.minBlockTime)
			}
		}
	}

	// Check against expected time
	expectedTime := dct.genesisTime.Add(time.Duration(blockHeight) * dct.blockInterval)
	drift := blockTime.Sub(expectedTime)
	if drift < 0 {
		drift = -drift
	}
	if drift > dct.maxTimeDrift {
		return fmt.Errorf("block time %v drifts too far from expected %v (drift: %v, max: %v)",
			blockTime, expectedTime, drift, dct.maxTimeDrift)
	}

	return nil
}

// cleanOldBlockTimes removes old block times to prevent unbounded growth
func (dct *DeterministicConsensusTime) cleanOldBlockTimes() {
	// Keep last 1000 blocks
	const keepBlocks = 1000

	if dct.currentBlock > keepBlocks {
		cutoff := dct.currentBlock - keepBlocks
		for height := range dct.blockTimes {
			if height < cutoff {
				delete(dct.blockTimes, height)
			}
		}
	}
}

// GetCurrentBlockHeight returns the current block height
func (dct *DeterministicConsensusTime) GetCurrentBlockHeight() uint64 {
	dct.mu.RLock()
	defer dct.mu.RUnlock()
	return dct.currentBlock
}

// TimeToBytes converts a Time to bytes for hashing
func TimeToBytes(t Time) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], t.BlockHeight)
	binary.BigEndian.PutUint64(b[8:16], uint64(t.Nanos))
	return b
}

// BytesToTime converts bytes back to Time
func BytesToTime(b []byte) (Time, error) {
	if len(b) < 16 {
		return Time{}, fmt.Errorf("invalid time bytes: need 16 bytes, got %d", len(b))
	}
	return Time{
		BlockHeight: binary.BigEndian.Uint64(b[0:8]),
		Nanos:       int64(binary.BigEndian.Uint64(b[8:16])),
	}, nil
}
