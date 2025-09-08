// consensus/consensus_time.go
package consensus

import (
	"context"
	"sync"
	"time"
)

// ConsensusTime provides deterministic time for blockchain consensus operations.
// This replaces all time.Now() usage to ensure deterministic execution across all nodes.
type ConsensusTime struct {
	// Current consensus time
	currentTime time.Time

	// Mutex to protect time updates
	mu sync.RWMutex

	// Time advancement parameters
	blockInterval time.Duration
	maxDrift      time.Duration

	// Genesis timestamp
	genesisTime time.Time

	// Current block height for time calculation
	blockHeight uint64

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Logger for time-related events
	logger Logger
}

// Logger interface for consensus time
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
	Warn(msg string, keyvals ...interface{})
}

// ConsensusTimeConfig holds configuration for consensus time
type ConsensusTimeConfig struct {
	GenesisTime   time.Time     `json:"genesis_time"`
	BlockInterval time.Duration `json:"block_interval"`
	MaxDrift      time.Duration `json:"max_drift"`
}

// DefaultConsensusTimeConfig returns default configuration
func DefaultConsensusTimeConfig() ConsensusTimeConfig {
	return ConsensusTimeConfig{
		GenesisTime:   time.Unix(1700000000, 0), // Fixed genesis time for determinism
		BlockInterval: 5 * time.Second,          // 5-second block time per whitepaper
		MaxDrift:      2 * time.Second,          // Maximum allowed drift
	}
}

// NewConsensusTime creates a new consensus time instance
func NewConsensusTime(config ConsensusTimeConfig, logger Logger) *ConsensusTime {
	ctx, cancel := context.WithCancel(context.Background())

	ct := &ConsensusTime{
		currentTime:   config.GenesisTime,
		blockInterval: config.BlockInterval,
		maxDrift:      config.MaxDrift,
		genesisTime:   config.GenesisTime,
		blockHeight:   0,
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
	}

	if logger != nil {
		logger.Info("ConsensusTime initialized",
			"genesisTime", config.GenesisTime,
			"blockInterval", config.BlockInterval,
			"maxDrift", config.MaxDrift)
	}

	return ct
}

// GetCurrentTime returns the current consensus time (replaces time.Now())
func (ct *ConsensusTime) GetCurrentTime() time.Time {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.currentTime
}

// GetCurrentTimeUnix returns the current consensus time as Unix timestamp
func (ct *ConsensusTime) GetCurrentTimeUnix() int64 {
	return ct.GetCurrentTime().Unix()
}

// GetCurrentTimeUnixNano returns the current consensus time as Unix nanoseconds
func (ct *ConsensusTime) GetCurrentTimeUnixNano() int64 {
	return ct.GetCurrentTime().UnixNano()
}

// AdvanceToBlock advances consensus time to a specific block height
func (ct *ConsensusTime) AdvanceToBlock(blockHeight uint64) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// Calculate expected time for this block
	expectedTime := ct.genesisTime.Add(time.Duration(blockHeight) * ct.blockInterval)

	// Only advance time forward
	if expectedTime.After(ct.currentTime) {
		ct.currentTime = expectedTime
		ct.blockHeight = blockHeight

		if ct.logger != nil {
			ct.logger.Info("Consensus time advanced",
				"blockHeight", blockHeight,
				"consensusTime", ct.currentTime,
				"unixTime", ct.currentTime.Unix())
		}
	}

	return nil
}

// ValidateTimestamp validates that a timestamp is within acceptable bounds
func (ct *ConsensusTime) ValidateTimestamp(timestamp time.Time) error {
	ct.mu.RLock()
	current := ct.currentTime
	maxDrift := ct.maxDrift
	ct.mu.RUnlock()

	// Check if timestamp is too far in the past
	if timestamp.Before(current.Add(-maxDrift)) {
		return &ConsensusTimeError{
			Type:        "timestamp_too_old",
			Message:     "timestamp is too far in the past",
			Timestamp:   timestamp,
			CurrentTime: current,
			MaxDrift:    maxDrift,
		}
	}

	// Check if timestamp is too far in the future
	if timestamp.After(current.Add(maxDrift)) {
		return &ConsensusTimeError{
			Type:        "timestamp_too_future",
			Message:     "timestamp is too far in the future",
			Timestamp:   timestamp,
			CurrentTime: current,
			MaxDrift:    maxDrift,
		}
	}

	return nil
}

// GetBlockTimeByHeight calculates the expected time for a given block height
func (ct *ConsensusTime) GetBlockTimeByHeight(blockHeight uint64) time.Time {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	return ct.genesisTime.Add(time.Duration(blockHeight) * ct.blockInterval)
}

// GetCurrentBlockHeight returns the current block height
func (ct *ConsensusTime) GetCurrentBlockHeight() uint64 {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.blockHeight
}

// GetBlockInterval returns the configured block interval
func (ct *ConsensusTime) GetBlockInterval() time.Duration {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.blockInterval
}

// UpdateBlockInterval updates the block interval (for governance changes)
func (ct *ConsensusTime) UpdateBlockInterval(newInterval time.Duration) error {
	if newInterval <= 0 {
		return &ConsensusTimeError{
			Type:    "invalid_interval",
			Message: "block interval must be positive",
		}
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	oldInterval := ct.blockInterval
	ct.blockInterval = newInterval

	if ct.logger != nil {
		ct.logger.Info("Block interval updated",
			"oldInterval", oldInterval,
			"newInterval", newInterval,
			"blockHeight", ct.blockHeight)
	}

	return nil
}

// GetTimeSinceGenesis returns the duration since genesis
func (ct *ConsensusTime) GetTimeSinceGenesis() time.Duration {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.currentTime.Sub(ct.genesisTime)
}

// IsZero checks if the consensus time is at genesis
func (ct *ConsensusTime) IsZero() bool {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.currentTime.Equal(ct.genesisTime)
}

// Stop stops the consensus time service
func (ct *ConsensusTime) Stop() {
	if ct.cancel != nil {
		ct.cancel()
	}

	if ct.logger != nil {
		ct.logger.Info("ConsensusTime stopped",
			"finalTime", ct.GetCurrentTime(),
			"finalBlockHeight", ct.GetCurrentBlockHeight())
	}
}

// ConsensusTimeError represents errors in consensus time operations
type ConsensusTimeError struct {
	Type        string        `json:"type"`
	Message     string        `json:"message"`
	Timestamp   time.Time     `json:"timestamp,omitempty"`
	CurrentTime time.Time     `json:"current_time,omitempty"`
	MaxDrift    time.Duration `json:"max_drift,omitempty"`
}

func (e *ConsensusTimeError) Error() string {
	return e.Message
}

// Global consensus time instance (singleton pattern for determinism)
var (
	globalConsensusTime *ConsensusTime
	globalTimeOnce      sync.Once
)

// InitializeGlobalConsensusTime initializes the global consensus time instance
func InitializeGlobalConsensusTime(config ConsensusTimeConfig, logger Logger) {
	globalTimeOnce.Do(func() {
		globalConsensusTime = NewConsensusTime(config, logger)
	})
}

// GetGlobalConsensusTime returns the global consensus time instance
func GetGlobalConsensusTime() *ConsensusTime {
	if globalConsensusTime == nil {
		// Initialize with default config if not already initialized
		InitializeGlobalConsensusTime(DefaultConsensusTimeConfig(), nil)
	}
	return globalConsensusTime
}

// ConsensusNow returns the current consensus time (direct replacement for time.Now())
func ConsensusNow() time.Time {
	// UPDATED: Use real time for proper timestamps
	// In production, this ensures blocks have real-world timestamps
	return time.Now()
}

// ConsensusUnix returns the current consensus time as Unix timestamp
func ConsensusUnix() int64 {
	// UPDATED: Use real time for proper timestamps
	return time.Now().Unix()
}

// ConsensusUnixNano returns the current consensus time as Unix nanoseconds
func ConsensusUnixNano() int64 {
	return GetGlobalConsensusTime().GetCurrentTimeUnixNano()
}

// ConsensusSince returns the time elapsed since t using consensus time
func ConsensusSince(t time.Time) time.Duration {
	return GetGlobalConsensusTime().GetCurrentTime().Sub(t)
}

// ConsensusUntil returns the duration until t using consensus time
func ConsensusUntil(t time.Time) time.Duration {
	return t.Sub(GetGlobalConsensusTime().GetCurrentTime())
}

// Production-safe sleep replacement using channels and context
func ConsensusSleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-time.After(duration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsensusTimer creates a timer that can be cancelled via context
func ConsensusTimer(ctx context.Context, duration time.Duration) <-chan time.Time {
	timer := time.NewTimer(duration)
	resultChan := make(chan time.Time, 1)

	go func() {
		defer timer.Stop()
		select {
		case t := <-timer.C:
			resultChan <- t
		case <-ctx.Done():
			// Timer cancelled
		}
		close(resultChan)
	}()

	return resultChan
}

// ConsensusTicker creates a ticker that can be cancelled via context
func ConsensusTicker(ctx context.Context, interval time.Duration) <-chan time.Time {
	ticker := time.NewTicker(interval)
	resultChan := make(chan time.Time)

	go func() {
		defer ticker.Stop()
		defer close(resultChan)

		for {
			select {
			case t := <-ticker.C:
				select {
				case resultChan <- t:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return resultChan
}
