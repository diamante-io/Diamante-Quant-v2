package common

import (
	"sync"
	"time"
)

// globalTimeSource is the singleton instance
var (
	globalTimeSource *ConsensusTimeSource
	once             sync.Once
)

// ConsensusTimeSource provides deterministic time for consensus operations
type ConsensusTimeSource struct {
	mu       sync.RWMutex
	baseTime time.Time
}

// GetGlobalConsensusTime returns the global consensus time source
func GetGlobalConsensusTime() *ConsensusTimeSource {
	once.Do(func() {
		globalTimeSource = &ConsensusTimeSource{
			baseTime: time.Unix(1700000000, 0), // Fixed reference time for determinism
		}
	})
	return globalTimeSource
}

// GetCurrentTime returns the current consensus time
func (cts *ConsensusTimeSource) GetCurrentTime() time.Time {
	cts.mu.RLock()
	defer cts.mu.RUnlock()

	// Always use real time for production
	return time.Now()
}

// ConsensusNow returns the current consensus time (direct replacement for time.Now())
func ConsensusNow() time.Time {
	return GetGlobalConsensusTime().GetCurrentTime()
}

// ConsensusUnix returns the current consensus time as Unix timestamp
func ConsensusUnix() int64 {
	return ConsensusNow().Unix()
}

// ConsensusUnixNano returns the current consensus time as Unix nanoseconds
func ConsensusUnixNano() int64 {
	return ConsensusNow().UnixNano()
}

// NOTE: Mock time functionality has been removed for production.
// For testing, use dependency injection or test-specific time sources.

// ConsensusSince returns the elapsed time since t
func ConsensusSince(t time.Time) time.Duration {
	return ConsensusNow().Sub(t)
}
