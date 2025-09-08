package security

import (
	"diamante/common"
	"sync"
	"time"
)

// RateLimiter provides rate limiting functionality for requests
type RateLimiter struct {
	mu       sync.RWMutex
	buckets  map[string]*bucket
	capacity int
	refill   time.Duration
	cleanup  time.Duration
}

type bucket struct {
	tokens   int
	lastSeen time.Time
}

// LastAccess returns the last access time for cleanup
func (rl *RateLimiter) LastAccess() time.Time {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	latest := time.Time{}
	for _, b := range rl.buckets {
		if b.lastSeen.After(latest) {
			latest = b.lastSeen
		}
	}
	return latest
}

// NewRateLimiter creates a new rate limiter with specified capacity and refill rate
func NewRateLimiter(capacity int, refillInterval time.Duration) *RateLimiter {
	rl := &RateLimiter{
		buckets:  make(map[string]*bucket),
		capacity: capacity,
		refill:   refillInterval,
		cleanup:  time.Minute * 5,
	}

	// Start cleanup goroutine
	go rl.cleanupExpiredBuckets()

	return rl
}

// Allow checks if a request from the given identifier is allowed
func (rl *RateLimiter) Allow(identifier string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := common.ConsensusNow()
	b, exists := rl.buckets[identifier]

	if !exists {
		rl.buckets[identifier] = &bucket{
			tokens:   rl.capacity - 1,
			lastSeen: now,
		}
		return true
	}

	// Calculate tokens to add based on time passed
	timePassed := now.Sub(b.lastSeen)
	tokensToAdd := int(timePassed / rl.refill)

	if tokensToAdd > 0 {
		b.tokens = minInt(rl.capacity, b.tokens+tokensToAdd)
		b.lastSeen = now
	}

	if b.tokens > 0 {
		b.tokens--
		b.lastSeen = now
		return true
	}

	return false
}

// Reset resets the rate limiter for a specific identifier
func (rl *RateLimiter) Reset(identifier string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	delete(rl.buckets, identifier)
}

// cleanupExpiredBuckets removes old buckets to prevent memory leaks
func (rl *RateLimiter) cleanupExpiredBuckets() {
	ticker := time.NewTicker(rl.cleanup)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := common.ConsensusNow()
		for id, b := range rl.buckets {
			if now.Sub(b.lastSeen) > rl.cleanup {
				delete(rl.buckets, id)
			}
		}
		rl.mu.Unlock()
	}
}

// GetStats returns current rate limiter statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return map[string]interface{}{
		"active_buckets":  len(rl.buckets),
		"capacity":        rl.capacity,
		"refill_interval": rl.refill.String(),
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
