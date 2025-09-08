package api

import (
	"sync"
	"time"

	"diamante/consensus"
)

// rateLimiter implements a simple token bucket.
type rateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	rate     float64
	capacity float64
	last     time.Time
}

// newRateLimiter creates a rateLimiter that refills `rate` tokens per second
// with a maximum capacity of `burst` tokens.
func newRateLimiter(rate, burst int) *rateLimiter {
	if rate <= 0 || burst <= 0 {
		return nil
	}
	return &rateLimiter{
		tokens:   float64(burst),
		rate:     float64(rate),
		capacity: float64(burst),
		last:     consensus.ConsensusNow(),
	}
}

// allow returns true if a request can proceed.
func (rl *rateLimiter) allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := consensus.ConsensusNow()
	elapsed := now.Sub(rl.last).Seconds()
	rl.last = now
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}
	if rl.tokens >= 1 {
		rl.tokens -= 1
		return true
	}
	return false
}
