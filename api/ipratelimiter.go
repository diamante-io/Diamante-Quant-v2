package api

import (
	"net"
	"sync"
)

// ipRateLimiter limits requests per IP.
type ipRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rateLimiter
	rate     int
	burst    int
}

func newIPRateLimiter(rate, burst int) *ipRateLimiter {
	if rate <= 0 || burst <= 0 {
		return nil
	}
	return &ipRateLimiter{
		limiters: make(map[string]*rateLimiter),
		rate:     rate,
		burst:    burst,
	}
}

func (i *ipRateLimiter) getLimiter(ip string) *rateLimiter {
	i.mu.Lock()
	defer i.mu.Unlock()
	rl, ok := i.limiters[ip]
	if !ok {
		rl = newRateLimiter(i.rate, i.burst)
		i.limiters[ip] = rl
	}
	return rl
}

func (i *ipRateLimiter) allow(ip string) bool {
	if i == nil {
		return true
	}
	limiter := i.getLimiter(ip)
	return limiter.allow()
}

func clientIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
