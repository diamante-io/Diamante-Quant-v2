package consensus

import (
	"diamante/types"
	"fmt"
	"sync"
	"time"
)

// TypedCacheKey represents a typed cache key for testing
type TypedCacheKey struct {
	Namespace string
	Key       string
}

// String returns the string representation of the key
func (k TypedCacheKey) String() string {
	return fmt.Sprintf("%s:%s", k.Namespace, k.Key)
}

// SystemTimeProvider provides system time for testing
type SystemTimeProvider struct{}

// Now returns the current time
func (s *SystemTimeProvider) Now() time.Time {
	return time.Now()
}

// CacheStats holds cache statistics
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	HitRate   float64
	ItemCount uint64
}

// TypedSimpleCache is a simple typed cache for testing
type TypedSimpleCache struct {
	mu           sync.RWMutex
	data         map[string]*types.CacheEntry
	capacity     int
	timeProvider *SystemTimeProvider
	hits         uint64
	misses       uint64
}

// NewTypedSimpleCache creates a new typed simple cache
func NewTypedSimpleCache(capacity int, timeProvider *SystemTimeProvider) *TypedSimpleCache {
	return &TypedSimpleCache{
		data:         make(map[string]*types.CacheEntry),
		capacity:     capacity,
		timeProvider: timeProvider,
	}
}

// Set adds an entry to the cache
func (c *TypedSimpleCache) Set(key TypedCacheKey, entry *types.CacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple eviction if at capacity
	if len(c.data) >= c.capacity {
		// Remove oldest entry (simple implementation)
		for k := range c.data {
			delete(c.data, k)
			break
		}
	}

	c.data[key.String()] = entry
}

// Get retrieves an entry from the cache
func (c *TypedSimpleCache) Get(key TypedCacheKey) (*types.CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.data[key.String()]
	if exists {
		c.hits++
	} else {
		c.misses++
	}
	return entry, exists
}

// Delete removes an entry from the cache
func (c *TypedSimpleCache) Delete(key TypedCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key.String())
}

// Clear removes all entries from the cache
func (c *TypedSimpleCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]*types.CacheEntry)
}

// Len returns the number of entries in the cache
func (c *TypedSimpleCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.data)
}

// Size returns the number of entries in the cache (alias for Len)
func (c *TypedSimpleCache) Size() int {
	return c.Len()
}

// Stats returns cache statistics
func (c *TypedSimpleCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return CacheStats{
		Hits:      c.hits,
		Misses:    c.misses,
		HitRate:   hitRate,
		ItemCount: uint64(len(c.data)),
	}
}

// NewTypedLRUCache creates a new LRU cache with the specified capacity
func NewTypedLRUCache(capacity int, timeProvider *SystemTimeProvider) *TypedSimpleCache {
	// For simplicity, we'll use the same implementation as TypedSimpleCache
	// In a real LRU implementation, we'd track access order
	return NewTypedSimpleCache(capacity, timeProvider)
}

// NewCacheAdapter creates a new cache adapter
func NewCacheAdapter(cache *TypedSimpleCache) *CacheAdapter {
	return &CacheAdapter{
		cache: cache,
	}
}

// CacheAdapter wraps a TypedSimpleCache
type CacheAdapter struct {
	cache *TypedSimpleCache
}

// Get retrieves a value from the cache
func (a *CacheAdapter) Get(key string) (interface{}, bool) {
	// Convert string key to TypedCacheKey
	typedKey := TypedCacheKey{Key: key}
	entry, exists := a.cache.Get(typedKey)
	if !exists {
		return nil, false
	}
	return entry.Value, true
}

// Set stores a value in the cache
func (a *CacheAdapter) Set(key string, value interface{}) {
	typedKey := TypedCacheKey{Key: key}
	entry := &types.CacheEntry{
		Key:   key,
		Value: value.([]byte),
	}
	a.cache.Set(typedKey, entry)
}

// Delete removes a value from the cache
func (a *CacheAdapter) Delete(key string) {
	typedKey := TypedCacheKey{Key: key}
	a.cache.Delete(typedKey)
}

// Clear removes all values from the cache
func (a *CacheAdapter) Clear() {
	a.cache.Clear()
}
