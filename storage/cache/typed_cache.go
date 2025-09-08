// Package cache provides typed cache implementations
package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/types"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// TypedCache is the interface for type-safe cache operations
type TypedCache interface {
	// Get retrieves a typed value from cache
	Get(key string) (*types.CacheValue, error)

	// Set stores a typed value in cache
	Set(key string, value *types.CacheValue) error

	// Delete removes a value from cache
	Delete(key string) error

	// Exists checks if a key exists
	Exists(key string) bool

	// Clear removes all items from cache
	Clear() error

	// GetStats returns cache statistics
	GetStats() *types.CacheStats
}

// TypedMemoryCache implements an in-memory cache with typed values
type TypedMemoryCache struct {
	items    map[string]*list.Element
	eviction *list.List
	config   *types.CacheConfig
	mu       sync.RWMutex
	stats    *types.CacheStats
	logger   *logrus.Logger
}

// memoryCacheEntry holds the cache value and metadata
type memoryCacheEntry struct {
	key   string
	value *types.CacheValue
}

// NewTypedMemoryCache creates a new typed memory cache
func NewTypedMemoryCache(config *types.CacheConfig, logger *logrus.Logger) *TypedMemoryCache {
	if logger == nil {
		logger = logrus.New()
	}

	return &TypedMemoryCache{
		items:    make(map[string]*list.Element),
		eviction: list.New(),
		config:   config,
		stats: &types.CacheStats{
			Hits:      0,
			Misses:    0,
			Evictions: 0,
		},
		logger: logger,
	}
}

// Get retrieves a typed value from the cache
func (c *TypedMemoryCache) Get(key string) (*types.CacheValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return nil, fmt.Errorf("key not found: %s", key)
	}

	entry := element.Value.(*memoryCacheEntry)

	// Check if expired
	if entry.value.TTL > 0 {
		expiryTime := entry.value.CreatedAt.Add(time.Duration(entry.value.TTL) * time.Second)
		if common.ConsensusNow().After(expiryTime) {
			// Remove expired item
			c.eviction.Remove(element)
			delete(c.items, key)
			c.stats.Misses++
			return nil, fmt.Errorf("key expired: %s", key)
		}
	}

	// Move to front (LRU)
	c.eviction.MoveToFront(element)
	entry.value.AccessedAt = common.ConsensusNow()
	entry.value.AccessCount++

	c.stats.Hits++
	return entry.value, nil
}

// Set stores a typed value in the cache
func (c *TypedMemoryCache) Set(key string, value *types.CacheValue) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set metadata
	value.Key = key
	value.CreatedAt = common.ConsensusNow()
	value.AccessedAt = value.CreatedAt
	value.AccessCount = 0

	// Check if key already exists
	if element, exists := c.items[key]; exists {
		// Update existing entry
		c.eviction.MoveToFront(element)
		entry := element.Value.(*memoryCacheEntry)
		entry.value = value
		return nil
	}

	// Check capacity
	if c.config.MaxSize > 0 && uint64(c.eviction.Len()) >= c.config.MaxSize {
		// Evict oldest
		c.evictOldest()
	}

	// Add new entry
	entry := &memoryCacheEntry{
		key:   key,
		value: value,
	}
	element := c.eviction.PushFront(entry)
	c.items[key] = element

	// Update stats
	c.updateStats()

	return nil
}

// Delete removes a value from the cache
func (c *TypedMemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.items[key]
	if !exists {
		return fmt.Errorf("key not found: %s", key)
	}

	c.eviction.Remove(element)
	delete(c.items, key)

	c.updateStats()
	return nil
}

// Exists checks if a key exists in the cache
func (c *TypedMemoryCache) Exists(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.items[key]
	return exists
}

// Clear removes all items from the cache
func (c *TypedMemoryCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.eviction = list.New()
	c.stats = &types.CacheStats{
		Hits:      0,
		Misses:    0,
		Evictions: 0,
	}

	return nil
}

// GetStats returns cache statistics
func (c *TypedMemoryCache) GetStats() *types.CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := *c.stats
	stats.ItemCount = uint64(c.eviction.Len())
	stats.MaxSize = c.config.MaxSize

	// Calculate hit rate
	total := stats.Hits + stats.Misses
	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	// Calculate sizes
	totalSize := uint64(0)
	for element := c.eviction.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*memoryCacheEntry)
		totalSize += entry.value.Size
	}
	stats.Size = totalSize

	if stats.ItemCount > 0 {
		stats.AvgItemSize = totalSize / stats.ItemCount
	}

	return &stats
}

// evictOldest removes the oldest item from the cache
func (c *TypedMemoryCache) evictOldest() {
	element := c.eviction.Back()
	if element != nil {
		entry := element.Value.(*memoryCacheEntry)
		c.eviction.Remove(element)
		delete(c.items, entry.key)
		c.stats.Evictions++

		c.logger.WithField("key", entry.key).Debug("Evicted cache entry")
	}
}

// updateStats updates cache statistics
func (c *TypedMemoryCache) updateStats() {
	// Size calculation
	totalSize := uint64(0)
	for element := c.eviction.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*memoryCacheEntry)
		totalSize += entry.value.Size
	}
	c.stats.Size = totalSize
	c.stats.ItemCount = uint64(c.eviction.Len())
}

// TypedRedisCache implements a Redis-backed cache with typed values
type TypedRedisCache struct {
	client *redis.Client
	config *types.CacheConfig
	logger *logrus.Logger
	stats  *types.CacheStats
	mu     sync.RWMutex
}

// NewTypedRedisCache creates a new typed Redis cache
func NewTypedRedisCache(redisURL string, config *types.CacheConfig, logger *logrus.Logger) (*TypedRedisCache, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Parse Redis options
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opt)

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &TypedRedisCache{
		client: client,
		config: config,
		logger: logger,
		stats: &types.CacheStats{
			Hits:   0,
			Misses: 0,
		},
	}, nil
}

// Get retrieves a typed value from Redis
func (c *TypedRedisCache) Get(key string) (*types.CacheValue, error) {
	ctx := context.Background()

	// Get raw data
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		c.mu.Lock()
		c.stats.Misses++
		c.mu.Unlock()
		return nil, fmt.Errorf("key not found: %s", key)
	} else if err != nil {
		return nil, fmt.Errorf("failed to get from Redis: %w", err)
	}

	// Deserialize
	var value types.CacheValue
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cache value: %w", err)
	}

	// Update stats
	c.mu.Lock()
	c.stats.Hits++
	value.AccessedAt = common.ConsensusNow()
	value.AccessCount++
	c.mu.Unlock()

	return &value, nil
}

// Set stores a typed value in Redis
func (c *TypedRedisCache) Set(key string, value *types.CacheValue) error {
	ctx := context.Background()

	// Set metadata
	value.Key = key
	value.CreatedAt = common.ConsensusNow()
	value.AccessedAt = value.CreatedAt

	// Serialize
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal cache value: %w", err)
	}

	// Determine TTL
	ttl := c.config.DefaultTTL
	if value.TTL > 0 {
		ttl = time.Duration(value.TTL) * time.Second
	}

	// Set in Redis
	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set in Redis: %w", err)
	}

	return nil
}

// Delete removes a value from Redis
func (c *TypedRedisCache) Delete(key string) error {
	ctx := context.Background()

	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete from Redis: %w", err)
	}

	return nil
}

// Exists checks if a key exists in Redis
func (c *TypedRedisCache) Exists(key string) bool {
	ctx := context.Background()

	n, err := c.client.Exists(ctx, key).Result()
	return err == nil && n > 0
}

// Clear removes all items from the cache (use with caution)
func (c *TypedRedisCache) Clear() error {
	ctx := context.Background()

	// Use FLUSHDB with caution - it removes all keys from the current database
	if err := c.client.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("failed to clear Redis: %w", err)
	}

	// Reset stats
	c.mu.Lock()
	c.stats = &types.CacheStats{
		Hits:   0,
		Misses: 0,
	}
	c.mu.Unlock()

	return nil
}

// GetStats returns cache statistics
func (c *TypedRedisCache) GetStats() *types.CacheStats {
	c.mu.RLock()
	stats := *c.stats
	c.mu.RUnlock()

	// Get Redis info
	ctx := context.Background()
	info := c.client.Info(ctx, "stats", "memory")

	// Parse info to extract additional stats
	// This is simplified - real implementation would parse more thoroughly
	if info.Err() == nil {
		// Extract some basic stats from Redis INFO
		// In production, parse the INFO output properly
		stats.Size = 0      // Would extract used_memory
		stats.ItemCount = 0 // Would extract number of keys
	}

	// Calculate hit rate
	total := stats.Hits + stats.Misses
	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	return &stats
}

// TypedCacheManager manages multiple cache levels
type TypedCacheManager struct {
	levels []TypedCache
	config *types.CacheConfig
	logger *logrus.Logger
}

// NewTypedCacheManager creates a new multi-level cache manager
func NewTypedCacheManager(config *types.CacheConfig, logger *logrus.Logger) *TypedCacheManager {
	return &TypedCacheManager{
		levels: make([]TypedCache, 0),
		config: config,
		logger: logger,
	}
}

// AddLevel adds a cache level
func (m *TypedCacheManager) AddLevel(cache TypedCache) {
	m.levels = append(m.levels, cache)
}

// Get retrieves from the first cache level that has the key
func (m *TypedCacheManager) Get(key string) (*types.CacheValue, error) {
	for i, cache := range m.levels {
		value, err := cache.Get(key)
		if err == nil {
			// Populate higher levels
			for j := 0; j < i; j++ {
				m.levels[j].Set(key, value)
			}
			return value, nil
		}
	}

	return nil, fmt.Errorf("key not found in any cache level: %s", key)
}

// Set stores in all cache levels
func (m *TypedCacheManager) Set(key string, value *types.CacheValue) error {
	var lastErr error

	for _, cache := range m.levels {
		if err := cache.Set(key, value); err != nil {
			lastErr = err
			m.logger.WithError(err).Error("Failed to set in cache level")
		}
	}

	return lastErr
}

// Delete removes from all cache levels
func (m *TypedCacheManager) Delete(key string) error {
	var lastErr error

	for _, cache := range m.levels {
		if err := cache.Delete(key); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// GetStats returns aggregated statistics from all levels
func (m *TypedCacheManager) GetStats() map[string]*types.CacheStats {
	stats := make(map[string]*types.CacheStats)

	for i, cache := range m.levels {
		levelName := fmt.Sprintf("level_%d", i)
		stats[levelName] = cache.GetStats()
	}

	return stats
}
