// consensus/cache.go

package consensus

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"diamante/types"
)

// TimeProvider interface allows caches to use consensus time instead of system time
type TimeProvider interface {
	Now() time.Time
}

// SystemTimeProvider uses system time
type SystemTimeProvider struct{}

func (s *SystemTimeProvider) Now() time.Time {
	return ConsensusNow()
}

// ConsensusTimeProvider uses consensus time for deterministic behavior
type ConsensusTimeProvider struct {
	consensus Consensus
}

func (c *ConsensusTimeProvider) Now() time.Time {
	return c.consensus.GetConsensusTime()
}

// Consensus interface for getting consensus time
type Consensus interface {
	GetConsensusTime() time.Time
}

// Cache is the generic interface that all cache implementations must satisfy
type Cache[K comparable, V any] interface {
	// Get retrieves a value from the cache
	Get(key K) (V, bool)

	// Set adds a value to the cache
	Set(key K, value V)

	// Delete removes a value from the cache
	Delete(key K)

	// Clear removes all values from the cache
	Clear()

	// Len returns the number of items in the cache
	Len() int

	// GetMetrics returns cache metrics
	GetMetrics() CacheMetrics
}

// TypedCache is a specialized cache for consensus-specific typed values
type TypedCache interface {
	// Get retrieves a typed value from the cache
	Get(key string) (*types.Value, bool)

	// Set adds a typed value to the cache
	Set(key string, value *types.Value)

	// Delete removes a value from the cache
	Delete(key string)

	// Clear removes all values from the cache
	Clear()

	// Len returns the number of items in the cache
	Len() int

	// GetMetrics returns cache metrics
	GetMetrics() CacheMetrics
}

// CacheMetrics contains statistics about cache usage
type CacheMetrics struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Size      int
	Capacity  int
	HitRate   float64
}

// CacheConfig contains configuration options for caches
type CacheConfig struct {
	// Type of cache to create
	Type string

	// Maximum number of items in the cache
	Capacity int

	// TTL for cache entries (0 means no expiration)
	TTL time.Duration

	// Whether to track metrics
	TrackMetrics bool

	// Time provider for deterministic time behavior
	TimeProvider TimeProvider
}

// DefaultCacheConfig returns a default cache configuration
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		Type:         "lru",
		Capacity:     1000,
		TTL:          0,
		TrackMetrics: true,
		TimeProvider: &SystemTimeProvider{},
	}
}

// NewConsensusAwareCacheConfig returns a cache configuration that uses consensus time
func NewConsensusAwareCacheConfig(consensus Consensus) *CacheConfig {
	return &CacheConfig{
		Type:         "lru",
		Capacity:     1000,
		TTL:          0,
		TrackMetrics: true,
		TimeProvider: &ConsensusTimeProvider{consensus: consensus},
	}
}

// NewTypedCache creates a new type-safe cache with the given configuration
func NewTypedCache[K comparable, V any](config *CacheConfig) Cache[K, V] {
	if config == nil {
		config = DefaultCacheConfig()
	}

	if config.TimeProvider == nil {
		config.TimeProvider = &SystemTimeProvider{}
	}

	switch config.Type {
	case "lru":
		return NewTypedLRUCache[K, V](config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	case "simple":
		return NewTypedSimpleCache[K, V](config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	default:
		// Default to LRU cache
		return NewTypedLRUCache[K, V](config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	}
}

// NewConsensusCache creates a new cache for consensus-specific typed values
func NewConsensusCache(config *CacheConfig) TypedCache {
	if config == nil {
		config = DefaultCacheConfig()
	}

	if config.TimeProvider == nil {
		config.TimeProvider = &SystemTimeProvider{}
	}

	switch config.Type {
	case "lru":
		return NewConsensusLRUCache(config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	case "simple":
		return NewConsensusSimpleCache(config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	default:
		// Default to LRU cache
		return NewConsensusLRUCache(config.Capacity, config.TTL, config.TrackMetrics, config.TimeProvider)
	}
}

// TypedSimpleCache is a type-safe basic cache implementation
type TypedSimpleCache[K comparable, V any] struct {
	items        map[K]typedCacheItem[V]
	mu           sync.RWMutex
	capacity     int
	ttl          time.Duration
	trackMetrics bool
	metrics      CacheMetrics
	timeProvider TimeProvider
}

// typedCacheItem represents an item in the typed cache
type typedCacheItem[V any] struct {
	value      V
	expiration time.Time
}

// NewTypedSimpleCache creates a new type-safe SimpleCache
func NewTypedSimpleCache[K comparable, V any](capacity int, ttl time.Duration, trackMetrics bool, timeProvider TimeProvider) *TypedSimpleCache[K, V] {
	if timeProvider == nil {
		timeProvider = &SystemTimeProvider{}
	}
	return &TypedSimpleCache[K, V]{
		items:        make(map[K]typedCacheItem[V]),
		capacity:     capacity,
		ttl:          ttl,
		trackMetrics: trackMetrics,
		timeProvider: timeProvider,
		metrics: CacheMetrics{
			Capacity: capacity,
		},
	}
}

// Get retrieves a value from the cache
func (c *TypedSimpleCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	item, found := c.items[key]
	c.mu.RUnlock()

	var zero V
	if !found {
		if c.trackMetrics {
			c.mu.Lock()
			c.metrics.Misses++
			c.mu.Unlock()
		}
		return zero, false
	}

	// Check if the item has expired
	if !item.expiration.IsZero() && c.timeProvider.Now().After(item.expiration) {
		c.mu.Lock()
		delete(c.items, key)
		if c.trackMetrics {
			c.metrics.Evictions++
			c.metrics.Misses++
		}
		c.mu.Unlock()
		return zero, false
	}

	if c.trackMetrics {
		c.mu.Lock()
		c.metrics.Hits++
		c.mu.Unlock()
	}

	return item.value, true
}

// Set adds a value to the cache
func (c *TypedSimpleCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict items
	if len(c.items) >= c.capacity && c.capacity > 0 {
		// Simple random eviction
		for k := range c.items {
			delete(c.items, k)
			if c.trackMetrics {
				c.metrics.Evictions++
			}
			break
		}
	}

	var expiration time.Time
	if c.ttl > 0 {
		expiration = c.timeProvider.Now().Add(c.ttl)
	}

	c.items[key] = typedCacheItem[V]{
		value:      value,
		expiration: expiration,
	}

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Delete removes a value from the cache
func (c *TypedSimpleCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Clear removes all values from the cache
func (c *TypedSimpleCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]typedCacheItem[V])

	if c.trackMetrics {
		c.metrics.Size = 0
		c.metrics.Evictions += uint64(c.metrics.Size)
	}
}

// Len returns the number of items in the cache
func (c *TypedSimpleCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}

// GetMetrics returns cache metrics
func (c *TypedSimpleCache[K, V]) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := c.metrics

	// Calculate hit rate
	totalAccesses := metrics.Hits + metrics.Misses
	if totalAccesses > 0 {
		metrics.HitRate = float64(metrics.Hits) / float64(totalAccesses)
	}

	return metrics
}

// TypedLRUCache is a type-safe Least Recently Used cache implementation
type TypedLRUCache[K comparable, V any] struct {
	items        map[K]*list.Element
	evictionList *list.List
	mu           sync.RWMutex
	capacity     int
	ttl          time.Duration
	trackMetrics bool
	metrics      CacheMetrics
	timeProvider TimeProvider
}

// typedLruItem represents an item in the typed LRU cache
type typedLruItem[K comparable, V any] struct {
	key        K
	value      V
	expiration time.Time
}

// NewTypedLRUCache creates a new type-safe LRUCache
func NewTypedLRUCache[K comparable, V any](capacity int, ttl time.Duration, trackMetrics bool, timeProvider TimeProvider) *TypedLRUCache[K, V] {
	if timeProvider == nil {
		timeProvider = &SystemTimeProvider{}
	}
	return &TypedLRUCache[K, V]{
		items:        make(map[K]*list.Element),
		evictionList: list.New(),
		capacity:     capacity,
		ttl:          ttl,
		trackMetrics: trackMetrics,
		timeProvider: timeProvider,
		metrics: CacheMetrics{
			Capacity: capacity,
		},
	}
}

// Get retrieves a value from the cache
func (c *TypedLRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	element, found := c.items[key]
	c.mu.RUnlock()

	var zero V
	if !found {
		if c.trackMetrics {
			c.mu.Lock()
			c.metrics.Misses++
			c.mu.Unlock()
		}
		return zero, false
	}

	item := element.Value.(*typedLruItem[K, V])

	// Check if the item has expired
	if !item.expiration.IsZero() && c.timeProvider.Now().After(item.expiration) {
		c.mu.Lock()
		c.evictionList.Remove(element)
		delete(c.items, key)
		if c.trackMetrics {
			c.metrics.Evictions++
			c.metrics.Misses++
			c.metrics.Size = len(c.items)
		}
		c.mu.Unlock()
		return zero, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.evictionList.MoveToFront(element)
	c.mu.Unlock()

	if c.trackMetrics {
		c.mu.Lock()
		c.metrics.Hits++
		c.mu.Unlock()
	}

	return item.value, true
}

// Set adds a value to the cache
func (c *TypedLRUCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the key already exists
	if element, found := c.items[key]; found {
		c.evictionList.MoveToFront(element)
		item := element.Value.(*typedLruItem[K, V])
		item.value = value

		// Update expiration if TTL is set
		if c.ttl > 0 {
			item.expiration = c.timeProvider.Now().Add(c.ttl)
		}
		return
	}

	// Check if we need to evict items
	if c.evictionList.Len() >= c.capacity && c.capacity > 0 {
		// Evict the least recently used item
		oldest := c.evictionList.Back()
		if oldest != nil {
			c.evictionList.Remove(oldest)
			item := oldest.Value.(*typedLruItem[K, V])
			delete(c.items, item.key)
			if c.trackMetrics {
				c.metrics.Evictions++
			}
		}
	}

	// Create expiration time if TTL is set
	var expiration time.Time
	if c.ttl > 0 {
		expiration = c.timeProvider.Now().Add(c.ttl)
	}

	// Add the new item
	item := &typedLruItem[K, V]{
		key:        key,
		value:      value,
		expiration: expiration,
	}
	element := c.evictionList.PushFront(item)
	c.items[key] = element

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Delete removes a value from the cache
func (c *TypedLRUCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, found := c.items[key]; found {
		c.evictionList.Remove(element)
		delete(c.items, key)

		if c.trackMetrics {
			c.metrics.Size = len(c.items)
		}
	}
}

// Clear removes all values from the cache
func (c *TypedLRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*list.Element)
	c.evictionList.Init()

	if c.trackMetrics {
		c.metrics.Evictions += uint64(c.metrics.Size)
		c.metrics.Size = 0
	}
}

// Len returns the number of items in the cache
func (c *TypedLRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}

// GetMetrics returns cache metrics
func (c *TypedLRUCache[K, V]) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := c.metrics

	// Calculate hit rate
	totalAccesses := metrics.Hits + metrics.Misses
	if totalAccesses > 0 {
		metrics.HitRate = float64(metrics.Hits) / float64(totalAccesses)
	}

	return metrics
}

// ConsensusSimpleCache is a cache implementation for consensus-specific typed values
type ConsensusSimpleCache struct {
	items        map[string]consensusCacheItem
	mu           sync.RWMutex
	capacity     int
	ttl          time.Duration
	trackMetrics bool
	metrics      CacheMetrics
	timeProvider TimeProvider
}

// consensusCacheItem represents an item in the consensus cache
type consensusCacheItem struct {
	value      *types.Value
	expiration time.Time
}

// NewConsensusSimpleCache creates a new ConsensusSimpleCache
func NewConsensusSimpleCache(capacity int, ttl time.Duration, trackMetrics bool, timeProvider TimeProvider) *ConsensusSimpleCache {
	if timeProvider == nil {
		timeProvider = &SystemTimeProvider{}
	}
	return &ConsensusSimpleCache{
		items:        make(map[string]consensusCacheItem),
		capacity:     capacity,
		ttl:          ttl,
		trackMetrics: trackMetrics,
		timeProvider: timeProvider,
		metrics: CacheMetrics{
			Capacity: capacity,
		},
	}
}

// Get retrieves a value from the cache
func (c *ConsensusSimpleCache) Get(key string) (*types.Value, bool) {
	c.mu.RLock()
	item, found := c.items[key]
	c.mu.RUnlock()

	if !found {
		if c.trackMetrics {
			c.mu.Lock()
			c.metrics.Misses++
			c.mu.Unlock()
		}
		return nil, false
	}

	// Check if the item has expired
	if !item.expiration.IsZero() && c.timeProvider.Now().After(item.expiration) {
		c.mu.Lock()
		delete(c.items, key)
		if c.trackMetrics {
			c.metrics.Evictions++
			c.metrics.Misses++
		}
		c.mu.Unlock()
		return nil, false
	}

	if c.trackMetrics {
		c.mu.Lock()
		c.metrics.Hits++
		c.mu.Unlock()
	}

	return item.value, true
}

// Set adds a value to the cache
func (c *ConsensusSimpleCache) Set(key string, value *types.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict items
	if len(c.items) >= c.capacity && c.capacity > 0 {
		// Simple random eviction
		for k := range c.items {
			delete(c.items, k)
			if c.trackMetrics {
				c.metrics.Evictions++
			}
			break
		}
	}

	var expiration time.Time
	if c.ttl > 0 {
		expiration = c.timeProvider.Now().Add(c.ttl)
	}

	c.items[key] = consensusCacheItem{
		value:      value,
		expiration: expiration,
	}

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Delete removes a value from the cache
func (c *ConsensusSimpleCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Clear removes all values from the cache
func (c *ConsensusSimpleCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]consensusCacheItem)

	if c.trackMetrics {
		c.metrics.Size = 0
		c.metrics.Evictions += uint64(c.metrics.Size)
	}
}

// Len returns the number of items in the cache
func (c *ConsensusSimpleCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}

// GetMetrics returns cache metrics
func (c *ConsensusSimpleCache) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := c.metrics

	// Calculate hit rate
	totalAccesses := metrics.Hits + metrics.Misses
	if totalAccesses > 0 {
		metrics.HitRate = float64(metrics.Hits) / float64(totalAccesses)
	}

	return metrics
}

// ConsensusLRUCache is an LRU cache implementation for consensus-specific typed values
type ConsensusLRUCache struct {
	items        map[string]*list.Element
	evictionList *list.List
	mu           sync.RWMutex
	capacity     int
	ttl          time.Duration
	trackMetrics bool
	metrics      CacheMetrics
	timeProvider TimeProvider
}

// consensusLruItem represents an item in the consensus LRU cache
type consensusLruItem struct {
	key        string
	value      *types.Value
	expiration time.Time
}

// NewConsensusLRUCache creates a new ConsensusLRUCache
func NewConsensusLRUCache(capacity int, ttl time.Duration, trackMetrics bool, timeProvider TimeProvider) *ConsensusLRUCache {
	if timeProvider == nil {
		timeProvider = &SystemTimeProvider{}
	}
	return &ConsensusLRUCache{
		items:        make(map[string]*list.Element),
		evictionList: list.New(),
		capacity:     capacity,
		ttl:          ttl,
		trackMetrics: trackMetrics,
		timeProvider: timeProvider,
		metrics: CacheMetrics{
			Capacity: capacity,
		},
	}
}

// Get retrieves a value from the cache
func (c *ConsensusLRUCache) Get(key string) (*types.Value, bool) {
	c.mu.RLock()
	element, found := c.items[key]
	c.mu.RUnlock()

	if !found {
		if c.trackMetrics {
			c.mu.Lock()
			c.metrics.Misses++
			c.mu.Unlock()
		}
		return nil, false
	}

	item := element.Value.(*consensusLruItem)

	// Check if the item has expired
	if !item.expiration.IsZero() && c.timeProvider.Now().After(item.expiration) {
		c.mu.Lock()
		c.evictionList.Remove(element)
		delete(c.items, key)
		if c.trackMetrics {
			c.metrics.Evictions++
			c.metrics.Misses++
			c.metrics.Size = len(c.items)
		}
		c.mu.Unlock()
		return nil, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.evictionList.MoveToFront(element)
	c.mu.Unlock()

	if c.trackMetrics {
		c.mu.Lock()
		c.metrics.Hits++
		c.mu.Unlock()
	}

	return item.value, true
}

// Set adds a value to the cache
func (c *ConsensusLRUCache) Set(key string, value *types.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the key already exists
	if element, found := c.items[key]; found {
		c.evictionList.MoveToFront(element)
		item := element.Value.(*consensusLruItem)
		item.value = value

		// Update expiration if TTL is set
		if c.ttl > 0 {
			item.expiration = c.timeProvider.Now().Add(c.ttl)
		}
		return
	}

	// Check if we need to evict items
	if c.evictionList.Len() >= c.capacity && c.capacity > 0 {
		// Evict the least recently used item
		oldest := c.evictionList.Back()
		if oldest != nil {
			c.evictionList.Remove(oldest)
			item := oldest.Value.(*consensusLruItem)
			delete(c.items, item.key)
			if c.trackMetrics {
				c.metrics.Evictions++
			}
		}
	}

	// Create expiration time if TTL is set
	var expiration time.Time
	if c.ttl > 0 {
		expiration = c.timeProvider.Now().Add(c.ttl)
	}

	// Add the new item
	item := &consensusLruItem{
		key:        key,
		value:      value,
		expiration: expiration,
	}
	element := c.evictionList.PushFront(item)
	c.items[key] = element

	if c.trackMetrics {
		c.metrics.Size = len(c.items)
	}
}

// Delete removes a value from the cache
func (c *ConsensusLRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, found := c.items[key]; found {
		c.evictionList.Remove(element)
		delete(c.items, key)

		if c.trackMetrics {
			c.metrics.Size = len(c.items)
		}
	}
}

// Clear removes all values from the cache
func (c *ConsensusLRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictionList.Init()

	if c.trackMetrics {
		c.metrics.Evictions += uint64(c.metrics.Size)
		c.metrics.Size = 0
	}
}

// Len returns the number of items in the cache
func (c *ConsensusLRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}

// GetMetrics returns cache metrics
func (c *ConsensusLRUCache) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := c.metrics

	// Calculate hit rate
	totalAccesses := metrics.Hits + metrics.Misses
	if totalAccesses > 0 {
		metrics.HitRate = float64(metrics.Hits) / float64(totalAccesses)
	}

	return metrics
}

// CacheManager manages multiple typed caches for different types of data
type CacheManager struct {
	caches map[string]TypedCache
	mu     sync.RWMutex
	logger *hybridConsensusLogger
}

// NewCacheManager creates a new CacheManager
func NewCacheManager(logger *hybridConsensusLogger) *CacheManager {
	return &CacheManager{
		caches: make(map[string]TypedCache),
		logger: logger,
	}
}

// GetCache returns a cache for the given name, creating it if it doesn't exist
func (cm *CacheManager) GetCache(name string, config *CacheConfig) TypedCache {
	cm.mu.RLock()
	cache, exists := cm.caches[name]
	cm.mu.RUnlock()

	if exists {
		return cache
	}

	// Create a new cache
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check again in case another goroutine created the cache
	if cache, exists = cm.caches[name]; exists {
		return cache
	}

	cache = NewConsensusCache(config)
	cm.caches[name] = cache

	cm.logger.Info("Created new cache",
		LogKeyValue{Key: "name", Value: name},
		LogKeyValue{Key: "type", Value: config.Type},
		LogKeyValue{Key: "capacity", Value: fmt.Sprintf("%d", config.Capacity)})

	return cache
}

// GetAllCaches returns all caches
func (cm *CacheManager) GetAllCaches() map[string]TypedCache {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to avoid race conditions
	caches := make(map[string]TypedCache)
	for name, cache := range cm.caches {
		caches[name] = cache
	}

	return caches
}

// ClearAllCaches clears all caches
func (cm *CacheManager) ClearAllCaches() {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for name, cache := range cm.caches {
		cache.Clear()
		cm.logger.Info("Cleared cache", LogKeyValue{Key: "name", Value: name})
	}
}

// GetAllMetrics returns metrics for all caches
func (cm *CacheManager) GetAllMetrics() map[string]CacheMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics := make(map[string]CacheMetrics)
	for name, cache := range cm.caches {
		metrics[name] = cache.GetMetrics()
	}

	return metrics
}

// LogMetrics logs metrics for all caches
func (cm *CacheManager) LogMetrics() {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for name, cache := range cm.caches {
		metrics := cache.GetMetrics()
		cm.logger.Info("Cache metrics",
			LogKeyValue{Key: "name", Value: name},
			LogKeyValue{Key: "size", Value: fmt.Sprintf("%d", metrics.Size)},
			LogKeyValue{Key: "capacity", Value: fmt.Sprintf("%d", metrics.Capacity)},
			LogKeyValue{Key: "hits", Value: fmt.Sprintf("%d", metrics.Hits)},
			LogKeyValue{Key: "misses", Value: fmt.Sprintf("%d", metrics.Misses)},
			LogKeyValue{Key: "hitRate", Value: fmt.Sprintf("%.4f", metrics.HitRate)},
			LogKeyValue{Key: "evictions", Value: fmt.Sprintf("%d", metrics.Evictions)})
	}
}
