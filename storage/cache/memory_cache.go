package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/types"
)

// Local consensus time functions to avoid import cycle
func consensusNow() time.Time {
	// Use consensus time for deterministic cache operations
	return common.ConsensusNow().UTC()
}

// MemoryCache is an in-memory LRU cache with optional TTL.
type MemoryCache struct {
	mu           sync.RWMutex
	items        map[string]*list.Element
	evictionList *list.List
	capacity     int
	ttl          time.Duration
	// Stats tracking
	hits        uint64
	misses      uint64
	evictions   uint64
	currentSize uint64
}

type memoryItem struct {
	key        string
	value      *types.CacheValue
	expiration time.Time
}

// NewMemoryCache creates a new MemoryCache.
func NewMemoryCache(capacity int, ttl time.Duration) *MemoryCache {
	return &MemoryCache{
		items:        make(map[string]*list.Element),
		evictionList: list.New(),
		capacity:     capacity,
		ttl:          ttl,
	}
}

// Get retrieves a value from the cache.
func (c *MemoryCache) Get(key string) (*types.CacheValue, bool) {
	c.mu.RLock()
	element, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}
	item := element.Value.(*memoryItem)
	if c.ttl > 0 && !item.expiration.IsZero() && consensusNow().After(item.expiration) {
		c.mu.Lock()
		delete(c.items, key)
		c.evictionList.Remove(element)
		atomic.AddUint64(&c.currentSize, ^uint64(item.value.Size-1))
		c.mu.Unlock()
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}
	c.mu.Lock()
	c.evictionList.MoveToFront(element)
	item.value.AccessedAt = consensusNow()
	item.value.AccessCount++
	c.mu.Unlock()
	atomic.AddUint64(&c.hits, 1)
	return item.value, true
}

// Set stores a value in the cache.
func (c *MemoryCache) Set(key string, value *types.CacheValue) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update timestamps
	now := consensusNow()
	value.CreatedAt = now
	value.AccessedAt = now

	if element, ok := c.items[key]; ok {
		item := element.Value.(*memoryItem)
		oldSize := item.value.Size
		item.value = value
		if c.ttl > 0 {
			item.expiration = now.Add(c.ttl)
		}
		c.evictionList.MoveToFront(element)
		// Update size
		atomic.AddUint64(&c.currentSize, value.Size-oldSize)
		return
	}
	if c.capacity > 0 && c.evictionList.Len() >= c.capacity {
		oldest := c.evictionList.Back()
		if oldest != nil {
			c.evictionList.Remove(oldest)
			oldItem := oldest.Value.(*memoryItem)
			delete(c.items, oldItem.key)
			atomic.AddUint64(&c.currentSize, ^uint64(oldItem.value.Size-1))
			atomic.AddUint64(&c.evictions, 1)
		}
	}
	exp := time.Time{}
	if c.ttl > 0 {
		exp = now.Add(c.ttl)
	}
	element := c.evictionList.PushFront(&memoryItem{key: key, value: value, expiration: exp})
	c.items[key] = element
	atomic.AddUint64(&c.currentSize, value.Size)
}

// Delete removes an item from the cache.
func (c *MemoryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		item := element.Value.(*memoryItem)
		delete(c.items, key)
		c.evictionList.Remove(element)
		atomic.AddUint64(&c.currentSize, ^uint64(item.value.Size-1))
	}
}

// Clear removes all items from the cache.
func (c *MemoryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.evictionList.Init()
	atomic.StoreUint64(&c.currentSize, 0)
}

// Len returns the number of items in the cache.
func (c *MemoryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictionList.Len()
}

// Stats returns cache statistics.
func (c *MemoryCache) Stats() *types.CacheStats {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	evictions := atomic.LoadUint64(&c.evictions)
	currentSize := atomic.LoadUint64(&c.currentSize)

	hitRate := float64(0)
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	c.mu.RLock()
	itemCount := uint64(c.evictionList.Len())
	c.mu.RUnlock()

	avgItemSize := uint64(0)
	if itemCount > 0 {
		avgItemSize = currentSize / itemCount
	}

	return &types.CacheStats{
		Hits:        hits,
		Misses:      misses,
		Evictions:   evictions,
		Size:        currentSize,
		MaxSize:     uint64(c.capacity),
		ItemCount:   itemCount,
		HitRate:     hitRate,
		AvgItemSize: avgItemSize,
	}
}
