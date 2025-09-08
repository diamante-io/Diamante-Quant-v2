package cache

import (
	"diamante/types"
	"sync"
	"time"
)

// Cache defines the basic operations for a cache implementation.
type Cache interface {
	Get(key string) (*types.CacheValue, bool)
	Set(key string, value *types.CacheValue)
	Delete(key string)
	Clear()
	Len() int
	Stats() *types.CacheStats
}

// Options configure cache creation.
type Options struct {
	Size         int
	TTL          time.Duration
	RedisAddress string
	RedisDB      int
	Config       *types.CacheConfig
}

// Manager manages named caches.
type Manager struct {
	mu     sync.RWMutex
	caches map[string]Cache
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{caches: make(map[string]Cache)}
}

// GetCache returns or creates a cache with the given name.
func (m *Manager) GetCache(name string, opts *Options) Cache {
	m.mu.RLock()
	c, ok := m.caches[name]
	m.mu.RUnlock()
	if ok {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok = m.caches[name]; ok {
		return c
	}
	if opts == nil {
		opts = &Options{Size: 1000, TTL: time.Minute}
	}
	l1 := NewMemoryCache(opts.Size, opts.TTL)
	var cacheInstance Cache = l1
	if opts.RedisAddress != "" {
		config := &RedisCacheConfig{
			Addr: opts.RedisAddress,
			DB:   opts.RedisDB,
			TTL:  opts.TTL,
		}
		l2, err := NewRedisCache(config)
		if err != nil {
			// Fall back to memory-only cache if Redis fails
			cacheInstance = l1
		} else {
			cacheInstance = NewMultiLevelCache(l1, l2)
		}
	}
	m.caches[name] = cacheInstance
	return cacheInstance
}

// MultiLevelCache combines a memory cache with an optional Redis cache.
type MultiLevelCache struct {
	l1 *MemoryCache
	l2 *RedisCache
}

// NewMultiLevelCache creates a multi-level cache.
func NewMultiLevelCache(l1 *MemoryCache, l2 *RedisCache) *MultiLevelCache {
	return &MultiLevelCache{l1: l1, l2: l2}
}

func (c *MultiLevelCache) Get(key string) (*types.CacheValue, bool) {
	if val, ok := c.l1.Get(key); ok {
		return val, true
	}
	if c.l2 != nil {
		if val, ok := c.l2.Get(key); ok {
			c.l1.Set(key, val)
			return val, true
		}
	}
	return nil, false
}

func (c *MultiLevelCache) Set(key string, value *types.CacheValue) {
	c.l1.Set(key, value)
	if c.l2 != nil {
		c.l2.Set(key, value)
	}
}

func (c *MultiLevelCache) Delete(key string) {
	c.l1.Delete(key)
	if c.l2 != nil {
		c.l2.Delete(key)
	}
}

func (c *MultiLevelCache) Clear() {
	c.l1.Clear()
	if c.l2 != nil {
		c.l2.Clear()
	}
}

func (c *MultiLevelCache) Len() int {
	return c.l1.Len()
}

func (c *MultiLevelCache) Stats() *types.CacheStats {
	return c.l1.Stats()
}
