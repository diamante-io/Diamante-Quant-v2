package consensus

import (
	"container/list"
	"sync"
)

// LRUCache is a simple thread-safe LRU cache implementation
type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*list.Element
	lru      *list.List
}

// cacheEntry holds the key and value for a cache entry
type cacheEntry struct {
	key   string
	value interface{}
}

// NewLRUCache creates a new LRU cache with the given capacity
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Get retrieves a value from the cache
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.lru.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}
	return nil, false
}

// Put adds or updates a value in the cache
func (c *LRUCache) Put(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry
	if elem, ok := c.items[key]; ok {
		c.lru.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, value: value}
	elem := c.lru.PushFront(entry)
	c.items[key] = elem

	// Evict oldest if at capacity
	if c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		if oldest != nil {
			c.lru.Remove(oldest)
			delete(c.items, oldest.Value.(*cacheEntry).key)
		}
	}
}

// Remove deletes a value from the cache
func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.lru.Remove(elem)
		delete(c.items, key)
	}
}

// Clear removes all entries from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru = list.New()
}

// Len returns the number of items in the cache
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lru.Len()
}
