package p2p

import (
	"container/list"
	"sync"
)

// TypedLRUItem represents an item that can be stored in the typed LRU cache
type TypedLRUItem interface {
	// GetKey returns the unique key for this item
	GetKey() string
}

// TypedLRU implements a type-safe LRU cache
type TypedLRU[K comparable, V TypedLRUItem] struct {
	capacity int
	items    map[K]*list.Element
	order    *list.List
	onEvict  func(key K, value V)
	mu       sync.RWMutex
}

type typedLRUItem[K comparable, V TypedLRUItem] struct {
	key   K
	value V
}

// NewTypedLRU creates a new type-safe LRU cache
func NewTypedLRU[K comparable, V TypedLRUItem](capacity int, onEvict func(key K, value V)) *TypedLRU[K, V] {
	return &TypedLRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		order:    list.New(),
		onEvict:  onEvict,
	}
}

// Add adds or updates an item in the cache
func (lru *TypedLRU[K, V]) Add(key K, value V) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		// Update existing item
		lru.order.MoveToFront(elem)
		elem.Value.(*typedLRUItem[K, V]).value = value
		return
	}

	// Add new item
	item := &typedLRUItem[K, V]{key: key, value: value}
	elem := lru.order.PushFront(item)
	lru.items[key] = elem

	// Check capacity and evict if necessary
	if lru.order.Len() > lru.capacity {
		lru.evictOldest()
	}
}

// Get retrieves an item from the cache
func (lru *TypedLRU[K, V]) Get(key K) (V, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		lru.order.MoveToFront(elem)
		return elem.Value.(*typedLRUItem[K, V]).value, true
	}

	var zero V
	return zero, false
}

// Peek retrieves an item without updating its position
func (lru *TypedLRU[K, V]) Peek(key K) (V, bool) {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	if elem, exists := lru.items[key]; exists {
		return elem.Value.(*typedLRUItem[K, V]).value, true
	}

	var zero V
	return zero, false
}

// Remove removes an item from the cache
func (lru *TypedLRU[K, V]) Remove(key K) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		lru.removeElement(elem)
	}
}

// Keys returns all keys in the cache (most recent first)
func (lru *TypedLRU[K, V]) Keys() []K {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	keys := make([]K, 0, len(lru.items))
	for elem := lru.order.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*typedLRUItem[K, V]).key)
	}

	return keys
}

// Values returns all values in the cache (most recent first)
func (lru *TypedLRU[K, V]) Values() []V {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	values := make([]V, 0, len(lru.items))
	for elem := lru.order.Front(); elem != nil; elem = elem.Next() {
		values = append(values, elem.Value.(*typedLRUItem[K, V]).value)
	}

	return values
}

// Len returns the current size of the cache
func (lru *TypedLRU[K, V]) Len() int {
	lru.mu.RLock()
	defer lru.mu.RUnlock()
	return lru.order.Len()
}

// Cap returns the capacity of the cache
func (lru *TypedLRU[K, V]) Cap() int {
	return lru.capacity
}

// Purge clears all items from the cache
func (lru *TypedLRU[K, V]) Purge() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if lru.onEvict != nil {
		for key, elem := range lru.items {
			lru.onEvict(key, elem.Value.(*typedLRUItem[K, V]).value)
		}
	}

	lru.items = make(map[K]*list.Element)
	lru.order.Init()
}

// Resize changes the capacity of the cache
func (lru *TypedLRU[K, V]) Resize(newCapacity int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.capacity = newCapacity

	// Evict items if over new capacity
	for lru.order.Len() > lru.capacity {
		lru.evictOldest()
	}
}

// Contains checks if a key exists in the cache without updating its position
func (lru *TypedLRU[K, V]) Contains(key K) bool {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	_, exists := lru.items[key]
	return exists
}

// evictOldest removes the oldest item from the cache
func (lru *TypedLRU[K, V]) evictOldest() {
	elem := lru.order.Back()
	if elem != nil {
		lru.removeElement(elem)
	}
}

// removeElement removes an element from the cache
func (lru *TypedLRU[K, V]) removeElement(elem *list.Element) {
	item := elem.Value.(*typedLRUItem[K, V])
	delete(lru.items, item.key)
	lru.order.Remove(elem)

	if lru.onEvict != nil {
		lru.onEvict(item.key, item.value)
	}
}

// ForEach iterates over all items in the cache (most recent first)
func (lru *TypedLRU[K, V]) ForEach(fn func(key K, value V) bool) {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	for elem := lru.order.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*typedLRUItem[K, V])
		if !fn(item.key, item.value) {
			break
		}
	}
}

// GetOldest returns the oldest item in the cache
func (lru *TypedLRU[K, V]) GetOldest() (K, V, bool) {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	elem := lru.order.Back()
	if elem != nil {
		item := elem.Value.(*typedLRUItem[K, V])
		return item.key, item.value, true
	}

	var zeroK K
	var zeroV V
	return zeroK, zeroV, false
}

// GetNewest returns the newest item in the cache
func (lru *TypedLRU[K, V]) GetNewest() (K, V, bool) {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	elem := lru.order.Front()
	if elem != nil {
		item := elem.Value.(*typedLRUItem[K, V])
		return item.key, item.value, true
	}

	var zeroK K
	var zeroV V
	return zeroK, zeroV, false
}

// Specific typed LRU implementations for common use cases

// MessageIDLRU is a typed LRU for message IDs
type MessageIDLRU = TypedLRU[[32]byte, *CachedMessage]

// NewMessageIDLRU creates a new LRU cache for message IDs
func NewMessageIDLRU(capacity int, onEvict func(key [32]byte, value *CachedMessage)) *MessageIDLRU {
	return NewTypedLRU[[32]byte, *CachedMessage](capacity, onEvict)
}

// StringLRU is a typed LRU for string keys and values
type StringLRU = TypedLRU[string, *StringLRUItem]

// StringLRUItem implements TypedLRUItem for string values
type StringLRUItem struct {
	Value string
}

// GetKey implements TypedLRUItem
func (s *StringLRUItem) GetKey() string {
	return s.Value
}

// NewStringLRU creates a new LRU cache for strings
func NewStringLRU(capacity int, onEvict func(key string, value *StringLRUItem)) *StringLRU {
	return NewTypedLRU[string, *StringLRUItem](capacity, onEvict)
}

// PeerLRU is a typed LRU for peer information
type PeerLRU = TypedLRU[string, *CachedPeerInfo]

// CachedPeerInfo represents cached peer information
type CachedPeerInfo struct {
	ID        string
	Address   string
	LastSeen  int64
	Score     float64
	Connected bool
}

// GetKey implements TypedLRUItem
func (p *CachedPeerInfo) GetKey() string {
	return p.ID
}

// NewPeerLRU creates a new LRU cache for peer information
func NewPeerLRU(capacity int, onEvict func(key string, value *CachedPeerInfo)) *PeerLRU {
	return NewTypedLRU[string, *CachedPeerInfo](capacity, onEvict)
}
