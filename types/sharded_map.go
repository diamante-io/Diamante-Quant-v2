// Package types provides generic concurrent data structures
package types

import (
	"fmt"
	"hash/fnv"
	"sync"
)

// ShardedMap is a generic thread-safe map that uses sharding to reduce lock contention
type ShardedMap[K comparable, V any] struct {
	shards    []*mapShard[K, V]
	shardMask uint32
}

// mapShard is a single shard of a ShardedMap
type mapShard[K comparable, V any] struct {
	items map[K]V
	mu    sync.RWMutex
}

// NewShardedMap creates a new ShardedMap with the given number of shards
func NewShardedMap[K comparable, V any](shardCount int) *ShardedMap[K, V] {
	// Ensure shard count is a power of 2
	shardCount = nextPowerOfTwo(shardCount)

	shards := make([]*mapShard[K, V], shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &mapShard[K, V]{
			items: make(map[K]V),
		}
	}

	return &ShardedMap[K, V]{
		shards:    shards,
		shardMask: uint32(shardCount - 1),
	}
}

// getShard returns the shard for the given key
func (sm *ShardedMap[K, V]) getShard(key K) *mapShard[K, V] {
	hash := hashKey(key)
	return sm.shards[hash&sm.shardMask]
}

// Get returns the value for the given key
func (sm *ShardedMap[K, V]) Get(key K) (V, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.items[key]
	return value, ok
}

// Set sets the value for the given key
func (sm *ShardedMap[K, V]) Set(key K, value V) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.items[key] = value
}

// Delete deletes the value for the given key
func (sm *ShardedMap[K, V]) Delete(key K) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.items, key)
}

// ForEach iterates over all key-value pairs
func (sm *ShardedMap[K, V]) ForEach(fn func(key K, value V) bool) {
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for k, v := range shard.items {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Clear removes all items from the map
func (sm *ShardedMap[K, V]) Clear() {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		shard.items = make(map[K]V)
		shard.mu.Unlock()
	}
}

// Len returns the total number of items across all shards
func (sm *ShardedMap[K, V]) Len() int {
	count := 0
	for _, shard := range sm.shards {
		shard.mu.RLock()
		count += len(shard.items)
		shard.mu.RUnlock()
	}
	return count
}

// Keys returns all keys in the map
func (sm *ShardedMap[K, V]) Keys() []K {
	keys := make([]K, 0, sm.Len())
	sm.ForEach(func(key K, value V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Values returns all values in the map
func (sm *ShardedMap[K, V]) Values() []V {
	values := make([]V, 0, sm.Len())
	sm.ForEach(func(key K, value V) bool {
		values = append(values, value)
		return true
	})
	return values
}

// hashKey generates a hash for the given key
func hashKey[K comparable](key K) uint32 {
	h := fnv.New32a()
	// Convert key to bytes for hashing
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return h.Sum32()
}

// nextPowerOfTwo returns the next power of two greater than or equal to n
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	power := 1
	for power < n {
		power *= 2
	}
	return power
}

// ConcurrentMap provides a simple concurrent map without sharding
type ConcurrentMap[K comparable, V any] struct {
	items map[K]V
	mu    sync.RWMutex
}

// NewConcurrentMap creates a new concurrent map
func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		items: make(map[K]V),
	}
}

// Get returns the value for the given key
func (cm *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	value, ok := cm.items[key]
	return value, ok
}

// Set sets the value for the given key
func (cm *ConcurrentMap[K, V]) Set(key K, value V) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.items[key] = value
}

// Delete deletes the value for the given key
func (cm *ConcurrentMap[K, V]) Delete(key K) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.items, key)
}

// Len returns the number of items in the map
func (cm *ConcurrentMap[K, V]) Len() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.items)
}

// Clear removes all items from the map
func (cm *ConcurrentMap[K, V]) Clear() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.items = make(map[K]V)
}
