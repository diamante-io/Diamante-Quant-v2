// Package consensus provides tests for typed cache implementations
package consensus

import (
	"testing"
	"time"

	"diamante/types"

	"github.com/stretchr/testify/assert"
)

func TestTypedSimpleCache(t *testing.T) {
	cache := NewTypedSimpleCache(10, &SystemTimeProvider{})

	t.Run("SetAndGet", func(t *testing.T) {
		key := TypedCacheKey{Namespace: "test", Key: "key1"}
		entry := &types.CacheEntry{
			Key:       key.String(),
			Value:     []byte("test value"),
			EntryType: types.CacheTypeBlock,
			TTL:       0,
		}

		cache.Set(key, entry)

		retrieved, exists := cache.Get(key)
		assert.True(t, exists)
		assert.Equal(t, entry.Value, retrieved.Value)
		assert.Equal(t, types.CacheTypeBlock, retrieved.EntryType)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		key := TypedCacheKey{Namespace: "test", Key: "nonexistent"}
		_, exists := cache.Get(key)
		assert.False(t, exists)
	})

	t.Run("TTLExpiration", func(t *testing.T) {
		key := TypedCacheKey{Namespace: "test", Key: "expiring"}
		entry := &types.CacheEntry{
			Key:       key.String(),
			Value:     []byte("expiring value"),
			EntryType: types.CacheTypeBlock,
			TTL:       1, // 1 second TTL
		}

		cache.Set(key, entry)

		// Should exist immediately
		_, exists := cache.Get(key)
		assert.True(t, exists)

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Should not exist after expiration
		_, exists = cache.Get(key)
		assert.False(t, exists)
	})

	t.Run("Eviction", func(t *testing.T) {
		// Fill cache to capacity
		for i := 0; i < 10; i++ {
			key := TypedCacheKey{Namespace: "test", Key: string(rune('a' + i))}
			entry := &types.CacheEntry{
				Key:   key.String(),
				Value: []byte("value"),
			}
			cache.Set(key, entry)
		}

		assert.Equal(t, 10, cache.Size())

		// Add one more item - should evict oldest
		newKey := TypedCacheKey{Namespace: "test", Key: "new"}
		newEntry := &types.CacheEntry{
			Key:   newKey.String(),
			Value: []byte("new value"),
		}
		cache.Set(newKey, newEntry)

		// Size should still be 10
		assert.Equal(t, 10, cache.Size())

		// New item should exist
		_, exists := cache.Get(newKey)
		assert.True(t, exists)
	})

	t.Run("Delete", func(t *testing.T) {
		key := TypedCacheKey{Namespace: "test", Key: "delete_me"}
		entry := &types.CacheEntry{
			Key:   key.String(),
			Value: []byte("delete value"),
		}

		cache.Set(key, entry)
		assert.Equal(t, 1, cache.Size())

		cache.Delete(key)
		assert.Equal(t, 0, cache.Size())

		_, exists := cache.Get(key)
		assert.False(t, exists)
	})

	t.Run("Clear", func(t *testing.T) {
		// Add multiple items
		for i := 0; i < 5; i++ {
			key := TypedCacheKey{Namespace: "test", Key: string(rune('a' + i))}
			entry := &types.CacheEntry{
				Key:   key.String(),
				Value: []byte("value"),
			}
			cache.Set(key, entry)
		}

		assert.Equal(t, 5, cache.Size())

		cache.Clear()
		assert.Equal(t, 0, cache.Size())
	})

	t.Run("Stats", func(t *testing.T) {
		cache.Clear()

		// Generate some hits and misses
		key1 := TypedCacheKey{Namespace: "test", Key: "key1"}
		entry1 := &types.CacheEntry{
			Key:   key1.String(),
			Value: []byte("value1"),
		}

		cache.Set(key1, entry1)

		// Hits
		cache.Get(key1)
		cache.Get(key1)

		// Misses
		cache.Get(TypedCacheKey{Namespace: "test", Key: "miss1"})
		cache.Get(TypedCacheKey{Namespace: "test", Key: "miss2"})

		stats := cache.Stats()
		assert.Equal(t, uint64(2), stats.Hits)
		assert.Equal(t, uint64(2), stats.Misses)
		assert.Equal(t, float64(0.5), stats.HitRate)
		assert.Equal(t, uint64(1), stats.ItemCount)
	})
}

func TestTypedLRUCache(t *testing.T) {
	cache := NewTypedLRUCache(3, &SystemTimeProvider{})

	t.Run("LRUEviction", func(t *testing.T) {
		// Add items in order a, b, c
		for _, key := range []string{"a", "b", "c"} {
			k := TypedCacheKey{Namespace: "test", Key: key}
			entry := &types.CacheEntry{
				Key:   k.String(),
				Value: []byte(key),
			}
			cache.Set(k, entry)
			time.Sleep(10 * time.Millisecond) // Ensure different timestamps
		}

		// Access 'a' to make it recently used
		keyA := TypedCacheKey{Namespace: "test", Key: "a"}
		_, exists := cache.Get(keyA)
		assert.True(t, exists)

		// Add 'd' - should evict 'b' (least recently used)
		keyD := TypedCacheKey{Namespace: "test", Key: "d"}
		entryD := &types.CacheEntry{
			Key:   keyD.String(),
			Value: []byte("d"),
		}
		cache.Set(keyD, entryD)

		// Check what exists
		_, existsA := cache.Get(keyA)
		assert.True(t, existsA, "a should exist (recently accessed)")

		keyB := TypedCacheKey{Namespace: "test", Key: "b"}
		_, existsB := cache.Get(keyB)
		assert.False(t, existsB, "b should be evicted (LRU)")

		keyC := TypedCacheKey{Namespace: "test", Key: "c"}
		_, existsC := cache.Get(keyC)
		assert.True(t, existsC, "c should exist")

		_, existsD := cache.Get(keyD)
		assert.True(t, existsD, "d should exist (just added)")
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		cache.Clear()

		key := TypedCacheKey{Namespace: "test", Key: "update"}
		entry1 := &types.CacheEntry{
			Key:   key.String(),
			Value: []byte("value1"),
		}

		cache.Set(key, entry1)

		// Update with new value
		entry2 := &types.CacheEntry{
			Key:   key.String(),
			Value: []byte("value2"),
		}
		cache.Set(key, entry2)

		// Should have updated value
		retrieved, exists := cache.Get(key)
		assert.True(t, exists)
		assert.Equal(t, []byte("value2"), retrieved.Value)

		// Size should still be 1
		assert.Equal(t, 1, cache.Size())
	})
}

func TestCacheAdapter(t *testing.T) {
	// Create typed cache and adapter
	typedCache := NewTypedSimpleCache(10, &SystemTimeProvider{})
	adapter := NewCacheAdapter(typedCache)

	t.Run("BackwardCompatibility", func(t *testing.T) {
		// Test with string key
		adapter.Set("string_key", "string_value")

		value, exists := adapter.Get("string_key")
		assert.True(t, exists)
		assert.NotNil(t, value)

		// Test with numeric key
		adapter.Set("123", []byte("numeric_key_value"))

		value, exists = adapter.Get("123")
		assert.True(t, exists)
		assert.NotNil(t, value)
	})

	t.Run("Delete", func(t *testing.T) {
		adapter.Set("delete_key", "delete_value")
		adapter.Delete("delete_key")

		_, exists := adapter.Get("delete_key")
		assert.False(t, exists)
	})

	t.Run("Clear", func(t *testing.T) {
		adapter.Set("key1", "value1")
		adapter.Set("key2", "value2")

		adapter.Clear()

		_, exists1 := adapter.Get("key1")
		_, exists2 := adapter.Get("key2")

		assert.False(t, exists1)
		assert.False(t, exists2)
	})
}

func BenchmarkTypedCache(b *testing.B) {
	cache := NewTypedSimpleCache(1000, &SystemTimeProvider{})

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := TypedCacheKey{Namespace: "bench", Key: string(rune(i))}
		entry := &types.CacheEntry{
			Key:   key.String(),
			Value: []byte("benchmark value"),
		}
		cache.Set(key, entry)
	}

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := TypedCacheKey{Namespace: "bench", Key: "set_test"}
			entry := &types.CacheEntry{
				Key:   key.String(),
				Value: []byte("benchmark value"),
			}
			cache.Set(key, entry)
		}
	})

	b.Run("Get", func(b *testing.B) {
		key := TypedCacheKey{Namespace: "bench", Key: "get_test"}
		entry := &types.CacheEntry{
			Key:   key.String(),
			Value: []byte("benchmark value"),
		}
		cache.Set(key, entry)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Get(key)
		}
	})

	b.Run("GetMiss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := TypedCacheKey{Namespace: "bench", Key: "miss"}
			cache.Get(key)
		}
	})
}

func TestConcurrency(t *testing.T) {
	cache := NewTypedSimpleCache(100, &SystemTimeProvider{})

	// Run concurrent operations
	done := make(chan bool)

	// Writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := TypedCacheKey{
					Namespace: "concurrent",
					Key:       string(rune('a'+id)) + string(rune(j)),
				}
				entry := &types.CacheEntry{
					Key:   key.String(),
					Value: []byte("concurrent value"),
				}
				cache.Set(key, entry)
			}
			done <- true
		}(i)
	}

	// Readers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := TypedCacheKey{
					Namespace: "concurrent",
					Key:       string(rune('a'+id)) + string(rune(j)),
				}
				cache.Get(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Cache should still be functional
	stats := cache.Stats()
	assert.True(t, stats.ItemCount > 0)
	assert.True(t, stats.Hits > 0 || stats.Misses > 0)
}
