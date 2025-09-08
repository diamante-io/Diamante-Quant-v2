// Package cache provides tests for typed cache implementations
package storage

import (
	"testing"
	"time"

	. "diamante/storage/cache"
	"diamante/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedMemoryCache(t *testing.T) {
	config := &types.CacheConfig{
		MaxSize:    10,
		DefaultTTL: 1 * time.Hour,
	}
	c := NewTypedMemoryCache(config, nil)

	t.Run("BasicOperations", func(t *testing.T) {
		// Test Set and Get
		value := &types.CacheValue{
			Key:  "test-key",
			Data: []byte("test data"),
			Size: uint64(len("test data")),
			TTL:  0, // No expiration
		}

		err := c.Set("test-key", value)
		assert.NoError(t, err)

		retrieved, err := c.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, retrieved.Data)
		assert.Equal(t, value.Key, retrieved.Key)
		assert.Greater(t, retrieved.AccessCount, uint64(0))
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		_, err := c.Get("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("TTLExpiration", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "expiring-key",
			Data: []byte("expiring data"),
			Size: uint64(len("expiring data")),
			TTL:  1, // 1 second TTL
		}

		err := c.Set("expiring-key", value)
		assert.NoError(t, err)

		// Should exist immediately
		_, err = c.Get("expiring-key")
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Should be expired
		_, err = c.Get("expiring-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expired")
	})

	t.Run("LRUEviction", func(t *testing.T) {
		// Clear cache first
		err := c.Clear()
		assert.NoError(t, err)

		// Fill cache to capacity
		for i := 0; i < 10; i++ {
			key := string(rune('a' + i))
			value := &types.CacheValue{
				Key:  key,
				Data: []byte(key),
				Size: 1,
			}
			err := c.Set(key, value)
			assert.NoError(t, err)
		}

		// Add one more item - should evict oldest
		extraValue := &types.CacheValue{
			Key:  "extra",
			Data: []byte("extra"),
			Size: 1,
		}
		err = c.Set("extra", extraValue)
		assert.NoError(t, err)

		// First item should be evicted
		_, err = c.Get("a")
		assert.Error(t, err)

		// Extra item should exist
		_, err = c.Get("extra")
		assert.NoError(t, err)

		stats := c.GetStats()
		assert.Equal(t, uint64(10), stats.ItemCount)
		assert.Greater(t, stats.Evictions, uint64(0))
	})

	t.Run("Delete", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "delete-key",
			Data: []byte("delete data"),
			Size: uint64(len("delete data")),
		}

		err := c.Set("delete-key", value)
		assert.NoError(t, err)

		err = c.Delete("delete-key")
		assert.NoError(t, err)

		_, err = c.Get("delete-key")
		assert.Error(t, err)
	})

	t.Run("Exists", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "exists-key",
			Data: []byte("exists data"),
			Size: uint64(len("exists data")),
		}

		assert.False(t, c.Exists("exists-key"))

		err := c.Set("exists-key", value)
		assert.NoError(t, err)

		assert.True(t, c.Exists("exists-key"))
	})

	t.Run("Stats", func(t *testing.T) {
		c.Clear()

		// Generate some activity
		for i := 0; i < 5; i++ {
			key := string(rune('a' + i))
			value := &types.CacheValue{
				Key:  key,
				Data: []byte(key),
				Size: uint64(i + 1),
			}
			c.Set(key, value)
		}

		// Some hits
		c.Get("a")
		c.Get("b")

		// Some misses
		c.Get("missing1")
		c.Get("missing2")
		c.Get("missing3")

		stats := c.GetStats()
		assert.Equal(t, uint64(2), stats.Hits)
		assert.Equal(t, uint64(3), stats.Misses)
		assert.Equal(t, uint64(5), stats.ItemCount)
		assert.Greater(t, stats.Size, uint64(0))
		assert.Equal(t, float64(2)/float64(5), stats.HitRate)
	})
}

func TestTypedRedisCache(t *testing.T) {
	// Start miniredis for testing
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	config := &types.CacheConfig{
		MaxSize:    100,
		DefaultTTL: 1 * time.Hour,
	}

	redisURL := "redis://" + mr.Addr()
	c, err := NewTypedRedisCache(redisURL, config, nil)
	require.NoError(t, err)

	t.Run("BasicOperations", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "redis-test-key",
			Data: []byte("redis test data"),
			Size: uint64(len("redis test data")),
			TTL:  0,
		}

		err := c.Set("redis-test-key", value)
		assert.NoError(t, err)

		retrieved, err := c.Get("redis-test-key")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, retrieved.Data)
		assert.Equal(t, "redis-test-key", retrieved.Key)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		_, err := c.Get("redis-non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("TTL", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "redis-ttl-key",
			Data: []byte("ttl data"),
			Size: uint64(len("ttl data")),
			TTL:  2, // 2 seconds
		}

		err := c.Set("redis-ttl-key", value)
		assert.NoError(t, err)

		// Should exist immediately
		exists := c.Exists("redis-ttl-key")
		assert.True(t, exists)

		// Fast forward time in miniredis
		mr.FastForward(3 * time.Second)

		// Should be expired
		exists = c.Exists("redis-ttl-key")
		assert.False(t, exists)
	})

	t.Run("Delete", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "redis-delete-key",
			Data: []byte("delete data"),
			Size: uint64(len("delete data")),
		}

		err := c.Set("redis-delete-key", value)
		assert.NoError(t, err)

		err = c.Delete("redis-delete-key")
		assert.NoError(t, err)

		exists := c.Exists("redis-delete-key")
		assert.False(t, exists)
	})

	t.Run("Clear", func(t *testing.T) {
		// Add multiple items
		for i := 0; i < 5; i++ {
			key := string(rune('a' + i))
			value := &types.CacheValue{
				Key:  key,
				Data: []byte(key),
				Size: 1,
			}
			c.Set(key, value)
		}

		// Clear all
		err := c.Clear()
		assert.NoError(t, err)

		// Verify all are gone
		for i := 0; i < 5; i++ {
			key := string(rune('a' + i))
			exists := c.Exists(key)
			assert.False(t, exists)
		}
	})
}

func TestTypedCacheManager(t *testing.T) {
	config := &types.CacheConfig{
		MaxSize:    10,
		DefaultTTL: 1 * time.Hour,
	}

	// Create multi-level cache
	l1Cache := NewTypedMemoryCache(config, nil)

	// Start miniredis for L2
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisURL := "redis://" + mr.Addr()
	l2Cache, err := NewTypedRedisCache(redisURL, config, nil)
	require.NoError(t, err)

	manager := NewTypedCacheManager(config, nil)
	manager.AddLevel(l1Cache)
	manager.AddLevel(l2Cache)

	t.Run("MultiLevelGet", func(t *testing.T) {
		// Set only in L2
		value := &types.CacheValue{
			Key:  "multi-test",
			Data: []byte("multi-level data"),
			Size: uint64(len("multi-level data")),
		}

		err := l2Cache.Set("multi-test", value)
		assert.NoError(t, err)

		// Get through manager - should populate L1
		retrieved, err := manager.Get("multi-test")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, retrieved.Data)

		// Should now exist in L1
		l1Value, err := l1Cache.Get("multi-test")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, l1Value.Data)
	})

	t.Run("MultiLevelSet", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "multi-set",
			Data: []byte("set data"),
			Size: uint64(len("set data")),
		}

		err := manager.Set("multi-set", value)
		assert.NoError(t, err)

		// Should exist in both levels
		l1Value, err := l1Cache.Get("multi-set")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, l1Value.Data)

		l2Value, err := l2Cache.Get("multi-set")
		assert.NoError(t, err)
		assert.Equal(t, value.Data, l2Value.Data)
	})

	t.Run("MultiLevelDelete", func(t *testing.T) {
		value := &types.CacheValue{
			Key:  "multi-delete",
			Data: []byte("delete data"),
			Size: uint64(len("delete data")),
		}

		manager.Set("multi-delete", value)

		err := manager.Delete("multi-delete")
		assert.NoError(t, err)

		// Should be gone from both levels
		assert.False(t, l1Cache.Exists("multi-delete"))
		assert.False(t, l2Cache.Exists("multi-delete"))
	})
}

func BenchmarkTypedMemoryCache(b *testing.B) {
	config := &types.CacheConfig{
		MaxSize:    1000,
		DefaultTTL: 1 * time.Hour,
	}
	c := NewTypedMemoryCache(config, nil)

	// Pre-populate
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		value := &types.CacheValue{
			Key:  key,
			Data: []byte("benchmark data"),
			Size: uint64(len("benchmark data")),
		}
		c.Set(key, value)
	}

	b.Run("Set", func(b *testing.B) {
		value := &types.CacheValue{
			Key:  "bench-set",
			Data: []byte("benchmark data"),
			Size: uint64(len("benchmark data")),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Set("bench-set", value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		value := &types.CacheValue{
			Key:  "bench-get",
			Data: []byte("benchmark data"),
			Size: uint64(len("benchmark data")),
		}
		c.Set("bench-get", value)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Get("bench-get")
		}
	})

	b.Run("GetMiss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.Get("miss")
		}
	})
}

func TestConcurrentCacheOperations(t *testing.T) {
	config := &types.CacheConfig{
		MaxSize:    100,
		DefaultTTL: 1 * time.Hour,
	}
	c := NewTypedMemoryCache(config, nil)

	done := make(chan bool)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('a'+id)) + string(rune(j))
				value := &types.CacheValue{
					Key:  key,
					Data: []byte("concurrent data"),
					Size: uint64(len("concurrent data")),
				}
				c.Set(key, value)
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('a'+id)) + string(rune(j))
				c.Get(key)
			}
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 20; i++ {
		<-done
	}

	// Cache should still be functional
	stats := c.GetStats()
	assert.Greater(t, stats.ItemCount, uint64(0))
	assert.Greater(t, stats.Hits+stats.Misses, uint64(0))
}
