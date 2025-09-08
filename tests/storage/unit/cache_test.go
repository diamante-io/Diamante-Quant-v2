package storage_test

import (
	"fmt"
	"testing"
	"time"

	"diamante/storage/cache"
	"diamante/types"

	"github.com/stretchr/testify/assert"
)

func TestMemoryCache(t *testing.T) {
	t.Run("Basic Operations", func(t *testing.T) {
		c := cache.NewMemoryCache(10, time.Minute)

		// Test Set and Get
		value := &types.CacheValue{
			Key:         "test-key",
			Data:        []byte("test-data"),
			Size:        9,
			CreatedAt:   time.Now(),
			AccessedAt:  time.Now(),
			AccessCount: 0,
			TTL:         60,
		}

		c.Set("test-key", value)

		retrieved, found := c.Get("test-key")
		assert.True(t, found)
		assert.Equal(t, value.Data, retrieved.Data)
		assert.Equal(t, uint64(1), retrieved.AccessCount)

		// Test Delete
		c.Delete("test-key")
		_, found = c.Get("test-key")
		assert.False(t, found)
	})

	t.Run("LRU Eviction", func(t *testing.T) {
		c := cache.NewMemoryCache(3, time.Minute)

		// Add 3 items
		for i := 0; i < 3; i++ {
			value := &types.CacheValue{
				Key:  string(rune('a' + i)),
				Data: []byte{byte(i)},
				Size: 1,
			}
			c.Set(string(rune('a'+i)), value)
		}

		// Access first item to make it most recently used
		c.Get("a")

		// Add fourth item - should evict 'b'
		value := &types.CacheValue{
			Key:  "d",
			Data: []byte{3},
			Size: 1,
		}
		c.Set("d", value)

		// Check eviction
		_, found := c.Get("b")
		assert.False(t, found)

		// Check others are still there
		_, found = c.Get("a")
		assert.True(t, found)
		_, found = c.Get("c")
		assert.True(t, found)
		_, found = c.Get("d")
		assert.True(t, found)
	})

	t.Run("TTL Expiration", func(t *testing.T) {
		c := cache.NewMemoryCache(10, 100*time.Millisecond)

		value := &types.CacheValue{
			Key:  "expire-test",
			Data: []byte("test"),
			Size: 4,
		}
		c.Set("expire-test", value)

		// Should exist immediately
		_, found := c.Get("expire-test")
		assert.True(t, found)

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		// Should be expired
		_, found = c.Get("expire-test")
		assert.False(t, found)
	})

	t.Run("Stats Tracking", func(t *testing.T) {
		c := cache.NewMemoryCache(10, time.Minute)

		// Initial stats
		stats := c.Stats()
		assert.Equal(t, uint64(0), stats.Hits)
		assert.Equal(t, uint64(0), stats.Misses)

		// Add item
		value := &types.CacheValue{
			Key:  "stats-test",
			Data: []byte("test"),
			Size: 4,
		}
		c.Set("stats-test", value)

		// Hit
		c.Get("stats-test")
		stats = c.Stats()
		assert.Equal(t, uint64(1), stats.Hits)

		// Miss
		c.Get("non-existent")
		stats = c.Stats()
		assert.Equal(t, uint64(1), stats.Misses)

		// Check other stats
		assert.Equal(t, uint64(4), stats.Size)
		assert.Equal(t, uint64(1), stats.ItemCount)
		assert.Equal(t, float64(0.5), stats.HitRate)
	})

	t.Run("Clear", func(t *testing.T) {
		c := cache.NewMemoryCache(10, time.Minute)

		// Add items
		for i := 0; i < 5; i++ {
			value := &types.CacheValue{
				Key:  string(rune('a' + i)),
				Data: []byte{byte(i)},
				Size: 1,
			}
			c.Set(string(rune('a'+i)), value)
		}

		assert.Equal(t, 5, c.Len())

		// Clear
		c.Clear()
		assert.Equal(t, 0, c.Len())

		// Verify all items are gone
		for i := 0; i < 5; i++ {
			_, found := c.Get(string(rune('a' + i)))
			assert.False(t, found)
		}
	})
}

func TestCacheManager(t *testing.T) {
	t.Run("Multiple Named Caches", func(t *testing.T) {
		manager := cache.NewManager()

		// Create different caches
		cache1 := manager.GetCache("cache1", &cache.Options{
			Size: 100,
			TTL:  time.Minute,
		})

		cache2 := manager.GetCache("cache2", &cache.Options{
			Size: 200,
			TTL:  2 * time.Minute,
		})

		// Add to cache1
		value1 := &types.CacheValue{
			Key:  "key1",
			Data: []byte("cache1-data"),
			Size: 11,
		}
		cache1.Set("key1", value1)

		// Add to cache2
		value2 := &types.CacheValue{
			Key:  "key1",
			Data: []byte("cache2-data"),
			Size: 11,
		}
		cache2.Set("key1", value2)

		// Verify isolation
		retrieved1, _ := cache1.Get("key1")
		retrieved2, _ := cache2.Get("key1")

		assert.NotEqual(t, retrieved1.Data, retrieved2.Data)
		assert.Equal(t, []byte("cache1-data"), retrieved1.Data)
		assert.Equal(t, []byte("cache2-data"), retrieved2.Data)
	})

	t.Run("Cache Reuse", func(t *testing.T) {
		manager := cache.NewManager()

		// Create cache
		cache1 := manager.GetCache("test", &cache.Options{
			Size: 100,
			TTL:  time.Minute,
		})

		value := &types.CacheValue{
			Key:  "key",
			Data: []byte("data"),
			Size: 4,
		}
		cache1.Set("key", value)

		// Get same cache
		cache2 := manager.GetCache("test", nil)

		// Should be same instance
		retrieved, found := cache2.Get("key")
		assert.True(t, found)
		assert.Equal(t, value.Data, retrieved.Data)
	})
}

func TestCacheWarming(t *testing.T) {
	t.Run("Warm Cache", func(t *testing.T) {
		manager := cache.NewManager()
		c := manager.GetCache("warm-test", &cache.Options{
			Size: 100,
			TTL:  time.Minute,
		})

		// Keys to warm
		keys := []string{"key1", "key2", "key3"}

		// Fetch function
		fetchCount := 0
		fetch := func(key string) (*types.CacheValue, error) {
			fetchCount++
			return &types.CacheValue{
				Key:         key,
				Data:        []byte("data-" + key),
				Size:        uint64(len("data-" + key)),
				CreatedAt:   time.Now(),
				AccessedAt:  time.Now(),
				AccessCount: 0,
				TTL:         60,
			}, nil
		}

		// Warm cache
		cache.WarmCache(c, keys, fetch)

		// Verify all keys are cached
		assert.Equal(t, 3, fetchCount)

		for _, key := range keys {
			value, found := c.Get(key)
			assert.True(t, found)
			assert.Equal(t, []byte("data-"+key), value.Data)
		}

		// Warm again - should not fetch
		fetchCount = 0
		cache.WarmCache(c, keys, fetch)
		assert.Equal(t, 0, fetchCount)
	})
}

func TestMultiLevelCache(t *testing.T) {
	t.Run("L1 Hit", func(t *testing.T) {
		l1 := cache.NewMemoryCache(10, time.Minute)
		multilevel := cache.NewMultiLevelCache(l1, nil)

		value := &types.CacheValue{
			Key:  "test",
			Data: []byte("data"),
			Size: 4,
		}
		multilevel.Set("test", value)

		// Should hit L1
		retrieved, found := multilevel.Get("test")
		assert.True(t, found)
		assert.Equal(t, value.Data, retrieved.Data)

		// Check L1 stats
		stats := l1.Stats()
		assert.Equal(t, uint64(1), stats.Hits)
	})

	t.Run("Clear All Levels", func(t *testing.T) {
		l1 := cache.NewMemoryCache(10, time.Minute)
		multilevel := cache.NewMultiLevelCache(l1, nil)

		// Add items
		for i := 0; i < 5; i++ {
			value := &types.CacheValue{
				Key:  string(rune('a' + i)),
				Data: []byte{byte(i)},
				Size: 1,
			}
			multilevel.Set(string(rune('a'+i)), value)
		}

		assert.Equal(t, 5, multilevel.Len())

		// Clear
		multilevel.Clear()
		assert.Equal(t, 0, multilevel.Len())

		// Verify cleared
		for i := 0; i < 5; i++ {
			_, found := multilevel.Get(string(rune('a' + i)))
			assert.False(t, found)
		}
	})
}

func BenchmarkMemoryCache(b *testing.B) {
	c := cache.NewMemoryCache(1000, time.Minute)

	// Pre-populate
	for i := 0; i < 500; i++ {
		value := &types.CacheValue{
			Key:  fmt.Sprintf("key-%d", i),
			Data: []byte(fmt.Sprintf("value-%d", i)),
			Size: uint64(len(fmt.Sprintf("value-%d", i))),
		}
		c.Set(value.Key, value)
	}

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i%500)
			c.Get(key)
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			value := &types.CacheValue{
				Key:  fmt.Sprintf("bench-%d", i),
				Data: []byte(fmt.Sprintf("value-%d", i)),
				Size: uint64(len(fmt.Sprintf("value-%d", i))),
			}
			c.Set(value.Key, value)
		}
	})

	b.Run("Mixed", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				key := fmt.Sprintf("key-%d", i%500)
				c.Get(key)
			} else {
				value := &types.CacheValue{
					Key:  fmt.Sprintf("mixed-%d", i),
					Data: []byte(fmt.Sprintf("value-%d", i)),
					Size: uint64(len(fmt.Sprintf("value-%d", i))),
				}
				c.Set(value.Key, value)
			}
		}
	})
}
