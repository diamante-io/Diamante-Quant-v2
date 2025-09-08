package cache

import (
	"diamante/common"
	"diamante/types"
	"time"
)

// WarmCache preloads the cache using the provided fetch function.
func WarmCache(c Cache, keys []string, fetch func(string) (*types.CacheValue, error)) {
	for _, k := range keys {
		if _, ok := c.Get(k); ok {
			continue
		}
		if val, err := fetch(k); err == nil {
			c.Set(k, val)
		}
	}
}

// ExampleWarmCache demonstrates how to preload a cache with data from a slow source.
func ExampleWarmCache() {
	mgr := NewManager()
	c := mgr.GetCache("accounts", &Options{Size: 100, TTL: time.Minute})

	keys := []string{"a1", "a2"}
	fetch := func(k string) (*types.CacheValue, error) {
		return &types.CacheValue{
			Key:         k,
			Data:        []byte("value-" + k),
			Size:        uint64(len("value-" + k)),
			CreatedAt:   common.ConsensusNow(),
			AccessedAt:  common.ConsensusNow(),
			AccessCount: 0,
			TTL:         int64(time.Minute.Seconds()),
		}, nil
	}

	WarmCache(c, keys, fetch)
}
