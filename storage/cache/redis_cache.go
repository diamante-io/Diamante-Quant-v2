package cache

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	dtypes "diamante/types"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// RedisCache provides a production-ready Redis-backed cache with advanced features
type RedisCache struct {
	client redisCommander
	ttl    time.Duration
	logger *logrus.Logger

	// Metrics
	hits      atomic.Uint64
	misses    atomic.Uint64
	errors    atomic.Uint64
	evictions atomic.Uint64

	// Configuration
	config *RedisCacheConfig

	// Circuit breaker
	circuitBreaker *CircuitBreaker

	// Connection pool monitoring
	poolStats   *redis.PoolStats
	poolStatsMu sync.RWMutex

	// Context for lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Health monitoring
	healthTicker *time.Ticker
	healthy      atomic.Bool

	// Serialization
	serializer Serializer
}

// RedisCacheConfig holds configuration for Redis cache
type RedisCacheConfig struct {
	Addr                 string
	DB                   int
	Password             string
	MaxRetries           int
	MinRetryBackoff      time.Duration
	MaxRetryBackoff      time.Duration
	DialTimeout          time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	PoolSize             int
	MinIdleConns         int
	MaxIdleConns         int
	ConnMaxIdleTime      time.Duration
	ConnMaxLifetime      time.Duration
	TTL                  time.Duration
	KeyPrefix            string
	EnableCompression    bool
	CompressionLevel     int
	EnableMetrics        bool
	HealthCheckInterval  time.Duration
	CircuitBreakerConfig *CircuitBreakerConfig
}

// DefaultRedisCacheConfig returns production-ready default configuration
func DefaultRedisCacheConfig() *RedisCacheConfig {
	return &RedisCacheConfig{
		Addr:                getEnvDefault("REDIS_ADDR", "127.0.0.1:6379"),
		DB:                  0,
		MaxRetries:          3,
		MinRetryBackoff:     8 * time.Millisecond,
		MaxRetryBackoff:     512 * time.Millisecond,
		DialTimeout:         5 * time.Second,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        3 * time.Second,
		PoolSize:            100,
		MinIdleConns:        10,
		MaxIdleConns:        50,
		ConnMaxIdleTime:     5 * time.Minute,
		ConnMaxLifetime:     30 * time.Minute,
		TTL:                 5 * time.Minute,
		EnableCompression:   true,
		CompressionLevel:    6,
		EnableMetrics:       true,
		HealthCheckInterval: 30 * time.Second,
		CircuitBreakerConfig: &CircuitBreakerConfig{
			Threshold:   5,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
		},
	}
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	config      *CircuitBreakerConfig
	failures    atomic.Uint32
	lastFailure atomic.Int64
	state       atomic.Uint32 // 0: closed, 1: open, 2: half-open
	mu          sync.Mutex
}

// CircuitBreakerConfig holds circuit breaker configuration
type CircuitBreakerConfig struct {
	Threshold   uint32
	Timeout     time.Duration
	MaxRequests uint32
}

// CircuitBreakerState represents the state of circuit breaker
type CircuitBreakerState uint32

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// Serializer interface for custom serialization
type Serializer interface {
	Marshal(v *dtypes.Value) ([]byte, error)
	Unmarshal(data []byte) (*dtypes.Value, error)
}

// JSONSerializer implements JSON serialization
type JSONSerializer struct{}

func (j *JSONSerializer) Marshal(v *dtypes.Value) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot marshal nil value")
	}
	// Use the cache serializer from types package
	s := &dtypes.CacheSerializer{}
	return s.ToJSON(v)
}

func (j *JSONSerializer) Unmarshal(data []byte) (*dtypes.Value, error) {
	s := &dtypes.CacheSerializer{}
	return s.FromJSON(data)
}

// redisCommander abstracts Redis operations for testability
type redisCommander interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Del(ctx context.Context, keys ...string) error
	FlushDB(ctx context.Context) error
	DBSize(ctx context.Context) (int64, error)
	Ping(ctx context.Context) error
	Close() error
	PoolStats() *redis.PoolStats
	MGet(ctx context.Context, keys ...string) ([]string, error)
	MSet(ctx context.Context, pairs ...string) error
	Expire(ctx context.Context, key string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	Pipeline() redis.Pipeliner
}

// redisClientWrapper wraps redis.Client to implement redisCommander
type redisClientWrapper struct{ *redis.Client }

func (r *redisClientWrapper) Get(ctx context.Context, key string) (string, error) {
	return r.Client.Get(ctx, key).Result()
}

func (r *redisClientWrapper) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return r.Client.Set(ctx, key, value, expiration).Err()
}

func (r *redisClientWrapper) Del(ctx context.Context, keys ...string) error {
	return r.Client.Del(ctx, keys...).Err()
}

func (r *redisClientWrapper) FlushDB(ctx context.Context) error {
	return r.Client.FlushDB(ctx).Err()
}

func (r *redisClientWrapper) DBSize(ctx context.Context) (int64, error) {
	return r.Client.DBSize(ctx).Result()
}

func (r *redisClientWrapper) Ping(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}

func (r *redisClientWrapper) Close() error {
	return r.Client.Close()
}

func (r *redisClientWrapper) PoolStats() *redis.PoolStats {
	return r.Client.PoolStats()
}

func (r *redisClientWrapper) MGet(ctx context.Context, keys ...string) ([]string, error) {
	vals, err := r.Client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	// Convert []interface{} to []string
	result := make([]string, len(vals))
	for i, v := range vals {
		if v != nil {
			result[i] = fmt.Sprint(v)
		} else {
			result[i] = ""
		}
	}
	return result, nil
}

func (r *redisClientWrapper) MSet(ctx context.Context, pairs ...string) error {
	// Convert string pairs to interface{} pairs for redis client
	args := make([]interface{}, len(pairs))
	for i, v := range pairs {
		args[i] = v
	}
	return r.Client.MSet(ctx, args...).Err()
}

func (r *redisClientWrapper) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.Client.Expire(ctx, key, expiration).Err()
}

func (r *redisClientWrapper) TTL(ctx context.Context, key string) (time.Duration, error) {
	return r.Client.TTL(ctx, key).Result()
}

func (r *redisClientWrapper) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return r.Client.Scan(ctx, cursor, match, count).Result()
}

func (r *redisClientWrapper) Pipeline() redis.Pipeliner {
	return r.Client.Pipeline()
}

// NewRedisCache creates a new production-ready RedisCache
func NewRedisCache(config *RedisCacheConfig) (*RedisCache, error) {
	if config == nil {
		config = DefaultRedisCacheConfig()
	}

	// Create Redis options with OnConnect hook to handle CLIENT SETINFO timeout
	opt := &redis.Options{
		Addr:            config.Addr,
		DB:              config.DB,
		Password:        config.Password,
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		ConnMaxIdleTime: config.ConnMaxIdleTime,
		ConnMaxLifetime: config.ConnMaxLifetime,

		// Add OnConnect hook to handle CLIENT SETINFO with timeout
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			// Use a shorter timeout for CLIENT SETINFO to prevent response ordering issues
			clientInfoCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			// Execute CLIENT SETINFO with timeout using the correct API
			libInfo := redis.LibraryInfo{
				LibName: &[]string{"diamante-cache"}[0],
			}
			if err := cn.ClientSetInfo(clientInfoCtx, libInfo).Err(); err != nil {
				// Log the error but don't fail the connection
				// This prevents CLIENT SETINFO timeout from causing out-of-order responses
				if logger := logrus.StandardLogger(); logger != nil {
					logger.WithError(err).Debug("CLIENT SETINFO failed, continuing without client info")
				}
			}
			return nil
		},
	}

	client := redis.NewClient(opt)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	cacheCtx, cacheCancel := context.WithCancel(context.Background())

	rc := &RedisCache{
		client:         &redisClientWrapper{client},
		ttl:            config.TTL,
		logger:         logger,
		config:         config,
		ctx:            cacheCtx,
		cancel:         cacheCancel,
		serializer:     &JSONSerializer{},
		circuitBreaker: newCircuitBreaker(config.CircuitBreakerConfig),
	}

	rc.healthy.Store(true)

	// Start health monitoring
	if config.HealthCheckInterval > 0 {
		rc.startHealthMonitoring()
	}

	return rc, nil
}

// NewRedisCacheFromCommander creates a RedisCache using a custom Redis client
func NewRedisCacheFromCommander(client redisCommander, config *RedisCacheConfig) *RedisCache {
	if config == nil {
		config = DefaultRedisCacheConfig()
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, cancel := context.WithCancel(context.Background())

	rc := &RedisCache{
		client:         client,
		ttl:            config.TTL,
		logger:         logger,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
		serializer:     &JSONSerializer{},
		circuitBreaker: newCircuitBreaker(config.CircuitBreakerConfig),
	}

	rc.healthy.Store(true)

	return rc
}

// newCircuitBreaker creates a new circuit breaker
func newCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = &CircuitBreakerConfig{
			Threshold:   5,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
		}
	}
	return &CircuitBreaker{
		config: config,
	}
}

// startHealthMonitoring starts periodic health checks
func (rc *RedisCache) startHealthMonitoring() {
	rc.healthTicker = time.NewTicker(rc.config.HealthCheckInterval)

	go func() {
		for {
			select {
			case <-rc.ctx.Done():
				return
			case <-rc.healthTicker.C:
				rc.performHealthCheck()
			}
		}
	}()
}

// performHealthCheck checks Redis connection health
func (rc *RedisCache) performHealthCheck() {
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	if err := rc.client.Ping(ctx); err != nil {
		rc.healthy.Store(false)
		rc.logger.WithError(err).Error("Redis health check failed")
	} else {
		rc.healthy.Store(true)

		// Update pool stats
		if stats := rc.client.PoolStats(); stats != nil {
			rc.poolStatsMu.Lock()
			rc.poolStats = stats
			rc.poolStatsMu.Unlock()
		}
	}
}

// GetTyped retrieves a value from Redis with circuit breaker protection
func (rc *RedisCache) GetTyped(key *dtypes.CacheKey) (*dtypes.Value, bool) {
	if !rc.healthy.Load() {
		rc.misses.Add(1)
		return nil, false
	}

	// Check circuit breaker
	if !rc.circuitBreaker.Allow() {
		rc.errors.Add(1)
		return nil, false
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.ReadTimeout)
	defer cancel()

	keyStr := rc.formatCacheKey(key)

	value, err := rc.client.Get(ctx, keyStr)
	if err != nil {
		if err == redis.Nil {
			rc.misses.Add(1)
			return nil, false
		}

		rc.circuitBreaker.RecordFailure()
		rc.errors.Add(1)
		rc.logger.WithError(err).WithField("key", keyStr).Error("Failed to get value from Redis")
		return nil, false
	}

	rc.circuitBreaker.RecordSuccess()
	rc.hits.Add(1)

	// Try to deserialize the value
	result, err := rc.serializer.Unmarshal([]byte(value))
	if err != nil {
		// If unmarshal fails, treat as string value
		return dtypes.StringToValue(value), true
	}

	return result, true
}

// SetTyped stores a value in Redis with circuit breaker protection
func (rc *RedisCache) SetTyped(key *dtypes.CacheKey, value *dtypes.Value) {
	if !rc.healthy.Load() {
		return
	}

	// Check circuit breaker
	if !rc.circuitBreaker.Allow() {
		rc.errors.Add(1)
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.WriteTimeout)
	defer cancel()

	keyStr := rc.formatCacheKey(key)

	// Serialize the value
	serialized, err := rc.serializer.Marshal(value)
	if err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).WithField("key", keyStr).Error("Failed to serialize value")
		return
	}

	if err := rc.client.Set(ctx, keyStr, string(serialized), rc.ttl); err != nil {
		rc.circuitBreaker.RecordFailure()
		rc.errors.Add(1)
		rc.logger.WithError(err).WithFields(logrus.Fields{
			"key": keyStr,
			"ttl": rc.ttl,
		}).Error("Failed to set value in Redis")
		return
	}

	rc.circuitBreaker.RecordSuccess()
}

// DeleteTyped removes a key from Redis
func (rc *RedisCache) DeleteTyped(key *dtypes.CacheKey) {
	if !rc.healthy.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.WriteTimeout)
	defer cancel()

	keyStr := rc.formatCacheKey(key)

	if err := rc.client.Del(ctx, keyStr); err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).WithField("key", keyStr).Error("Failed to delete key from Redis")
	} else {
		rc.evictions.Add(1)
	}
}

// Clear flushes the Redis database
func (rc *RedisCache) Clear() {
	if !rc.healthy.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.WriteTimeout)
	defer cancel()

	if err := rc.client.FlushDB(ctx); err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).Error("Failed to flush Redis database")
	}
}

// Len returns the number of keys
func (rc *RedisCache) Len() int {
	if !rc.healthy.Load() {
		return 0
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.ReadTimeout)
	defer cancel()

	size, err := rc.client.DBSize(ctx)
	if err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).Error("Failed to get Redis database size")
		return 0
	}

	return int(size)
}

// GetMultiple retrieves multiple values at once
func (rc *RedisCache) GetMultiple(keys []*dtypes.CacheKey) map[string]*dtypes.Value {
	if !rc.healthy.Load() || len(keys) == 0 {
		return make(map[string]*dtypes.Value)
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.ReadTimeout)
	defer cancel()

	// Convert keys to strings
	keyStrs := make([]string, len(keys))
	keyMap := make(map[string]*dtypes.CacheKey)
	for i, k := range keys {
		keyStr := rc.formatCacheKey(k)
		keyStrs[i] = keyStr
		keyMap[keyStr] = k
	}

	// Get values
	values, err := rc.client.MGet(ctx, keyStrs...)
	if err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).Error("Failed to get multiple values from Redis")
		return make(map[string]*dtypes.Value)
	}

	// Build result map
	result := make(map[string]*dtypes.Value)
	for i, val := range values {
		if val != "" {
			originalKey := keyMap[keyStrs[i]]
			// Try to deserialize the value
			if value, err := rc.serializer.Unmarshal([]byte(val)); err == nil {
				result[originalKey.String()] = value
			} else {
				// Fall back to string value
				result[originalKey.String()] = dtypes.StringToValue(val)
			}
			rc.hits.Add(1)
		} else {
			rc.misses.Add(1)
		}
	}

	return result
}

// SetMultiple sets multiple key-value pairs at once
func (rc *RedisCache) SetMultiple(items map[*dtypes.CacheKey]*dtypes.Value) {
	if !rc.healthy.Load() || len(items) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.WriteTimeout)
	defer cancel()

	// Build pipeline
	pipe := rc.client.Pipeline()

	for key, value := range items {
		keyStr := rc.formatCacheKey(key)

		// Serialize the value
		serialized, err := rc.serializer.Marshal(value)
		if err != nil {
			rc.errors.Add(1)
			continue
		}

		pipe.Set(ctx, keyStr, string(serialized), rc.ttl)
	}

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		rc.errors.Add(1)
		rc.logger.WithError(err).Error("Failed to set multiple values in Redis")
	}
}

// formatKey formats a key with optional prefix
func (rc *RedisCache) formatKey(key interface{}) string {
	keyStr := fmt.Sprint(key)
	if rc.config.KeyPrefix != "" {
		return rc.config.KeyPrefix + ":" + keyStr
	}
	return keyStr
}

// formatCacheKey formats a CacheKey with optional prefix
func (rc *RedisCache) formatCacheKey(key *dtypes.CacheKey) string {
	keyStr := key.String()
	if rc.config.KeyPrefix != "" {
		return rc.config.KeyPrefix + ":" + keyStr
	}
	return keyStr
}

// SetLogger sets a custom logger for the Redis cache
func (rc *RedisCache) SetLogger(logger *logrus.Logger) {
	if logger != nil {
		rc.logger = logger
	}
}

// SetSerializer sets a custom serializer
func (rc *RedisCache) SetSerializer(serializer Serializer) {
	if serializer != nil {
		rc.serializer = serializer
	}
}

// GetStats returns cache statistics
func (rc *RedisCache) GetStats() CacheStats {
	var poolStats redis.PoolStats
	rc.poolStatsMu.RLock()
	if rc.poolStats != nil {
		poolStats = *rc.poolStats
	}
	rc.poolStatsMu.RUnlock()

	return CacheStats{
		Hits:      rc.hits.Load(),
		Misses:    rc.misses.Load(),
		Errors:    rc.errors.Load(),
		Evictions: rc.evictions.Load(),
		Size:      rc.Len(),
		Healthy:   rc.healthy.Load(),
		PoolStats: poolStats,
	}
}

// CacheStats holds cache statistics
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Errors    uint64
	Evictions uint64
	Size      int
	Healthy   bool
	PoolStats redis.PoolStats
}

// Close cleanly shuts down the Redis cache
func (rc *RedisCache) Close() error {
	// Cancel context
	rc.cancel()

	// Stop health ticker
	if rc.healthTicker != nil {
		rc.healthTicker.Stop()
	}

	// Close Redis connection
	if err := rc.client.Close(); err != nil {
		return fmt.Errorf("failed to close Redis connection: %w", err)
	}

	rc.logger.Info("Redis cache closed")
	return nil
}

// Cache interface compatibility methods

// Get (string key) - implements Cache interface
func (rc *RedisCache) Get(key string) (*dtypes.CacheValue, bool) {
	cacheKey := &dtypes.CacheKey{Key: key}
	value, ok := rc.GetTyped(cacheKey)
	if !ok {
		return nil, false
	}

	// Convert Value to CacheValue
	cacheValue := &dtypes.CacheValue{
		Key:        key,
		Data:       value.Data,
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
		TTL:        int64(rc.ttl.Seconds()),
	}

	return cacheValue, true
}

// Set (string key) - implements Cache interface
func (rc *RedisCache) Set(key string, value *dtypes.CacheValue) {
	cacheKey := &dtypes.CacheKey{Key: key}
	typedValue := &dtypes.Value{
		Type: dtypes.ValueTypeBytes,
		Data: value.Data,
	}
	rc.SetTyped(cacheKey, typedValue)
}

// Delete (string key) - implements Cache interface
func (rc *RedisCache) Delete(key string) {
	cacheKey := &dtypes.CacheKey{Key: key}
	rc.DeleteTyped(cacheKey)
}

// Stats returns cache statistics - implements Cache interface
func (rc *RedisCache) Stats() *dtypes.CacheStats {
	stats := rc.GetStats()
	return &dtypes.CacheStats{
		Hits:      stats.Hits,
		Misses:    stats.Misses,
		Evictions: stats.Evictions,
		Size:      uint64(stats.Size),
		ItemCount: uint64(stats.Size),
		HitRate:   float64(stats.Hits) / float64(stats.Hits+stats.Misses),
	}
}

// Circuit Breaker methods

// Allow checks if request is allowed
func (cb *CircuitBreaker) Allow() bool {
	state := CircuitBreakerState(cb.state.Load())

	switch state {
	case CircuitOpen:
		// Check if timeout has passed
		lastFailure := cb.lastFailure.Load()
		if time.Since(time.Unix(0, lastFailure)) > cb.config.Timeout {
			cb.mu.Lock()
			cb.state.Store(uint32(CircuitHalfOpen))
			cb.failures.Store(0)
			cb.mu.Unlock()
			return true
		}
		return false

	case CircuitHalfOpen:
		// Allow limited requests
		return true

	default: // CircuitClosed
		return true
	}
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	state := CircuitBreakerState(cb.state.Load())

	if state == CircuitHalfOpen {
		cb.mu.Lock()
		cb.state.Store(uint32(CircuitClosed))
		cb.failures.Store(0)
		cb.mu.Unlock()
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	failures := cb.failures.Add(1)
	cb.lastFailure.Store(common.ConsensusNow().UnixNano())

	if failures >= cb.config.Threshold {
		cb.mu.Lock()
		if cb.state.Load() != uint32(CircuitOpen) {
			cb.state.Store(uint32(CircuitOpen))
		}
		cb.mu.Unlock()
	}
}

// ExpireAfter sets expiration time for a key
func (rc *RedisCache) ExpireAfter(key *dtypes.CacheKey, duration time.Duration) error {
	if !rc.healthy.Load() {
		return fmt.Errorf("cache is unhealthy")
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.WriteTimeout)
	defer cancel()

	keyStr := rc.formatCacheKey(key)

	if err := rc.client.Expire(ctx, keyStr, duration); err != nil {
		rc.errors.Add(1)
		return fmt.Errorf("failed to set expiration: %w", err)
	}

	return nil
}

// GetTTL returns the remaining TTL for a key
func (rc *RedisCache) GetTTL(key *dtypes.CacheKey) (time.Duration, error) {
	if !rc.healthy.Load() {
		return 0, fmt.Errorf("cache is unhealthy")
	}

	ctx, cancel := context.WithTimeout(rc.ctx, rc.config.ReadTimeout)
	defer cancel()

	keyStr := rc.formatCacheKey(key)

	ttl, err := rc.client.TTL(ctx, keyStr)
	if err != nil {
		rc.errors.Add(1)
		return 0, fmt.Errorf("failed to get TTL: %w", err)
	}

	return ttl, nil
}

// Scan iterates over keys matching a pattern
func (rc *RedisCache) Scan(pattern string, handler func(key string) error) error {
	if !rc.healthy.Load() {
		return fmt.Errorf("cache is unhealthy")
	}

	ctx := rc.ctx
	var cursor uint64

	for {
		keys, nextCursor, err := rc.client.Scan(ctx, cursor, pattern, 100)
		if err != nil {
			rc.errors.Add(1)
			return fmt.Errorf("scan failed: %w", err)
		}

		for _, key := range keys {
			if err := handler(key); err != nil {
				return err
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return nil
}

// getEnvDefault returns the value of an environment variable or a default value if not set
func getEnvDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
