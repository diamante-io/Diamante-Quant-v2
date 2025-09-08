package optimization

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// Pool is a production-ready object pool with advanced features
type Pool struct {
	pool         sync.Pool
	newFunc      func() interface{}
	resetFunc    func(interface{})
	validateFunc func(interface{}) bool

	// Metrics
	created        atomic.Int64
	gets           atomic.Int64
	puts           atomic.Int64
	resets         atomic.Int64
	validationFail atomic.Int64

	// Configuration
	config *PoolConfig

	// Health monitoring
	logger        *logrus.Logger
	healthMonitor *PoolHealthMonitor
}

// PoolConfig holds configuration for the pool
type PoolConfig struct {
	Name                string
	InitialSize         int
	MaxSize             int
	EnableMetrics       bool
	EnableHealthCheck   bool
	HealthCheckInterval time.Duration
	GCInterval          time.Duration
	EnableReset         bool
	EnableValidation    bool
}

// DefaultPoolConfig returns production-ready default configuration
func DefaultPoolConfig(name string) *PoolConfig {
	return &PoolConfig{
		Name:                name,
		InitialSize:         10,
		MaxSize:             1000,
		EnableMetrics:       true,
		EnableHealthCheck:   true,
		HealthCheckInterval: 30 * time.Second,
		GCInterval:          5 * time.Minute,
		EnableReset:         true,
		EnableValidation:    true,
	}
}

// PoolHealthMonitor monitors pool health
type PoolHealthMonitor struct {
	pool          *Pool
	ticker        *time.Ticker
	lastCheckTime atomic.Int64
	isHealthy     atomic.Bool
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// PoolStats holds pool statistics
type PoolStats struct {
	Created        int64
	Gets           int64
	Puts           int64
	Resets         int64
	ValidationFail int64
	HitRate        float64
	Healthy        bool
	LastCheck      time.Time
}

// NewPool creates a new production-ready pool
func NewPool(newFunc func() interface{}, config *PoolConfig) *Pool {
	if newFunc == nil {
		// Return a safe pool that creates empty interfaces
		// This prevents panic but logs the issue
		logger := logrus.New()
		logger.Error("NewPool called with nil newFunc - using default empty interface factory")
		newFunc = func() interface{} { return struct{}{} }
	}

	if config == nil {
		config = DefaultPoolConfig("unnamed")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	p := &Pool{
		newFunc: newFunc,
		config:  config,
		logger:  logger,
	}

	// Set up the sync.Pool with custom New function
	p.pool.New = func() interface{} {
		p.created.Add(1)
		return newFunc()
	}

	// Pre-warm the pool if configured
	if config.InitialSize > 0 {
		p.prewarm()
	}

	// Start health monitoring if enabled
	if config.EnableHealthCheck {
		p.healthMonitor = &PoolHealthMonitor{
			pool:   p,
			stopCh: make(chan struct{}),
		}
		p.healthMonitor.Start()
	}

	// Start GC ticker if configured
	if config.GCInterval > 0 {
		go p.gcRoutine()
	}

	return p
}

// SetResetFunc sets a function to reset objects before returning to pool
func (p *Pool) SetResetFunc(resetFunc func(interface{})) {
	p.resetFunc = resetFunc
}

// SetValidateFunc sets a function to validate objects before reuse
func (p *Pool) SetValidateFunc(validateFunc func(interface{}) bool) {
	p.validateFunc = validateFunc
}

// SetLogger sets a custom logger
func (p *Pool) SetLogger(logger *logrus.Logger) {
	if logger != nil {
		p.logger = logger
	}
}

// Get retrieves an object from the pool
func (p *Pool) Get() interface{} {
	p.gets.Add(1)

	obj := p.pool.Get()

	// Validate object if validation is enabled
	if p.config.EnableValidation && p.validateFunc != nil {
		if !p.validateFunc(obj) {
			p.validationFail.Add(1)
			p.logger.WithField("pool", p.config.Name).Debug("Object failed validation, creating new one")
			// Create a new object if validation fails
			obj = p.newFunc()
			p.created.Add(1)
		}
	}

	return obj
}

// Put returns an object to the pool
func (p *Pool) Put(x interface{}) {
	if x == nil {
		return
	}

	p.puts.Add(1)

	// Reset object if reset function is provided and enabled
	if p.config.EnableReset && p.resetFunc != nil {
		p.resetFunc(x)
		p.resets.Add(1)
	}

	// Validate before putting back if validation is enabled
	if p.config.EnableValidation && p.validateFunc != nil {
		if !p.validateFunc(x) {
			p.validationFail.Add(1)
			p.logger.WithField("pool", p.config.Name).Debug("Object failed validation, not returning to pool")
			return
		}
	}

	p.pool.Put(x)
}

// GetStats returns pool statistics
func (p *Pool) GetStats() PoolStats {
	gets := p.gets.Load()
	puts := p.puts.Load()

	hitRate := float64(0)
	if gets > 0 {
		hits := gets - p.created.Load()
		if hits > 0 {
			hitRate = float64(hits) / float64(gets)
		}
	}

	stats := PoolStats{
		Created:        p.created.Load(),
		Gets:           gets,
		Puts:           puts,
		Resets:         p.resets.Load(),
		ValidationFail: p.validationFail.Load(),
		HitRate:        hitRate,
	}

	if p.healthMonitor != nil {
		stats.Healthy = p.healthMonitor.isHealthy.Load()
		stats.LastCheck = time.Unix(p.healthMonitor.lastCheckTime.Load(), 0)
	}

	return stats
}

// prewarm pre-fills the pool with objects
func (p *Pool) prewarm() {
	objects := make([]interface{}, p.config.InitialSize)

	// Create objects
	for i := 0; i < p.config.InitialSize; i++ {
		objects[i] = p.Get()
	}

	// Put them back
	for _, obj := range objects {
		p.Put(obj)
	}

	p.logger.WithFields(logrus.Fields{
		"pool":  p.config.Name,
		"count": p.config.InitialSize,
	}).Info("Pool pre-warmed")
}

// gcRoutine periodically triggers garbage collection
func (p *Pool) gcRoutine() {
	ticker := time.NewTicker(p.config.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		runtime.GC()

		if p.config.EnableMetrics {
			stats := p.GetStats()
			p.logger.WithFields(logrus.Fields{
				"pool":     p.config.Name,
				"created":  stats.Created,
				"hit_rate": fmt.Sprintf("%.2f%%", stats.HitRate*100),
			}).Debug("Pool GC cycle completed")
		}
	}
}

// Close shuts down the pool
func (p *Pool) Close() error {
	// Stop health monitor
	if p.healthMonitor != nil {
		p.healthMonitor.Stop()
	}

	p.logger.WithField("pool", p.config.Name).Info("Pool closed")
	return nil
}

// Health Monitor methods

// Start begins health monitoring
func (hm *PoolHealthMonitor) Start() {
	hm.ticker = time.NewTicker(hm.pool.config.HealthCheckInterval)
	hm.isHealthy.Store(true)

	hm.wg.Add(1)
	go func() {
		defer hm.wg.Done()

		for {
			select {
			case <-hm.stopCh:
				return
			case <-hm.ticker.C:
				hm.performCheck()
			}
		}
	}()

	// Perform initial check
	hm.performCheck()
}

// Stop stops health monitoring
func (hm *PoolHealthMonitor) Stop() {
	if hm.ticker != nil {
		hm.ticker.Stop()
	}

	close(hm.stopCh)
	hm.wg.Wait()
}

// performCheck performs a health check
func (hm *PoolHealthMonitor) performCheck() {
	hm.lastCheckTime.Store(common.ConsensusNow().Unix())

	// Simple health check based on hit rate and validation failures
	stats := hm.pool.GetStats()

	isHealthy := true

	// Check hit rate (warn if too low)
	if stats.Gets > 100 && stats.HitRate < 0.5 {
		hm.pool.logger.WithFields(logrus.Fields{
			"pool":     hm.pool.config.Name,
			"hit_rate": fmt.Sprintf("%.2f%%", stats.HitRate*100),
		}).Warn("Pool hit rate is low")
		isHealthy = false
	}

	// Check validation failures
	if stats.ValidationFail > 0 {
		failRate := float64(stats.ValidationFail) / float64(stats.Gets)
		if failRate > 0.1 { // More than 10% validation failures
			hm.pool.logger.WithFields(logrus.Fields{
				"pool":      hm.pool.config.Name,
				"fail_rate": fmt.Sprintf("%.2f%%", failRate*100),
			}).Warn("High validation failure rate")
			isHealthy = false
		}
	}

	hm.isHealthy.Store(isHealthy)
}

// Specialized pool implementations

// ByteSlicePool is a specialized pool for byte slices
type ByteSlicePool struct {
	*Pool
	size int
}

// NewByteSlicePool creates a pool for byte slices of specific size
func NewByteSlicePool(size int, config *PoolConfig) *ByteSlicePool {
	if config == nil {
		config = DefaultPoolConfig(fmt.Sprintf("byte_slice_%d", size))
	}

	pool := &ByteSlicePool{
		size: size,
	}

	pool.Pool = NewPool(func() interface{} {
		return make([]byte, size)
	}, config)

	// Set reset function to clear the slice
	pool.SetResetFunc(func(obj interface{}) {
		if b, ok := obj.([]byte); ok {
			// Clear sensitive data
			for i := range b {
				b[i] = 0
			}
		}
	})

	// Set validation function
	pool.SetValidateFunc(func(obj interface{}) bool {
		b, ok := obj.([]byte)
		return ok && len(b) == size
	})

	return pool
}

// Get retrieves a byte slice from the pool
func (p *ByteSlicePool) Get() []byte {
	return p.Pool.Get().([]byte)
}

// Put returns a byte slice to the pool
func (p *ByteSlicePool) Put(b []byte) {
	if len(b) != p.size {
		// Don't put back slices of wrong size
		return
	}
	p.Pool.Put(b)
}

// BufferPool is a specialized pool for buffers
type BufferPool struct {
	*Pool
	initialSize int
	maxSize     int
}

// Buffer represents a reusable buffer
type Buffer struct {
	B []byte
}

// Reset resets the buffer
func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

// Grow grows the buffer to at least n bytes
func (b *Buffer) Grow(n int) {
	if cap(b.B) >= n {
		return
	}

	// Allocate new buffer with some headroom
	newSize := n + n/4
	newBuf := make([]byte, len(b.B), newSize)
	copy(newBuf, b.B)
	b.B = newBuf
}

// Write implements io.Writer
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.Grow(len(b.B) + len(p))
	b.B = append(b.B, p...)
	return len(p), nil
}

// NewBufferPool creates a pool for buffers
func NewBufferPool(initialSize, maxSize int, config *PoolConfig) *BufferPool {
	if config == nil {
		config = DefaultPoolConfig("buffer_pool")
	}

	pool := &BufferPool{
		initialSize: initialSize,
		maxSize:     maxSize,
	}

	pool.Pool = NewPool(func() interface{} {
		return &Buffer{
			B: make([]byte, 0, initialSize),
		}
	}, config)

	// Set reset function
	pool.SetResetFunc(func(obj interface{}) {
		if buf, ok := obj.(*Buffer); ok {
			buf.Reset()
			// Shrink if too large
			if cap(buf.B) > maxSize {
				buf.B = make([]byte, 0, initialSize)
			}
		}
	})

	// Set validation function
	pool.SetValidateFunc(func(obj interface{}) bool {
		buf, ok := obj.(*Buffer)
		return ok && buf != nil
	})

	return pool
}

// Get retrieves a buffer from the pool
func (p *BufferPool) Get() *Buffer {
	return p.Pool.Get().(*Buffer)
}

// Put returns a buffer to the pool
func (p *BufferPool) Put(buf *Buffer) {
	if buf == nil {
		return
	}
	p.Pool.Put(buf)
}

// ConnectionPool is a generic connection pool
type ConnectionPool struct {
	*Pool
	dial    func() (interface{}, error)
	close   func(interface{}) error
	ping    func(interface{}) error
	maxIdle time.Duration
	connMap sync.Map // Track connection creation time
}

// PooledConnection wraps a connection with metadata
type PooledConnection struct {
	Conn      interface{}
	CreatedAt time.Time
	LastUsed  time.Time
}

// NewConnectionPool creates a generic connection pool
func NewConnectionPool(
	dial func() (interface{}, error),
	close func(interface{}) error,
	ping func(interface{}) error,
	maxIdle time.Duration,
	config *PoolConfig,
) *ConnectionPool {
	if config == nil {
		config = DefaultPoolConfig("connection_pool")
	}

	cp := &ConnectionPool{
		dial:    dial,
		close:   close,
		ping:    ping,
		maxIdle: maxIdle,
	}

	cp.Pool = NewPool(func() interface{} {
		conn, err := dial()
		if err != nil {
			cp.logger.WithError(err).Error("Failed to create connection")
			return nil
		}

		pc := &PooledConnection{
			Conn:      conn,
			CreatedAt: common.ConsensusNow(),
			LastUsed:  common.ConsensusNow(),
		}

		return pc
	}, config)

	// Set validation function
	cp.SetValidateFunc(func(obj interface{}) bool {
		pc, ok := obj.(*PooledConnection)
		if !ok || pc == nil || pc.Conn == nil {
			return false
		}

		// Check if connection is too old
		if time.Since(pc.CreatedAt) > maxIdle {
			if close != nil {
				close(pc.Conn)
			}
			return false
		}

		// Ping to check if connection is alive
		if ping != nil {
			if err := ping(pc.Conn); err != nil {
				if close != nil {
					close(pc.Conn)
				}
				return false
			}
		}

		return true
	})

	// Set reset function
	cp.SetResetFunc(func(obj interface{}) {
		if pc, ok := obj.(*PooledConnection); ok {
			pc.LastUsed = common.ConsensusNow()
		}
	})

	return cp
}

// Get retrieves a connection from the pool
func (p *ConnectionPool) Get() (interface{}, error) {
	obj := p.Pool.Get()
	if obj == nil {
		return nil, fmt.Errorf("failed to get connection from pool")
	}

	pc, ok := obj.(*PooledConnection)
	if !ok || pc == nil || pc.Conn == nil {
		return nil, fmt.Errorf("invalid connection from pool")
	}

	return pc.Conn, nil
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn interface{}) {
	if conn == nil {
		return
	}

	// Find the pooled connection wrapper
	p.connMap.Range(func(key, value interface{}) bool {
		if pc, ok := value.(*PooledConnection); ok && pc.Conn == conn {
			p.Pool.Put(pc)
			return false
		}
		return true
	})
}

// Close closes all connections and shuts down the pool
func (p *ConnectionPool) Close() error {
	// Close all connections
	p.connMap.Range(func(key, value interface{}) bool {
		if pc, ok := value.(*PooledConnection); ok && pc.Conn != nil {
			if p.close != nil {
				p.close(pc.Conn)
			}
		}
		return true
	})

	return p.Pool.Close()
}
