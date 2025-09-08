package optimization

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/types"

	"github.com/sirupsen/logrus"
)

// TypedPoolItem represents an item that can be pooled
type TypedPoolItem interface {
	// Reset resets the item to its initial state
	Reset()
	// Validate checks if the item is valid for reuse
	Validate() bool
	// GetType returns the type identifier for this item
	GetType() string
}

// TypedMemoryPool is a type-safe object pool
type TypedMemoryPool[T TypedPoolItem] struct {
	pool         sync.Pool
	newFunc      func() T
	resetFunc    func(T)
	validateFunc func(T) bool

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
	healthMonitor *TypedPoolHealthMonitor[T]
}

// TypedPoolHealthMonitor monitors typed pool health
type TypedPoolHealthMonitor[T TypedPoolItem] struct {
	pool          *TypedMemoryPool[T]
	ticker        *time.Ticker
	lastCheckTime atomic.Int64
	isHealthy     atomic.Bool
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewTypedMemoryPool creates a new type-safe pool
func NewTypedMemoryPool[T TypedPoolItem](newFunc func() T, config *PoolConfig) *TypedMemoryPool[T] {
	if newFunc == nil {
		// Return nil instead of panic - caller should check for nil
		logger := logrus.New()
		logger.Error("NewTypedMemoryPool: newFunc cannot be nil")
		return nil
	}

	if config == nil {
		config = DefaultPoolConfig("unnamed")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	p := &TypedMemoryPool[T]{
		newFunc: newFunc,
		config:  config,
		logger:  logger,
	}

	// Set up the sync.Pool with custom New function
	p.pool.New = func() interface{} {
		p.created.Add(1)
		return newFunc()
	}

	// Set default reset and validate functions
	p.resetFunc = func(item T) {
		item.Reset()
	}
	p.validateFunc = func(item T) bool {
		return item.Validate()
	}

	// Pre-warm the pool if configured
	if config.InitialSize > 0 {
		p.prewarm()
	}

	// Start health monitoring if enabled
	if config.EnableHealthCheck {
		p.healthMonitor = &TypedPoolHealthMonitor[T]{
			pool:   p,
			stopCh: make(chan struct{}),
		}
		p.healthMonitor.Start()
	}

	// Start GC routine if configured
	if config.GCInterval > 0 {
		go p.gcRoutine()
	}

	return p
}

// Get retrieves an object from the pool
func (p *TypedMemoryPool[T]) Get() T {
	p.gets.Add(1)

	// Try to get from pool first
	if obj := p.pool.Get(); obj != nil {
		if item, ok := obj.(T); ok {
			// Validate if enabled
			if p.config.EnableValidation && p.validateFunc != nil {
				if !p.validateFunc(item) {
					p.validationFail.Add(1)
					p.logger.WithField("pool", p.config.Name).Warn("Object failed validation")
					// Create new object instead
					return p.newFunc()
				}
			}
			return item
		}
	}

	// Create new object if pool is empty or type assertion failed
	return p.newFunc()
}

// Put returns an object to the pool
func (p *TypedMemoryPool[T]) Put(item T) {
	// Check if item is zero value (equivalent to nil check for pointers)
	var zero T
	if any(item) == any(zero) {
		return
	}

	p.puts.Add(1)

	// Reset if enabled
	if p.config.EnableReset && p.resetFunc != nil {
		p.resetFunc(item)
		p.resets.Add(1)
	}

	// Return to pool
	p.pool.Put(item)
}

// SetResetFunc sets a custom reset function
func (p *TypedMemoryPool[T]) SetResetFunc(resetFunc func(T)) {
	p.resetFunc = resetFunc
}

// SetValidateFunc sets a custom validate function
func (p *TypedMemoryPool[T]) SetValidateFunc(validateFunc func(T) bool) {
	p.validateFunc = validateFunc
}

// GetStats returns current pool statistics
func (p *TypedMemoryPool[T]) GetStats() PoolStats {
	created := p.created.Load()
	gets := p.gets.Load()
	puts := p.puts.Load()

	hitRate := float64(0)
	if gets > 0 {
		hitRate = float64(gets-created) / float64(gets)
		if hitRate < 0 {
			hitRate = 0
		}
	}

	healthy := true
	lastCheck := time.Time{}
	if p.healthMonitor != nil {
		healthy = p.healthMonitor.isHealthy.Load()
		lastCheckUnix := p.healthMonitor.lastCheckTime.Load()
		if lastCheckUnix > 0 {
			lastCheck = time.Unix(lastCheckUnix, 0)
		}
	}

	return PoolStats{
		Created:        created,
		Gets:           gets,
		Puts:           puts,
		Resets:         p.resets.Load(),
		ValidationFail: p.validationFail.Load(),
		HitRate:        hitRate,
		Healthy:        healthy,
		LastCheck:      lastCheck,
	}
}

// prewarm pre-populates the pool
func (p *TypedMemoryPool[T]) prewarm() {
	items := make([]T, p.config.InitialSize)
	for i := 0; i < p.config.InitialSize; i++ {
		items[i] = p.newFunc()
	}
	for _, item := range items {
		p.pool.Put(item)
	}
	p.logger.WithFields(logrus.Fields{
		"pool":  p.config.Name,
		"count": p.config.InitialSize,
	}).Info("Pool pre-warmed")
}

// gcRoutine runs periodic garbage collection
func (p *TypedMemoryPool[T]) gcRoutine() {
	ticker := time.NewTicker(p.config.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		runtime.GC()
		stats := p.GetStats()
		p.logger.WithFields(logrus.Fields{
			"pool":    p.config.Name,
			"created": stats.Created,
			"hitRate": fmt.Sprintf("%.2f%%", stats.HitRate*100),
		}).Debug("Pool GC cycle completed")
	}
}

// Stop stops the pool and its health monitor
func (p *TypedMemoryPool[T]) Stop() {
	if p.healthMonitor != nil {
		p.healthMonitor.Stop()
	}
}

// Start starts the health monitor
func (m *TypedPoolHealthMonitor[T]) Start() {
	m.ticker = time.NewTicker(m.pool.config.HealthCheckInterval)
	m.wg.Add(1)
	go m.monitorLoop()
}

// Stop stops the health monitor
func (m *TypedPoolHealthMonitor[T]) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.wg.Wait()
}

// monitorLoop runs the health monitoring loop
func (m *TypedPoolHealthMonitor[T]) monitorLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ticker.C:
			m.performHealthCheck()
		case <-m.stopCh:
			return
		}
	}
}

// performHealthCheck performs a health check
func (m *TypedPoolHealthMonitor[T]) performHealthCheck() {
	stats := m.pool.GetStats()

	// Check if pool is healthy based on metrics
	healthy := true

	// Check hit rate
	if stats.HitRate < 0.1 && stats.Gets > 100 {
		healthy = false
		m.pool.logger.WithFields(logrus.Fields{
			"pool":    m.pool.config.Name,
			"hitRate": fmt.Sprintf("%.2f%%", stats.HitRate*100),
		}).Warn("Pool hit rate is low")
	}

	// Check validation failures
	if stats.ValidationFail > 0 && float64(stats.ValidationFail)/float64(stats.Gets) > 0.1 {
		healthy = false
		m.pool.logger.WithFields(logrus.Fields{
			"pool":           m.pool.config.Name,
			"validationFail": stats.ValidationFail,
		}).Warn("High validation failure rate")
	}

	m.isHealthy.Store(healthy)
	m.lastCheckTime.Store(time.Now().Unix())

	// Log health status
	m.pool.logger.WithFields(logrus.Fields{
		"pool":    m.pool.config.Name,
		"healthy": healthy,
		"stats":   stats,
	}).Debug("Health check completed")
}

// Specific typed pool implementations

// ByteSlicePoolItem implements TypedPoolItem for byte slices
type ByteSlicePoolItem struct {
	Data []byte
	size int
}

// NewByteSlicePoolItem creates a new byte slice pool item
func NewByteSlicePoolItem(size int) *ByteSlicePoolItem {
	return &ByteSlicePoolItem{
		Data: make([]byte, size),
		size: size,
	}
}

// Reset implements TypedPoolItem
func (b *ByteSlicePoolItem) Reset() {
	// Clear the byte slice
	for i := range b.Data {
		b.Data[i] = 0
	}
}

// Validate implements TypedPoolItem
func (b *ByteSlicePoolItem) Validate() bool {
	return len(b.Data) == b.size
}

// GetType implements TypedPoolItem
func (b *ByteSlicePoolItem) GetType() string {
	return fmt.Sprintf("byte_slice_%d", b.size)
}

// BufferPoolItem implements TypedPoolItem for buffers
type BufferPoolItem struct {
	Buffer []byte
}

// NewBufferPoolItem creates a new buffer pool item
func NewBufferPoolItem() *BufferPoolItem {
	return &BufferPoolItem{
		Buffer: make([]byte, 4096),
	}
}

// Reset implements TypedPoolItem
func (b *BufferPoolItem) Reset() {
	// Clear the byte slice
	for i := range b.Buffer {
		b.Buffer[i] = 0
	}
	b.Buffer = b.Buffer[:0] // Reset length to 0
}

// Validate implements TypedPoolItem
func (b *BufferPoolItem) Validate() bool {
	return b.Buffer != nil
}

// GetType implements TypedPoolItem
func (b *BufferPoolItem) GetType() string {
	return "buffer"
}

// ValuePoolItem implements TypedPoolItem for types.Value
type ValuePoolItem struct {
	Value *types.Value
}

// NewValuePoolItem creates a new value pool item
func NewValuePoolItem() *ValuePoolItem {
	return &ValuePoolItem{
		Value: types.NewValue(types.ValueTypeNull, nil),
	}
}

// Reset implements TypedPoolItem
func (v *ValuePoolItem) Reset() {
	v.Value.Type = types.ValueTypeNull
	v.Value.Data = nil
}

// Validate implements TypedPoolItem
func (v *ValuePoolItem) Validate() bool {
	return v.Value != nil
}

// GetType implements TypedPoolItem
func (v *ValuePoolItem) GetType() string {
	return "value"
}

// CreateTypedByteSlicePool creates a typed pool for byte slices
func CreateTypedByteSlicePool(size int, config *PoolConfig) *TypedMemoryPool[*ByteSlicePoolItem] {
	return NewTypedMemoryPool(func() *ByteSlicePoolItem {
		return NewByteSlicePoolItem(size)
	}, config)
}

// CreateTypedBufferPool creates a typed pool for buffers
func CreateTypedBufferPool(config *PoolConfig) *TypedMemoryPool[*BufferPoolItem] {
	return NewTypedMemoryPool(func() *BufferPoolItem {
		return NewBufferPoolItem()
	}, config)
}

// CreateTypedValuePool creates a typed pool for values
func CreateTypedValuePool(config *PoolConfig) *TypedMemoryPool[*ValuePoolItem] {
	return NewTypedMemoryPool(func() *ValuePoolItem {
		return NewValuePoolItem()
	}, config)
}
