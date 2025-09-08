package optimization

import (
	"encoding/json"
	"fmt"
	"time"

	dtypes "diamante/types"
)

// TypedPool wraps Pool to provide type-safe operations with MemoryPoolObject
type TypedPool struct {
	*Pool
}

// NewTypedPool creates a new typed pool
func NewTypedPool(factory dtypes.MemoryPoolFactory, config *PoolConfig) *TypedPool {
	pool := NewPool(func() interface{} {
		return factory.Create()
	}, config)

	pool.SetResetFunc(func(obj interface{}) {
		if poolObj, ok := obj.(*dtypes.MemoryPoolObject); ok {
			factory.Reset(poolObj)
		}
	})

	pool.SetValidateFunc(func(obj interface{}) bool {
		poolObj, ok := obj.(*dtypes.MemoryPoolObject)
		if !ok {
			return false
		}
		return factory.Validate(poolObj)
	})

	return &TypedPool{Pool: pool}
}

// Get retrieves a typed object from the pool
func (tp *TypedPool) Get() *dtypes.MemoryPoolObject {
	obj := tp.Pool.Get()
	if poolObj, ok := obj.(*dtypes.MemoryPoolObject); ok {
		poolObj.AccessedAt = time.Now()
		poolObj.AccessCount++
		return poolObj
	}
	// This shouldn't happen if factory is working correctly
	return nil
}

// Put returns a typed object to the pool
func (tp *TypedPool) Put(obj *dtypes.MemoryPoolObject) {
	if obj == nil {
		return
	}
	tp.Pool.Put(obj)
}

// TypedByteSlicePool is a specialized pool for byte slices using typed objects
type TypedByteSlicePool struct {
	*TypedPool
	size int
}

// NewTypedByteSlicePool creates a typed byte slice pool
func NewTypedByteSlicePool(size int, config *PoolConfig) *TypedByteSlicePool {
	if config == nil {
		config = DefaultPoolConfig(fmt.Sprintf("typed_byte_slice_%d", size))
	}

	factory := &dtypes.ByteSliceFactory{Size: size}
	typedPool := NewTypedPool(factory, config)

	return &TypedByteSlicePool{
		TypedPool: typedPool,
		size:      size,
	}
}

// GetBytes retrieves a byte slice from the pool
func (p *TypedByteSlicePool) GetBytes() []byte {
	obj := p.Get()
	if obj != nil && obj.Value != nil && obj.Value.Type == dtypes.ValueTypeBytes {
		return obj.Value.Data
	}
	// Fallback to creating new slice
	return make([]byte, p.size)
}

// PutBytes returns a byte slice to the pool
func (p *TypedByteSlicePool) PutBytes(data []byte) {
	if len(data) != p.size {
		// Don't put back slices of wrong size
		return
	}

	obj := &dtypes.MemoryPoolObject{
		Type:       dtypes.ValueTypeBytes,
		Value:      dtypes.NewBytesValue(data),
		Size:       uint64(p.size),
		AccessedAt: time.Now(),
	}

	p.Put(obj)
}

// TypedBufferPool is a specialized pool for buffers using typed objects
type TypedBufferPool struct {
	*TypedPool
	initialSize int
	maxSize     int
}

// NewTypedBufferPool creates a typed buffer pool
func NewTypedBufferPool(initialSize, maxSize int, config *PoolConfig) *TypedBufferPool {
	if config == nil {
		config = DefaultPoolConfig("typed_buffer_pool")
	}

	factory := &dtypes.BufferFactory{
		InitialSize: initialSize,
		MaxSize:     maxSize,
	}
	typedPool := NewTypedPool(factory, config)

	return &TypedBufferPool{
		TypedPool:   typedPool,
		initialSize: initialSize,
		maxSize:     maxSize,
	}
}

// GetBuffer retrieves a buffer from the pool
func (p *TypedBufferPool) GetBuffer() *dtypes.TypedBuffer {
	obj := p.Get()
	if obj != nil && obj.Value != nil && obj.Value.Type == dtypes.ValueTypeBytes {
		// Try to extract the TypedBuffer from JSON
		custom := obj.Value.GetCustom()
		if custom != nil {
			// Try to convert the custom data to TypedBuffer
			data, err := json.Marshal(custom)
			if err == nil {
				var buf dtypes.TypedBuffer
				if err := json.Unmarshal(data, &buf); err == nil {
					return &buf
				}
			}
		}
	}

	// Fallback to creating new buffer
	return &dtypes.TypedBuffer{
		Data: make([]byte, 0, p.initialSize),
		Type: dtypes.ValueTypeBytes,
	}
}

// PutBuffer returns a buffer to the pool
func (p *TypedBufferPool) PutBuffer(buf *dtypes.TypedBuffer) {
	if buf == nil {
		return
	}

	obj := &dtypes.MemoryPoolObject{
		Type:       dtypes.ValueTypeBytes,
		Value:      dtypes.NewCustomValue(dtypes.ValueTypeBytes, buf),
		Size:       uint64(cap(buf.Data) + 16),
		AccessedAt: time.Now(),
	}

	p.Put(obj)
}

// TypedConnectionPool is a generic connection pool using typed objects
type TypedConnectionPool struct {
	*TypedPool
	factory dtypes.ConnectionFactory
	maxIdle time.Duration
}

// NewTypedConnectionPool creates a typed connection pool
func NewTypedConnectionPool(factory dtypes.ConnectionFactory, maxIdle time.Duration, config *PoolConfig) *TypedConnectionPool {
	if config == nil {
		config = DefaultPoolConfig("typed_connection_pool")
	}

	pool := NewPool(func() interface{} {
		conn, err := factory.Dial()
		if err != nil {
			// Log error and return nil
			return nil
		}

		return &dtypes.MemoryPoolObject{
			ID:        conn.ID,
			Type:      dtypes.ValueTypeJSON,
			Value:     dtypes.NewCustomValue(dtypes.ValueTypeJSON, conn),
			CreatedAt: conn.CreatedAt,
			Metadata:  conn.Metadata,
		}
	}, config)

	pool.SetValidateFunc(func(obj interface{}) bool {
		poolObj, ok := obj.(*dtypes.MemoryPoolObject)
		if !ok || poolObj == nil || poolObj.Value == nil {
			return false
		}

		// Check if connection is too old
		if time.Since(poolObj.CreatedAt) > maxIdle {
			// Extract connection and close it
			if custom := poolObj.Value.GetCustom(); custom != nil {
				if data, err := json.Marshal(custom); err == nil {
					var conn dtypes.ConnectionWrapper
					if err := json.Unmarshal(data, &conn); err == nil {
						factory.Close(&conn)
					}
				}
			}
			return false
		}

		// Extract and validate connection
		if custom := poolObj.Value.GetCustom(); custom != nil {
			if data, err := json.Marshal(custom); err == nil {
				var conn dtypes.ConnectionWrapper
				if err := json.Unmarshal(data, &conn); err == nil {
					return factory.Validate(&conn)
				}
			}
		}

		return false
	})

	pool.SetResetFunc(func(obj interface{}) {
		if poolObj, ok := obj.(*dtypes.MemoryPoolObject); ok {
			poolObj.AccessedAt = time.Now()
		}
	})

	typedPool := &TypedPool{Pool: pool}

	return &TypedConnectionPool{
		TypedPool: typedPool,
		factory:   factory,
		maxIdle:   maxIdle,
	}
}

// GetConnection retrieves a connection from the pool
func (p *TypedConnectionPool) GetConnection() (*dtypes.ConnectionWrapper, error) {
	obj := p.Get()
	if obj == nil || obj.Value == nil {
		return nil, fmt.Errorf("failed to get connection from pool")
	}

	// Extract connection from the typed object
	if custom := obj.Value.GetCustom(); custom != nil {
		if data, err := json.Marshal(custom); err == nil {
			var conn dtypes.ConnectionWrapper
			if err := json.Unmarshal(data, &conn); err == nil {
				conn.LastUsed = time.Now()
				return &conn, nil
			}
		}
	}

	return nil, fmt.Errorf("failed to extract connection from pool object")
}

// PutConnection returns a connection to the pool
func (p *TypedConnectionPool) PutConnection(conn *dtypes.ConnectionWrapper) {
	if conn == nil {
		return
	}

	obj := &dtypes.MemoryPoolObject{
		ID:         conn.ID,
		Type:       dtypes.ValueTypeJSON,
		Value:      dtypes.NewCustomValue(dtypes.ValueTypeJSON, conn),
		CreatedAt:  conn.CreatedAt,
		AccessedAt: time.Now(),
		Metadata:   conn.Metadata,
	}

	p.Put(obj)
}

// Close closes all connections and shuts down the pool
func (p *TypedConnectionPool) Close() error {
	// The underlying pool doesn't track connections separately,
	// so we just close the pool itself
	return p.TypedPool.Pool.Close()
}
