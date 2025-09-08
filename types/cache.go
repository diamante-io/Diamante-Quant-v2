// Package types provides cache-specific type definitions
package types

import (
	"encoding/json"
	"fmt"
	"time"

	"diamante/common"
)

// TimeProvider interface allows different time sources
type TimeProvider interface {
	Now() time.Time
}

// ConsensusTimeProvider uses consensus time for deterministic behavior
type ConsensusTimeProvider struct{}

func (ctp *ConsensusTimeProvider) Now() time.Time {
	return common.ConsensusNow()
}

// defaultTimeProvider is used when no time provider is set
var defaultTimeProvider TimeProvider = &ConsensusTimeProvider{}

// CacheOperation represents a cache operation type
type CacheOperation uint8

const (
	CacheOperationGet CacheOperation = iota
	CacheOperationSet
	CacheOperationDelete
	CacheOperationClear
	CacheOperationExpire
	CacheOperationBulkGet
	CacheOperationBulkSet
)

// CacheKey represents a typed cache key
type CacheKey struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   uint64 `json:"version,omitempty"`
}

// String returns the string representation of the cache key
func (k *CacheKey) String() string {
	if k.Namespace != "" {
		return fmt.Sprintf("%s:%s", k.Namespace, k.Key)
	}
	return k.Key
}

// TypedCacheEntry represents a typed cache entry
type TypedCacheEntry struct {
	Key         *CacheKey  `json:"key"`
	Value       *Value     `json:"value"`
	Metadata    *Metadata  `json:"metadata,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	AccessedAt  time.Time  `json:"accessed_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	TTL         int64      `json:"ttl,omitempty"`
	AccessCount uint64     `json:"access_count"`
}

// CacheBulkRequest represents a bulk cache request
type CacheBulkRequest struct {
	Keys []*CacheKey `json:"keys"`
}

// CacheBulkResponse represents a bulk cache response
type CacheBulkResponse struct {
	Entries map[string]*TypedCacheEntry `json:"entries"`
	Missing []*CacheKey                 `json:"missing"`
}

// CacheBulkSetRequest represents a bulk set request
type CacheBulkSetRequest struct {
	Entries []*TypedCacheEntry `json:"entries"`
}

// CacheSerializer provides methods for serializing/deserializing cache values
type CacheSerializer struct{}

// Marshal serializes a Value to bytes for cache storage
func (s *CacheSerializer) Marshal(v *Value) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot marshal nil value")
	}

	// For simple types, we can store the data directly with a type prefix
	prefix := []byte{byte(v.Type)}
	return append(prefix, v.Data...), nil
}

// Unmarshal deserializes bytes from cache storage to a Value
func (s *CacheSerializer) Unmarshal(data []byte) (*Value, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid cache data: too short")
	}

	vtype := ValueType(data[0])
	return &Value{
		Type: vtype,
		Data: data[1:],
	}, nil
}

// ToJSON provides JSON marshaling for cache entries
func (s *CacheSerializer) ToJSON(v *Value) ([]byte, error) {
	wrapper := struct {
		Type ValueType       `json:"type"`
		Data json.RawMessage `json:"data"`
	}{
		Type: v.Type,
	}

	// Convert data based on type for JSON representation
	switch v.Type {
	case ValueTypeString:
		str, _ := v.String()
		wrapper.Data, _ = json.Marshal(str)
	case ValueTypeInt64:
		i64, _ := v.Int64()
		wrapper.Data, _ = json.Marshal(i64)
	case ValueTypeUint64:
		u64, _ := v.Uint64()
		wrapper.Data, _ = json.Marshal(u64)
	case ValueTypeBool:
		b, _ := v.Bool()
		wrapper.Data, _ = json.Marshal(b)
	case ValueTypeFloat64:
		f64, _ := v.Float64()
		wrapper.Data, _ = json.Marshal(f64)
	case ValueTypeJSON:
		wrapper.Data = v.Data
	default:
		// For other types, encode as base64
		wrapper.Data, _ = json.Marshal(v.Data)
	}

	return json.Marshal(wrapper)
}

// FromJSON provides JSON unmarshaling for cache entries
func (s *CacheSerializer) FromJSON(data []byte) (*Value, error) {
	var wrapper struct {
		Type ValueType       `json:"type"`
		Data json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}

	v := &Value{Type: wrapper.Type}

	// Convert data based on type
	switch wrapper.Type {
	case ValueTypeString:
		var str string
		if err := json.Unmarshal(wrapper.Data, &str); err != nil {
			return nil, err
		}
		v.Data = []byte(str)
	case ValueTypeInt64:
		var i64 int64
		if err := json.Unmarshal(wrapper.Data, &i64); err != nil {
			return nil, err
		}
		v.Data = Uint64ToBytes(uint64(i64))
	case ValueTypeUint64:
		var u64 uint64
		if err := json.Unmarshal(wrapper.Data, &u64); err != nil {
			return nil, err
		}
		v.Data = Uint64ToBytes(u64)
	case ValueTypeBool:
		var b bool
		if err := json.Unmarshal(wrapper.Data, &b); err != nil {
			return nil, err
		}
		if b {
			v.Data = []byte{1}
		} else {
			v.Data = []byte{0}
		}
	case ValueTypeFloat64:
		var f64 float64
		if err := json.Unmarshal(wrapper.Data, &f64); err != nil {
			return nil, err
		}
		v.Data = []byte(fmt.Sprintf("%f", f64))
	case ValueTypeJSON:
		v.Data = wrapper.Data
	default:
		// For other types, decode from base64 or raw bytes
		if err := json.Unmarshal(wrapper.Data, &v.Data); err != nil {
			return nil, err
		}
	}

	return v, nil
}

// CacheKeyFormatter provides methods for formatting cache keys
type CacheKeyFormatter struct {
	Prefix string
}

// Format formats a cache key with optional prefix
func (f *CacheKeyFormatter) Format(key *CacheKey) string {
	formatted := key.String()
	if f.Prefix != "" {
		return fmt.Sprintf("%s:%s", f.Prefix, formatted)
	}
	return formatted
}

// Parse parses a formatted string back to a CacheKey
func (f *CacheKeyFormatter) Parse(formatted string) *CacheKey {
	// Remove prefix if present
	if f.Prefix != "" && len(formatted) > len(f.Prefix)+1 {
		if formatted[:len(f.Prefix)+1] == f.Prefix+":" {
			formatted = formatted[len(f.Prefix)+1:]
		}
	}

	// Split namespace and key
	parts := splitFirst(formatted, ":")
	if len(parts) == 2 {
		return &CacheKey{
			Namespace: parts[0],
			Key:       parts[1],
		}
	}

	return &CacheKey{
		Key: formatted,
	}
}

// splitFirst splits a string on the first occurrence of sep
func splitFirst(s, sep string) []string {
	idx := indexOf(s, sep)
	if idx < 0 {
		return []string{s}
	}
	return []string{s[:idx], s[idx+len(sep):]}
}

// indexOf returns the index of the first occurrence of substr in s, or -1 if not found
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// MemoryPoolObject represents a typed object in the memory pool
type MemoryPoolObject struct {
	ID          string    `json:"id"`
	Type        ValueType `json:"type"`
	Value       *Value    `json:"value"`
	Size        uint64    `json:"size"`
	CreatedAt   time.Time `json:"created_at"`
	AccessedAt  time.Time `json:"accessed_at"`
	AccessCount uint64    `json:"access_count"`
	Priority    uint32    `json:"priority"`
	Metadata    *Metadata `json:"metadata,omitempty"`
}

// MemoryPoolFactory provides methods for creating pool objects
type MemoryPoolFactory interface {
	Create() *MemoryPoolObject
	Reset(obj *MemoryPoolObject)
	Validate(obj *MemoryPoolObject) bool
}

// ByteSliceFactory implements MemoryPoolFactory for byte slices
type ByteSliceFactory struct {
	Size int
}

func (f *ByteSliceFactory) Create() *MemoryPoolObject {
	return &MemoryPoolObject{
		Type:      ValueTypeBytes,
		Value:     NewBytesValue(make([]byte, f.Size)),
		Size:      uint64(f.Size),
		CreatedAt: defaultTimeProvider.Now(),
	}
}

func (f *ByteSliceFactory) Reset(obj *MemoryPoolObject) {
	if obj.Value.Type == ValueTypeBytes && len(obj.Value.Data) == f.Size {
		// Clear sensitive data
		for i := range obj.Value.Data {
			obj.Value.Data[i] = 0
		}
	}
	obj.AccessCount = 0
	obj.AccessedAt = time.Time{}
}

func (f *ByteSliceFactory) Validate(obj *MemoryPoolObject) bool {
	return obj != nil &&
		obj.Value != nil &&
		obj.Value.Type == ValueTypeBytes &&
		len(obj.Value.Data) == f.Size
}

// BufferFactory implements MemoryPoolFactory for buffers
type BufferFactory struct {
	InitialSize int
	MaxSize     int
}

// TypedBuffer represents a reusable buffer with type information
type TypedBuffer struct {
	Data     []byte    `json:"data"`
	Type     ValueType `json:"type"`
	Metadata *Metadata `json:"metadata,omitempty"`
}

func (b *TypedBuffer) Reset() {
	b.Data = b.Data[:0]
	b.Metadata = nil
}

func (b *TypedBuffer) Grow(n int) {
	if cap(b.Data) >= n {
		return
	}
	// Allocate new buffer with some headroom
	newSize := n + n/4
	newBuf := make([]byte, len(b.Data), newSize)
	copy(newBuf, b.Data)
	b.Data = newBuf
}

func (b *TypedBuffer) Write(p []byte) (n int, err error) {
	b.Grow(len(b.Data) + len(p))
	b.Data = append(b.Data, p...)
	return len(p), nil
}

func (f *BufferFactory) Create() *MemoryPoolObject {
	buf := &TypedBuffer{
		Data: make([]byte, 0, f.InitialSize),
		Type: ValueTypeBytes,
	}

	// Calculate initial size
	size := uint64(cap(buf.Data) + 16) // 16 bytes for metadata overhead

	return &MemoryPoolObject{
		Type:      ValueTypeBytes,
		Value:     NewCustomValue(ValueTypeBytes, buf),
		Size:      size,
		CreatedAt: defaultTimeProvider.Now(),
	}
}

func (f *BufferFactory) Reset(obj *MemoryPoolObject) {
	if obj.Value.Type == ValueTypeBytes {
		// Try to decode the JSON data to get the TypedBuffer
		var buf TypedBuffer
		if err := json.Unmarshal(obj.Value.Data, &buf); err == nil {
			buf.Reset()
			// Shrink if too large
			if cap(buf.Data) > f.MaxSize {
				buf.Data = make([]byte, 0, f.InitialSize)
			}
			// Re-encode the reset buffer
			obj.Value = NewCustomValue(ValueTypeBytes, &buf)
		}
	}
	obj.AccessCount = 0
	obj.AccessedAt = time.Time{}
}

func (f *BufferFactory) Validate(obj *MemoryPoolObject) bool {
	if obj == nil || obj.Value == nil || obj.Value.Type != ValueTypeBytes {
		return false
	}

	// Try to decode the JSON data to validate it's a TypedBuffer
	var buf TypedBuffer
	err := json.Unmarshal(obj.Value.Data, &buf)
	return err == nil
}

// ConnectionWrapper wraps a connection with metadata
type ConnectionWrapper struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Connection *Value    `json:"connection"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsed   time.Time `json:"last_used"`
	Healthy    bool      `json:"healthy"`
	Metadata   *Metadata `json:"metadata,omitempty"`
}

// ConnectionFactory provides methods for creating connection pool objects
type ConnectionFactory interface {
	Dial() (*ConnectionWrapper, error)
	Close(conn *ConnectionWrapper) error
	Ping(conn *ConnectionWrapper) error
	Validate(conn *ConnectionWrapper) bool
}
