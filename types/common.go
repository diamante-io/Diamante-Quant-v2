// Package types provides common type definitions used across the Diamante blockchain
package types

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"diamante/common"
)

// ValueType represents the type of a typed value
type ValueType uint8

const (
	ValueTypeUnknown ValueType = iota
	ValueTypeString
	ValueTypeInt64
	ValueTypeUint64
	ValueTypeFloat64
	ValueTypeBool
	ValueTypeBytes
	ValueTypeJSON
	ValueTypeAddress
	ValueTypeHash
	ValueTypeSignature
	ValueTypePublicKey
	ValueTypePrivateKey
	ValueTypeTimestamp
	ValueTypeBlockData
	ValueTypeTransactionData
	ValueTypeStateData
	ValueTypeConfigData
	ValueTypeNull
	ValueTypeInt   = ValueTypeInt64   // Alias for Int64
	ValueTypeFloat = ValueTypeFloat64 // Alias for Float64
)

// Value represents a typed value that can be safely used across modules
type Value struct {
	Type ValueType
	Data []byte
}

// NewValue creates a new typed value
func NewValue(vtype ValueType, data []byte) *Value {
	return &Value{
		Type: vtype,
		Data: data,
	}
}

// String returns the string representation of the value
func (v *Value) String() (string, error) {
	if v.Type != ValueTypeString {
		return "", fmt.Errorf("value is not a string, type: %d", v.Type)
	}
	return string(v.Data), nil
}

// Int64 returns the int64 representation of the value
func (v *Value) Int64() (int64, error) {
	if v.Type != ValueTypeInt64 {
		return 0, fmt.Errorf("value is not an int64, type: %d", v.Type)
	}
	if len(v.Data) != 8 {
		return 0, fmt.Errorf("invalid int64 data length: %d", len(v.Data))
	}
	return int64(BytesToUint64(v.Data)), nil
}

// Uint64 returns the uint64 representation of the value
func (v *Value) Uint64() (uint64, error) {
	if v.Type != ValueTypeUint64 {
		return 0, fmt.Errorf("value is not a uint64, type: %d", v.Type)
	}
	if len(v.Data) != 8 {
		return 0, fmt.Errorf("invalid uint64 data length: %d", len(v.Data))
	}
	return BytesToUint64(v.Data), nil
}

// Bool returns the bool representation of the value
func (v *Value) Bool() (bool, error) {
	if v.Type != ValueTypeBool {
		return false, fmt.Errorf("value is not a bool, type: %d", v.Type)
	}
	if len(v.Data) != 1 {
		return false, fmt.Errorf("invalid bool data length: %d", len(v.Data))
	}
	return v.Data[0] != 0, nil
}

// Float64 returns the float64 representation of the value
func (v *Value) Float64() (float64, error) {
	if v.Type != ValueTypeFloat64 {
		return 0, fmt.Errorf("value is not a float64, type: %d", v.Type)
	}
	return strconv.ParseFloat(string(v.Data), 64)
}

// Bytes returns the raw bytes of the value
func (v *Value) Bytes() []byte {
	return v.Data
}

// BytesToUint64 converts bytes to uint64
func BytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// Uint64ToBytes converts uint64 to bytes
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}

// Metadata represents common metadata for blockchain entities
type Metadata struct {
	Created    time.Time         `json:"created"`
	Modified   time.Time         `json:"modified"`
	Version    uint64            `json:"version"`
	Creator    string            `json:"creator"`
	Modifier   string            `json:"modifier"`
	Tags       map[string]string `json:"tags,omitempty"`
	Attributes map[string]*Value `json:"attributes,omitempty"`
}

// NewMetadata creates new metadata with current timestamp
func NewMetadata(creator string) *Metadata {
	now := common.ConsensusNow()
	return &Metadata{
		Created:    now,
		Modified:   now,
		Version:    1,
		Creator:    creator,
		Modifier:   creator,
		Tags:       make(map[string]string),
		Attributes: make(map[string]*Value),
	}
}

// Update updates the metadata for a modification
func (m *Metadata) Update(modifier string) {
	m.Modified = common.ConsensusNow()
	m.Version++
	m.Modifier = modifier
}

// BlockchainData represents data that can be stored on the blockchain
type BlockchainData struct {
	Type     string    `json:"type"`
	Data     []byte    `json:"data"`
	Metadata *Metadata `json:"metadata"`
}

// ConsensusData represents data used in consensus operations
type ConsensusData struct {
	Type      string            `json:"type"`
	NodeID    string            `json:"node_id"`
	Timestamp int64             `json:"timestamp"`
	Data      map[string]*Value `json:"data"`
}

// TransactionMetadata represents typed metadata for transactions
type TransactionMetadata struct {
	// Standard fields
	Category    string `json:"category,omitempty"`
	Type        string `json:"type,omitempty"`
	Priority    int    `json:"priority,omitempty"`
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination,omitempty"`

	// String properties
	Properties map[string]string `json:"properties,omitempty"`

	// Typed values for complex data
	Values map[string]*Value `json:"values,omitempty"`

	// Numeric values
	Metrics map[string]uint64 `json:"metrics,omitempty"`

	// Boolean flags
	Flags map[string]bool `json:"flags,omitempty"`

	// Timestamps
	Timestamps map[string]time.Time `json:"timestamps,omitempty"`
}

// NewTransactionMetadata creates a new transaction metadata
func NewTransactionMetadata() *TransactionMetadata {
	return &TransactionMetadata{
		Properties: make(map[string]string),
		Values:     make(map[string]*Value),
		Metrics:    make(map[string]uint64),
		Flags:      make(map[string]bool),
		Timestamps: make(map[string]time.Time),
	}
}

// Set sets a typed value in the metadata
func (tm *TransactionMetadata) Set(key string, vtype ValueType, data []byte) {
	if tm.Values == nil {
		tm.Values = make(map[string]*Value)
	}
	tm.Values[key] = NewValue(vtype, data)
}

// Get retrieves a typed value from the metadata
func (tm *TransactionMetadata) Get(key string) (*Value, bool) {
	if tm.Values == nil {
		return nil, false
	}
	val, ok := tm.Values[key]
	return val, ok
}

// StorageData represents data for storage operations
type StorageData struct {
	Key      string    `json:"key"`
	Value    *Value    `json:"value"`
	Metadata *Metadata `json:"metadata"`
}

// NetworkMessage represents a typed network message
type NetworkMessage struct {
	Type      string            `json:"type"`
	Sender    string            `json:"sender"`
	Recipient string            `json:"recipient,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Payload   map[string]*Value `json:"payload"`
	Signature []byte            `json:"signature,omitempty"`
}

// TransactionData represents typed transaction data
type TransactionData struct {
	Type       string            `json:"type"`
	From       string            `json:"from"`
	To         string            `json:"to,omitempty"`
	Value      uint64            `json:"value"`
	Data       []byte            `json:"data,omitempty"`
	Parameters map[string]*Value `json:"parameters,omitempty"`
}

// StateEntry represents a state storage entry
type StateEntry struct {
	Key       string    `json:"key"`
	Value     *Value    `json:"value"`
	StateRoot string    `json:"state_root"`
	Proof     [][]byte  `json:"proof,omitempty"`
	Metadata  *Metadata `json:"metadata"`
}

// ConfigValue represents a configuration value
type ConfigValue struct {
	Key         string    `json:"key"`
	Value       *Value    `json:"value"`
	Description string    `json:"description"`
	Validator   string    `json:"validator,omitempty"`
	Metadata    *Metadata `json:"metadata"`
}

// EventData represents typed event data
type EventData struct {
	Type       string            `json:"type"`
	Source     string            `json:"source"`
	Timestamp  int64             `json:"timestamp"`
	Data       map[string]*Value `json:"data"`
	Sequential bool              `json:"sequential"`
}

// Result represents a typed operation result
type Result struct {
	Success bool              `json:"success"`
	Data    map[string]*Value `json:"data,omitempty"`
	Error   string            `json:"error,omitempty"`
	Code    int               `json:"code"`
}

// Helper functions for type conversions

// StringToValue converts a string to a typed value
func StringToValue(s string) *Value {
	return &Value{
		Type: ValueTypeString,
		Data: []byte(s),
	}
}

// Int64ToValue converts an int64 to a typed value
func Int64ToValue(i int64) *Value {
	return &Value{
		Type: ValueTypeInt64,
		Data: Uint64ToBytes(uint64(i)),
	}
}

// Uint64ToValue converts a uint64 to a typed value
func Uint64ToValue(u uint64) *Value {
	return &Value{
		Type: ValueTypeUint64,
		Data: Uint64ToBytes(u),
	}
}

// BoolToValue converts a bool to a typed value
func BoolToValue(b bool) *Value {
	data := byte(0)
	if b {
		data = 1
	}
	return &Value{
		Type: ValueTypeBool,
		Data: []byte{data},
	}
}

// BytesToValue converts bytes to a typed value
func BytesToValue(b []byte) *Value {
	return &Value{
		Type: ValueTypeBytes,
		Data: b,
	}
}

// FloatToValue converts a float64 to a typed value
func FloatToValue(f float64) *Value {
	data, _ := json.Marshal(f)
	return &Value{
		Type: ValueTypeFloat64,
		Data: data,
	}
}

// IntToValue converts an int64 to a typed value (alias for Int64ToValue)
func IntToValue(i int64) *Value {
	return Int64ToValue(i)
}

// NewBoolValue creates a new boolean Value
func NewBoolValue(b bool) *Value {
	data := []byte{0}
	if b {
		data[0] = 1
	}
	return &Value{
		Type: ValueTypeBool,
		Data: data,
	}
}

// JSONToValue converts a JSON-marshalable object to a typed value
func JSONToValue(v interface{}) (*Value, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
	}
	return &Value{
		Type: ValueTypeJSON,
		Data: data,
	}, nil
}

// ValueToJSON unmarshals a JSON value
func ValueToJSON(v *Value, target interface{}) error {
	if v.Type != ValueTypeJSON {
		return fmt.Errorf("value is not JSON type")
	}
	return json.Unmarshal(v.Data, target)
}

// TypedMap provides a type-safe map implementation
type TypedMap struct {
	data map[string]*Value
}

// NewTypedMap creates a new typed map
func NewTypedMap() *TypedMap {
	return &TypedMap{
		data: make(map[string]*Value),
	}
}

// Set sets a value in the map
func (m *TypedMap) Set(key string, value *Value) {
	m.data[key] = value
}

// Get retrieves a value from the map
func (m *TypedMap) Get(key string) (*Value, bool) {
	v, ok := m.data[key]
	return v, ok
}

// GetString retrieves a string value
func (m *TypedMap) GetString(key string) (string, error) {
	v, ok := m.data[key]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return v.String()
}

// GetInt64 retrieves an int64 value
func (m *TypedMap) GetInt64(key string) (int64, error) {
	v, ok := m.data[key]
	if !ok {
		return 0, fmt.Errorf("key not found: %s", key)
	}
	return v.Int64()
}

// GetBool retrieves a bool value
func (m *TypedMap) GetBool(key string) (bool, error) {
	v, ok := m.data[key]
	if !ok {
		return false, fmt.Errorf("key not found: %s", key)
	}
	return v.Bool()
}

// Keys returns all keys in the map
func (m *TypedMap) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of items in the map
func (m *TypedMap) Len() int {
	return len(m.data)
}

// Clear removes all items from the map
func (m *TypedMap) Clear() {
	m.data = make(map[string]*Value)
}

// InterfaceToValue converts an interface{} to a typed Value
func InterfaceToValue(v interface{}) *Value {
	switch val := v.(type) {
	case string:
		return NewValue(ValueTypeString, []byte(val))
	case int:
		return NewValue(ValueTypeUint64, Uint64ToBytes(uint64(val)))
	case int32:
		return NewValue(ValueTypeUint64, Uint64ToBytes(uint64(val)))
	case int64:
		return NewValue(ValueTypeUint64, Uint64ToBytes(uint64(val)))
	case uint:
		return NewValue(ValueTypeUint64, Uint64ToBytes(uint64(val)))
	case uint32:
		return NewValue(ValueTypeUint64, Uint64ToBytes(uint64(val)))
	case uint64:
		return NewValue(ValueTypeUint64, Uint64ToBytes(val))
	case float32:
		return NewValue(ValueTypeFloat64, []byte(fmt.Sprintf("%f", val)))
	case float64:
		return NewValue(ValueTypeFloat64, []byte(fmt.Sprintf("%f", val)))
	case bool:
		if val {
			return NewValue(ValueTypeBool, []byte{1})
		}
		return NewValue(ValueTypeBool, []byte{0})
	case time.Time:
		return NewValue(ValueTypeTimestamp, []byte(val.Format(time.RFC3339)))
	case []byte:
		return NewValue(ValueTypeBytes, val)
	case nil:
		return NewValue(ValueTypeNull, nil)
	default:
		// For complex types, try JSON encoding
		if data, err := json.Marshal(val); err == nil {
			return NewValue(ValueTypeJSON, data)
		}
		// Fallback to string representation
		return NewValue(ValueTypeString, []byte(fmt.Sprintf("%v", val)))
	}
}

// NewBytesValue creates a new Value with byte array type
func NewBytesValue(data []byte) *Value {
	return &Value{
		Type: ValueTypeBytes,
		Data: data,
	}
}

// CustomValue represents a value that holds custom data with JSON encoding
type CustomValue struct {
	Type ValueType
	Data interface{}
}

// NewCustomValue creates a new Value that holds custom data
func NewCustomValue(vtype ValueType, data interface{}) *Value {
	// Encode the custom data as JSON
	encoded, _ := json.Marshal(data)
	return &Value{
		Type: vtype,
		Data: encoded,
	}
}

// GetCustom decodes the custom value data
func (v *Value) GetCustom() interface{} {
	if v.Data == nil {
		return nil
	}

	var result interface{}
	// Try to decode as JSON
	if err := json.Unmarshal(v.Data, &result); err == nil {
		return result
	}

	// Return raw data if JSON decode fails
	return v.Data
}

// MarshalJSON implements json.Marshaler for Value
func (v *Value) MarshalJSON() ([]byte, error) {
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

// UnmarshalJSON implements json.Unmarshaler for Value
func (v *Value) UnmarshalJSON(data []byte) error {
	var wrapper struct {
		Type ValueType       `json:"type"`
		Data json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}

	v.Type = wrapper.Type

	// Convert JSON data back to binary based on type
	switch wrapper.Type {
	case ValueTypeString:
		var str string
		if err := json.Unmarshal(wrapper.Data, &str); err != nil {
			return err
		}
		v.Data = []byte(str)
	case ValueTypeInt64:
		var i64 int64
		if err := json.Unmarshal(wrapper.Data, &i64); err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(i64))
		v.Data = buf
	case ValueTypeUint64:
		var u64 uint64
		if err := json.Unmarshal(wrapper.Data, &u64); err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, u64)
		v.Data = buf
	case ValueTypeBool:
		var b bool
		if err := json.Unmarshal(wrapper.Data, &b); err != nil {
			return err
		}
		if b {
			v.Data = []byte{1}
		} else {
			v.Data = []byte{0}
		}
	case ValueTypeFloat64:
		var f64 float64
		if err := json.Unmarshal(wrapper.Data, &f64); err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(f64))
		v.Data = buf
	case ValueTypeJSON:
		v.Data = wrapper.Data
	default:
		// For other types, decode from base64 or raw
		if err := json.Unmarshal(wrapper.Data, &v.Data); err != nil {
			return err
		}
	}

	return nil
}
