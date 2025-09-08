// Package common provides typed validation context
package common

import (
	"encoding/json"
	"fmt"
	"time"
)

// ValidationContextValue represents a typed value that can be stored in validation context
type ValidationContextValue struct {
	Type  ValidationContextType `json:"type"`
	Value interface{}           `json:"value"`
}

// ValidationContextType represents the type of value stored in context
type ValidationContextType int

const (
	ValidationContextTypeString ValidationContextType = iota
	ValidationContextTypeInt
	ValidationContextTypeUint64
	ValidationContextTypeBool
	ValidationContextTypeTime
	ValidationContextTypeFloat64
	ValidationContextTypeBytes
	ValidationContextTypeStake
	ValidationContextTypeEventID
	ValidationContextTypeValidatorID
)

// TypedValidationContext provides type-safe context storage
type TypedValidationContext struct {
	values map[string]*ValidationContextValue
}

// NewTypedValidationContext creates a new typed validation context
func NewTypedValidationContext() *TypedValidationContext {
	return &TypedValidationContext{
		values: make(map[string]*ValidationContextValue),
	}
}

// SetString sets a string value in the context
func (c *TypedValidationContext) SetString(key, value string) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeString,
		Value: value,
	}
}

// SetInt sets an int value in the context
func (c *TypedValidationContext) SetInt(key string, value int) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeInt,
		Value: value,
	}
}

// SetUint64 sets a uint64 value in the context
func (c *TypedValidationContext) SetUint64(key string, value uint64) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeUint64,
		Value: value,
	}
}

// SetBool sets a bool value in the context
func (c *TypedValidationContext) SetBool(key string, value bool) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeBool,
		Value: value,
	}
}

// SetTime sets a time value in the context
func (c *TypedValidationContext) SetTime(key string, value time.Time) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeTime,
		Value: value,
	}
}

// SetFloat64 sets a float64 value in the context
func (c *TypedValidationContext) SetFloat64(key string, value float64) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeFloat64,
		Value: value,
	}
}

// SetBytes sets a byte array value in the context
func (c *TypedValidationContext) SetBytes(key string, value []byte) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeBytes,
		Value: value,
	}
}

// SetStake sets a stake value in the context
func (c *TypedValidationContext) SetStake(key string, value uint64) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeStake,
		Value: value,
	}
}

// SetEventID sets an event ID in the context
func (c *TypedValidationContext) SetEventID(key string, value [32]byte) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeEventID,
		Value: value,
	}
}

// SetValidatorID sets a validator ID in the context
func (c *TypedValidationContext) SetValidatorID(key string, value [32]byte) {
	c.values[key] = &ValidationContextValue{
		Type:  ValidationContextTypeValidatorID,
		Value: value,
	}
}

// GetString gets a string value from the context
func (c *TypedValidationContext) GetString(key string) (string, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeString {
		return "", false
	}
	str, ok := v.Value.(string)
	return str, ok
}

// GetInt gets an int value from the context
func (c *TypedValidationContext) GetInt(key string) (int, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeInt {
		return 0, false
	}
	i, ok := v.Value.(int)
	return i, ok
}

// GetUint64 gets a uint64 value from the context
func (c *TypedValidationContext) GetUint64(key string) (uint64, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeUint64 {
		return 0, false
	}
	u, ok := v.Value.(uint64)
	return u, ok
}

// GetBool gets a bool value from the context
func (c *TypedValidationContext) GetBool(key string) (bool, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeBool {
		return false, false
	}
	b, ok := v.Value.(bool)
	return b, ok
}

// GetTime gets a time value from the context
func (c *TypedValidationContext) GetTime(key string) (time.Time, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeTime {
		return time.Time{}, false
	}
	t, ok := v.Value.(time.Time)
	return t, ok
}

// GetFloat64 gets a float64 value from the context
func (c *TypedValidationContext) GetFloat64(key string) (float64, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeFloat64 {
		return 0, false
	}
	f, ok := v.Value.(float64)
	return f, ok
}

// GetBytes gets a byte array value from the context
func (c *TypedValidationContext) GetBytes(key string) ([]byte, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeBytes {
		return nil, false
	}
	b, ok := v.Value.([]byte)
	return b, ok
}

// GetStake gets a stake value from the context
func (c *TypedValidationContext) GetStake(key string) (uint64, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeStake {
		return 0, false
	}
	s, ok := v.Value.(uint64)
	return s, ok
}

// GetEventID gets an event ID from the context
func (c *TypedValidationContext) GetEventID(key string) ([32]byte, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeEventID {
		return [32]byte{}, false
	}
	id, ok := v.Value.([32]byte)
	return id, ok
}

// GetValidatorID gets a validator ID from the context
func (c *TypedValidationContext) GetValidatorID(key string) ([32]byte, bool) {
	v, exists := c.values[key]
	if !exists || v.Type != ValidationContextTypeValidatorID {
		return [32]byte{}, false
	}
	id, ok := v.Value.([32]byte)
	return id, ok
}

// ToMap converts the context to a map for serialization
func (c *TypedValidationContext) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range c.values {
		switch v.Type {
		case ValidationContextTypeEventID, ValidationContextTypeValidatorID:
			if id, ok := v.Value.([32]byte); ok {
				result[k] = fmt.Sprintf("%x", id)
			}
		case ValidationContextTypeBytes:
			if b, ok := v.Value.([]byte); ok {
				result[k] = fmt.Sprintf("%x", b)
			}
		case ValidationContextTypeTime:
			if t, ok := v.Value.(time.Time); ok {
				result[k] = t.Format(time.RFC3339)
			}
		default:
			result[k] = v.Value
		}
	}
	return result
}

// MarshalJSON implements json.Marshaler
func (c *TypedValidationContext) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.ToMap())
}
