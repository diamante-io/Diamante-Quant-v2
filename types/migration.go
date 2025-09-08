// Package types provides migration utilities for converting interface{} to typed values
package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// Migrator provides utilities for migrating from interface{} to typed values
type Migrator struct {
	strict bool // If true, fail on unknown types; if false, convert to JSON
}

// NewMigrator creates a new migrator
func NewMigrator(strict bool) *Migrator {
	return &Migrator{strict: strict}
}

// MigrateValue converts an interface{} to a typed Value
func (m *Migrator) MigrateValue(v interface{}) (*Value, error) {
	if v == nil {
		return &Value{Type: ValueTypeUnknown, Data: nil}, nil
	}

	// Check if already a Value
	if val, ok := v.(*Value); ok {
		return val, nil
	}

	// Use reflection to determine type
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return StringToValue(rv.String()), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Int64ToValue(rv.Int()), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Uint64ToValue(rv.Uint()), nil

	case reflect.Float32, reflect.Float64:
		// Convert float to string for precision
		str := strconv.FormatFloat(rv.Float(), 'f', -1, 64)
		return &Value{Type: ValueTypeFloat64, Data: []byte(str)}, nil

	case reflect.Bool:
		return BoolToValue(rv.Bool()), nil

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			// []byte
			return BytesToValue(rv.Bytes()), nil
		}
		// Other slices -> JSON
		return m.toJSON(v)

	case reflect.Map, reflect.Struct, reflect.Array:
		// Complex types -> JSON
		return m.toJSON(v)

	default:
		if m.strict {
			return nil, fmt.Errorf("unsupported type: %v", rv.Kind())
		}
		return m.toJSON(v)
	}
}

// MigrateMap converts a map[string]interface{} to a TypedMap
func (m *Migrator) MigrateMap(data map[string]interface{}) (*TypedMap, error) {
	tm := NewTypedMap()

	for key, value := range data {
		typedValue, err := m.MigrateValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate key %s: %w", key, err)
		}
		tm.Set(key, typedValue)
	}

	return tm, nil
}

// MigrateToConsensusData converts generic data to ConsensusData
func (m *Migrator) MigrateToConsensusData(nodeID string, timestamp int64, data map[string]interface{}) (*ConsensusData, error) {
	typedData := make(map[string]*Value)

	for key, value := range data {
		typedValue, err := m.MigrateValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate consensus data key %s: %w", key, err)
		}
		typedData[key] = typedValue
	}

	return &ConsensusData{
		Type:      "migrated",
		NodeID:    nodeID,
		Timestamp: timestamp,
		Data:      typedData,
	}, nil
}

// MigrateToStorageData converts generic key-value pairs to StorageData
func (m *Migrator) MigrateToStorageData(key string, value interface{}, metadata map[string]interface{}) (*StorageData, error) {
	typedValue, err := m.MigrateValue(value)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate storage value: %w", err)
	}

	// Create metadata
	meta := NewMetadata("migration")
	if metadata != nil {
		for k, v := range metadata {
			if val, err := m.MigrateValue(v); err == nil {
				meta.Attributes[k] = val
			}
		}
	}

	return &StorageData{
		Key:      key,
		Value:    typedValue,
		Metadata: meta,
	}, nil
}

// MigrateToNetworkMessage converts generic message data to NetworkMessage
func (m *Migrator) MigrateToNetworkMessage(msgType, sender string, timestamp int64, payload map[string]interface{}) (*NetworkMessage, error) {
	typedPayload := make(map[string]*Value)

	for key, value := range payload {
		typedValue, err := m.MigrateValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate network payload key %s: %w", key, err)
		}
		typedPayload[key] = typedValue
	}

	return &NetworkMessage{
		Type:      msgType,
		Sender:    sender,
		Timestamp: timestamp,
		Payload:   typedPayload,
	}, nil
}

// MigrateCacheEntry converts generic cache data to CacheEntry
func (m *Migrator) MigrateCacheEntry(key string, value interface{}, entryType CacheType, ttl int64) (*CacheEntry, error) {
	data, err := m.valueToBytes(value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert cache value to bytes: %w", err)
	}

	return &CacheEntry{
		Key:       key,
		Value:     data,
		EntryType: entryType,
		Timestamp: 0, // Will be set by caller
		TTL:       ttl,
	}, nil
}

// Helper to convert value to JSON
func (m *Migrator) toJSON(v interface{}) (*Value, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
	}
	return &Value{Type: ValueTypeJSON, Data: data}, nil
}

// Helper to convert value to bytes
func (m *Migrator) valueToBytes(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		// Try JSON encoding
		return json.Marshal(v)
	}
}

// BatchMigrator helps migrate large amounts of data
type BatchMigrator struct {
	migrator     *Migrator
	batchSize    int
	errorCount   int
	successCount int
}

// NewBatchMigrator creates a new batch migrator
func NewBatchMigrator(strict bool, batchSize int) *BatchMigrator {
	return &BatchMigrator{
		migrator:  NewMigrator(strict),
		batchSize: batchSize,
	}
}

// MigrateMapBatch migrates a large map in batches
func (b *BatchMigrator) MigrateMapBatch(data map[string]interface{}) (*TypedMap, []error) {
	tm := NewTypedMap()
	errors := []error{}

	batch := make(map[string]interface{})
	count := 0

	for key, value := range data {
		batch[key] = value
		count++

		if count >= b.batchSize {
			if err := b.processBatch(tm, batch); err != nil {
				errors = append(errors, err...)
			}
			batch = make(map[string]interface{})
			count = 0
		}
	}

	// Process remaining items
	if len(batch) > 0 {
		if err := b.processBatch(tm, batch); err != nil {
			errors = append(errors, err...)
		}
	}

	return tm, errors
}

func (b *BatchMigrator) processBatch(tm *TypedMap, batch map[string]interface{}) []error {
	errors := []error{}

	for key, value := range batch {
		typedValue, err := b.migrator.MigrateValue(value)
		if err != nil {
			errors = append(errors, fmt.Errorf("key %s: %w", key, err))
			b.errorCount++
		} else {
			tm.Set(key, typedValue)
			b.successCount++
		}
	}

	return errors
}

// GetStats returns migration statistics
func (b *BatchMigrator) GetStats() (success int, errors int) {
	return b.successCount, b.errorCount
}

// TypeDetector helps detect the appropriate type for interface{} values
type TypeDetector struct{}

// DetectType attempts to determine the most appropriate ValueType
func (t *TypeDetector) DetectType(v interface{}) ValueType {
	if v == nil {
		return ValueTypeUnknown
	}

	switch v.(type) {
	case string:
		return ValueTypeString
	case int, int8, int16, int32, int64:
		return ValueTypeInt64
	case uint, uint8, uint16, uint32, uint64:
		return ValueTypeUint64
	case float32, float64:
		return ValueTypeFloat64
	case bool:
		return ValueTypeBool
	case []byte:
		return ValueTypeBytes
	default:
		// Check if it's a known type that should be JSON
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map, reflect.Struct, reflect.Array, reflect.Slice:
			return ValueTypeJSON
		default:
			return ValueTypeUnknown
		}
	}
}

// MigrationHelper provides convenience functions for common migrations
type MigrationHelper struct {
	migrator *Migrator
}

// NewMigrationHelper creates a new migration helper
func NewMigrationHelper() *MigrationHelper {
	return &MigrationHelper{
		migrator: NewMigrator(false), // Non-strict mode by default
	}
}

// MigrateConfigMap migrates a configuration map
func (h *MigrationHelper) MigrateConfigMap(config map[string]interface{}) (map[string]*ConfigValue, error) {
	result := make(map[string]*ConfigValue)

	for key, value := range config {
		typedValue, err := h.migrator.MigrateValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate config key %s: %w", key, err)
		}

		result[key] = &ConfigValue{
			Key:      key,
			Value:    typedValue,
			Metadata: NewMetadata("config-migration"),
		}
	}

	return result, nil
}

// MigrateEventData migrates event data
func (h *MigrationHelper) MigrateEventData(eventType, source string, timestamp int64, data map[string]interface{}) (*EventData, error) {
	typedData := make(map[string]*Value)

	for key, value := range data {
		typedValue, err := h.migrator.MigrateValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate event data key %s: %w", key, err)
		}
		typedData[key] = typedValue
	}

	return &EventData{
		Type:      eventType,
		Source:    source,
		Timestamp: timestamp,
		Data:      typedData,
	}, nil
}
