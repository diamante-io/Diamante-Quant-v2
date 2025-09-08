// Package types provides tests for common type definitions
package types

import (
	"testing"
	"time"

	dtypes "diamante/types"
)

func TestValue(t *testing.T) {
	tests := []struct {
		name     string
		create   func() *dtypes.Value
		validate func(*testing.T, *dtypes.Value)
	}{
		{
			name: "String value",
			create: func() *dtypes.Value {
				return dtypes.StringToValue("test string")
			},
			validate: func(t *testing.T, v *dtypes.Value) {
				if v.Type != dtypes.ValueTypeString {
					t.Errorf("Expected type %d, got %d", dtypes.ValueTypeString, v.Type)
				}
				str, err := v.String()
				if err != nil {
					t.Errorf("Failed to get string: %v", err)
				}
				if str != "test string" {
					t.Errorf("Expected 'test string', got '%s'", str)
				}
			},
		},
		{
			name: "Int64 value",
			create: func() *dtypes.Value {
				return dtypes.Int64ToValue(42)
			},
			validate: func(t *testing.T, v *dtypes.Value) {
				if v.Type != dtypes.ValueTypeInt64 {
					t.Errorf("Expected type %d, got %d", dtypes.ValueTypeInt64, v.Type)
				}
				val, err := v.Int64()
				if err != nil {
					t.Errorf("Failed to get int64: %v", err)
				}
				if val != 42 {
					t.Errorf("Expected 42, got %d", val)
				}
			},
		},
		{
			name: "Bool value",
			create: func() *dtypes.Value {
				return dtypes.BoolToValue(true)
			},
			validate: func(t *testing.T, v *dtypes.Value) {
				if v.Type != dtypes.ValueTypeBool {
					t.Errorf("Expected type %d, got %d", dtypes.ValueTypeBool, v.Type)
				}
				val, err := v.Bool()
				if err != nil {
					t.Errorf("Failed to get bool: %v", err)
				}
				if !val {
					t.Errorf("Expected true, got false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := tt.create()
			tt.validate(t, value)
		})
	}
}

func TestTypedMap(t *testing.T) {
	m := dtypes.NewTypedMap()

	// Test Set and Get
	m.Set("string", dtypes.StringToValue("hello"))
	m.Set("number", dtypes.Int64ToValue(123))
	m.Set("bool", dtypes.BoolToValue(true))

	// Test GetString
	str, err := m.GetString("string")
	if err != nil {
		t.Errorf("Failed to get string: %v", err)
	}
	if str != "hello" {
		t.Errorf("Expected 'hello', got '%s'", str)
	}

	// Test GetInt64
	num, err := m.GetInt64("number")
	if err != nil {
		t.Errorf("Failed to get int64: %v", err)
	}
	if num != 123 {
		t.Errorf("Expected 123, got %d", num)
	}

	// Test GetBool
	b, err := m.GetBool("bool")
	if err != nil {
		t.Errorf("Failed to get bool: %v", err)
	}
	if !b {
		t.Errorf("Expected true, got false")
	}

	// Test non-existent key
	_, err = m.GetString("nonexistent")
	if err == nil {
		t.Errorf("Expected error for non-existent key")
	}

	// Test Keys and Len
	keys := m.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	if m.Len() != 3 {
		t.Errorf("Expected length 3, got %d", m.Len())
	}

	// Test Clear
	m.Clear()
	if m.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", m.Len())
	}
}

func TestMetadata(t *testing.T) {
	creator := "test-creator"
	meta := dtypes.NewMetadata(creator)

	if meta.Creator != creator {
		t.Errorf("Expected creator %s, got %s", creator, meta.Creator)
	}

	if meta.Version != 1 {
		t.Errorf("Expected version 1, got %d", meta.Version)
	}

	// Test Update
	modifier := "test-modifier"
	originalModified := meta.Modified
	time.Sleep(10 * time.Millisecond) // Ensure time difference

	meta.Update(modifier)

	if meta.Modifier != modifier {
		t.Errorf("Expected modifier %s, got %s", modifier, meta.Modifier)
	}

	if meta.Version != 2 {
		t.Errorf("Expected version 2, got %d", meta.Version)
	}

	if !meta.Modified.After(originalModified) {
		t.Errorf("Modified time should be updated")
	}
}

func TestJSONValue(t *testing.T) {
	type TestStruct struct {
		Name   string `json:"name"`
		Count  int    `json:"count"`
		Active bool   `json:"active"`
	}

	original := TestStruct{
		Name:   "test",
		Count:  42,
		Active: true,
	}

	// Convert to JSON value
	value, err := dtypes.JSONToValue(original)
	if err != nil {
		t.Fatalf("Failed to convert to JSON value: %v", err)
	}

	if value.Type != dtypes.ValueTypeJSON {
		t.Errorf("Expected type %d, got %d", dtypes.ValueTypeJSON, value.Type)
	}

	// Convert back from JSON value
	var result TestStruct
	err = dtypes.ValueToJSON(value, &result)
	if err != nil {
		t.Fatalf("Failed to convert from JSON value: %v", err)
	}

	if result.Name != original.Name {
		t.Errorf("Expected name %s, got %s", original.Name, result.Name)
	}

	if result.Count != original.Count {
		t.Errorf("Expected count %d, got %d", original.Count, result.Count)
	}

	if result.Active != original.Active {
		t.Errorf("Expected active %v, got %v", original.Active, result.Active)
	}
}

func TestConsensusData(t *testing.T) {
	data := &dtypes.ConsensusData{
		Type:      "test",
		NodeID:    "node-123",
		Timestamp: time.Now().Unix(),
		Data:      make(map[string]*dtypes.Value),
	}

	// Add typed data
	data.Data["height"] = dtypes.Uint64ToValue(1000)
	data.Data["hash"] = dtypes.BytesToValue([]byte{0x01, 0x02, 0x03})
	data.Data["valid"] = dtypes.BoolToValue(true)

	// Verify data
	height, err := data.Data["height"].Uint64()
	if err != nil || height != 1000 {
		t.Errorf("Expected height 1000, got %d", height)
	}

	hash := data.Data["hash"].Bytes()
	if len(hash) != 3 || hash[0] != 0x01 {
		t.Errorf("Unexpected hash value")
	}

	valid, err := data.Data["valid"].Bool()
	if err != nil || !valid {
		t.Errorf("Expected valid true")
	}
}

func TestByteConversion(t *testing.T) {
	// Test dtypes.Uint64ToBytes and dtypes.BytesToUint64
	testValues := []uint64{0, 1, 255, 256, 65535, 4294967295, 18446744073709551615}

	for _, original := range testValues {
		bytes := dtypes.Uint64ToBytes(original)
		if len(bytes) != 8 {
			t.Errorf("Expected 8 bytes, got %d", len(bytes))
		}

		result := dtypes.BytesToUint64(bytes)
		if result != original {
			t.Errorf("Expected %d, got %d", original, result)
		}
	}

	// Test with invalid byte length
	invalidBytes := []byte{1, 2, 3} // Only 3 bytes
	result := dtypes.BytesToUint64(invalidBytes)
	if result != 0 {
		t.Errorf("Expected 0 for invalid bytes, got %d", result)
	}
}

func BenchmarkTypedMap(b *testing.B) {
	m := dtypes.NewTypedMap()

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		m.Set(string(rune(i)), dtypes.Int64ToValue(int64(i)))
	}

	b.ResetTimer()

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set("bench", dtypes.Int64ToValue(int64(i)))
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get("bench")
		}
	})

	b.Run("GetInt64", func(b *testing.B) {
		m.Set("bench", dtypes.Int64ToValue(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.GetInt64("bench")
		}
	})
}

func BenchmarkValueConversion(b *testing.B) {
	b.Run("dtypes.StringToValue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dtypes.StringToValue("benchmark string")
		}
	})

	b.Run("dtypes.Int64ToValue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dtypes.Int64ToValue(int64(i))
		}
	})

	b.Run("dtypes.JSONToValue", func(b *testing.B) {
		data := map[string]string{"key": "value"}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = dtypes.JSONToValue(data)
		}
	})
}
