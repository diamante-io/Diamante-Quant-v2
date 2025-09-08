// Package common provides typed metrics structures
package common

import (
	"fmt"
	"time"
)

// MetricValue represents a typed metric value
type MetricValue struct {
	Type  MetricType  `json:"type"`
	Value interface{} `json:"-"` // Internal use only, will be replaced
}

// MetricType represents the type of metric value
type MetricType int

const (
	MetricTypeUint64 MetricType = iota
	MetricTypeFloat64
	MetricTypeDuration
	MetricTypeTimestamp
	MetricTypeString
	MetricTypeTimerStats
)

// TimerStats represents timer statistics
type TimerStats struct {
	Count uint64        `json:"count"`
	Total time.Duration `json:"total"`
	Last  time.Duration `json:"last"`
	Avg   time.Duration `json:"avg,omitempty"`
	Min   time.Duration `json:"min,omitempty"`
	Max   time.Duration `json:"max,omitempty"`
}

// TypedMetricValue provides type-safe metric storage
type TypedMetricValue struct {
	Uint64Value     uint64
	Float64Value    float64
	DurationValue   time.Duration
	TimestampValue  time.Time
	StringValue     string
	TimerStatsValue *TimerStats
	Type            MetricType
}

// NewUint64Metric creates a uint64 metric value
func NewUint64Metric(value uint64) *TypedMetricValue {
	return &TypedMetricValue{
		Uint64Value: value,
		Type:        MetricTypeUint64,
	}
}

// NewFloat64Metric creates a float64 metric value
func NewFloat64Metric(value float64) *TypedMetricValue {
	return &TypedMetricValue{
		Float64Value: value,
		Type:         MetricTypeFloat64,
	}
}

// NewDurationMetric creates a duration metric value
func NewDurationMetric(value time.Duration) *TypedMetricValue {
	return &TypedMetricValue{
		DurationValue: value,
		Type:          MetricTypeDuration,
	}
}

// NewTimestampMetric creates a timestamp metric value
func NewTimestampMetric(value time.Time) *TypedMetricValue {
	return &TypedMetricValue{
		TimestampValue: value,
		Type:           MetricTypeTimestamp,
	}
}

// NewStringMetric creates a string metric value
func NewStringMetric(value string) *TypedMetricValue {
	return &TypedMetricValue{
		StringValue: value,
		Type:        MetricTypeString,
	}
}

// NewTimerStatsMetric creates a timer stats metric value
func NewTimerStatsMetric(stats *TimerStats) *TypedMetricValue {
	return &TypedMetricValue{
		TimerStatsValue: stats,
		Type:            MetricTypeTimerStats,
	}
}

// String returns the string representation of the metric value
func (tmv *TypedMetricValue) String() string {
	switch tmv.Type {
	case MetricTypeUint64:
		return fmt.Sprintf("%d", tmv.Uint64Value)
	case MetricTypeFloat64:
		return fmt.Sprintf("%f", tmv.Float64Value)
	case MetricTypeDuration:
		return tmv.DurationValue.String()
	case MetricTypeTimestamp:
		return tmv.TimestampValue.Format(time.RFC3339)
	case MetricTypeString:
		return tmv.StringValue
	case MetricTypeTimerStats:
		if tmv.TimerStatsValue != nil {
			return fmt.Sprintf("count=%d,avg=%v,min=%v,max=%v",
				tmv.TimerStatsValue.Count,
				tmv.TimerStatsValue.Avg,
				tmv.TimerStatsValue.Min,
				tmv.TimerStatsValue.Max)
		}
		return "null"
	default:
		return "unknown"
	}
}

// AsUint64 returns the value as uint64 if the type matches
func (tmv *TypedMetricValue) AsUint64() (uint64, bool) {
	if tmv.Type != MetricTypeUint64 {
		return 0, false
	}
	return tmv.Uint64Value, true
}

// AsFloat64 returns the value as float64 if the type matches
func (tmv *TypedMetricValue) AsFloat64() (float64, bool) {
	if tmv.Type != MetricTypeFloat64 {
		return 0, false
	}
	return tmv.Float64Value, true
}

// AsDuration returns the value as duration if the type matches
func (tmv *TypedMetricValue) AsDuration() (time.Duration, bool) {
	if tmv.Type != MetricTypeDuration {
		return 0, false
	}
	return tmv.DurationValue, true
}

// AsTimestamp returns the value as timestamp if the type matches
func (tmv *TypedMetricValue) AsTimestamp() (time.Time, bool) {
	if tmv.Type != MetricTypeTimestamp {
		return time.Time{}, false
	}
	return tmv.TimestampValue, true
}

// AsString returns the value as string if the type matches
func (tmv *TypedMetricValue) AsString() (string, bool) {
	if tmv.Type != MetricTypeString {
		return "", false
	}
	return tmv.StringValue, true
}

// AsTimerStats returns the value as timer stats if the type matches
func (tmv *TypedMetricValue) AsTimerStats() (*TimerStats, bool) {
	if tmv.Type != MetricTypeTimerStats {
		return nil, false
	}
	return tmv.TimerStatsValue, true
}

// TypedMetricsMap represents a map of typed metrics
type TypedMetricsMap map[string]*TypedMetricValue

// GetUint64 gets a uint64 metric value
func (tmm TypedMetricsMap) GetUint64(key string) (uint64, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsUint64()
	}
	return 0, false
}

// GetFloat64 gets a float64 metric value
func (tmm TypedMetricsMap) GetFloat64(key string) (float64, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsFloat64()
	}
	return 0, false
}

// GetDuration gets a duration metric value
func (tmm TypedMetricsMap) GetDuration(key string) (time.Duration, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsDuration()
	}
	return 0, false
}

// GetTimestamp gets a timestamp metric value
func (tmm TypedMetricsMap) GetTimestamp(key string) (time.Time, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsTimestamp()
	}
	return time.Time{}, false
}

// GetString gets a string metric value
func (tmm TypedMetricsMap) GetString(key string) (string, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsString()
	}
	return "", false
}

// GetTimerStats gets timer stats metric value
func (tmm TypedMetricsMap) GetTimerStats(key string) (*TimerStats, bool) {
	if v, exists := tmm[key]; exists {
		return v.AsTimerStats()
	}
	return nil, false
}

// SetUint64 sets a uint64 metric value
func (tmm TypedMetricsMap) SetUint64(key string, value uint64) {
	tmm[key] = NewUint64Metric(value)
}

// SetFloat64 sets a float64 metric value
func (tmm TypedMetricsMap) SetFloat64(key string, value float64) {
	tmm[key] = NewFloat64Metric(value)
}

// SetDuration sets a duration metric value
func (tmm TypedMetricsMap) SetDuration(key string, value time.Duration) {
	tmm[key] = NewDurationMetric(value)
}

// SetTimestamp sets a timestamp metric value
func (tmm TypedMetricsMap) SetTimestamp(key string, value time.Time) {
	tmm[key] = NewTimestampMetric(value)
}

// SetString sets a string metric value
func (tmm TypedMetricsMap) SetString(key string, value string) {
	tmm[key] = NewStringMetric(value)
}

// SetTimerStats sets timer stats metric value
func (tmm TypedMetricsMap) SetTimerStats(key string, value *TimerStats) {
	tmm[key] = NewTimerStatsMetric(value)
}

// Clone creates a copy of the metrics map
func (tmm TypedMetricsMap) Clone() TypedMetricsMap {
	clone := make(TypedMetricsMap, len(tmm))
	for k, v := range tmm {
		// Create a new TypedMetricValue with the same values
		newValue := &TypedMetricValue{
			Uint64Value:    v.Uint64Value,
			Float64Value:   v.Float64Value,
			DurationValue:  v.DurationValue,
			TimestampValue: v.TimestampValue,
			StringValue:    v.StringValue,
			Type:           v.Type,
		}
		if v.TimerStatsValue != nil {
			newValue.TimerStatsValue = &TimerStats{
				Count: v.TimerStatsValue.Count,
				Total: v.TimerStatsValue.Total,
				Last:  v.TimerStatsValue.Last,
				Avg:   v.TimerStatsValue.Avg,
				Min:   v.TimerStatsValue.Min,
				Max:   v.TimerStatsValue.Max,
			}
		}
		clone[k] = newValue
	}
	return clone
}

// Merge merges another metrics map into this one
func (tmm TypedMetricsMap) Merge(other TypedMetricsMap) {
	for k, v := range other {
		tmm[k] = v
	}
}
