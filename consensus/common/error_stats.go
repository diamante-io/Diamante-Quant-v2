// Package common provides typed error statistics
package common

import (
	"time"

	"diamante/consensus"
)

// ErrorStats represents typed error statistics
type ErrorStats struct {
	TotalErrors         uint64                         `json:"total_errors"`
	Uptime              time.Duration                  `json:"uptime"`
	ErrorCounts         map[ConsensusErrorCode]uint64  `json:"error_counts"`
	ErrorRates          map[ConsensusErrorCode]float64 `json:"error_rates"`
	MostFrequentErrors  map[string]uint64              `json:"most_frequent_errors"`
	LastErrorTime       time.Time                      `json:"last_error_time"`
	ErrorsByCategory    map[ErrorCategory]uint64       `json:"errors_by_category"`
	RecoverableErrors   uint64                         `json:"recoverable_errors"`
	UnrecoverableErrors uint64                         `json:"unrecoverable_errors"`
}

// NewErrorStats creates a new error stats instance
func NewErrorStats() *ErrorStats {
	return &ErrorStats{
		ErrorCounts:        make(map[ConsensusErrorCode]uint64),
		ErrorRates:         make(map[ConsensusErrorCode]float64),
		MostFrequentErrors: make(map[string]uint64),
		ErrorsByCategory:   make(map[ErrorCategory]uint64),
	}
}

// AddError updates the statistics with a new error
func (es *ErrorStats) AddError(code ConsensusErrorCode, category ErrorCategory, recoverable bool) {
	es.TotalErrors++
	es.ErrorCounts[code]++
	es.ErrorsByCategory[category]++
	es.LastErrorTime = consensus.ConsensusNow()

	if recoverable {
		es.RecoverableErrors++
	} else {
		es.UnrecoverableErrors++
	}
}

// CalculateRates calculates error rates based on uptime
func (es *ErrorStats) CalculateRates(uptime time.Duration) {
	if uptime.Seconds() == 0 {
		return
	}

	uptimeHours := uptime.Hours()
	for code, count := range es.ErrorCounts {
		es.ErrorRates[code] = float64(count) / uptimeHours
	}
}

// GetTopErrors returns the top N most frequent errors
func (es *ErrorStats) GetTopErrors(n int) []struct {
	Code  string
	Count uint64
} {
	// Convert to slice for sorting
	type errorCount struct {
		Code  string
		Count uint64
	}

	errors := make([]errorCount, 0, len(es.MostFrequentErrors))
	for code, count := range es.MostFrequentErrors {
		errors = append(errors, errorCount{Code: code, Count: count})
	}

	// Sort by count descending
	for i := 0; i < len(errors); i++ {
		for j := i + 1; j < len(errors); j++ {
			if errors[j].Count > errors[i].Count {
				errors[i], errors[j] = errors[j], errors[i]
			}
		}
	}

	// Return top N
	if n > len(errors) {
		n = len(errors)
	}

	result := make([]struct {
		Code  string
		Count uint64
	}, n)

	for i := 0; i < n; i++ {
		result[i].Code = errors[i].Code
		result[i].Count = errors[i].Count
	}

	return result
}

// GetRecoverabilityRate returns the percentage of recoverable errors
func (es *ErrorStats) GetRecoverabilityRate() float64 {
	if es.TotalErrors == 0 {
		return 100.0
	}
	return float64(es.RecoverableErrors) * 100.0 / float64(es.TotalErrors)
}

// GetErrorRate returns the overall error rate per hour
func (es *ErrorStats) GetErrorRate() float64 {
	if es.Uptime.Hours() == 0 {
		return 0
	}
	return float64(es.TotalErrors) / es.Uptime.Hours()
}

// GetCategoryDistribution returns the percentage distribution of errors by category
func (es *ErrorStats) GetCategoryDistribution() map[ErrorCategory]float64 {
	dist := make(map[ErrorCategory]float64)
	if es.TotalErrors == 0 {
		return dist
	}

	for cat, count := range es.ErrorsByCategory {
		dist[cat] = float64(count) * 100.0 / float64(es.TotalErrors)
	}

	return dist
}
