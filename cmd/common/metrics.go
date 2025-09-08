// Package common provides shared utilities for cmd services
package common

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"diamante/consensus"
)

// MetricsCollector collects and exposes application metrics
type MetricsCollector struct {
	counters   map[string]int64
	histograms map[string]*Histogram
	gauges     map[string]float64
	mutex      sync.RWMutex
}

// Histogram represents a histogram metric
type Histogram struct {
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		counters:   make(map[string]int64),
		histograms: make(map[string]*Histogram),
		gauges:     make(map[string]float64),
	}
}

// IncrementCounter increments a counter metric
func (mc *MetricsCollector) IncrementCounter(name string) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.counters[name]++
}

// AddToCounter adds a value to a counter metric
func (mc *MetricsCollector) AddToCounter(name string, value int64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.counters[name] += value
}

// SetGauge sets a gauge metric value
func (mc *MetricsCollector) SetGauge(name string, value float64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.gauges[name] = value
}

// ObserveHistogram records an observation for a histogram metric
func (mc *MetricsCollector) ObserveHistogram(name string, value float64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	histogram, exists := mc.histograms[name]
	if !exists {
		// Create default buckets: 0.001, 0.01, 0.1, 1, 10 seconds
		histogram = &Histogram{
			buckets: []float64{0.001, 0.01, 0.1, 1.0, 10.0},
			counts:  make([]int64, 6), // +1 for +Inf bucket
		}
		mc.histograms[name] = histogram
	}

	histogram.sum += value
	histogram.count++

	// Find appropriate bucket
	for i, bucket := range histogram.buckets {
		if value <= bucket {
			histogram.counts[i]++
		}
	}
	// Always increment the +Inf bucket
	histogram.counts[len(histogram.buckets)]++
}

// RecordHTTPRequest records metrics for HTTP requests
func (mc *MetricsCollector) RecordHTTPRequest(method, path string, statusCode int, duration time.Duration) {
	// Increment request counter
	mc.IncrementCounter(fmt.Sprintf("http_requests_total_%s_%s", method, path))

	// Record status code
	mc.IncrementCounter(fmt.Sprintf("http_requests_status_%d", statusCode))

	// Record duration
	mc.ObserveHistogram(fmt.Sprintf("http_request_duration_seconds_%s_%s", method, path), duration.Seconds())
}

// MetricsHandler returns an HTTP handler that exposes metrics in Prometheus format
func (mc *MetricsCollector) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mc.mutex.RLock()
		defer mc.mutex.RUnlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		// Write counters
		for name, value := range mc.counters {
			fmt.Fprintf(w, "# TYPE %s counter\n", name)
			fmt.Fprintf(w, "%s %d\n", name, value)
		}

		// Write gauges
		for name, value := range mc.gauges {
			fmt.Fprintf(w, "# TYPE %s gauge\n", name)
			fmt.Fprintf(w, "%s %f\n", name, value)
		}

		// Write histograms
		for name, histogram := range mc.histograms {
			fmt.Fprintf(w, "# TYPE %s histogram\n", name)

			// Write buckets
			for i, bucket := range histogram.buckets {
				fmt.Fprintf(w, "%s_bucket{le=\"%f\"} %d\n", name, bucket, histogram.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, histogram.counts[len(histogram.buckets)])

			// Write sum and count
			fmt.Fprintf(w, "%s_sum %f\n", name, histogram.sum)
			fmt.Fprintf(w, "%s_count %d\n", name, histogram.count)
		}
	}
}

// GetCounterValue returns the current value of a counter
func (mc *MetricsCollector) GetCounterValue(name string) int64 {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	return mc.counters[name]
}

// GetGaugeValue returns the current value of a gauge
func (mc *MetricsCollector) GetGaugeValue(name string) float64 {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	return mc.gauges[name]
}

// MetricsMiddleware creates middleware that records HTTP metrics
func (mc *MetricsCollector) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := consensus.ConsensusNow()

		// Wrap response writer to capture status code
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrappedWriter, r)

		duration := consensus.ConsensusSince(start)
		mc.RecordHTTPRequest(r.Method, r.URL.Path, wrappedWriter.statusCode, duration)
	})
}

// UpdateSystemMetrics updates system-related metrics (can be called periodically)
func (mc *MetricsCollector) UpdateSystemMetrics() {
	mc.SetGauge("system_timestamp_seconds", float64(consensus.ConsensusUnix()))

	// Note: For uptime, we need to track start time separately
	// This is a placeholder - in production, track actual service start time
	mc.SetGauge("system_uptime_seconds", 0) // Will be updated with actual uptime tracking
}

func (mc *MetricsCollector) measureDuration(operation string, fn func()) {
	start := consensus.ConsensusNow()
	fn()
	duration := consensus.ConsensusSince(start)
	mc.ObserveHistogram(operation, duration.Seconds())
}
