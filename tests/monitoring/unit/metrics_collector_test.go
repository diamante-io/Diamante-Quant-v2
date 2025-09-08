package unit

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"diamante/monitoring"
	"diamante/tests/monitoring/testutil"
	"diamante/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedMetricsCollectorCreation(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	tests := []struct {
		name   string
		config *monitoring.MetricsConfig
		logger *logrus.Logger
	}{
		{
			name:   "create with default config",
			config: monitoring.DefaultMetricsConfig(),
			logger: logger,
		},
		{
			name:   "create with nil logger",
			config: monitoring.DefaultMetricsConfig(),
			logger: nil,
		},
		{
			name: "create with custom config",
			config: &monitoring.MetricsConfig{
				EnableSystemMetrics: true,
				ListenAddress:       ":9999",
				MetricsPath:         "/metrics",
				CollectionInterval:  10 * time.Second,
			},
			logger: logger,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := monitoring.NewMetricsCollectorSimple(tt.config, tt.logger)
			require.NotNil(t, collector)
		})
	}
}

func TestMetricsCollectorStartStop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// No context needed for Start/Stop

	// Test start
	collector.Start()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Test stop
	collector.Stop()
}

func TestMetricsCollectorRecordTransactionMetrics(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test recording various transaction metrics
	tests := []struct {
		name     string
		tx       *types.TypedTransaction
		status   string
		duration time.Duration
	}{
		{
			name: "successful transaction",
			tx: &types.TypedTransaction{
				Hash: []byte("success-hash"),
				Type: types.TransactionTypeTransfer,
			},
			status:   "success",
			duration: 100 * time.Millisecond,
		},
		{
			name: "failed transaction",
			tx: &types.TypedTransaction{
				Hash: []byte("failed-hash"),
				Type: types.TransactionTypeContractCall,
			},
			status:   "failed",
			duration: 50 * time.Millisecond,
		},
		{
			name: "pending transaction",
			tx: &types.TypedTransaction{
				Hash: []byte("pending-hash"),
				Type: types.TransactionTypeContractDeploy,
			},
			status:   "pending",
			duration: 200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.RecordTransactionMetrics(tt.tx, tt.status, tt.duration)
		})
	}
}

func TestMetricsCollectorRecordBlockMetrics(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test recording various block metrics
	tests := []struct {
		name     string
		height   uint64
		size     int
		duration time.Duration
		status   string
	}{
		{
			name:     "normal block",
			height:   1000,
			size:     1024,
			duration: 50 * time.Millisecond,
			status:   "success",
		},
		{
			name:     "large block",
			height:   1001,
			size:     10240,
			duration: 200 * time.Millisecond,
			status:   "success",
		},
		{
			name:     "failed block",
			height:   1002,
			size:     512,
			duration: 25 * time.Millisecond,
			status:   "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.RecordBlockMetrics(tt.height, tt.size, tt.duration, tt.status)
		})
	}
}

func TestMetricsCollectorRecordConsensusMetrics(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test recording various consensus metrics
	tests := []struct {
		name           string
		result         string
		duration       time.Duration
		validatorCount int
	}{
		{
			name:           "successful consensus",
			result:         "success",
			duration:       150 * time.Millisecond,
			validatorCount: 10,
		},
		{
			name:           "failed consensus",
			result:         "failed",
			duration:       300 * time.Millisecond,
			validatorCount: 8,
		},
		{
			name:           "timeout consensus",
			result:         "timeout",
			duration:       5 * time.Second,
			validatorCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.RecordConsensusMetrics(tt.result, tt.duration, tt.validatorCount)
		})
	}
}

func TestMetricsCollectorCounterOperations(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test counter operations
	tests := []struct {
		name   string
		metric string
		labels map[string]string
	}{
		{
			name:   "simple counter",
			metric: "test_counter",
			labels: map[string]string{"component": "test"},
		},
		{
			name:   "counter with multiple labels",
			metric: "multi_label_counter",
			labels: map[string]string{
				"component": "test",
				"type":      "unit",
				"status":    "active",
			},
		},
		{
			name:   "counter with empty labels",
			metric: "empty_labels_counter",
			labels: map[string]string{},
		},
		{
			name:   "counter with nil labels",
			metric: "nil_labels_counter",
			labels: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.IncrementCounter(tt.metric, tt.labels)
		})
	}
}

func TestMetricsCollectorGaugeOperations(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test gauge operations
	tests := []struct {
		name   string
		metric string
		value  float64
		labels map[string]string
	}{
		{
			name:   "integer value gauge",
			metric: "int_gauge",
			value:  42.0,
			labels: map[string]string{"type": "integer"},
		},
		{
			name:   "float value gauge",
			metric: "float_gauge",
			value:  3.14159,
			labels: map[string]string{"type": "float"},
		},
		{
			name:   "zero value gauge",
			metric: "zero_gauge",
			value:  0.0,
			labels: map[string]string{"type": "zero"},
		},
		{
			name:   "negative value gauge",
			metric: "negative_gauge",
			value:  -100.5,
			labels: map[string]string{"type": "negative"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.SetGauge(tt.metric, tt.value, tt.labels)
		})
	}
}

func TestMetricsCollectorHistogramOperations(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Test histogram operations
	tests := []struct {
		name   string
		metric string
		value  float64
		labels map[string]string
	}{
		{
			name:   "small value histogram",
			metric: "duration_histogram",
			value:  0.001, // 1ms
			labels: map[string]string{"operation": "fast"},
		},
		{
			name:   "medium value histogram",
			metric: "duration_histogram",
			value:  0.1, // 100ms
			labels: map[string]string{"operation": "medium"},
		},
		{
			name:   "large value histogram",
			metric: "duration_histogram",
			value:  5.0, // 5s
			labels: map[string]string{"operation": "slow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			collector.ObserveHistogram(tt.metric, tt.value, tt.labels)
		})
	}
}

func TestMetricsCollectorGetMetrics(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Record some metrics first
	collector.IncrementCounter("test_counter", map[string]string{"test": "get_metrics"})
	collector.SetGauge("test_gauge", 123.45, map[string]string{"test": "get_metrics"})
	collector.ObserveHistogram("test_histogram", 0.5, map[string]string{"test": "get_metrics"})

	// Get metrics
	metrics, err := collector.GetMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	// Verify structure
	assert.Contains(t, metrics, "counters")
	assert.Contains(t, metrics, "gauges")
	assert.Contains(t, metrics, "histograms")

	// Verify types
	counters, ok := metrics["counters"].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, counters)

	gauges, ok := metrics["gauges"].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, gauges)

	histograms, ok := metrics["histograms"].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, histograms)
}

func TestMetricsCollectorHighLoad(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	// Start collector
	collector.Start()
	defer collector.Stop()

	// Generate high load
	const loadCount = 1000

	// Record many transactions
	for i := 0; i < loadCount; i++ {
		tx := &types.TypedTransaction{
			ID:   RandomString(64),
			Type: types.TransactionTypeTransfer,
		}
		collector.RecordTransactionMetrics(tx, "success", time.Millisecond*time.Duration(i%100))

		if i%10 == 0 {
			collector.RecordBlockMetrics(uint64(1000+i/10), 1024+i, time.Millisecond*time.Duration(i%50), "success")
		}

		if i%5 == 0 {
			collector.RecordConsensusMetrics("success", time.Millisecond*time.Duration(i%200), 10+i%5)
		}

		collector.IncrementCounter("load_test_counter", map[string]string{"iteration": string(rune('0' + i%10))})
		collector.SetGauge("load_test_gauge", float64(i), map[string]string{"batch": string(rune('A' + i%26))})
		collector.ObserveHistogram("load_test_histogram", float64(i%100)/100.0, map[string]string{"test": "load"})
	}

	// Get final metrics
	metrics, err := collector.GetMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)
}

func TestMetricsCollectorConcurrentAccess(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)
	require.NotNil(t, collector)

	const numGoroutines = 10
	const operationsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	// Start concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < operationsPerGoroutine; j++ {
				// Mix different operations
				switch j % 4 {
				case 0:
					tx := &types.TypedTransaction{
						ID:   fmt.Sprintf("concurrent-hash-%d-%d", id, j),
						Type: types.TransactionTypeTransfer,
					}
					collector.RecordTransactionMetrics(tx, "success", time.Millisecond*10)
				case 1:
					collector.RecordBlockMetrics(uint64(id*1000+j), 1024, time.Millisecond*5, "success")
				case 2:
					collector.IncrementCounter("concurrent_counter", map[string]string{"worker": string(rune('0' + id))})
				case 3:
					collector.SetGauge("concurrent_gauge", float64(id*100+j), map[string]string{"worker": string(rune('0' + id))})
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
			// Goroutine completed
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent operations timed out")
		}
	}

	// Verify metrics can still be retrieved
	metrics, err := collector.GetMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)
}

// Benchmark tests

func BenchmarkMetricsCollectorTransactionRecording(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)

	tx := &types.TypedTransaction{
		ID:   "benchmark-hash",
		Type: types.TransactionTypeTransfer,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		collector.RecordTransactionMetrics(tx, "success", 10*time.Millisecond)
	}
}

func BenchmarkMetricsCollectorCounterOperations(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)

	labels := map[string]string{"component": "benchmark"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		collector.IncrementCounter("benchmark_counter", labels)
	}
}

func BenchmarkMetricsCollectorGaugeOperations(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)

	labels := map[string]string{"component": "benchmark"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		collector.SetGauge("benchmark_gauge", float64(i), labels)
	}
}

func BenchmarkMetricsCollectorHistogramOperations(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)

	labels := map[string]string{"component": "benchmark"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		collector.ObserveHistogram("benchmark_histogram", float64(i%1000)/1000.0, labels)
	}
}

func BenchmarkMetricsCollectorGetMetrics(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := testutil.CreateTestMetricsConfig()
	collector := monitoring.NewMetricsCollectorSimple(config, logger)

	// Pre-populate with some metrics
	for i := 0; i < 100; i++ {
		collector.IncrementCounter("bench_counter", map[string]string{"id": string(rune('0' + i%10))})
		collector.SetGauge("bench_gauge", float64(i), map[string]string{"id": string(rune('0' + i%10))})
		collector.ObserveHistogram("bench_histogram", float64(i%100)/100.0, map[string]string{"id": string(rune('0' + i%10))})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		metrics, err := collector.GetMetrics()
		if err != nil {
			b.Fatal(err)
		}
		_ = metrics
	}
}

// Helper function for generating random strings (simple implementation)
func RandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
