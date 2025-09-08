// Package monitoring provides comprehensive metrics collection and monitoring
package monitoring

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"diamante/common"
	"diamante/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// PrometheusCollector collects and exposes metrics
type PrometheusCollector struct {
	registry   *prometheus.Registry
	httpServer *http.Server
	logger     *logrus.Logger
	config     *MetricsConfig

	// Core metrics
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	summaries  map[string]*prometheus.SummaryVec

	// Custom metrics
	customMetrics map[string]prometheus.Collector

	mu        sync.RWMutex
	isRunning bool
}

// MetricsConfig contains configuration for metrics collection
type MetricsConfig struct {
	ListenAddress       string            `json:"listen_address"`
	MetricsPath         string            `json:"metrics_path"`
	EnableSystemMetrics bool              `json:"enable_system_metrics"`
	CollectionInterval  time.Duration     `json:"collection_interval"`
	RetentionPeriod     time.Duration     `json:"retention_period"`
	Labels              map[string]string `json:"labels"`
}

// PrometheusSystemMetrics contains system-level metrics
type PrometheusSystemMetrics struct {
	CPUUsage       float64 `json:"cpu_usage"`
	MemoryUsage    float64 `json:"memory_usage"`
	DiskUsage      float64 `json:"disk_usage"`
	NetworkRx      float64 `json:"network_rx"`
	NetworkTx      float64 `json:"network_tx"`
	GoroutineCount int     `json:"goroutine_count"`
	HeapSize       float64 `json:"heap_size"`
	GCPause        float64 `json:"gc_pause"`
}

// NewPrometheusCollector creates a new metrics collector
func NewPrometheusCollector(config *MetricsConfig, logger *logrus.Logger) *PrometheusCollector {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = DefaultMetricsConfig()
	}

	registry := prometheus.NewRegistry()

	collector := &PrometheusCollector{
		registry:      registry,
		logger:        logger,
		config:        config,
		counters:      make(map[string]*prometheus.CounterVec),
		gauges:        make(map[string]*prometheus.GaugeVec),
		histograms:    make(map[string]*prometheus.HistogramVec),
		summaries:     make(map[string]*prometheus.SummaryVec),
		customMetrics: make(map[string]prometheus.Collector),
	}

	// Register default Go metrics
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	// Register core blockchain metrics
	collector.registerCoreMetrics()

	return collector
}

// DefaultMetricsConfig returns default metrics configuration
func DefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		ListenAddress:       ":9090",
		MetricsPath:         "/metrics",
		EnableSystemMetrics: true,
		CollectionInterval:  15 * time.Second,
		RetentionPeriod:     24 * time.Hour,
		Labels: map[string]string{
			"service": "diamante",
			"version": "1.0.0",
		},
	}
}

// Start starts the metrics collector
func (mc *PrometheusCollector) Start(ctx context.Context) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.isRunning {
		return fmt.Errorf("metrics collector already running")
	}

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.Handle(mc.config.MetricsPath, promhttp.HandlerFor(mc.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", mc.healthHandler)
	mux.HandleFunc("/ready", mc.readyHandler)

	mc.httpServer = &http.Server{
		Addr:    mc.config.ListenAddress,
		Handler: mux,
	}

	// Start server
	go func() {
		mc.logger.WithField("address", mc.config.ListenAddress).Info("Starting metrics server")
		if err := mc.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			mc.logger.WithError(err).Error("Metrics server failed")
		}
	}()

	// Start system metrics collection if enabled
	if mc.config.EnableSystemMetrics {
		go mc.collectSystemMetrics(ctx)
	}

	mc.isRunning = true
	mc.logger.Info("Metrics collector started")

	return nil
}

// Stop stops the metrics collector
func (mc *PrometheusCollector) Stop() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if !mc.isRunning {
		return nil
	}

	if mc.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := mc.httpServer.Shutdown(ctx); err != nil {
			mc.logger.WithError(err).Error("Failed to shutdown metrics server")
		}
	}

	mc.isRunning = false
	mc.logger.Info("Metrics collector stopped")

	return nil
}

// registerCoreMetrics registers core blockchain metrics
func (mc *PrometheusCollector) registerCoreMetrics() {
	// Transaction metrics
	mc.RegisterCounter("diamante_transactions_total", "Total number of transactions", []string{"type", "status"})
	mc.RegisterGauge("diamante_transaction_pool_size", "Current transaction pool size", []string{})
	mc.RegisterHistogram("diamante_transaction_processing_duration", "Transaction processing duration", []string{"type"})

	// Block metrics
	mc.RegisterCounter("diamante_blocks_total", "Total number of blocks", []string{"status"})
	mc.RegisterGauge("diamante_block_height", "Current block height", []string{})
	mc.RegisterHistogram("diamante_block_processing_duration", "Block processing duration", []string{})
	mc.RegisterGauge("diamante_block_size_bytes", "Block size in bytes", []string{})

	// Consensus metrics
	mc.RegisterCounter("diamante_consensus_rounds_total", "Total consensus rounds", []string{"result"})
	mc.RegisterHistogram("diamante_consensus_duration", "Consensus round duration", []string{})
	mc.RegisterGauge("diamante_validator_count", "Number of active validators", []string{})
	mc.RegisterCounter("diamante_votes_total", "Total votes processed", []string{"type"})

	// Network metrics
	mc.RegisterGauge("diamante_peer_count", "Number of connected peers", []string{"type"})
	mc.RegisterCounter("diamante_network_messages_total", "Total network messages", []string{"type", "direction"})
	mc.RegisterHistogram("diamante_network_message_size", "Network message size", []string{"type"})
	mc.RegisterHistogram("diamante_network_latency", "Network message latency", []string{})

	// Storage metrics
	mc.RegisterCounter("diamante_storage_operations_total", "Total storage operations", []string{"operation", "status"})
	mc.RegisterHistogram("diamante_storage_operation_duration", "Storage operation duration", []string{"operation"})
	mc.RegisterGauge("diamante_storage_size_bytes", "Storage size in bytes", []string{"type"})

	// Security metrics
	mc.RegisterCounter("diamante_security_events_total", "Total security events", []string{"type", "severity"})
	mc.RegisterGauge("diamante_threat_level", "Current threat level", []string{})
	mc.RegisterCounter("diamante_auth_attempts_total", "Authentication attempts", []string{"result"})

	// API metrics
	mc.RegisterCounter("diamante_api_requests_total", "Total API requests", []string{"method", "endpoint", "status"})
	mc.RegisterHistogram("diamante_api_request_duration", "API request duration", []string{"method", "endpoint"})
	mc.RegisterGauge("diamante_api_active_connections", "Active API connections", []string{})

	// System metrics
	mc.RegisterGauge("diamante_system_cpu_usage", "CPU usage percentage", []string{})
	mc.RegisterGauge("diamante_system_memory_usage", "Memory usage in bytes", []string{"type"})
	mc.RegisterGauge("diamante_system_disk_usage", "Disk usage in bytes", []string{"mount"})
	mc.RegisterGauge("diamante_system_goroutines", "Number of goroutines", []string{})
}

// RegisterCounter registers a new counter metric
func (mc *PrometheusCollector) RegisterCounter(name, help string, labels []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.counters[name]; exists {
		return
	}

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        name,
			Help:        help,
			ConstLabels: mc.config.Labels,
		},
		labels,
	)

	mc.registry.MustRegister(counter)
	mc.counters[name] = counter
}

// RegisterGauge registers a new gauge metric
func (mc *PrometheusCollector) RegisterGauge(name, help string, labels []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.gauges[name]; exists {
		return
	}

	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: mc.config.Labels,
		},
		labels,
	)

	mc.registry.MustRegister(gauge)
	mc.gauges[name] = gauge
}

// RegisterHistogram registers a new histogram metric
func (mc *PrometheusCollector) RegisterHistogram(name, help string, labels []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.histograms[name]; exists {
		return
	}

	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        name,
			Help:        help,
			ConstLabels: mc.config.Labels,
			Buckets:     prometheus.DefBuckets,
		},
		labels,
	)

	mc.registry.MustRegister(histogram)
	mc.histograms[name] = histogram
}

// RegisterSummary registers a new summary metric
func (mc *PrometheusCollector) RegisterSummary(name, help string, labels []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exists := mc.summaries[name]; exists {
		return
	}

	summary := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:        name,
			Help:        help,
			ConstLabels: mc.config.Labels,
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		labels,
	)

	mc.registry.MustRegister(summary)
	mc.summaries[name] = summary
}

// IncrementCounter increments a counter metric
func (mc *PrometheusCollector) IncrementCounter(name string, labels map[string]string) {
	mc.mu.RLock()
	counter, exists := mc.counters[name]
	mc.mu.RUnlock()

	if exists {
		counter.With(labels).Inc()
	}
}

// SetGauge sets a gauge metric value
func (mc *PrometheusCollector) SetGauge(name string, value float64, labels map[string]string) {
	mc.mu.RLock()
	gauge, exists := mc.gauges[name]
	mc.mu.RUnlock()

	if exists {
		gauge.With(labels).Set(value)
	}
}

// ObserveHistogram observes a value for a histogram metric
func (mc *PrometheusCollector) ObserveHistogram(name string, value float64, labels map[string]string) {
	mc.mu.RLock()
	histogram, exists := mc.histograms[name]
	mc.mu.RUnlock()

	if exists {
		histogram.With(labels).Observe(value)
	}
}

// ObserveSummary observes a value for a summary metric
func (mc *PrometheusCollector) ObserveSummary(name string, value float64, labels map[string]string) {
	mc.mu.RLock()
	summary, exists := mc.summaries[name]
	mc.mu.RUnlock()

	if exists {
		summary.With(labels).Observe(value)
	}
}

// RecordTransactionMetrics records transaction-related metrics
func (mc *PrometheusCollector) RecordTransactionMetrics(tx *types.TypedTransaction, status string, duration time.Duration) {
	labels := map[string]string{
		"type":   string(tx.Type),
		"status": status,
	}

	mc.IncrementCounter("diamante_transactions_total", labels)
	mc.ObserveHistogram("diamante_transaction_processing_duration", duration.Seconds(), map[string]string{"type": string(tx.Type)})
}

// RecordBlockMetrics records block-related metrics
func (mc *PrometheusCollector) RecordBlockMetrics(height uint64, size int, duration time.Duration, status string) {
	mc.IncrementCounter("diamante_blocks_total", map[string]string{"status": status})
	mc.SetGauge("diamante_block_height", float64(height), map[string]string{})
	mc.SetGauge("diamante_block_size_bytes", float64(size), map[string]string{})
	mc.ObserveHistogram("diamante_block_processing_duration", duration.Seconds(), map[string]string{})
}

// RecordConsensusMetrics records consensus-related metrics
func (mc *PrometheusCollector) RecordConsensusMetrics(result string, duration time.Duration, validatorCount int) {
	mc.IncrementCounter("diamante_consensus_rounds_total", map[string]string{"result": result})
	mc.ObserveHistogram("diamante_consensus_duration", duration.Seconds(), map[string]string{})
	mc.SetGauge("diamante_validator_count", float64(validatorCount), map[string]string{})
}

// collectSystemMetrics collects system-level metrics
func (mc *PrometheusCollector) collectSystemMetrics(ctx context.Context) {
	ticker := time.NewTicker(mc.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := mc.getSystemMetrics()
			mc.updateSystemMetrics(metrics)
		}
	}
}

// getSystemMetrics gets current system metrics
func (mc *PrometheusCollector) getSystemMetrics() *PrometheusSystemMetrics {
	// This would integrate with system monitoring libraries
	// For now, return mock data
	return &PrometheusSystemMetrics{
		CPUUsage:       50.0,
		MemoryUsage:    1024 * 1024 * 1024,      // 1GB
		DiskUsage:      10 * 1024 * 1024 * 1024, // 10GB
		GoroutineCount: 100,
		HeapSize:       512 * 1024 * 1024, // 512MB
		GCPause:        1.5,               // 1.5ms
	}
}

// updateSystemMetrics updates system metrics
func (mc *PrometheusCollector) updateSystemMetrics(metrics *PrometheusSystemMetrics) {
	mc.SetGauge("diamante_system_cpu_usage", metrics.CPUUsage, map[string]string{})
	mc.SetGauge("diamante_system_memory_usage", metrics.MemoryUsage, map[string]string{"type": "used"})
	mc.SetGauge("diamante_system_disk_usage", metrics.DiskUsage, map[string]string{"mount": "/"})
	mc.SetGauge("diamante_system_goroutines", float64(metrics.GoroutineCount), map[string]string{})
}

// healthHandler handles health check requests
func (mc *PrometheusCollector) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","timestamp":"` + common.ConsensusNow().Format(time.RFC3339) + `"}`))
}

// readyHandler handles readiness check requests
func (mc *PrometheusCollector) readyHandler(w http.ResponseWriter, r *http.Request) {
	mc.mu.RLock()
	ready := mc.isRunning
	mc.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	if ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready","timestamp":"` + common.ConsensusNow().Format(time.RFC3339) + `"}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"not_ready","timestamp":"` + common.ConsensusNow().Format(time.RFC3339) + `"}`))
	}
}

// GetMetrics returns current metric values
func (mc *PrometheusCollector) GetMetrics() (map[string]interface{}, error) {
	// This would collect current values from all registered metrics
	metrics := make(map[string]interface{})

	// Add basic metrics
	metrics["timestamp"] = common.ConsensusNow().Unix()
	metrics["uptime"] = time.Since(common.ConsensusNow()).Seconds() // Placeholder

	return metrics, nil
}
