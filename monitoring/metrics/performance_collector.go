// monitoring/metrics/performance_collector.go

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// PerformanceCollector collects performance-related metrics
type PerformanceCollector struct {
	// Performance metrics
	CPUUsage       prometheus.Gauge
	MemoryUsage    prometheus.Gauge
	DiskUsage      prometheus.Gauge
	GoroutineCount prometheus.Gauge
	GCDuration     prometheus.Histogram
	ResponseTime   prometheus.Histogram
	Throughput     prometheus.Counter
	ErrorRate      prometheus.Counter

	logger *logrus.Logger
	mu     sync.RWMutex
}

// NewPerformanceCollector creates a new performance metrics collector
func NewPerformanceCollector() *PerformanceCollector {
	return &PerformanceCollector{
		CPUUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_cpu_usage_percent",
			Help: "Current CPU usage percentage",
		}),
		MemoryUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_memory_usage_bytes",
			Help: "Current memory usage in bytes",
		}),
		DiskUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_disk_usage_bytes",
			Help: "Current disk usage in bytes",
		}),
		GoroutineCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_goroutines_count",
			Help: "Current number of goroutines",
		}),
		GCDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_gc_duration_seconds",
			Help:    "Garbage collection duration in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		ResponseTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_response_time_seconds",
			Help:    "Response time in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		Throughput: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_throughput_total",
			Help: "Total throughput operations",
		}),
		ErrorRate: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_errors_total",
			Help: "Total number of errors",
		}),
		logger: logrus.New(),
	}
}

// Register registers all metrics with the provided registry
func (c *PerformanceCollector) Register(registry prometheus.Registerer) error {
	metrics := []prometheus.Collector{
		c.CPUUsage,
		c.MemoryUsage,
		c.DiskUsage,
		c.GoroutineCount,
		c.GCDuration,
		c.ResponseTime,
		c.Throughput,
		c.ErrorRate,
	}

	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			return err
		}
	}

	return nil
}

// SetCPUUsage sets the current CPU usage percentage
func (c *PerformanceCollector) SetCPUUsage(percent float64) {
	c.CPUUsage.Set(percent)
}

// SetMemoryUsage sets the current memory usage in bytes
func (c *PerformanceCollector) SetMemoryUsage(bytes uint64) {
	c.MemoryUsage.Set(float64(bytes))
}

// SetDiskUsage sets the current disk usage in bytes
func (c *PerformanceCollector) SetDiskUsage(bytes uint64) {
	c.DiskUsage.Set(float64(bytes))
}

// SetGoroutineCount sets the current number of goroutines
func (c *PerformanceCollector) SetGoroutineCount(count int) {
	c.GoroutineCount.Set(float64(count))
}

// ObserveGCDuration records garbage collection duration
func (c *PerformanceCollector) ObserveGCDuration(duration time.Duration) {
	c.GCDuration.Observe(duration.Seconds())
}

// ObserveResponseTime records response time
func (c *PerformanceCollector) ObserveResponseTime(duration time.Duration) {
	c.ResponseTime.Observe(duration.Seconds())
}

// IncThroughput increments the throughput counter
func (c *PerformanceCollector) IncThroughput() {
	c.Throughput.Inc()
}

// IncErrorRate increments the error rate counter
func (c *PerformanceCollector) IncErrorRate() {
	c.ErrorRate.Inc()
}

// PerformanceMetrics contains typed performance metrics for updates
type PerformanceMetrics struct {
	CPUUsage       float64 `json:"cpu_usage"`
	MemoryUsage    uint64  `json:"memory_usage"`
	DiskUsage      uint64  `json:"disk_usage"`
	GoroutineCount int     `json:"goroutine_count"`
}

// Update updates performance metrics with current state
func (c *PerformanceCollector) Update(metrics PerformanceMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update metrics based on the provided data
	if metrics.CPUUsage > 0 {
		c.SetCPUUsage(metrics.CPUUsage)
	}
	if metrics.MemoryUsage > 0 {
		c.SetMemoryUsage(metrics.MemoryUsage)
	}
	if metrics.DiskUsage > 0 {
		c.SetDiskUsage(metrics.DiskUsage)
	}
	if metrics.GoroutineCount > 0 {
		c.SetGoroutineCount(metrics.GoroutineCount)
	}
}
