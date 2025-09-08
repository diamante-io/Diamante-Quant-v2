// Package runtime provides performance monitoring metrics for the runtime registry
package runtime

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RegistryMetrics contains Prometheus metrics for runtime registry
type RegistryMetrics struct {
	// Registration metrics
	RegistrationTotal    prometheus.Counter
	RegistrationFailures prometheus.Counter
	UnregistrationTotal  prometheus.Counter

	// Lookup metrics
	LookupLatency  prometheus.Histogram
	LookupTotal    prometheus.Counter
	LookupFailures prometheus.Counter

	// Health check metrics
	HealthCheckTotal    prometheus.Counter
	HealthCheckFailures prometheus.Counter
	HealthCheckDuration prometheus.Histogram

	// Runtime metrics per type
	RuntimesRegistered prometheus.GaugeVec
	RuntimesHealthy    prometheus.GaugeVec

	// Capability metrics
	CapabilityQueries prometheus.CounterVec
}

// NewRegistryMetrics creates new registry metrics
func NewRegistryMetrics() *RegistryMetrics {
	return &RegistryMetrics{
		RegistrationTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_registration_total",
			Help: "Total number of runtime registrations attempted",
		}),
		RegistrationFailures: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_registration_failures_total",
			Help: "Total number of failed runtime registrations",
		}),
		UnregistrationTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_unregistration_total",
			Help: "Total number of runtime unregistrations",
		}),
		LookupLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_runtime_lookup_duration_seconds",
			Help:    "Histogram of runtime lookup durations",
			Buckets: prometheus.DefBuckets,
		}),
		LookupTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_lookup_total",
			Help: "Total number of runtime lookups",
		}),
		LookupFailures: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_lookup_failures_total",
			Help: "Total number of failed runtime lookups",
		}),
		HealthCheckTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_health_check_total",
			Help: "Total number of runtime health checks",
		}),
		HealthCheckFailures: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diamante_runtime_health_check_failures_total",
			Help: "Total number of failed runtime health checks",
		}),
		HealthCheckDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "diamante_runtime_health_check_duration_seconds",
			Help:    "Histogram of health check durations",
			Buckets: prometheus.DefBuckets,
		}),
		RuntimesRegistered: *promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "diamante_runtimes_registered",
			Help: "Number of registered runtimes by type",
		}, []string{"runtime_type"}),
		RuntimesHealthy: *promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "diamante_runtimes_healthy",
			Help: "Number of healthy runtimes by type",
		}, []string{"runtime_type"}),
		CapabilityQueries: *promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "diamante_runtime_capability_queries_total",
			Help: "Total number of capability queries by type",
		}, []string{"capability"}),
	}
}

// ObserveRegistration records a registration attempt
func (m *RegistryMetrics) ObserveRegistration(success bool, runtimeType RuntimeType) {
	m.RegistrationTotal.Inc()
	if !success {
		m.RegistrationFailures.Inc()
	} else {
		m.RuntimesRegistered.WithLabelValues(string(runtimeType)).Inc()
	}
}

// ObserveUnregistration records an unregistration
func (m *RegistryMetrics) ObserveUnregistration(runtimeType RuntimeType) {
	m.UnregistrationTotal.Inc()
	m.RuntimesRegistered.WithLabelValues(string(runtimeType)).Dec()
}

// ObserveLookup records a runtime lookup with timing
func (m *RegistryMetrics) ObserveLookup(start time.Time, success bool) {
	duration := time.Since(start).Seconds()
	m.LookupLatency.Observe(duration)
	m.LookupTotal.Inc()
	if !success {
		m.LookupFailures.Inc()
	}
}

// ObserveHealthCheck records a health check result
func (m *RegistryMetrics) ObserveHealthCheck(start time.Time, runtimeType RuntimeType, success bool) {
	duration := time.Since(start).Seconds()
	m.HealthCheckDuration.Observe(duration)
	m.HealthCheckTotal.Inc()

	if !success {
		m.HealthCheckFailures.Inc()
		m.RuntimesHealthy.WithLabelValues(string(runtimeType)).Set(0)
	} else {
		m.RuntimesHealthy.WithLabelValues(string(runtimeType)).Set(1)
	}
}

// ObserveCapabilityQuery records a capability query
func (m *RegistryMetrics) ObserveCapabilityQuery(capability RuntimeCapability) {
	m.CapabilityQueries.WithLabelValues(string(capability)).Inc()
}
