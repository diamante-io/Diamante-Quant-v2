// metrics/partition_metrics.go
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
)

// PartitionMetricsProvider defines the interface for components that provide partition metrics.
// This interface follows Go idioms by returning errors for operations that can fail.
type PartitionMetricsProvider interface {
	// GetPartitionStatus returns the current partition status as a string with error handling.
	// Returns an error if the status cannot be retrieved.
	GetPartitionStatus() (string, error)

	// GetPartitionMetrics returns a map of partition metrics with error handling.
	// Returns an error if the metrics cannot be retrieved.
	GetPartitionMetrics() (map[string]interface{}, error)
}

// PartitionReporter collects and reports metrics related to network partitions
type PartitionReporter struct {
	mu                sync.RWMutex
	logger            *logrus.Logger
	partitionProvider PartitionMetricsProvider
	updateInterval    time.Duration
	prometheusStats   *PartitionPrometheusStats
	stopChan          chan struct{}
	alertThresholds   PartitionAlertThresholds
	alertCallbacks    map[string]func(string, map[string]interface{})
}

// PartitionPrometheusStats holds Prometheus metrics for partition monitoring
type PartitionPrometheusStats struct {
	// Partition status metrics
	partitionStatus     prometheus.Gauge
	partitionsDetected  prometheus.Counter
	partitionsRecovered prometheus.Counter

	// Partition duration metrics
	currentPartitionDuration prometheus.Gauge
	avgPartitionDuration     prometheus.Gauge
	maxPartitionDuration     prometheus.Gauge

	// Conflict metrics
	conflictsDetected prometheus.Counter
	conflictsResolved prometheus.Counter

	// Recovery metrics
	recoveryAttempts  prometheus.Counter
	recoverySuccesses prometheus.Counter
	recoveryFailures  prometheus.Counter
	avgRecoveryTime   prometheus.Gauge

	// Peer metrics
	respondingPeers prometheus.Gauge
	suspectedPeers  prometheus.Gauge
	confirmedPeers  prometheus.Gauge
}

// PartitionAlertThresholds defines thresholds for generating alerts
type PartitionAlertThresholds struct {
	MaxPartitionDuration   time.Duration
	MaxRecoveryAttempts    int
	MaxUnresolvedConflicts int
	MinRespondingPeers     int
}

// DefaultPartitionAlertThresholds returns default alert thresholds
func DefaultPartitionAlertThresholds() PartitionAlertThresholds {
	return PartitionAlertThresholds{
		MaxPartitionDuration:   30 * time.Minute,
		MaxRecoveryAttempts:    5,
		MaxUnresolvedConflicts: 3,
		MinRespondingPeers:     3,
	}
}

// NewPartitionReporter creates a new reporter for partition metrics
func NewPartitionReporter(
	provider PartitionMetricsProvider,
	updateInterval time.Duration,
	logger *logrus.Logger,
	thresholds *PartitionAlertThresholds,
) *PartitionReporter {
	if logger == nil {
		logger = logrus.New()
	}

	if thresholds == nil {
		defaultThresholds := DefaultPartitionAlertThresholds()
		thresholds = &defaultThresholds
	}

	stats := &PartitionPrometheusStats{
		partitionStatus: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_partition_status",
			Help: "Current partition status (0=Normal, 1=Suspected, 2=Confirmed, 3=Recovering)",
		}),
		partitionsDetected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_partitions_detected_total",
			Help: "Total number of network partitions detected",
		}),
		partitionsRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_partitions_recovered_total",
			Help: "Total number of network partitions recovered from",
		}),
		currentPartitionDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_current_partition_duration_seconds",
			Help: "Duration of the current partition in seconds, or 0 if no active partition",
		}),
		avgPartitionDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_avg_partition_duration_seconds",
			Help: "Average duration of network partitions in seconds",
		}),
		maxPartitionDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_max_partition_duration_seconds",
			Help: "Maximum duration of any network partition in seconds",
		}),
		conflictsDetected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_conflicts_detected_total",
			Help: "Total number of state conflicts detected during partition recovery",
		}),
		conflictsResolved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_conflicts_resolved_total",
			Help: "Total number of state conflicts successfully resolved",
		}),
		recoveryAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_recovery_attempts_total",
			Help: "Total number of partition recovery attempts",
		}),
		recoverySuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_recovery_successes_total",
			Help: "Total number of successful partition recoveries",
		}),
		recoveryFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "diamante_recovery_failures_total",
			Help: "Total number of failed partition recoveries",
		}),
		avgRecoveryTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_avg_recovery_time_seconds",
			Help: "Average time to recover from a partition in seconds",
		}),
		respondingPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_responding_peers",
			Help: "Number of peers currently responding to heartbeats",
		}),
		suspectedPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_suspected_peers",
			Help: "Number of peers suspected to be partitioned",
		}),
		confirmedPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "diamante_confirmed_peers",
			Help: "Number of peers confirmed to be partitioned",
		}),
	}

	// Register all metrics with Prometheus
	prometheus.MustRegister(
		stats.partitionStatus,
		stats.partitionsDetected,
		stats.partitionsRecovered,
		stats.currentPartitionDuration,
		stats.avgPartitionDuration,
		stats.maxPartitionDuration,
		stats.conflictsDetected,
		stats.conflictsResolved,
		stats.recoveryAttempts,
		stats.recoverySuccesses,
		stats.recoveryFailures,
		stats.avgRecoveryTime,
		stats.respondingPeers,
		stats.suspectedPeers,
		stats.confirmedPeers,
	)

	return &PartitionReporter{
		partitionProvider: provider,
		updateInterval:    updateInterval,
		logger:            logger,
		prometheusStats:   stats,
		stopChan:          make(chan struct{}),
		alertThresholds:   *thresholds,
		alertCallbacks:    make(map[string]func(string, map[string]interface{})),
	}
}

// Start begins the metrics collection and reporting
func (pr *PartitionReporter) Start() error {
	pr.logger.Info("Starting partition metrics reporter...")
	go pr.reportingLoop()
	return nil
}

// Stop halts the metrics collection and reporting
func (pr *PartitionReporter) Stop() error {
	pr.logger.Info("Stopping partition metrics reporter...")
	close(pr.stopChan)
	return nil
}

// reportingLoop periodically collects and reports metrics
func (pr *PartitionReporter) reportingLoop() {
	ticker := time.NewTicker(pr.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pr.collectAndReportMetrics()
			pr.checkAlertConditions()
		case <-pr.stopChan:
			pr.logger.Info("Partition metrics reporter stopped")
			return
		}
	}
}

// collectAndReportMetrics gathers metrics from the partition provider
func (pr *PartitionReporter) collectAndReportMetrics() {
	// Get current partition status with error handling
	status, err := pr.partitionProvider.GetPartitionStatus()
	if err != nil {
		pr.logger.WithError(err).Error("Failed to get partition status")
		status = "Unknown" // Set default value to continue operation
	}

	// Map status string to numeric value for Prometheus
	var statusValue float64
	switch status {
	case "Normal":
		statusValue = 0
	case "Suspected":
		statusValue = 1
	case "Confirmed":
		statusValue = 2
	case "Recovering":
		statusValue = 3
	default:
		statusValue = -1 // Unknown status
	}

	pr.prometheusStats.partitionStatus.Set(statusValue)

	// Get detailed metrics with error handling
	metrics, err := pr.partitionProvider.GetPartitionMetrics()
	if err != nil {
		pr.logger.WithError(err).Error("Failed to get partition metrics")
		return // Cannot continue without metrics
	}

	// Update partition counters
	if partitionsDetected, ok := metrics["partitionsDetected"].(int); ok {
		pr.prometheusStats.partitionsDetected.Add(float64(partitionsDetected) - getPartitionCounterValue(pr.prometheusStats.partitionsDetected))
	}

	if partitionsRecovered, ok := metrics["partitionsRecovered"].(int); ok {
		pr.prometheusStats.partitionsRecovered.Add(float64(partitionsRecovered) - getPartitionCounterValue(pr.prometheusStats.partitionsRecovered))
	}

	// Update duration metrics
	if currentDuration, ok := metrics["currentPartitionDuration"].(time.Duration); ok {
		pr.prometheusStats.currentPartitionDuration.Set(currentDuration.Seconds())
	}

	if avgDuration, ok := metrics["averagePartitionDuration"].(time.Duration); ok {
		pr.prometheusStats.avgPartitionDuration.Set(avgDuration.Seconds())
	}

	if maxDuration, ok := metrics["maxPartitionDuration"].(time.Duration); ok {
		pr.prometheusStats.maxPartitionDuration.Set(maxDuration.Seconds())
	}

	// Update conflict metrics
	if conflictsDetected, ok := metrics["conflictsDetected"].(int); ok {
		pr.prometheusStats.conflictsDetected.Add(float64(conflictsDetected) - getPartitionCounterValue(pr.prometheusStats.conflictsDetected))
	}

	if conflictsResolved, ok := metrics["conflictsResolved"].(int); ok {
		pr.prometheusStats.conflictsResolved.Add(float64(conflictsResolved) - getPartitionCounterValue(pr.prometheusStats.conflictsResolved))
	}

	// Update recovery metrics
	if recoveryAttempts, ok := metrics["recoveryAttempts"].(int); ok {
		pr.prometheusStats.recoveryAttempts.Add(float64(recoveryAttempts) - getPartitionCounterValue(pr.prometheusStats.recoveryAttempts))
	}

	if recoverySuccesses, ok := metrics["recoverySuccesses"].(int); ok {
		pr.prometheusStats.recoverySuccesses.Add(float64(recoverySuccesses) - getPartitionCounterValue(pr.prometheusStats.recoverySuccesses))
	}

	if recoveryFailures, ok := metrics["recoveryFailures"].(int); ok {
		pr.prometheusStats.recoveryFailures.Add(float64(recoveryFailures) - getPartitionCounterValue(pr.prometheusStats.recoveryFailures))
	}

	if avgRecoveryTime, ok := metrics["averageRecoveryTime"].(time.Duration); ok {
		pr.prometheusStats.avgRecoveryTime.Set(avgRecoveryTime.Seconds())
	}

	// Update peer metrics
	if respondingPeers, ok := metrics["respondingPeers"].(int); ok {
		pr.prometheusStats.respondingPeers.Set(float64(respondingPeers))
	}

	if suspectedPeers, ok := metrics["suspectedPeers"].(int); ok {
		pr.prometheusStats.suspectedPeers.Set(float64(suspectedPeers))
	}

	if confirmedPeers, ok := metrics["confirmedPeers"].(int); ok {
		pr.prometheusStats.confirmedPeers.Set(float64(confirmedPeers))
	}

	pr.logger.Debug("Partition metrics updated", "status", status)
}

// checkAlertConditions checks if any alert thresholds have been crossed
func (pr *PartitionReporter) checkAlertConditions() {
	metrics, err := pr.partitionProvider.GetPartitionMetrics()
	if err != nil {
		pr.logger.WithError(err).Error("Failed to get partition metrics for alert checking")
		return // Cannot check alerts without metrics
	}

	// Check for long-running partitions
	if currentDuration, ok := metrics["currentPartitionDuration"].(time.Duration); ok {
		if currentDuration > pr.alertThresholds.MaxPartitionDuration {
			pr.triggerAlert("LongPartition", "Partition duration exceeds threshold", map[string]interface{}{
				"duration":  currentDuration,
				"threshold": pr.alertThresholds.MaxPartitionDuration,
			})
		}
	}

	// Check for too many recovery attempts
	if recoveryAttempts, ok := metrics["recoveryAttempts"].(int); ok {
		if recoveryAttempts > pr.alertThresholds.MaxRecoveryAttempts {
			pr.triggerAlert("ExcessiveRecoveryAttempts", "Too many recovery attempts", map[string]interface{}{
				"attempts":  recoveryAttempts,
				"threshold": pr.alertThresholds.MaxRecoveryAttempts,
			})
		}
	}

	// Check for unresolved conflicts
	if detected, ok := metrics["conflictsDetected"].(int); ok {
		if resolved, ok := metrics["conflictsResolved"].(int); ok {
			unresolved := detected - resolved
			if unresolved > pr.alertThresholds.MaxUnresolvedConflicts {
				pr.triggerAlert("UnresolvedConflicts", "Too many unresolved conflicts", map[string]interface{}{
					"unresolved": unresolved,
					"threshold":  pr.alertThresholds.MaxUnresolvedConflicts,
				})
			}
		}
	}

	// Check for too few responding peers
	if respondingPeers, ok := metrics["respondingPeers"].(int); ok {
		if respondingPeers < pr.alertThresholds.MinRespondingPeers {
			pr.triggerAlert("LowRespondingPeers", "Too few responding peers", map[string]interface{}{
				"responding": respondingPeers,
				"threshold":  pr.alertThresholds.MinRespondingPeers,
			})
		}
	}
}

// RegisterAlertCallback registers a callback function for a specific alert type
func (pr *PartitionReporter) RegisterAlertCallback(alertType string, callback func(string, map[string]interface{})) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.alertCallbacks[alertType] = callback
}

// triggerAlert triggers an alert of the specified type with context information
func (pr *PartitionReporter) triggerAlert(alertType, message string, context map[string]interface{}) {
	pr.mu.RLock()
	callback, exists := pr.alertCallbacks[alertType]
	pr.mu.RUnlock()

	// Log the alert
	pr.logger.Warn("Partition alert triggered",
		"type", alertType,
		"message", message,
		"context", context)

	// Call the registered callback if it exists
	if exists {
		go callback(message, context)
	}
}

// GetMetricsSnapshot returns a snapshot of current partition metrics
func (pr *PartitionReporter) GetMetricsSnapshot() map[string]interface{} {
	snapshot := make(map[string]interface{})

	status, err := pr.partitionProvider.GetPartitionStatus()
	if err != nil {
		pr.logger.WithError(err).Error("Failed to get partition status for snapshot")
		status = "Unknown"
	}
	snapshot["partitionStatus"] = status
	snapshot["partitionsDetected"] = getPartitionCounterValue(pr.prometheusStats.partitionsDetected)
	snapshot["partitionsRecovered"] = getPartitionCounterValue(pr.prometheusStats.partitionsRecovered)
	snapshot["currentPartitionDuration"] = time.Duration(getPartitionGaugeValue(pr.prometheusStats.currentPartitionDuration) * float64(time.Second))
	snapshot["avgPartitionDuration"] = time.Duration(getPartitionGaugeValue(pr.prometheusStats.avgPartitionDuration) * float64(time.Second))
	snapshot["conflictsDetected"] = getPartitionCounterValue(pr.prometheusStats.conflictsDetected)
	snapshot["conflictsResolved"] = getPartitionCounterValue(pr.prometheusStats.conflictsResolved)
	snapshot["recoveryAttempts"] = getPartitionCounterValue(pr.prometheusStats.recoveryAttempts)
	snapshot["recoverySuccesses"] = getPartitionCounterValue(pr.prometheusStats.recoverySuccesses)
	snapshot["recoveryFailures"] = getPartitionCounterValue(pr.prometheusStats.recoveryFailures)
	snapshot["avgRecoveryTime"] = time.Duration(getPartitionGaugeValue(pr.prometheusStats.avgRecoveryTime) * float64(time.Second))
	snapshot["respondingPeers"] = getPartitionGaugeValue(pr.prometheusStats.respondingPeers)
	snapshot["suspectedPeers"] = getPartitionGaugeValue(pr.prometheusStats.suspectedPeers)
	snapshot["confirmedPeers"] = getPartitionGaugeValue(pr.prometheusStats.confirmedPeers)

	return snapshot
}

// getPartitionCounterValue extracts the current float64 value from a Prometheus counter
func getPartitionCounterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

// getPartitionGaugeValue extracts the current float64 value from a Prometheus gauge
func getPartitionGaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	_ = g.Write(&m)
	return m.GetGauge().GetValue()
}
