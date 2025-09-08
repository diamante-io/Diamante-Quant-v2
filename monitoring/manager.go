// Package monitoring provides comprehensive monitoring management
package monitoring

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// Manager coordinates all monitoring components
type Manager struct {
	metricsCollector *MetricsCollector
	healthMonitor    *HealthMonitor
	alertManager     *AlertManager
	dashboardGen     *DashboardGenerator
	advancedProfiler *consensus.AdvancedProfiler

	config *ManagerConfig
	logger *logrus.Logger

	// State
	isRunning bool
	mu        sync.RWMutex
}

// ManagerConfig contains configuration for the monitoring manager
type ManagerConfig struct {
	EnableMetrics    bool `json:"enable_metrics"`
	EnableHealth     bool `json:"enable_health"`
	EnableAlerting   bool `json:"enable_alerting"`
	EnableDashboards bool `json:"enable_dashboards"`
	EnableProfiling  bool `json:"enable_profiling"`

	MetricsConfig   *MetricsConfig   `json:"metrics_config"`
	HealthConfig    *HealthConfig    `json:"health_config"`
	AlertConfig     *AlertConfig     `json:"alert_config"`
	DashboardConfig *DashboardConfig `json:"dashboard_config"`

	// Integration settings
	PrometheusURL string `json:"prometheus_url"`
	GrafanaURL    string `json:"grafana_url"`
	PprofPort     int    `json:"pprof_port"`

	// Output settings
	OutputDir string `json:"output_dir"`
}

// MonitoringReport provides a comprehensive monitoring report
type MonitoringReport struct {
	Timestamp      time.Time                 `json:"timestamp"`
	SystemHealth   *HealthReport             `json:"system_health"`
	ActiveAlerts   []*Alert                  `json:"active_alerts"`
	Metrics        map[string]interface{}    `json:"metrics"`
	SystemProfile  *consensus.SystemProfile  `json:"system_profile,omitempty"`
	RuntimeProfile *consensus.RuntimeProfile `json:"runtime_profile,omitempty"`
	Summary        *MonitoringSummary        `json:"summary"`
}

// MonitoringSummary provides summary statistics
type MonitoringSummary struct {
	OverallStatus  string  `json:"overall_status"`
	HealthScore    float64 `json:"health_score"`
	AlertCount     int     `json:"alert_count"`
	CriticalAlerts int     `json:"critical_alerts"`
	WarningAlerts  int     `json:"warning_alerts"`
	UptimePercent  float64 `json:"uptime_percent"`
}

// NewManager creates a new monitoring manager
func NewManager(config *ManagerConfig, logger *logrus.Logger) (*Manager, error) {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = DefaultManagerConfig()
	}

	manager := &Manager{
		config: config,
		logger: logger,
	}

	// Initialize components based on configuration
	if err := manager.initializeComponents(); err != nil {
		return nil, fmt.Errorf("failed to initialize monitoring components: %w", err)
	}

	return manager, nil
}

// DefaultManagerConfig returns default manager configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		EnableMetrics:    true,
		EnableHealth:     true,
		EnableAlerting:   true,
		EnableDashboards: true,
		EnableProfiling:  true,

		MetricsConfig: DefaultMetricsConfig(),
		HealthConfig:  DefaultHealthConfig(),
		AlertConfig:   DefaultAlertConfig(),
		DashboardConfig: &DashboardConfig{
			OutputDir:   "./dashboards",
			DataSource:  "Prometheus",
			RefreshRate: "5s",
			TimeRange:   "1h",
		},

		PrometheusURL: getEnvOrDefault("PROMETHEUS_URL", "http://prometheus:9090"),
		GrafanaURL:    getEnvOrDefault("GRAFANA_URL", "http://grafana:3000"),
		PprofPort:     6060,
		OutputDir:     "./monitoring-output",
	}
}

// initializeComponents initializes monitoring components
func (m *Manager) initializeComponents() error {
	// Initialize metrics collector
	if m.config.EnableMetrics {
		m.metricsCollector = NewMetricsCollectorSimple(m.config.MetricsConfig, m.logger)
		m.logger.Debug("Metrics collector initialized")
	}

	// Initialize health monitor
	if m.config.EnableHealth {
		m.healthMonitor = NewHealthMonitor(m.config.HealthConfig, m.logger)
		m.logger.Debug("Health monitor initialized")
	}

	// Initialize alert manager
	if m.config.EnableAlerting {
		m.alertManager = NewAlertManager(m.config.AlertConfig, m.logger)
		m.logger.Debug("Alert manager initialized")
	}

	// Initialize dashboard generator
	if m.config.EnableDashboards {
		m.dashboardGen = NewDashboardGenerator(m.config.DashboardConfig)
		m.logger.Debug("Dashboard generator initialized")
	}

	// Initialize advanced profiler
	if m.config.EnableProfiling {
		// Create a base profiler first
		baseProfiler := consensus.NewPerformanceProfiler(nil) // Will need proper logger
		m.advancedProfiler = consensus.NewAdvancedProfiler(baseProfiler, m.config.PprofPort)
		m.logger.Debug("Advanced profiler initialized")
	}

	return nil
}

// Start starts all monitoring components
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isRunning {
		return fmt.Errorf("monitoring manager already running")
	}

	m.logger.Info("Starting monitoring manager")

	// Start metrics collector
	if m.metricsCollector != nil {
		m.metricsCollector.Start()
	}

	// Start health monitor
	if m.healthMonitor != nil {
		if err := m.healthMonitor.Start(ctx); err != nil {
			return fmt.Errorf("failed to start health monitor: %w", err)
		}
	}

	// Start alert manager
	if m.alertManager != nil {
		if err := m.alertManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start alert manager: %w", err)
		}
	}

	// Generate dashboards
	if m.dashboardGen != nil {
		if err := m.dashboardGen.GenerateAllDashboards(); err != nil {
			m.logger.WithError(err).Warn("Failed to generate dashboards")
		} else {
			m.logger.Info("Dashboards generated successfully")
		}
	}

	// Start advanced profiler
	if m.advancedProfiler != nil {
		if err := m.advancedProfiler.StartAdvancedProfiling(ctx); err != nil {
			m.logger.WithError(err).Warn("Failed to start advanced profiler")
		} else {
			m.logger.Info("Advanced profiler started successfully")
		}
	}

	m.isRunning = true
	m.logger.Info("Monitoring manager started successfully")

	return nil
}

// Stop stops all monitoring components
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return nil
	}

	m.logger.Info("Stopping monitoring manager")

	// Stop components in reverse order
	if m.alertManager != nil {
		if err := m.alertManager.Stop(); err != nil {
			m.logger.WithError(err).Error("Failed to stop alert manager")
		}
	}

	if m.healthMonitor != nil {
		if err := m.healthMonitor.Stop(); err != nil {
			m.logger.WithError(err).Error("Failed to stop health monitor")
		}
	}

	if m.metricsCollector != nil {
		m.metricsCollector.Stop()
	}

	// Stop advanced profiler
	if m.advancedProfiler != nil {
		if err := m.advancedProfiler.StopAdvancedProfiling(); err != nil {
			m.logger.WithError(err).Error("Failed to stop advanced profiler")
		}
	}

	m.isRunning = false
	m.logger.Info("Monitoring manager stopped")

	return nil
}

// GetMetricsCollector returns the metrics collector
func (m *Manager) GetMetricsCollector() *MetricsCollector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metricsCollector
}

// GetHealthMonitor returns the health monitor
func (m *Manager) GetHealthMonitor() *HealthMonitor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthMonitor
}

// GetAlertManager returns the alert manager
func (m *Manager) GetAlertManager() *AlertManager {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.alertManager
}

// GetAdvancedProfiler returns the advanced profiler
func (m *Manager) GetAdvancedProfiler() *consensus.AdvancedProfiler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.advancedProfiler
}

// RecordTransaction records transaction metrics
func (m *Manager) RecordTransaction(tx *types.TypedTransaction, status string, duration time.Duration) {
	if m.metricsCollector != nil {
		m.metricsCollector.RecordTransactionMetrics(tx, status, duration)
	}
}

// RecordBlock records block metrics
func (m *Manager) RecordBlock(height uint64, size int, duration time.Duration, status string) {
	if m.metricsCollector != nil {
		m.metricsCollector.RecordBlockMetrics(height, size, duration, status)
	}
}

// RecordConsensus records consensus metrics
func (m *Manager) RecordConsensus(result string, duration time.Duration, validatorCount int) {
	if m.metricsCollector != nil {
		m.metricsCollector.RecordConsensusMetrics(result, duration, validatorCount)
	}
}

// IncrementCounter increments a metric counter
func (m *Manager) IncrementCounter(name string, labels map[string]string) {
	if m.metricsCollector != nil {
		m.metricsCollector.IncrementCounter(name, labels)
	}
}

// SetGauge sets a gauge metric
func (m *Manager) SetGauge(name string, value float64, labels map[string]string) {
	if m.metricsCollector != nil {
		m.metricsCollector.SetGauge(name, value, labels)
	}
}

// ObserveHistogram observes a histogram metric
func (m *Manager) ObserveHistogram(name string, value float64, labels map[string]string) {
	if m.metricsCollector != nil {
		m.metricsCollector.ObserveHistogram(name, value, labels)
	}
}

// GetHealthReport gets the current health report
func (m *Manager) GetHealthReport(ctx context.Context) *HealthReport {
	if m.healthMonitor != nil {
		return m.healthMonitor.RunChecks(ctx)
	}
	return nil
}

// GetActiveAlerts gets active alerts
func (m *Manager) GetActiveAlerts() []*Alert {
	if m.alertManager != nil {
		return m.alertManager.GetActiveAlerts()
	}
	return nil
}

// FireAlert fires a custom alert
func (m *Manager) FireAlert(ruleID string, value float64, labels map[string]string) error {
	if m.alertManager != nil {
		return m.alertManager.FireAlert(ruleID, value, labels)
	}
	return fmt.Errorf("alert manager not enabled")
}

// GenerateMonitoringReport generates a comprehensive monitoring report
func (m *Manager) GenerateMonitoringReport(ctx context.Context) (*MonitoringReport, error) {
	report := &MonitoringReport{
		Timestamp: common.ConsensusNow(),
	}

	// Get health report
	if m.healthMonitor != nil {
		report.SystemHealth = m.healthMonitor.RunChecks(ctx)
	}

	// Get active alerts
	if m.alertManager != nil {
		report.ActiveAlerts = m.alertManager.GetActiveAlerts()
	}

	// Get metrics
	if m.metricsCollector != nil {
		metrics, err := m.metricsCollector.GetMetrics()
		if err != nil {
			m.logger.WithError(err).Warn("Failed to get metrics")
		} else {
			report.Metrics = metrics
		}
	}

	// Get profiling data
	if m.advancedProfiler != nil {
		systemProfile := m.advancedProfiler.GetSystemProfile()
		report.SystemProfile = &systemProfile

		runtimeProfile := m.advancedProfiler.GetRuntimeProfile()
		report.RuntimeProfile = &runtimeProfile
	}

	// Generate summary
	report.Summary = m.generateSummary(report)

	return report, nil
}

// generateSummary generates monitoring summary
func (m *Manager) generateSummary(report *MonitoringReport) *MonitoringSummary {
	summary := &MonitoringSummary{
		OverallStatus: "unknown",
		HealthScore:   0.0,
		AlertCount:    len(report.ActiveAlerts),
	}

	// Calculate health score
	if report.SystemHealth != nil {
		total := report.SystemHealth.Summary.TotalChecks
		healthy := report.SystemHealth.Summary.HealthyChecks

		if total > 0 {
			summary.HealthScore = float64(healthy) / float64(total) * 100
		}

		// Determine overall status
		switch report.SystemHealth.Status {
		case HealthStatusHealthy:
			summary.OverallStatus = "healthy"
		case HealthStatusWarning:
			summary.OverallStatus = "warning"
		case HealthStatusUnhealthy:
			summary.OverallStatus = "unhealthy"
		default:
			summary.OverallStatus = "unknown"
		}
	}

	// Count alert severities
	for _, alert := range report.ActiveAlerts {
		switch alert.Severity {
		case AlertSeverityCritical:
			summary.CriticalAlerts++
		case AlertSeverityWarning:
			summary.WarningAlerts++
		}
	}

	// Calculate uptime (mock for now)
	summary.UptimePercent = 99.9

	return summary
}

// RegisterCustomHealthCheck registers a custom health check
func (m *Manager) RegisterCustomHealthCheck(check HealthCheck) {
	if m.healthMonitor != nil {
		m.healthMonitor.RegisterCheck(check)
	}
}

// RegisterCustomAlertRule registers a custom alert rule
func (m *Manager) RegisterCustomAlertRule(rule *AlertRule) error {
	if m.alertManager != nil {
		return m.alertManager.RegisterRule(rule)
	}
	return fmt.Errorf("alert manager not enabled")
}

// RegisterCustomAlertChannel registers a custom alert channel
func (m *Manager) RegisterCustomAlertChannel(channel AlertChannel) {
	if m.alertManager != nil {
		m.alertManager.RegisterChannel(channel)
	}
}

// GetConfiguration returns the current configuration
func (m *Manager) GetConfiguration() *ManagerConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// UpdateConfiguration updates the manager configuration
func (m *Manager) UpdateConfiguration(config *ManagerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isRunning {
		return fmt.Errorf("cannot update configuration while running")
	}

	m.config = config
	return m.initializeComponents()
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isRunning
}

// GetStatus returns the current monitoring status
func (m *Manager) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"running":           m.isRunning,
		"metrics_enabled":   m.config.EnableMetrics,
		"health_enabled":    m.config.EnableHealth,
		"alerts_enabled":    m.config.EnableAlerting,
		"profiling_enabled": m.config.EnableProfiling,
	}

	if m.isRunning {
		status["uptime"] = time.Since(common.ConsensusNow()).String() // Placeholder

		if m.healthMonitor != nil {
			status["health_checks"] = len(m.healthMonitor.checks)
		}

		if m.alertManager != nil {
			status["alert_rules"] = len(m.alertManager.rules)
			status["active_alerts"] = len(m.alertManager.activeAlerts)
		}
	}

	return status
}

// Example custom health check for blockchain-specific monitoring
type BlockchainHealthCheck struct {
	name string
}

func (c *BlockchainHealthCheck) Name() string           { return c.name }
func (c *BlockchainHealthCheck) Timeout() time.Duration { return 10 * time.Second }
func (c *BlockchainHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	// Mock blockchain-specific health check
	status := HealthStatusHealthy
	message := "Blockchain is operating normally"
	details := map[string]interface{}{
		"block_height":    12345,
		"peer_count":      25,
		"sync_status":     "synced",
		"last_block_time": common.ConsensusNow().Add(-10 * time.Second),
	}

	return HealthResult{
		Status:    status,
		Message:   message,
		Details:   details,
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// Factory function to create a monitoring manager with blockchain-specific checks
func NewDiamanteMonitoringManager(config *ManagerConfig, logger *logrus.Logger) (*Manager, error) {
	manager, err := NewManager(config, logger)
	if err != nil {
		return nil, err
	}

	// Register blockchain-specific health checks
	manager.RegisterCustomHealthCheck(&BlockchainHealthCheck{name: "blockchain_sync"})
	manager.RegisterCustomHealthCheck(&BlockchainHealthCheck{name: "validator_status"})

	// Register blockchain-specific alert rules
	blockchainRules := []*AlertRule{
		{
			ID:          "block_height_stalled",
			Name:        "Block Height Stalled",
			Description: "Block height hasn't increased recently",
			Query:       "increase(diamante_block_height[5m])",
			Condition:   AlertConditionEquals,
			Threshold:   0,
			Duration:    3 * time.Minute,
			Severity:    AlertSeverityCritical,
			Labels:      map[string]string{"component": "blockchain"},
			Channels:    []string{"log", "webhook"},
			Enabled:     true,
		},
	}

	for _, rule := range blockchainRules {
		manager.RegisterCustomAlertRule(rule)
	}

	return manager, nil
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
