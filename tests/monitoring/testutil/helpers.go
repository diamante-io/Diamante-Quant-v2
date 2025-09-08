package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"diamante/monitoring"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// TestConfig provides a standard test configuration for monitoring
type TestConfig struct {
	Logger           *logrus.Logger
	EnableMetrics    bool
	EnableHealth     bool
	EnableAlerting   bool
	EnableDashboards bool
	EnableProfiling  bool
	TestTimeout      time.Duration
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() *TestConfig {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce noise in tests

	return &TestConfig{
		Logger:           logger,
		EnableMetrics:    true,
		EnableHealth:     true,
		EnableAlerting:   true,
		EnableDashboards: false, // Usually not needed in tests
		EnableProfiling:  false, // Usually not needed in tests
		TestTimeout:      5 * time.Second,
	}
}

// CreateTestManager creates a monitoring manager configured for testing
func CreateTestManager(config *TestConfig) (*monitoring.Manager, error) {
	if config == nil {
		config = DefaultTestConfig()
	}

	managerConfig := &monitoring.ManagerConfig{
		EnableMetrics:    config.EnableMetrics,
		EnableHealth:     config.EnableHealth,
		EnableAlerting:   config.EnableAlerting,
		EnableDashboards: config.EnableDashboards,
		EnableProfiling:  config.EnableProfiling,

		MetricsConfig:   CreateTestMetricsConfig(),
		HealthConfig:    CreateTestHealthConfig(),
		AlertConfig:     CreateTestAlertConfig(),
		DashboardConfig: CreateTestDashboardConfig(),

		PrometheusURL: "http://localhost:9090",
		GrafanaURL:    "http://localhost:3000",
		PprofPort:     0, // Use random port
		OutputDir:     "/tmp/monitoring-test-output",
	}

	return monitoring.NewManager(managerConfig, config.Logger)
}

// CreateTestMetricsConfig creates a metrics configuration suitable for testing
func CreateTestMetricsConfig() *monitoring.MetricsConfig {
	return &monitoring.MetricsConfig{
		ListenAddress:       ":0", // Random port
		MetricsPath:         "/metrics",
		EnableSystemMetrics: true,
		CollectionInterval:  100 * time.Millisecond, // Fast for testing
		RetentionPeriod:     1 * time.Hour,
		Labels:              make(map[string]string),
	}
}

// CreateTestHealthConfig creates a health configuration suitable for testing
func CreateTestHealthConfig() *monitoring.HealthConfig {
	return &monitoring.HealthConfig{
		ListenAddress:   ":0",                   // Random port
		CheckInterval:   100 * time.Millisecond, // Fast for testing
		CheckTimeout:    1 * time.Second,
		EnableEndpoints: false, // Usually disabled in tests to avoid port conflicts
		HealthPath:      "/health",
		ReadyPath:       "/ready",
		LivePath:        "/live",
	}
}

// CreateTestAlertConfig creates an alert configuration suitable for testing
func CreateTestAlertConfig() *monitoring.AlertConfig {
	return &monitoring.AlertConfig{
		CheckInterval:   50 * time.Millisecond, // Fast for testing
		DefaultSeverity: monitoring.AlertSeverityWarning,
		MaxAlerts:       100,           // Smaller for tests
		RetentionPeriod: 1 * time.Hour, // Shorter for tests
		EnableWebhooks:  false,
	}
}

// CreateTestDashboardConfig creates a dashboard configuration suitable for testing
func CreateTestDashboardConfig() *monitoring.DashboardConfig {
	return &monitoring.DashboardConfig{
		OutputDir:   "/tmp/test-dashboards",
		DataSource:  "Prometheus",
		RefreshRate: "1s",
		TimeRange:   "5m",
	}
}

// MockHealthCheck implements monitoring.HealthCheck for testing
type MockHealthCheck struct {
	Name_    string
	Timeout_ time.Duration
	Result   monitoring.HealthResult
}

func (m *MockHealthCheck) Name() string {
	return m.Name_
}

func (m *MockHealthCheck) Timeout() time.Duration {
	return m.Timeout_
}

func (m *MockHealthCheck) Check(ctx context.Context) monitoring.HealthResult {
	return m.Result
}

// CreateMockHealthCheck creates a mock health check with the specified status
func CreateMockHealthCheck(name string, status monitoring.HealthStatus, message string) *MockHealthCheck {
	return &MockHealthCheck{
		Name_:    name,
		Timeout_: 5 * time.Second,
		Result: monitoring.HealthResult{
			Status:    status,
			Message:   message,
			Duration:  10 * time.Millisecond,
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"mock": true,
				"name": name,
			},
		},
	}
}

// MockAlertChannel implements monitoring.AlertChannel for testing
type MockAlertChannel struct {
	Name_                string
	SupportedSeverities_ []monitoring.AlertSeverity
	SentAlerts           []*monitoring.Alert
	SendError            error
}

func (m *MockAlertChannel) Name() string {
	return m.Name_
}

func (m *MockAlertChannel) SupportedSeverities() []monitoring.AlertSeverity {
	return m.SupportedSeverities_
}

func (m *MockAlertChannel) Send(ctx context.Context, alert *monitoring.Alert) error {
	if m.SendError != nil {
		return m.SendError
	}
	m.SentAlerts = append(m.SentAlerts, alert)
	return nil
}

// CreateMockAlertChannel creates a mock alert channel
func CreateMockAlertChannel(name string, severities []monitoring.AlertSeverity) *MockAlertChannel {
	return &MockAlertChannel{
		Name_:                name,
		SupportedSeverities_: severities,
		SentAlerts:           make([]*monitoring.Alert, 0),
	}
}

// TestAlertRule creates a test alert rule with sensible defaults
func CreateTestAlertRule(id, name string, condition monitoring.AlertCondition, threshold float64) *monitoring.AlertRule {
	return &monitoring.AlertRule{
		ID:          id,
		Name:        name,
		Description: fmt.Sprintf("Test alert rule: %s", name),
		Query:       fmt.Sprintf("test_metric_%s", id),
		Condition:   condition,
		Threshold:   threshold,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "test", "rule": id},
		Annotations: map[string]string{
			"summary":     fmt.Sprintf("Test rule %s triggered", name),
			"description": fmt.Sprintf("Test rule %s has threshold %f", name, threshold),
		},
		Channels: []string{"log"},
		Enabled:  true,
	}
}

// GenerateTestTransactions creates a slice of test transactions
func GenerateTestTransactions(count int) []*types.TypedTransaction {
	txs := make([]*types.TypedTransaction, count)
	for i := 0; i < count; i++ {
		txs[i] = &types.TypedTransaction{
			Hash: []byte(fmt.Sprintf("test-hash-%d", i)),
			Type: types.TransactionTypeTransfer,
		}
	}
	return txs
}

// SimulateTransactionLoad simulates transaction processing load on a monitoring manager
func SimulateTransactionLoad(manager *monitoring.Manager, txCount int, duration time.Duration) {
	if txCount <= 0 || duration <= 0 {
		return
	}

	interval := duration / time.Duration(txCount)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := 0; i < txCount; i++ {
		tx := &types.TypedTransaction{
			Hash: []byte(fmt.Sprintf("load-test-hash-%d", i)),
			Type: types.TransactionTypeTransfer,
		}

		// Vary the processing time and status
		processingTime := time.Duration(rand.Intn(100)) * time.Millisecond
		status := "success"
		if rand.Float32() < 0.1 { // 10% failure rate
			status = "failed"
		}

		manager.RecordTransaction(tx, status, processingTime)

		// Also record some block metrics
		if i%10 == 0 {
			blockSize := 1024 + rand.Intn(2048)
			blockTime := time.Duration(rand.Intn(50)) * time.Millisecond
			manager.RecordBlock(uint64(1000+i/10), blockSize, blockTime, "success")
		}

		// Add some consensus metrics
		if i%5 == 0 {
			consensusTime := time.Duration(rand.Intn(200)) * time.Millisecond
			validatorCount := 5 + rand.Intn(10)
			manager.RecordConsensus("success", consensusTime, validatorCount)
		}

		<-ticker.C
	}
}

// WaitForAlerts waits for a specific number of alerts to be active
func WaitForAlerts(manager *monitoring.Manager, expectedCount int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		alerts := manager.GetActiveAlerts()
		if len(alerts) >= expectedCount {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return false
}

// WaitForHealthStatus waits for a specific health status
func WaitForHealthStatus(manager *monitoring.Manager, expectedStatus monitoring.HealthStatus, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ctx := context.Background()

	for time.Now().Before(deadline) {
		report := manager.GetHealthReport(ctx)
		if report != nil && report.Status == expectedStatus {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return false
}

// AssertHealthCheckExists verifies that a health check with the given name exists in the report
func AssertHealthCheckExists(report *monitoring.HealthReport, checkName string) bool {
	if report == nil || report.Checks == nil {
		return false
	}

	_, exists := report.Checks[checkName]
	return exists
}

// AssertAlertExists verifies that an alert with the given rule ID exists in the active alerts
func AssertAlertExists(alerts []*monitoring.Alert, ruleID string) bool {
	for _, alert := range alerts {
		if alert.RuleID == ruleID {
			return true
		}
	}
	return false
}

// CreateStressTestScenario creates a monitoring scenario with multiple components under stress
func CreateStressTestScenario(manager *monitoring.Manager, ctx context.Context) {
	// Add multiple health checks with varying results
	healthyCheck := CreateMockHealthCheck("stress_healthy", monitoring.HealthStatusHealthy, "Healthy under stress")
	warningCheck := CreateMockHealthCheck("stress_warning", monitoring.HealthStatusWarning, "Warning under stress")
	unhealthyCheck := CreateMockHealthCheck("stress_unhealthy", monitoring.HealthStatusUnhealthy, "Unhealthy under stress")

	manager.RegisterCustomHealthCheck(healthyCheck)
	manager.RegisterCustomHealthCheck(warningCheck)
	manager.RegisterCustomHealthCheck(unhealthyCheck)

	// Add multiple alert rules
	rules := []*monitoring.AlertRule{
		CreateTestAlertRule("stress_high_load", "High Load", monitoring.AlertConditionGreaterThan, 80.0),
		CreateTestAlertRule("stress_low_peers", "Low Peers", monitoring.AlertConditionLessThan, 5.0),
		CreateTestAlertRule("stress_memory", "High Memory", monitoring.AlertConditionGreaterThan, 90.0),
		CreateTestAlertRule("stress_errors", "Error Rate", monitoring.AlertConditionGreaterThan, 0.1),
	}

	for _, rule := range rules {
		manager.RegisterCustomAlertRule(rule)
	}

	// Add custom alert channel
	stressChannel := CreateMockAlertChannel("stress_channel", nil)
	manager.RegisterCustomAlertChannel(stressChannel)

	// Simulate high transaction load
	go SimulateTransactionLoad(manager, 100, 5*time.Second)

	// Fire some alerts
	go func() {
		time.Sleep(100 * time.Millisecond)
		manager.FireAlert("stress_high_load", 95.0, map[string]string{"component": "stress_test"})

		time.Sleep(200 * time.Millisecond)
		manager.FireAlert("stress_memory", 92.0, map[string]string{"component": "stress_test"})
	}()
}

// ValidateMonitoringReport performs comprehensive validation of a monitoring report
func ValidateMonitoringReport(report *monitoring.MonitoringReport) []string {
	var issues []string

	if report == nil {
		return []string{"report is nil"}
	}

	if report.Timestamp.IsZero() {
		issues = append(issues, "timestamp is zero")
	}

	if report.SystemHealth == nil {
		issues = append(issues, "system health is nil")
	} else {
		if report.SystemHealth.Checks == nil {
			issues = append(issues, "health checks are nil")
		}
		if report.SystemHealth.Summary.TotalChecks <= 0 {
			issues = append(issues, "no health checks found")
		}
	}

	if report.ActiveAlerts == nil {
		issues = append(issues, "active alerts is nil")
	}

	if report.Summary == nil {
		issues = append(issues, "summary is nil")
	} else {
		if report.Summary.HealthScore < 0 || report.Summary.HealthScore > 100 {
			issues = append(issues, fmt.Sprintf("invalid health score: %f", report.Summary.HealthScore))
		}
	}

	return issues
}

// CreatePerformanceTestManager creates a monitoring manager optimized for performance testing
func CreatePerformanceTestManager() (*monitoring.Manager, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := &monitoring.ManagerConfig{
		EnableMetrics:    true,
		EnableHealth:     true,
		EnableAlerting:   true,
		EnableDashboards: false,
		EnableProfiling:  false,

		MetricsConfig: &monitoring.MetricsConfig{
			ListenAddress:       "localhost:9091",
			MetricsPath:         "/metrics",
			EnableSystemMetrics: true,
			CollectionInterval:  10 * time.Millisecond, // Very fast
		},

		HealthConfig: &monitoring.HealthConfig{
			CheckInterval:   10 * time.Millisecond, // Very fast
			CheckTimeout:    100 * time.Millisecond,
			EnableEndpoints: false,
		},

		AlertConfig: &monitoring.AlertConfig{
			CheckInterval:   10 * time.Millisecond, // Very fast
			DefaultSeverity: monitoring.AlertSeverityWarning,
			MaxAlerts:       10000,
			RetentionPeriod: 10 * time.Minute,
		},

		OutputDir: "/tmp/perf-test-monitoring",
	}

	return monitoring.NewManager(config, logger)
}

// RandomHealthStatus returns a random health status for testing
func RandomHealthStatus() monitoring.HealthStatus {
	statuses := []monitoring.HealthStatus{
		monitoring.HealthStatusHealthy,
		monitoring.HealthStatusWarning,
		monitoring.HealthStatusUnhealthy,
		monitoring.HealthStatusUnknown,
	}
	return statuses[rand.Intn(len(statuses))]
}

// RandomAlertSeverity returns a random alert severity for testing
func RandomAlertSeverity() monitoring.AlertSeverity {
	severities := []monitoring.AlertSeverity{
		monitoring.AlertSeverityCritical,
		monitoring.AlertSeverityWarning,
		monitoring.AlertSeverityInfo,
	}
	return severities[rand.Intn(len(severities))]
}

// CleanupTestManager properly shuts down a test monitoring manager
func CleanupTestManager(manager *monitoring.Manager) error {
	if manager == nil {
		return nil
	}
	return manager.Stop()
}
