package integration

import (
	"context"
	"testing"
	"time"

	"diamante/monitoring"
	"diamante/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFullMonitoringStackIntegration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce noise in tests

	// Create monitoring manager with all components enabled
	config := monitoring.DefaultManagerConfig()
	config.EnableMetrics = true
	config.EnableHealth = true
	config.EnableAlerting = true
	config.EnableDashboards = true
	config.EnableProfiling = false // Disable profiling for simpler test

	// Configure health monitoring with disabled endpoints to avoid port conflicts
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)
	require.NotNil(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the monitoring stack
	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Verify all components are accessible
	assert.NotNil(t, manager.GetMetricsCollector())
	assert.NotNil(t, manager.GetHealthMonitor())
	assert.NotNil(t, manager.GetAlertManager())

	// Test metrics recording
	tx := &types.TypedTransaction{
		Hash: []byte("integration-test-hash"),
		Type: types.TransactionTypeTransfer,
	}
	manager.RecordTransaction(tx, "success", 100*time.Millisecond)
	manager.RecordBlock(1001, 2048, 75*time.Millisecond, "success")
	manager.RecordConsensus("success", 150*time.Millisecond, 5)

	// Test health report generation
	healthReport := manager.GetHealthReport(ctx)
	require.NotNil(t, healthReport)
	assert.NotEmpty(t, healthReport.Checks)
	assert.Greater(t, healthReport.Summary.TotalChecks, 0)

	// Test alert firing and retrieval
	customRule := &monitoring.AlertRule{
		ID:          "integration_test_rule",
		Name:        "Integration Test Rule",
		Description: "Test rule for integration testing",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   50.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "integration_test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	err = manager.RegisterCustomAlertRule(customRule)
	require.NoError(t, err)

	err = manager.FireAlert("integration_test_rule", 100.0, map[string]string{"test": "integration"})
	require.NoError(t, err)

	activeAlerts := manager.GetActiveAlerts()
	assert.NotNil(t, activeAlerts)

	// Test comprehensive monitoring report
	monitoringReport, err := manager.GenerateMonitoringReport(ctx)
	require.NoError(t, err)
	require.NotNil(t, monitoringReport)

	// Verify report completeness
	assert.False(t, monitoringReport.Timestamp.IsZero())
	assert.NotNil(t, monitoringReport.SystemHealth)
	assert.NotNil(t, monitoringReport.ActiveAlerts)
	assert.NotNil(t, monitoringReport.Summary)

	// Test status reporting
	status := manager.GetStatus()
	require.NotNil(t, status)
	assert.True(t, status["running"].(bool))
	assert.True(t, status["metrics_enabled"].(bool))
	assert.True(t, status["health_enabled"].(bool))
	assert.True(t, status["alerts_enabled"].(bool))
}

func TestMonitoringWithFailingHealthChecks(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	// Register a failing health check
	failingCheck := &FailingHealthCheck{
		name:    "failing_test_check",
		timeout: 5 * time.Second,
	}
	manager.RegisterCustomHealthCheck(failingCheck)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Get health report
	healthReport := manager.GetHealthReport(ctx)
	require.NotNil(t, healthReport)

	// Should have at least one failing check
	assert.Greater(t, healthReport.Summary.FailedChecks, 0)
	assert.Equal(t, monitoring.HealthStatusUnhealthy, healthReport.Status)

	// Verify the failing check is in the results
	failingResult, exists := healthReport.Checks["failing_test_check"]
	assert.True(t, exists)
	assert.Equal(t, monitoring.HealthStatusUnhealthy, failingResult.Status)
}

func TestMonitoringWithCustomAlertChannels(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	// Register custom alert channel
	customChannel := &TestIntegrationAlertChannel{
		name: "integration_test_channel",
	}
	manager.RegisterCustomAlertChannel(customChannel)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Register rule that uses custom channel
	rule := &monitoring.AlertRule{
		ID:          "custom_channel_test",
		Name:        "Custom Channel Test",
		Description: "Test rule with custom channel",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   10.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityCritical,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"integration_test_channel"},
		Enabled:     true,
	}

	err = manager.RegisterCustomAlertRule(rule)
	require.NoError(t, err)

	// Fire alert
	err = manager.FireAlert("custom_channel_test", 50.0, map[string]string{"test": "custom"})
	require.NoError(t, err)

	// Give time for notification to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify custom channel received the alert
	assert.True(t, customChannel.receivedAlert)
	assert.NotNil(t, customChannel.lastAlert)
	assert.Equal(t, "Custom Channel Test", customChannel.lastAlert.RuleName)
}

func TestMonitoringHealthEndpointsIntegration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = true
	config.HealthConfig.ListenAddress = ":0" // Use random port

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test health endpoints are working (indirect test since we can't easily get the actual port)
	healthMonitor := manager.GetHealthMonitor()
	require.NotNil(t, healthMonitor)

	// Run health checks manually
	report := healthMonitor.RunChecks(ctx)
	require.NotNil(t, report)
	assert.Greater(t, len(report.Checks), 0)
}

func TestMonitoringMetricsIntegration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Record various metrics
	for i := 0; i < 10; i++ {
		tx := &types.TypedTransaction{
			Hash: []byte("test-hash-" + string(rune('0'+i))),
			Type: types.TransactionTypeTransfer,
		}
		manager.RecordTransaction(tx, "success", time.Duration(i*10)*time.Millisecond)
		manager.RecordBlock(uint64(1000+i), 1024+i*100, time.Duration(i*5)*time.Millisecond, "success")
		manager.IncrementCounter("test_counter", map[string]string{"iteration": string(rune('0' + i))})
		manager.SetGauge("test_gauge", float64(i*10), map[string]string{"iteration": string(rune('0' + i))})
	}

	// Get metrics collector and verify it's collecting
	metricsCollector := manager.GetMetricsCollector()
	require.NotNil(t, metricsCollector)

	// Let metrics collection run
	time.Sleep(200 * time.Millisecond)

	// Generate monitoring report to verify metrics are included
	report, err := manager.GenerateMonitoringReport(ctx)
	require.NoError(t, err)
	require.NotNil(t, report.Metrics)
}

func TestMonitoringAlertWorkflow(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false
	config.AlertConfig.CheckInterval = 100 * time.Millisecond // Fast checking

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Register test rule
	rule := &monitoring.AlertRule{
		ID:          "workflow_test_rule",
		Name:        "Workflow Test Rule",
		Description: "Test rule for alert workflow",
		Query:       "workflow_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   100.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "workflow_test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	err = manager.RegisterCustomAlertRule(rule)
	require.NoError(t, err)

	// Fire alert
	err = manager.FireAlert("workflow_test_rule", 150.0, map[string]string{"test": "workflow"})
	require.NoError(t, err)

	// Verify alert is active
	activeAlerts := manager.GetActiveAlerts()
	assert.Greater(t, len(activeAlerts), 0)

	// Find our alert
	var testAlert *monitoring.Alert
	for _, alert := range activeAlerts {
		if alert.RuleID == "workflow_test_rule" {
			testAlert = alert
			break
		}
	}
	require.NotNil(t, testAlert)
	assert.Equal(t, monitoring.AlertStatusFiring, testAlert.Status)

	// Resolve alert
	err = manager.GetAlertManager().ResolveAlert("workflow_test_rule", map[string]string{"test": "workflow"})
	require.NoError(t, err)

	// Verify alert is no longer active
	activeAlerts = manager.GetActiveAlerts()
	alertStillActive := false
	for _, alert := range activeAlerts {
		if alert.RuleID == "workflow_test_rule" && alert.Status == monitoring.AlertStatusFiring {
			alertStillActive = true
			break
		}
	}
	assert.False(t, alertStillActive)
}

func TestMonitoringConcurrentOperations(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Run concurrent operations
	done := make(chan bool, 50)

	// Concurrent metrics recording
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 5; j++ {
				tx := &types.TypedTransaction{
					Hash: []byte("concurrent-hash-" + string(rune('0'+id)) + string(rune('0'+j))),
					Type: types.TransactionTypeTransfer,
				}
				manager.RecordTransaction(tx, "success", 10*time.Millisecond)
				manager.IncrementCounter("concurrent_counter", map[string]string{"worker": string(rune('0' + id))})
			}
		}(i)
	}

	// Concurrent health checks
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 5; j++ {
				report := manager.GetHealthReport(ctx)
				_ = report // Use the report
			}
		}(i)
	}

	// Concurrent alert operations
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			rule := &monitoring.AlertRule{
				ID:          "concurrent_rule_" + string(rune('0'+id)),
				Name:        "Concurrent Rule " + string(rune('0'+id)),
				Description: "Concurrent test rule",
				Query:       "concurrent_metric",
				Condition:   monitoring.AlertConditionGreaterThan,
				Threshold:   50.0,
				Duration:    1 * time.Minute,
				Severity:    monitoring.AlertSeverityInfo,
				Labels:      map[string]string{"worker": string(rune('0' + id))},
				Channels:    []string{"log"},
				Enabled:     true,
			}

			manager.RegisterCustomAlertRule(rule)
			manager.FireAlert(rule.ID, 75.0, map[string]string{"test": "concurrent"})
		}(i)
	}

	// Concurrent status checks
	for i := 0; i < 20; i++ {
		go func() {
			defer func() { done <- true }()

			status := manager.GetStatus()
			_ = status

			alerts := manager.GetActiveAlerts()
			_ = alerts
		}()
	}

	// Wait for all operations to complete
	for i := 0; i < 50; i++ {
		select {
		case <-done:
			// Operation completed
		case <-time.After(3 * time.Second):
			t.Fatal("Concurrent operations timed out")
		}
	}

	// Verify system is still functioning
	finalReport, err := manager.GenerateMonitoringReport(ctx)
	require.NoError(t, err)
	require.NotNil(t, finalReport)
}

// Test helper implementations

type FailingHealthCheck struct {
	name    string
	timeout time.Duration
}

func (c *FailingHealthCheck) Name() string {
	return c.name
}

func (c *FailingHealthCheck) Timeout() time.Duration {
	return c.timeout
}

func (c *FailingHealthCheck) Check(ctx context.Context) monitoring.HealthResult {
	return monitoring.HealthResult{
		Status:    monitoring.HealthStatusUnhealthy,
		Message:   "Intentionally failing health check for testing",
		Duration:  10 * time.Millisecond,
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"reason": "test_failure",
			"error":  "simulated failure",
		},
	}
}

type TestIntegrationAlertChannel struct {
	name          string
	receivedAlert bool
	lastAlert     *monitoring.Alert
}

func (c *TestIntegrationAlertChannel) Name() string {
	return c.name
}

func (c *TestIntegrationAlertChannel) SupportedSeverities() []monitoring.AlertSeverity {
	return nil // All severities
}

func (c *TestIntegrationAlertChannel) Send(ctx context.Context, alert *monitoring.Alert) error {
	c.receivedAlert = true
	c.lastAlert = alert
	return nil
}

// Benchmark integration tests

func BenchmarkFullMonitoringStack(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false
	config.EnableProfiling = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(b, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(b, err)
	defer manager.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Record metrics
		tx := &types.TypedTransaction{
			Hash: []byte("bench-hash"),
			Type: types.TransactionTypeTransfer,
		}
		manager.RecordTransaction(tx, "success", 10*time.Millisecond)

		// Get health report
		report := manager.GetHealthReport(ctx)
		_ = report

		// Get status
		status := manager.GetStatus()
		_ = status
	}
}

func BenchmarkMonitoringReportGeneration(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	config.HealthConfig.EnableEndpoints = false

	manager, err := monitoring.NewManager(config, logger)
	require.NoError(b, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = manager.Start(ctx)
	require.NoError(b, err)
	defer manager.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		report, err := manager.GenerateMonitoringReport(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = report
	}
}
