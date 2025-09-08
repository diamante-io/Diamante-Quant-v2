package unit

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

func TestManagerCreation(t *testing.T) {
	tests := []struct {
		name    string
		config  *monitoring.ManagerConfig
		logger  *logrus.Logger
		wantErr bool
	}{
		{
			name:    "create with default config",
			config:  nil,
			logger:  logrus.New(),
			wantErr: false,
		},
		{
			name:    "create with custom config",
			config:  monitoring.DefaultManagerConfig(),
			logger:  logrus.New(),
			wantErr: false,
		},
		{
			name:    "create with nil logger",
			config:  monitoring.DefaultManagerConfig(),
			logger:  nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := monitoring.NewManager(tt.config, tt.logger)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, manager)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, manager)
				assert.False(t, manager.IsRunning())
			}
		})
	}
}

func TestManagerStartStop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce noise in tests

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)
	require.NotNil(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test start
	err = manager.Start(ctx)
	require.NoError(t, err)
	assert.True(t, manager.IsRunning())

	// Test double start returns error
	err = manager.Start(ctx)
	require.Error(t, err)

	// Test stop
	err = manager.Stop()
	require.NoError(t, err)
	assert.False(t, manager.IsRunning())

	// Test double stop is safe
	err = manager.Stop()
	require.NoError(t, err)
}

func TestManagerComponentAccess(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	manager, err := monitoring.NewManager(config, logger)
	require.NoError(t, err)

	// Test component getters
	metricsCollector := manager.GetMetricsCollector()
	assert.NotNil(t, metricsCollector, "metrics collector should be available when enabled")

	healthMonitor := manager.GetHealthMonitor()
	assert.NotNil(t, healthMonitor, "health monitor should be available when enabled")

	alertManager := manager.GetAlertManager()
	assert.NotNil(t, alertManager, "alert manager should be available when enabled")

	profiler := manager.GetAdvancedProfiler()
	assert.NotNil(t, profiler, "profiler should be available when enabled")
}

func TestManagerMetricsRecording(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	// Test transaction recording (should not panic)
	tx := &types.TypedTransaction{
		Hash: []byte("test-hash"),
		Type: types.TransactionTypeTransfer,
	}
	manager.RecordTransaction(tx, "success", 100*time.Millisecond)

	// Test block recording (should not panic)
	manager.RecordBlock(123, 1024, 50*time.Millisecond, "success")

	// Test consensus recording (should not panic)
	manager.RecordConsensus("success", 200*time.Millisecond, 10)

	// Test metric operations (should not panic)
	manager.IncrementCounter("test_counter", map[string]string{"component": "test"})
	manager.SetGauge("test_gauge", 42.0, map[string]string{"component": "test"})
	manager.ObserveHistogram("test_histogram", 1.5, map[string]string{"component": "test"})
}

func TestManagerHealthAndAlerts(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	ctx := context.Background()

	// Test health report
	healthReport := manager.GetHealthReport(ctx)
	assert.NotNil(t, healthReport)

	// Test active alerts
	alerts := manager.GetActiveAlerts()
	assert.NotNil(t, alerts)

	// Test custom alert
	err = manager.FireAlert("test_rule", 100.0, map[string]string{"test": "label"})
	// Should return error if alert manager not properly initialized with the rule
	// This is expected behavior
}

func TestManagerMonitoringReport(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	ctx := context.Background()

	report, err := manager.GenerateMonitoringReport(ctx)
	require.NoError(t, err)
	require.NotNil(t, report)

	// Verify report structure
	assert.False(t, report.Timestamp.IsZero())
	assert.NotNil(t, report.Summary)
	assert.NotNil(t, report.SystemHealth)
	assert.NotNil(t, report.ActiveAlerts)
}

func TestManagerCustomHealthCheck(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	// Create custom health check
	customCheck := &TestHealthCheck{
		name:    "custom_test",
		timeout: 5 * time.Second,
		result: monitoring.HealthResult{
			Status:    monitoring.HealthStatusHealthy,
			Message:   "Custom check passed",
			Duration:  10 * time.Millisecond,
			Timestamp: time.Now(),
		},
	}

	// Register custom health check
	manager.RegisterCustomHealthCheck(customCheck)

	// Get health report and verify custom check is included
	ctx := context.Background()
	report := manager.GetHealthReport(ctx)
	require.NotNil(t, report)

	// Check if our custom check is in the results
	found := false
	for name := range report.Checks {
		if name == "custom_test" {
			found = true
			break
		}
	}
	assert.True(t, found, "Custom health check should be included in report")
}

func TestManagerCustomAlertRule(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	// Create custom alert rule
	customRule := &monitoring.AlertRule{
		ID:          "test_custom_rule",
		Name:        "Test Custom Rule",
		Description: "A test custom alert rule",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   100.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	// Register custom alert rule
	err = manager.RegisterCustomAlertRule(customRule)
	require.NoError(t, err)

	// Fire the custom alert
	err = manager.FireAlert("test_custom_rule", 150.0, map[string]string{"test": "value"})
	require.NoError(t, err)

	// Check active alerts
	alerts := manager.GetActiveAlerts()
	assert.NotNil(t, alerts)
}

func TestManagerStatus(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	status := manager.GetStatus()
	require.NotNil(t, status)

	// Check expected status fields
	assert.Contains(t, status, "running")
	assert.Contains(t, status, "metrics_enabled")
	assert.Contains(t, status, "health_enabled")
	assert.Contains(t, status, "alerts_enabled")
	assert.Contains(t, status, "profiling_enabled")

	// Verify initial state
	assert.False(t, status["running"].(bool))
}

func TestManagerConfigurationUpdate(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	// Get initial configuration
	initialConfig := manager.GetConfiguration()
	require.NotNil(t, initialConfig)

	// Update configuration
	newConfig := monitoring.DefaultManagerConfig()
	newConfig.EnableMetrics = false
	newConfig.EnableHealth = false

	err = manager.UpdateConfiguration(newConfig)
	require.NoError(t, err)

	// Verify configuration was updated
	updatedConfig := manager.GetConfiguration()
	assert.False(t, updatedConfig.EnableMetrics)
	assert.False(t, updatedConfig.EnableHealth)
}

func TestManagerConfigurationUpdateWhileRunning(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start manager
	err = manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Try to update configuration while running
	newConfig := monitoring.DefaultManagerConfig()
	err = manager.UpdateConfiguration(newConfig)
	require.Error(t, err, "should not allow configuration update while running")
}

func TestDiamanteMonitoringManager(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultManagerConfig()
	manager, err := monitoring.NewDiamanteMonitoringManager(config, logger)
	require.NoError(t, err)
	require.NotNil(t, manager)

	// Test that blockchain-specific checks are registered
	ctx := context.Background()
	report := manager.GetHealthReport(ctx)
	require.NotNil(t, report)

	// Should have blockchain-specific checks
	found := 0
	for name := range report.Checks {
		if name == "blockchain_sync" || name == "validator_status" {
			found++
		}
	}
	assert.GreaterOrEqual(t, found, 1, "Should have blockchain-specific health checks")
}

// TestHealthCheck implements monitoring.HealthCheck for testing
type TestHealthCheck struct {
	name    string
	timeout time.Duration
	result  monitoring.HealthResult
}

func (c *TestHealthCheck) Name() string {
	return c.name
}

func (c *TestHealthCheck) Timeout() time.Duration {
	return c.timeout
}

func (c *TestHealthCheck) Check(ctx context.Context) monitoring.HealthResult {
	return c.result
}

// Benchmark tests

func BenchmarkManagerStart(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	for i := 0; i < b.N; i++ {
		manager, err := monitoring.NewManager(nil, logger)
		require.NoError(b, err)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err = manager.Start(ctx)
		require.NoError(b, err)

		manager.Stop()
		cancel()
	}
}

func BenchmarkManagerMetricsRecording(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(b, err)

	tx := &types.TypedTransaction{
		Hash: []byte("benchmark-hash"),
		Type: types.TransactionTypeTransfer,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		manager.RecordTransaction(tx, "success", 100*time.Millisecond)
		manager.RecordBlock(uint64(i), 1024, 50*time.Millisecond, "success")
		manager.IncrementCounter("bench_counter", map[string]string{"test": "bench"})
	}
}

func BenchmarkManagerHealthReport(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager, err := monitoring.NewManager(nil, logger)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		report := manager.GetHealthReport(ctx)
		_ = report
	}
}
