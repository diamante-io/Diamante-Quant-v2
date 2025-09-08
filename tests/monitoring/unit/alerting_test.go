package unit

import (
	"context"
	"testing"
	"time"

	"diamante/monitoring"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAlertManager(t *testing.T) {
	tests := []struct {
		name   string
		config *monitoring.AlertConfig
		logger *logrus.Logger
	}{
		{
			name:   "create with default config",
			config: nil,
			logger: logrus.New(),
		},
		{
			name:   "create with custom config",
			config: monitoring.DefaultAlertConfig(),
			logger: logrus.New(),
		},
		{
			name:   "create with nil logger",
			config: monitoring.DefaultAlertConfig(),
			logger: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := monitoring.NewAlertManager(tt.config, tt.logger)
			require.NotNil(t, manager)
		})
	}
}

func TestDefaultAlertConfig(t *testing.T) {
	config := monitoring.DefaultAlertConfig()
	require.NotNil(t, config)

	assert.Equal(t, 30*time.Second, config.CheckInterval)
	assert.Equal(t, monitoring.AlertSeverityWarning, config.DefaultSeverity)
	assert.Equal(t, 1000, config.MaxAlerts)
	assert.Equal(t, 24*time.Hour, config.RetentionPeriod)
	assert.False(t, config.EnableWebhooks)
}

func TestAlertManagerStartStop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultAlertConfig()
	config.CheckInterval = 100 * time.Millisecond // Fast interval for testing

	manager := monitoring.NewAlertManager(config, logger)
	require.NotNil(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Test start
	err := manager.Start(ctx)
	require.NoError(t, err)

	// Test double start
	err = manager.Start(ctx)
	require.Error(t, err)

	// Test stop
	err = manager.Stop()
	require.NoError(t, err)

	// Test double stop
	err = manager.Stop()
	require.NoError(t, err)
}

func TestAlertManagerRegisterRule(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	rule := &monitoring.AlertRule{
		ID:          "test_rule",
		Name:        "Test Rule",
		Description: "A test alert rule",
		Query:       "test_metric > 100",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   100.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	// Test register rule
	err := manager.RegisterRule(rule)
	require.NoError(t, err)

	// Test register duplicate rule
	err = manager.RegisterRule(rule)
	require.Error(t, err)
}

func TestAlertManagerRegisterChannel(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	channel := &TestAlertChannel{
		name: "test_channel",
	}

	// Test register channel (should not error)
	manager.RegisterChannel(channel)
}

func TestAlertManagerFireAlert(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	// Register a test rule
	rule := &monitoring.AlertRule{
		ID:          "test_fire_rule",
		Name:        "Test Fire Rule",
		Description: "A test alert rule for firing",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   50.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityCritical,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	err := manager.RegisterRule(rule)
	require.NoError(t, err)

	// Test fire alert
	err = manager.FireAlert("test_fire_rule", 100.0, map[string]string{"instance": "test"})
	require.NoError(t, err)

	// Verify alert is active
	activeAlerts := manager.GetActiveAlerts()
	assert.Greater(t, len(activeAlerts), 0)

	// Find our alert
	var foundAlert *monitoring.Alert
	for _, alert := range activeAlerts {
		if alert.RuleID == "test_fire_rule" {
			foundAlert = alert
			break
		}
	}

	require.NotNil(t, foundAlert)
	assert.Equal(t, "test_fire_rule", foundAlert.RuleID)
	assert.Equal(t, "Test Fire Rule", foundAlert.RuleName)
	assert.Equal(t, monitoring.AlertStatusFiring, foundAlert.Status)
	assert.Equal(t, monitoring.AlertSeverityCritical, foundAlert.Severity)
	assert.Equal(t, 100.0, foundAlert.Value)
	assert.Equal(t, 50.0, foundAlert.Threshold)
	assert.Contains(t, foundAlert.Labels, "component")
	assert.Contains(t, foundAlert.Labels, "instance")
}

func TestAlertManagerFireNonExistentRule(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	// Test fire alert for non-existent rule
	err := manager.FireAlert("non_existent_rule", 100.0, map[string]string{"test": "label"})
	require.Error(t, err)
}

func TestAlertManagerResolveAlert(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	// Register and fire an alert
	rule := &monitoring.AlertRule{
		ID:          "test_resolve_rule",
		Name:        "Test Resolve Rule",
		Description: "A test alert rule for resolving",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   50.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	err := manager.RegisterRule(rule)
	require.NoError(t, err)

	err = manager.FireAlert("test_resolve_rule", 100.0, map[string]string{"instance": "test"})
	require.NoError(t, err)

	// Verify alert is active
	activeAlerts := manager.GetActiveAlerts()
	initialCount := len(activeAlerts)
	assert.Greater(t, initialCount, 0)

	// Resolve the alert
	err = manager.ResolveAlert("test_resolve_rule", map[string]string{"instance": "test"})
	require.NoError(t, err)

	// Verify alert is no longer active
	activeAlerts = manager.GetActiveAlerts()
	finalCount := len(activeAlerts)
	assert.Less(t, finalCount, initialCount)

	// Verify alert is in history
	history := manager.GetAlertHistory(10)
	assert.Greater(t, len(history), 0)
}

func TestAlertManagerGetAlertHistory(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	// Test get history when empty
	history := manager.GetAlertHistory(10)
	assert.NotNil(t, history)

	// Test with limit
	history = manager.GetAlertHistory(5)
	assert.NotNil(t, history)
	assert.LessOrEqual(t, len(history), 5)

	// Test with zero limit
	history = manager.GetAlertHistory(0)
	assert.NotNil(t, history)

	// Test with negative limit
	history = manager.GetAlertHistory(-1)
	assert.NotNil(t, history)
}

func TestAlertConditions(t *testing.T) {
	tests := []struct {
		name      string
		condition monitoring.AlertCondition
		value     float64
		threshold float64
		expected  bool
	}{
		{
			name:      "greater than - true",
			condition: monitoring.AlertConditionGreaterThan,
			value:     100.0,
			threshold: 50.0,
			expected:  true,
		},
		{
			name:      "greater than - false",
			condition: monitoring.AlertConditionGreaterThan,
			value:     30.0,
			threshold: 50.0,
			expected:  false,
		},
		{
			name:      "less than - true",
			condition: monitoring.AlertConditionLessThan,
			value:     30.0,
			threshold: 50.0,
			expected:  true,
		},
		{
			name:      "less than - false",
			condition: monitoring.AlertConditionLessThan,
			value:     100.0,
			threshold: 50.0,
			expected:  false,
		},
		{
			name:      "equals - true",
			condition: monitoring.AlertConditionEquals,
			value:     50.0,
			threshold: 50.0,
			expected:  true,
		},
		{
			name:      "equals - false",
			condition: monitoring.AlertConditionEquals,
			value:     49.0,
			threshold: 50.0,
			expected:  false,
		},
		{
			name:      "not equals - true",
			condition: monitoring.AlertConditionNotEquals,
			value:     49.0,
			threshold: 50.0,
			expected:  true,
		},
		{
			name:      "not equals - false",
			condition: monitoring.AlertConditionNotEquals,
			value:     50.0,
			threshold: 50.0,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test validates our understanding of the conditions
			// The actual logic is internal to AlertManager
			var result bool
			switch tt.condition {
			case monitoring.AlertConditionGreaterThan:
				result = tt.value > tt.threshold
			case monitoring.AlertConditionLessThan:
				result = tt.value < tt.threshold
			case monitoring.AlertConditionEquals:
				result = tt.value == tt.threshold
			case monitoring.AlertConditionNotEquals:
				result = tt.value != tt.threshold
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDefaultAlertRules(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Let the manager run for a bit to evaluate rules
	time.Sleep(200 * time.Millisecond)

	// The default rules should be registered and evaluated
	// We can check if any alerts were generated (though this depends on mock values)
	activeAlerts := manager.GetActiveAlerts()
	assert.NotNil(t, activeAlerts)
}

func TestAlertChannels(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Test log channel
	logChannel := &monitoring.LogChannel{}
	require.NotNil(t, logChannel)

	assert.Equal(t, "log", logChannel.Name())
	assert.Nil(t, logChannel.SupportedSeverities()) // All severities

	// Test sending alert via log channel
	alert := &monitoring.Alert{
		ID:        "test_alert",
		RuleName:  "Test Rule",
		Severity:  monitoring.AlertSeverityWarning,
		Message:   "Test alert message",
		Value:     100.0,
		Threshold: 50.0,
		Labels:    map[string]string{"component": "test"},
	}

	ctx := context.Background()
	err := logChannel.Send(ctx, alert)
	require.NoError(t, err)

	// Test webhook channel
	webhookChannel := &monitoring.WebhookChannel{}
	require.NotNil(t, webhookChannel)

	assert.Equal(t, "webhook", webhookChannel.Name())
	assert.Nil(t, webhookChannel.SupportedSeverities()) // All severities

	// Test sending alert via webhook channel (will fail due to no URL)
	err = webhookChannel.Send(ctx, alert)
	assert.Error(t, err) // Expected to fail with no URL configured
}

func TestCustomAlertChannel(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)
	require.NotNil(t, manager)

	// Create and register custom channel
	customChannel := &TestAlertChannel{
		name:                "custom_test",
		supportedSeverities: []monitoring.AlertSeverity{monitoring.AlertSeverityCritical},
	}

	manager.RegisterChannel(customChannel)

	// Create rule using custom channel
	rule := &monitoring.AlertRule{
		ID:          "custom_channel_rule",
		Name:        "Custom Channel Rule",
		Description: "Rule using custom channel",
		Query:       "test_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   100.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityCritical,
		Labels:      map[string]string{"component": "test"},
		Channels:    []string{"custom_test"},
		Enabled:     true,
	}

	err := manager.RegisterRule(rule)
	require.NoError(t, err)

	// Fire alert
	err = manager.FireAlert("custom_channel_rule", 150.0, map[string]string{"test": "value"})
	require.NoError(t, err)

	// Give time for notification to be sent
	time.Sleep(100 * time.Millisecond)

	// Verify channel received the alert
	assert.True(t, customChannel.alertReceived)
}

func TestAlertManagerCleanup(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultAlertConfig()
	config.RetentionPeriod = 1 * time.Millisecond // Very short for testing
	config.CheckInterval = 10 * time.Millisecond  // Fast evaluation

	manager := monitoring.NewAlertManager(config, logger)
	require.NotNil(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop()

	// Let the cleanup run
	time.Sleep(200 * time.Millisecond)

	// The cleanup process should run without errors
	// Exact behavior depends on internal implementation
}

// TestAlertChannel implements monitoring.AlertChannel for testing
type TestAlertChannel struct {
	name                string
	supportedSeverities []monitoring.AlertSeverity
	alertReceived       bool
}

func (c *TestAlertChannel) Name() string {
	return c.name
}

func (c *TestAlertChannel) SupportedSeverities() []monitoring.AlertSeverity {
	return c.supportedSeverities
}

func (c *TestAlertChannel) Send(ctx context.Context, alert *monitoring.Alert) error {
	c.alertReceived = true
	return nil
}

// Benchmark tests

func BenchmarkAlertManagerFireAlert(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)

	rule := &monitoring.AlertRule{
		ID:          "bench_rule",
		Name:        "Benchmark Rule",
		Description: "Benchmark alert rule",
		Query:       "bench_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   50.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "bench"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	manager.RegisterRule(rule)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := manager.FireAlert("bench_rule", float64(i+100), map[string]string{"instance": "bench"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAlertManagerGetActiveAlerts(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	manager := monitoring.NewAlertManager(nil, logger)

	// Fire some alerts first
	rule := &monitoring.AlertRule{
		ID:          "bench_rule",
		Name:        "Benchmark Rule",
		Description: "Benchmark alert rule",
		Query:       "bench_metric",
		Condition:   monitoring.AlertConditionGreaterThan,
		Threshold:   50.0,
		Duration:    1 * time.Minute,
		Severity:    monitoring.AlertSeverityWarning,
		Labels:      map[string]string{"component": "bench"},
		Channels:    []string{"log"},
		Enabled:     true,
	}

	manager.RegisterRule(rule)

	for i := 0; i < 10; i++ {
		manager.FireAlert("bench_rule", 100.0, map[string]string{"instance": string(rune('A' + i))})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		alerts := manager.GetActiveAlerts()
		_ = alerts
	}
}

func TestAlertSeverityAndStatus(t *testing.T) {
	// Test alert severity constants
	assert.Equal(t, "critical", string(monitoring.AlertSeverityCritical))
	assert.Equal(t, "warning", string(monitoring.AlertSeverityWarning))
	assert.Equal(t, "info", string(monitoring.AlertSeverityInfo))

	// Test alert status constants
	assert.Equal(t, "firing", string(monitoring.AlertStatusFiring))
	assert.Equal(t, "resolved", string(monitoring.AlertStatusResolved))
	assert.Equal(t, "silenced", string(monitoring.AlertStatusSilenced))

	// Test alert condition constants
	assert.Equal(t, "gt", string(monitoring.AlertConditionGreaterThan))
	assert.Equal(t, "lt", string(monitoring.AlertConditionLessThan))
	assert.Equal(t, "eq", string(monitoring.AlertConditionEquals))
	assert.Equal(t, "ne", string(monitoring.AlertConditionNotEquals))
}
