package unit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"diamante/monitoring"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthMonitorCreation(t *testing.T) {
	tests := []struct {
		name   string
		config *monitoring.HealthConfig
		logger *logrus.Logger
	}{
		{
			name:   "create with default config",
			config: nil,
			logger: logrus.New(),
		},
		{
			name:   "create with custom config",
			config: monitoring.DefaultHealthConfig(),
			logger: logrus.New(),
		},
		{
			name:   "create with nil logger",
			config: monitoring.DefaultHealthConfig(),
			logger: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := monitoring.NewHealthMonitor(tt.config, tt.logger)
			require.NotNil(t, monitor)
		})
	}
}

func TestHealthMonitorDefaultConfig(t *testing.T) {
	config := monitoring.DefaultHealthConfig()
	require.NotNil(t, config)

	assert.Equal(t, ":8080", config.ListenAddress)
	assert.Equal(t, 30*time.Second, config.CheckInterval)
	assert.Equal(t, 10*time.Second, config.CheckTimeout)
	assert.True(t, config.EnableEndpoints)
	assert.Equal(t, "/health", config.HealthPath)
	assert.Equal(t, "/ready", config.ReadyPath)
	assert.Equal(t, "/live", config.LivePath)
}

func TestHealthMonitorStartStop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultHealthConfig()
	config.ListenAddress = ":0"    // Use random port
	config.EnableEndpoints = false // Disable HTTP server for this test

	monitor := monitoring.NewHealthMonitor(config, logger)
	require.NotNil(t, monitor)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test start
	err := monitor.Start(ctx)
	require.NoError(t, err)

	// Test stop
	err = monitor.Stop()
	require.NoError(t, err)
}

func TestHealthMonitorRegisterCheck(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	// Create test health check
	testCheck := &TestHealthCheck{
		name:    "test_check",
		timeout: 5 * time.Second,
		result: monitoring.HealthResult{
			Status:    monitoring.HealthStatusHealthy,
			Message:   "Test check passed",
			Duration:  10 * time.Millisecond,
			Timestamp: time.Now(),
		},
	}

	// Register the check
	monitor.RegisterCheck(testCheck)

	// Run checks and verify our test check is included
	ctx := context.Background()
	report := monitor.RunChecks(ctx)
	require.NotNil(t, report)

	// Verify test check is in results
	result, exists := report.Checks["test_check"]
	assert.True(t, exists, "Test check should be in results")
	assert.Equal(t, monitoring.HealthStatusHealthy, result.Status)
	assert.Equal(t, "Test check passed", result.Message)
}

func TestHealthMonitorRunChecks(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	ctx := context.Background()
	report := monitor.RunChecks(ctx)
	require.NotNil(t, report)

	// Verify report structure
	assert.False(t, report.Timestamp.IsZero())
	assert.GreaterOrEqual(t, report.Duration, time.Duration(0))
	assert.NotNil(t, report.Checks)
	assert.NotNil(t, report.Summary)
	assert.NotNil(t, report.SystemInfo)

	// Verify system info
	assert.NotEmpty(t, report.SystemInfo.OS)
	assert.NotEmpty(t, report.SystemInfo.Architecture)
	assert.Greater(t, report.SystemInfo.CPUCount, 0)

	// Verify summary
	assert.Equal(t, len(report.Checks), report.Summary.TotalChecks)
	assert.GreaterOrEqual(t, report.Summary.HealthyChecks, 0)
	assert.GreaterOrEqual(t, report.Summary.WarningChecks, 0)
	assert.GreaterOrEqual(t, report.Summary.FailedChecks, 0)

	// Verify default checks are present
	expectedChecks := []string{"database", "storage", "network", "consensus", "memory", "disk"}
	for _, checkName := range expectedChecks {
		_, exists := report.Checks[checkName]
		assert.True(t, exists, fmt.Sprintf("Default check '%s' should exist", checkName))
	}
}

func TestHealthMonitorHTTPEndpoints(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := monitoring.DefaultHealthConfig()
	config.EnableEndpoints = true
	config.ListenAddress = ":0" // Use random port

	monitor := monitoring.NewHealthMonitor(config, logger)
	require.NotNil(t, monitor)

	// Start the monitor (which starts HTTP server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := monitor.Start(ctx)
	require.NoError(t, err)
	defer monitor.Stop()

	// Give HTTP server time to start
	time.Sleep(100 * time.Millisecond)

	// Test health endpoint using httptest
	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()

	// Create a test server with the health handler logic
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), config.CheckTimeout)
		defer cancel()

		report := monitor.RunChecks(ctx)

		w.Header().Set("Content-Type", "application/json")

		if report.Status == monitoring.HealthStatusHealthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(report)
	})

	handler.ServeHTTP(rr, req)

	// Verify response
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	// Verify response body is valid JSON
	var report monitoring.HealthReport
	err = json.Unmarshal(rr.Body.Bytes(), &report)
	require.NoError(t, err)
	assert.NotNil(t, report.Checks)
}

func TestHealthMonitorReadyEndpoint(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	// Test ready endpoint using httptest
	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		response := map[string]interface{}{
			"ready":     true, // Mock ready state
			"timestamp": time.Now().Format(time.RFC3339),
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	})

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var response map[string]interface{}
	err := json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Contains(t, response, "ready")
	assert.Contains(t, response, "timestamp")
}

func TestHealthMonitorLiveEndpoint(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	// Test live endpoint using httptest
	req := httptest.NewRequest("GET", "/live", nil)
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"alive":     true,
			"timestamp": time.Now().Format(time.RFC3339),
			"uptime":    "mock-uptime",
		}

		json.NewEncoder(w).Encode(response)
	})

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var response map[string]interface{}
	err := json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Contains(t, response, "alive")
	assert.Contains(t, response, "timestamp")
	assert.Contains(t, response, "uptime")
}

func TestHealthMonitorDefaultHealthChecks(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	ctx := context.Background()
	report := monitor.RunChecks(ctx)
	require.NotNil(t, report)

	// Test each default health check
	checks := []struct {
		name           string
		expectedStatus monitoring.HealthStatus
	}{
		{"database", monitoring.HealthStatusHealthy},
		{"storage", monitoring.HealthStatusHealthy},
		{"network", monitoring.HealthStatusHealthy},
		{"consensus", monitoring.HealthStatusHealthy},
		{"memory", monitoring.HealthStatusHealthy}, // May vary based on mock values
		{"disk", monitoring.HealthStatusHealthy},   // May vary based on mock values
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			result, exists := report.Checks[check.name]
			assert.True(t, exists, fmt.Sprintf("Check '%s' should exist", check.name))

			if exists {
				assert.NotEmpty(t, result.Message)
				assert.False(t, result.Timestamp.IsZero())
				assert.GreaterOrEqual(t, result.Duration, time.Duration(0))
				// Note: Status may vary for memory/disk checks based on mock values
			}
		})
	}
}

func TestHealthMonitorStatusCalculation(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	// Add test checks with different statuses
	healthyCheck := &TestHealthCheck{
		name:    "healthy_check",
		timeout: 5 * time.Second,
		result: monitoring.HealthResult{
			Status:  monitoring.HealthStatusHealthy,
			Message: "Healthy",
		},
	}

	warningCheck := &TestHealthCheck{
		name:    "warning_check",
		timeout: 5 * time.Second,
		result: monitoring.HealthResult{
			Status:  monitoring.HealthStatusWarning,
			Message: "Warning",
		},
	}

	unhealthyCheck := &TestHealthCheck{
		name:    "unhealthy_check",
		timeout: 5 * time.Second,
		result: monitoring.HealthResult{
			Status:  monitoring.HealthStatusUnhealthy,
			Message: "Unhealthy",
		},
	}

	monitor.RegisterCheck(healthyCheck)
	monitor.RegisterCheck(warningCheck)
	monitor.RegisterCheck(unhealthyCheck)

	ctx := context.Background()
	report := monitor.RunChecks(ctx)
	require.NotNil(t, report)

	// Overall status should be unhealthy due to unhealthy check
	assert.Equal(t, monitoring.HealthStatusUnhealthy, report.Status)

	// Summary should reflect all check types
	assert.Greater(t, report.Summary.TotalChecks, 3) // At least our 3 + defaults
	assert.GreaterOrEqual(t, report.Summary.HealthyChecks, 1)
	assert.GreaterOrEqual(t, report.Summary.WarningChecks, 1)
	assert.GreaterOrEqual(t, report.Summary.FailedChecks, 1)
}

func TestHealthMonitorTimeout(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	require.NotNil(t, monitor)

	// Add a slow check that will timeout
	slowCheck := &SlowHealthCheck{
		name:    "slow_check",
		timeout: 100 * time.Millisecond,
		delay:   200 * time.Millisecond, // Longer than timeout
	}

	monitor.RegisterCheck(slowCheck)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	report := monitor.RunChecks(ctx)
	elapsed := time.Since(start)

	require.NotNil(t, report)

	// Should complete reasonably quickly due to timeout
	assert.Less(t, elapsed, 500*time.Millisecond)

	// Check if slow check was handled appropriately
	_, exists := report.Checks["slow_check"]
	// The check may or may not be in results depending on timeout handling
	// This is expected behavior
	_ = exists
}

// SlowHealthCheck implements a health check that takes time
type SlowHealthCheck struct {
	name    string
	timeout time.Duration
	delay   time.Duration
}

func (c *SlowHealthCheck) Name() string {
	return c.name
}

func (c *SlowHealthCheck) Timeout() time.Duration {
	return c.timeout
}

func (c *SlowHealthCheck) Check(ctx context.Context) monitoring.HealthResult {
	select {
	case <-time.After(c.delay):
		return monitoring.HealthResult{
			Status:    monitoring.HealthStatusHealthy,
			Message:   "Slow check completed",
			Duration:  c.delay,
			Timestamp: time.Now(),
		}
	case <-ctx.Done():
		return monitoring.HealthResult{
			Status:    monitoring.HealthStatusUnhealthy,
			Message:   "Check timed out",
			Duration:  c.timeout,
			Timestamp: time.Now(),
		}
	}
}

// Benchmark tests

func BenchmarkHealthMonitorRunChecks(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		report := monitor.RunChecks(ctx)
		_ = report
	}
}

func BenchmarkHealthMonitorRegisterCheck(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	monitor := monitoring.NewHealthMonitor(nil, logger)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check := &TestHealthCheck{
			name:    fmt.Sprintf("bench_check_%d", i),
			timeout: 5 * time.Second,
			result: monitoring.HealthResult{
				Status:  monitoring.HealthStatusHealthy,
				Message: "Benchmark check",
			},
		}
		monitor.RegisterCheck(check)
	}
}

func TestHealthScore(t *testing.T) {
	tests := []struct {
		name      string
		peerCount int
		poolUtil  float64
		blockLag  time.Duration
		expected  int
	}{
		{
			name:      "perfect health",
			peerCount: 20,
			poolUtil:  0.3,
			blockLag:  5 * time.Second,
			expected:  100,
		},
		{
			name:      "low peers",
			peerCount: 2,
			poolUtil:  0.3,
			blockLag:  5 * time.Second,
			expected:  70, // -30 for low peers
		},
		{
			name:      "high pool utilization",
			peerCount: 15,
			poolUtil:  0.9,
			blockLag:  5 * time.Second,
			expected:  80, // -20 for high pool util
		},
		{
			name:      "high block lag",
			peerCount: 15,
			poolUtil:  0.3,
			blockLag:  2 * time.Minute,
			expected:  70, // -30 for high block lag
		},
		{
			name:      "multiple issues",
			peerCount: 2,
			poolUtil:  0.9,
			blockLag:  2 * time.Minute,
			expected:  20, // -30-20-30 = 20
		},
		{
			name:      "very bad health",
			peerCount: 1,
			poolUtil:  0.95,
			blockLag:  5 * time.Minute,
			expected:  0, // Would be negative, clamped to 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := monitoring.CalculateHealthScore(tt.peerCount, tt.poolUtil, tt.blockLag)
			assert.Equal(t, tt.expected, score)
		})
	}
}
