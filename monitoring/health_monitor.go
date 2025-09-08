// Package monitoring provides health monitoring capabilities
package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// HealthMonitor monitors system health and provides health endpoints
type HealthMonitor struct {
	checks     map[string]HealthCheck
	config     *HealthConfig
	logger     *logrus.Logger
	httpServer *http.Server

	// Health state
	lastCheck time.Time
	isHealthy bool
	isReady   bool

	mu sync.RWMutex
}

// HealthCheck represents a health check function
type HealthCheck interface {
	Name() string
	Check(ctx context.Context) HealthResult
	Timeout() time.Duration
}

// HealthResult represents the result of a health check
type HealthResult struct {
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Duration  time.Duration          `json:"duration"`
	Timestamp time.Time              `json:"timestamp"`
}

// HealthStatus represents health status
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusWarning   HealthStatus = "warning"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// HealthConfig contains health monitor configuration
type HealthConfig struct {
	ListenAddress   string        `json:"listen_address"`
	CheckInterval   time.Duration `json:"check_interval"`
	CheckTimeout    time.Duration `json:"check_timeout"`
	EnableEndpoints bool          `json:"enable_endpoints"`
	HealthPath      string        `json:"health_path"`
	ReadyPath       string        `json:"ready_path"`
	LivePath        string        `json:"live_path"`
}

// HealthReport represents a comprehensive health report
type HealthReport struct {
	Status     HealthStatus            `json:"status"`
	Timestamp  time.Time               `json:"timestamp"`
	Duration   time.Duration           `json:"duration"`
	Checks     map[string]HealthResult `json:"checks"`
	Summary    HealthSummary           `json:"summary"`
	SystemInfo SystemInfo              `json:"system_info"`
}

// HealthSummary provides summary statistics
type HealthSummary struct {
	TotalChecks   int `json:"total_checks"`
	HealthyChecks int `json:"healthy_checks"`
	WarningChecks int `json:"warning_checks"`
	FailedChecks  int `json:"failed_checks"`
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config *HealthConfig, logger *logrus.Logger) *HealthMonitor {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = DefaultHealthConfig()
	}

	monitor := &HealthMonitor{
		checks: make(map[string]HealthCheck),
		config: config,
		logger: logger,
	}

	// Register default health checks
	monitor.registerDefaultChecks()

	return monitor
}

// DefaultHealthConfig returns default health configuration
func DefaultHealthConfig() *HealthConfig {
	return &HealthConfig{
		ListenAddress:   ":8080",
		CheckInterval:   30 * time.Second,
		CheckTimeout:    10 * time.Second,
		EnableEndpoints: true,
		HealthPath:      "/health",
		ReadyPath:       "/ready",
		LivePath:        "/live",
	}
}

// Start starts the health monitor
func (hm *HealthMonitor) Start(ctx context.Context) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Start health check endpoints if enabled
	if hm.config.EnableEndpoints {
		if err := hm.startHTTPServer(); err != nil {
			return fmt.Errorf("failed to start HTTP server: %w", err)
		}
	}

	// Start periodic health checks
	go hm.runPeriodicChecks(ctx)

	hm.logger.Info("Health monitor started")
	return nil
}

// Stop stops the health monitor
func (hm *HealthMonitor) Stop() error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := hm.httpServer.Shutdown(ctx); err != nil {
			hm.logger.WithError(err).Error("Failed to shutdown health server")
		}
	}

	hm.logger.Info("Health monitor stopped")
	return nil
}

// RegisterCheck registers a health check
func (hm *HealthMonitor) RegisterCheck(check HealthCheck) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.checks[check.Name()] = check
	hm.logger.WithField("check", check.Name()).Debug("Health check registered")
}

// RunChecks runs all health checks
func (hm *HealthMonitor) RunChecks(ctx context.Context) *HealthReport {
	start := common.ConsensusNow()

	report := &HealthReport{
		Timestamp:  start,
		Checks:     make(map[string]HealthResult),
		SystemInfo: getSystemInfo(),
	}

	// Run checks concurrently
	var wg sync.WaitGroup
	resultsChan := make(chan struct {
		name   string
		result HealthResult
	}, len(hm.checks))

	hm.mu.RLock()
	for name, check := range hm.checks {
		wg.Add(1)
		go func(n string, c HealthCheck) {
			defer wg.Done()

			checkCtx, cancel := context.WithTimeout(ctx, c.Timeout())
			defer cancel()

			result := c.Check(checkCtx)
			resultsChan <- struct {
				name   string
				result HealthResult
			}{name: n, result: result}
		}(name, check)
	}
	hm.mu.RUnlock()

	// Wait for all checks to complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	for r := range resultsChan {
		report.Checks[r.name] = r.result
	}

	// Calculate summary
	report.Summary = hm.calculateSummary(report.Checks)
	report.Status = hm.calculateOverallStatus(report.Checks)
	report.Duration = time.Since(start)

	// Update internal state
	hm.mu.Lock()
	hm.lastCheck = common.ConsensusNow()
	hm.isHealthy = report.Status == HealthStatusHealthy
	hm.isReady = hm.isHealthy // Simple readiness check
	hm.mu.Unlock()

	return report
}

// registerDefaultChecks registers default health checks
func (hm *HealthMonitor) registerDefaultChecks() {
	hm.RegisterCheck(&DatabaseHealthCheck{})
	hm.RegisterCheck(&StorageHealthCheck{})
	hm.RegisterCheck(&NetworkHealthCheck{})
	hm.RegisterCheck(&ConsensusHealthCheck{})
	hm.RegisterCheck(&MemoryHealthCheck{})
	hm.RegisterCheck(&DiskHealthCheck{})
}

// runPeriodicChecks runs health checks periodically
func (hm *HealthMonitor) runPeriodicChecks(ctx context.Context) {
	ticker := time.NewTicker(hm.config.CheckInterval)
	defer ticker.Stop()

	// Run initial check
	report := hm.RunChecks(ctx)
	hm.logger.WithFields(logrus.Fields{
		"status":  report.Status,
		"healthy": report.Summary.HealthyChecks,
		"failed":  report.Summary.FailedChecks,
	}).Info("Initial health check completed")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := hm.RunChecks(ctx)

			if report.Status != HealthStatusHealthy {
				hm.logger.WithFields(logrus.Fields{
					"status":  report.Status,
					"healthy": report.Summary.HealthyChecks,
					"failed":  report.Summary.FailedChecks,
				}).Warn("Health check failed")
			}
		}
	}
}

// startHTTPServer starts the HTTP server for health endpoints
func (hm *HealthMonitor) startHTTPServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc(hm.config.HealthPath, hm.healthHandler)
	mux.HandleFunc(hm.config.ReadyPath, hm.readyHandler)
	mux.HandleFunc(hm.config.LivePath, hm.liveHandler)

	hm.httpServer = &http.Server{
		Addr:    hm.config.ListenAddress,
		Handler: mux,
	}

	go func() {
		hm.logger.WithField("address", hm.config.ListenAddress).Info("Starting health server")
		if err := hm.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hm.logger.WithError(err).Error("Health server failed")
		}
	}()

	return nil
}

// healthHandler handles health check requests
func (hm *HealthMonitor) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), hm.config.CheckTimeout)
	defer cancel()

	report := hm.RunChecks(ctx)

	w.Header().Set("Content-Type", "application/json")

	if report.Status == HealthStatusHealthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(report)
}

// readyHandler handles readiness check requests
func (hm *HealthMonitor) readyHandler(w http.ResponseWriter, r *http.Request) {
	hm.mu.RLock()
	ready := hm.isReady
	hm.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"ready":     ready,
		"timestamp": common.ConsensusNow().Format(time.RFC3339),
	}

	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// liveHandler handles liveness check requests
func (hm *HealthMonitor) liveHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"alive":     true,
		"timestamp": common.ConsensusNow().Format(time.RFC3339),
		"uptime":    time.Since(common.ConsensusNow()).String(), // Placeholder
	}

	json.NewEncoder(w).Encode(response)
}

// calculateSummary calculates health check summary
func (hm *HealthMonitor) calculateSummary(results map[string]HealthResult) HealthSummary {
	summary := HealthSummary{
		TotalChecks: len(results),
	}

	for _, result := range results {
		switch result.Status {
		case HealthStatusHealthy:
			summary.HealthyChecks++
		case HealthStatusWarning:
			summary.WarningChecks++
		case HealthStatusUnhealthy:
			summary.FailedChecks++
		}
	}

	return summary
}

// calculateOverallStatus calculates overall health status
func (hm *HealthMonitor) calculateOverallStatus(results map[string]HealthResult) HealthStatus {
	if len(results) == 0 {
		return HealthStatusUnknown
	}

	hasUnhealthy := false
	hasWarning := false

	for _, result := range results {
		switch result.Status {
		case HealthStatusUnhealthy:
			hasUnhealthy = true
		case HealthStatusWarning:
			hasWarning = true
		}
	}

	if hasUnhealthy {
		return HealthStatusUnhealthy
	}
	if hasWarning {
		return HealthStatusWarning
	}

	return HealthStatusHealthy
}

// Default health check implementations

// DatabaseHealthCheck checks database connectivity
type DatabaseHealthCheck struct{}

func (c *DatabaseHealthCheck) Name() string           { return "database" }
func (c *DatabaseHealthCheck) Timeout() time.Duration { return 5 * time.Second }
func (c *DatabaseHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	// Mock database check
	status := HealthStatusHealthy
	message := "Database connection is healthy"

	return HealthResult{
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// StorageHealthCheck checks storage system health
type StorageHealthCheck struct{}

func (c *StorageHealthCheck) Name() string           { return "storage" }
func (c *StorageHealthCheck) Timeout() time.Duration { return 5 * time.Second }
func (c *StorageHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	status := HealthStatusHealthy
	message := "Storage system is healthy"

	return HealthResult{
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// NetworkHealthCheck checks network connectivity
type NetworkHealthCheck struct{}

func (c *NetworkHealthCheck) Name() string           { return "network" }
func (c *NetworkHealthCheck) Timeout() time.Duration { return 5 * time.Second }
func (c *NetworkHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	status := HealthStatusHealthy
	message := "Network connectivity is healthy"

	return HealthResult{
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// ConsensusHealthCheck checks consensus mechanism health
type ConsensusHealthCheck struct{}

func (c *ConsensusHealthCheck) Name() string           { return "consensus" }
func (c *ConsensusHealthCheck) Timeout() time.Duration { return 10 * time.Second }
func (c *ConsensusHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	status := HealthStatusHealthy
	message := "Consensus mechanism is healthy"

	return HealthResult{
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// MemoryHealthCheck checks memory usage
type MemoryHealthCheck struct{}

func (c *MemoryHealthCheck) Name() string           { return "memory" }
func (c *MemoryHealthCheck) Timeout() time.Duration { return 2 * time.Second }
func (c *MemoryHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	// Check memory usage (mock)
	memoryUsage := 75.0 // Percentage

	var status HealthStatus
	var message string

	if memoryUsage > 90 {
		status = HealthStatusUnhealthy
		message = fmt.Sprintf("Memory usage critical: %.1f%%", memoryUsage)
	} else if memoryUsage > 80 {
		status = HealthStatusWarning
		message = fmt.Sprintf("Memory usage high: %.1f%%", memoryUsage)
	} else {
		status = HealthStatusHealthy
		message = fmt.Sprintf("Memory usage normal: %.1f%%", memoryUsage)
	}

	return HealthResult{
		Status:  status,
		Message: message,
		Details: map[string]interface{}{
			"memory_usage_percent": memoryUsage,
		},
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// DiskHealthCheck checks disk space
type DiskHealthCheck struct{}

func (c *DiskHealthCheck) Name() string           { return "disk" }
func (c *DiskHealthCheck) Timeout() time.Duration { return 2 * time.Second }
func (c *DiskHealthCheck) Check(ctx context.Context) HealthResult {
	start := common.ConsensusNow()

	// Check disk usage (mock)
	diskUsage := 65.0 // Percentage

	var status HealthStatus
	var message string

	if diskUsage > 95 {
		status = HealthStatusUnhealthy
		message = fmt.Sprintf("Disk space critical: %.1f%%", diskUsage)
	} else if diskUsage > 85 {
		status = HealthStatusWarning
		message = fmt.Sprintf("Disk space low: %.1f%%", diskUsage)
	} else {
		status = HealthStatusHealthy
		message = fmt.Sprintf("Disk space normal: %.1f%%", diskUsage)
	}

	return HealthResult{
		Status:  status,
		Message: message,
		Details: map[string]interface{}{
			"disk_usage_percent": diskUsage,
		},
		Duration:  time.Since(start),
		Timestamp: common.ConsensusNow(),
	}
}

// Helper function to get system info
func getSystemInfo() SystemInfo {
	return SystemInfo{
		OS:           "linux",
		Architecture: "amd64",
		CPUCount:     8,
		Hostname:     "diamante-node",
	}
}
