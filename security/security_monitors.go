package security

import (
	"diamante/common"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// SecurityCheckMonitor defines the interface for security check monitors
type SecurityCheckMonitor interface {
	Check() []SecurityAlert
	Start() error
	Stop() error
}

// AttackPatternMonitor monitors for attack patterns
type AttackPatternMonitor struct {
	logger     *logrus.Logger
	mu         sync.RWMutex
	patterns   map[string]int
	thresholds map[string]int
	lastCheck  time.Time
	running    bool
}

// NewAttackPatternMonitor creates a new attack pattern monitor
func NewAttackPatternMonitor(logger *logrus.Logger) *AttackPatternMonitor {
	return &AttackPatternMonitor{
		logger:   logger,
		patterns: make(map[string]int),
		thresholds: map[string]int{
			"sql_injection":     5,
			"xss":               5,
			"path_traversal":    10,
			"command_injection": 3,
		},
		lastCheck: common.ConsensusNow(),
	}
}

// Check checks for attack patterns
func (apm *AttackPatternMonitor) Check() []SecurityAlert {
	apm.mu.RLock()
	defer apm.mu.RUnlock()

	var alerts []SecurityAlert
	now := common.ConsensusNow()

	for pattern, count := range apm.patterns {
		if threshold, exists := apm.thresholds[pattern]; exists && count > threshold {
			alerts = append(alerts, SecurityAlert{
				Type:        "ATTACK_PATTERN_DETECTED",
				Severity:    "high",
				Source:      "attack_pattern_monitor",
				Description: fmt.Sprintf("Attack pattern '%s' detected %d times", pattern, count),
				Timestamp:   now,
				Metadata: map[string]interface{}{
					"pattern":   pattern,
					"count":     count,
					"threshold": threshold,
				},
			})
		}
	}

	return alerts
}

// Start starts the monitor
func (apm *AttackPatternMonitor) Start() error {
	apm.mu.Lock()
	defer apm.mu.Unlock()
	apm.running = true
	return nil
}

// Stop stops the monitor
func (apm *AttackPatternMonitor) Stop() error {
	apm.mu.Lock()
	defer apm.mu.Unlock()
	apm.running = false
	return nil
}

// ResourceMonitor monitors system resources
type ResourceMonitor struct {
	logger     *logrus.Logger
	mu         sync.RWMutex
	thresholds map[string]float64
	lastCheck  time.Time
	running    bool
}

// NewResourceMonitor creates a new resource monitor
func NewResourceMonitor(logger *logrus.Logger) *ResourceMonitor {
	return &ResourceMonitor{
		logger: logger,
		thresholds: map[string]float64{
			"memory_usage":    80.0, // 80% memory usage
			"goroutine_count": 1000, // 1000 goroutines
			"gc_frequency":    10.0, // 10 GC runs per minute
		},
		lastCheck: common.ConsensusNow(),
	}
}

// Check checks system resources
func (rm *ResourceMonitor) Check() []SecurityAlert {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var alerts []SecurityAlert
	now := common.ConsensusNow()

	// Check memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	memUsage := float64(m.Alloc) / float64(m.Sys) * 100
	if memUsage > rm.thresholds["memory_usage"] {
		alerts = append(alerts, SecurityAlert{
			Type:        "HIGH_MEMORY_USAGE",
			Severity:    "medium",
			Source:      "resource_monitor",
			Description: fmt.Sprintf("Memory usage %.2f%% exceeds threshold %.2f%%", memUsage, rm.thresholds["memory_usage"]),
			Timestamp:   now,
			Metadata: map[string]interface{}{
				"memory_usage": memUsage,
				"threshold":    rm.thresholds["memory_usage"],
				"alloc":        m.Alloc,
				"sys":          m.Sys,
			},
		})
	}

	// Check goroutine count
	goroutines := float64(runtime.NumGoroutine())
	if goroutines > rm.thresholds["goroutine_count"] {
		alerts = append(alerts, SecurityAlert{
			Type:        "HIGH_GOROUTINE_COUNT",
			Severity:    "medium",
			Source:      "resource_monitor",
			Description: fmt.Sprintf("Goroutine count %.0f exceeds threshold %.0f", goroutines, rm.thresholds["goroutine_count"]),
			Timestamp:   now,
			Metadata: map[string]interface{}{
				"goroutine_count": goroutines,
				"threshold":       rm.thresholds["goroutine_count"],
			},
		})
	}

	return alerts
}

// Start starts the monitor
func (rm *ResourceMonitor) Start() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.running = true
	return nil
}

// Stop stops the monitor
func (rm *ResourceMonitor) Stop() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.running = false
	return nil
}

// NetworkAnomalyMonitor monitors network anomalies
type NetworkAnomalyMonitor struct {
	logger      *logrus.Logger
	mu          sync.RWMutex
	connections map[string]int
	thresholds  map[string]int
	lastCheck   time.Time
	running     bool
}

// NewNetworkAnomalyMonitor creates a new network anomaly monitor
func NewNetworkAnomalyMonitor(logger *logrus.Logger) *NetworkAnomalyMonitor {
	return &NetworkAnomalyMonitor{
		logger:      logger,
		connections: make(map[string]int),
		thresholds: map[string]int{
			"connections_per_ip": 100,
			"total_connections":  1000,
		},
		lastCheck: common.ConsensusNow(),
	}
}

// Check checks for network anomalies
func (nam *NetworkAnomalyMonitor) Check() []SecurityAlert {
	nam.mu.RLock()
	defer nam.mu.RUnlock()

	var alerts []SecurityAlert
	now := common.ConsensusNow()

	// Check connections per IP
	for ip, count := range nam.connections {
		if count > nam.thresholds["connections_per_ip"] {
			alerts = append(alerts, SecurityAlert{
				Type:        "HIGH_CONNECTION_COUNT",
				Severity:    "medium",
				Source:      "network_anomaly_monitor",
				Description: fmt.Sprintf("IP %s has %d connections, exceeding threshold %d", ip, count, nam.thresholds["connections_per_ip"]),
				Timestamp:   now,
				Metadata: map[string]interface{}{
					"ip":        ip,
					"count":     count,
					"threshold": nam.thresholds["connections_per_ip"],
				},
			})
		}
	}

	return alerts
}

// Start starts the monitor
func (nam *NetworkAnomalyMonitor) Start() error {
	nam.mu.Lock()
	defer nam.mu.Unlock()
	nam.running = true
	return nil
}

// Stop stops the monitor
func (nam *NetworkAnomalyMonitor) Stop() error {
	nam.mu.Lock()
	defer nam.mu.Unlock()
	nam.running = false
	return nil
}
