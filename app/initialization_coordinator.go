// Package app provides application-level coordination and management
package app

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/consensus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// InitPhase represents an initialization phase
type InitPhase string

const (
	PhaseConfig    InitPhase = "config"
	PhaseStorage   InitPhase = "storage"
	PhaseNetwork   InitPhase = "network"
	PhaseConsensus InitPhase = "consensus"
	PhaseRuntime   InitPhase = "runtime"
	PhaseAPI       InitPhase = "api"
	PhaseComplete  InitPhase = "complete"
)

// InitHandler handles initialization for a phase
type InitHandler func(ctx context.Context) error

// ShutdownHandler handles shutdown for a phase
type ShutdownHandler func(ctx context.Context) error

// InitStatus tracks phase status
type InitStatus struct {
	Started   time.Time
	Completed time.Time
	Error     error
	Success   bool
	Retries   int
}

// InitPhaseConfig holds configuration for a phase
type InitPhaseConfig struct {
	Handler         InitHandler
	ShutdownHandler ShutdownHandler
	Timeout         time.Duration
	RetryCount      int
	RetryDelay      time.Duration
	Dependencies    []InitPhase
}

// InitializationCoordinator manages ordered initialization
type InitializationCoordinator struct {
	phases        []InitPhase
	phaseConfigs  map[InitPhase]*InitPhaseConfig
	status        map[InitPhase]*InitStatus
	mu            sync.RWMutex
	logger        *logrus.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	initialized   atomic.Bool
	shutdownOrder []InitPhase

	// Metrics
	phaseStartTime        *prometheus.GaugeVec
	phaseDuration         *prometheus.HistogramVec
	phaseErrors           *prometheus.CounterVec
	phaseRetries          *prometheus.CounterVec
	totalInitTime         prometheus.Histogram
	initializationSuccess prometheus.Counter
	initializationFailure prometheus.Counter
}

// NewInitializationCoordinator creates a coordinator
func NewInitializationCoordinator(logger *logrus.Logger) *InitializationCoordinator {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ic := &InitializationCoordinator{
		phases: []InitPhase{
			PhaseConfig,
			PhaseStorage,
			PhaseNetwork,
			PhaseConsensus,
			PhaseRuntime,
			PhaseAPI,
		},
		phaseConfigs:  make(map[InitPhase]*InitPhaseConfig),
		status:        make(map[InitPhase]*InitStatus),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		shutdownOrder: make([]InitPhase, 0),
	}

	ic.initMetrics()
	return ic
}

// initMetrics initializes Prometheus metrics
func (ic *InitializationCoordinator) initMetrics() {
	ic.phaseStartTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "diamante_init_phase_start_timestamp",
			Help: "Timestamp when initialization phase started",
		},
		[]string{"phase"},
	)

	ic.phaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "diamante_init_phase_duration_seconds",
			Help:    "Duration of initialization phases in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"phase", "status"},
	)

	ic.phaseErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "diamante_init_phase_errors_total",
			Help: "Total number of initialization phase errors",
		},
		[]string{"phase"},
	)

	ic.phaseRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "diamante_init_phase_retries_total",
			Help: "Total number of initialization phase retries",
		},
		[]string{"phase"},
	)

	ic.totalInitTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "diamante_total_init_duration_seconds",
			Help:    "Total initialization duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	ic.initializationSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "diamante_initialization_success_total",
			Help: "Total number of successful initializations",
		},
	)

	ic.initializationFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "diamante_initialization_failure_total",
			Help: "Total number of failed initializations",
		},
	)

	// Register metrics
	prometheus.MustRegister(
		ic.phaseStartTime,
		ic.phaseDuration,
		ic.phaseErrors,
		ic.phaseRetries,
		ic.totalInitTime,
		ic.initializationSuccess,
		ic.initializationFailure,
	)
}

// RegisterPhase registers a handler for a phase with default configuration
func (ic *InitializationCoordinator) RegisterPhase(phase InitPhase, handler InitHandler) error {
	return ic.RegisterPhaseWithConfig(phase, &InitPhaseConfig{
		Handler:    handler,
		Timeout:    30 * time.Second,
		RetryCount: 3,
		RetryDelay: 1 * time.Second,
	})
}

// RegisterPhaseWithConfig registers a handler for a phase with custom configuration
func (ic *InitializationCoordinator) RegisterPhaseWithConfig(phase InitPhase, config *InitPhaseConfig) error {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if ic.initialized.Load() {
		return fmt.Errorf("cannot register phase after initialization has started")
	}

	if config == nil {
		return fmt.Errorf("phase config cannot be nil")
	}

	if config.Handler == nil {
		return fmt.Errorf("phase handler cannot be nil")
	}

	// Set defaults if not provided
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.RetryCount == 0 {
		config.RetryCount = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 1 * time.Second
	}

	ic.phaseConfigs[phase] = config
	ic.logger.WithField("phase", phase).Debug("Registered initialization phase")

	return nil
}

// validateDependencies validates that all dependencies are satisfied
func (ic *InitializationCoordinator) validateDependencies() error {
	// Build dependency graph
	for phase, config := range ic.phaseConfigs {
		for _, dep := range config.Dependencies {
			if _, exists := ic.phaseConfigs[dep]; !exists {
				return fmt.Errorf("phase %s depends on unregistered phase %s", phase, dep)
			}
		}
	}

	// Check for circular dependencies using DFS
	visited := make(map[InitPhase]bool)
	recStack := make(map[InitPhase]bool)

	var hasCycle func(phase InitPhase) bool
	hasCycle = func(phase InitPhase) bool {
		visited[phase] = true
		recStack[phase] = true

		if config, exists := ic.phaseConfigs[phase]; exists {
			for _, dep := range config.Dependencies {
				if !visited[dep] {
					if hasCycle(dep) {
						return true
					}
				} else if recStack[dep] {
					return true
				}
			}
		}

		recStack[phase] = false
		return false
	}

	for phase := range ic.phaseConfigs {
		if !visited[phase] {
			if hasCycle(phase) {
				return fmt.Errorf("circular dependency detected")
			}
		}
	}

	return nil
}

// Initialize runs all initialization phases in order
func (ic *InitializationCoordinator) Initialize() error {
	if !ic.initialized.CompareAndSwap(false, true) {
		return fmt.Errorf("initialization already in progress or completed")
	}

	startTime := consensus.ConsensusNow()
	ic.logger.Info("Starting initialization sequence")

	// Validate all phases are registered
	for _, phase := range ic.phases {
		if _, exists := ic.phaseConfigs[phase]; !exists {
			return fmt.Errorf("required phase %s not registered", phase)
		}
	}

	// Validate dependencies
	if err := ic.validateDependencies(); err != nil {
		return fmt.Errorf("dependency validation failed: %w", err)
	}

	// Run phases in order
	for _, phase := range ic.phases {
		if err := ic.runPhase(phase); err != nil {
			ic.logger.WithError(err).Errorf("Initialization failed at phase: %s", phase)
			ic.initializationFailure.Inc()
			ic.totalInitTime.Observe(time.Since(startTime).Seconds())
			return fmt.Errorf("initialization failed at %s: %w", phase, err)
		}
		// Track successful phases for shutdown ordering
		ic.shutdownOrder = append([]InitPhase{phase}, ic.shutdownOrder...)
	}

	// Mark completion
	ic.mu.Lock()
	ic.status[PhaseComplete] = &InitStatus{
		Started:   startTime,
		Completed: consensus.ConsensusNow(),
		Success:   true,
	}
	ic.mu.Unlock()

	duration := consensus.ConsensusSince(startTime)
	ic.totalInitTime.Observe(duration.Seconds())
	ic.initializationSuccess.Inc()

	ic.logger.WithField("duration", duration).Info("Initialization sequence completed successfully")
	return nil
}

// runPhase executes a single initialization phase with retries
func (ic *InitializationCoordinator) runPhase(phase InitPhase) error {
	config, exists := ic.phaseConfigs[phase]
	if !exists {
		return fmt.Errorf("no handler registered for phase %s", phase)
	}

	// Check dependencies
	for _, dep := range config.Dependencies {
		if err := ic.WaitForPhase(dep, 5*time.Minute); err != nil {
			return fmt.Errorf("dependency %s failed: %w", dep, err)
		}
	}

	ic.logger.WithField("phase", phase).Info("Starting initialization phase")
	ic.phaseStartTime.WithLabelValues(string(phase)).SetToCurrentTime()

	// Initialize status
	ic.mu.Lock()
	status := &InitStatus{
		Started: consensus.ConsensusNow(),
	}
	ic.status[phase] = status
	ic.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt <= config.RetryCount; attempt++ {
		if attempt > 0 {
			ic.logger.WithFields(logrus.Fields{
				"phase":   phase,
				"attempt": attempt,
			}).Warn("Retrying initialization phase")
			ic.phaseRetries.WithLabelValues(string(phase)).Inc()

			// Use context-aware timer instead of time.Sleep
			select {
			case <-ic.ctx.Done():
				return fmt.Errorf("context cancelled during retry delay")
			case <-time.After(config.RetryDelay):
				// Continue with retry
			}
		}

		// Run handler with timeout
		ctx, cancel := context.WithTimeout(ic.ctx, config.Timeout)
		err := config.Handler(ctx)
		cancel()

		if err == nil {
			// Success
			ic.mu.Lock()
			status.Completed = consensus.ConsensusNow()
			status.Success = true
			status.Retries = attempt
			ic.mu.Unlock()

			duration := status.Completed.Sub(status.Started)
			ic.phaseDuration.WithLabelValues(string(phase), "success").Observe(duration.Seconds())

			ic.logger.WithFields(logrus.Fields{
				"phase":    phase,
				"duration": duration,
				"retries":  attempt,
			}).Info("Initialization phase completed")

			return nil
		}

		lastErr = err
		ic.logger.WithError(err).WithFields(logrus.Fields{
			"phase":   phase,
			"attempt": attempt,
		}).Error("Initialization phase failed")
		ic.phaseErrors.WithLabelValues(string(phase)).Inc()
	}

	// All retries failed
	ic.mu.Lock()
	status.Completed = consensus.ConsensusNow()
	status.Success = false
	status.Error = lastErr
	status.Retries = config.RetryCount
	ic.mu.Unlock()

	duration := status.Completed.Sub(status.Started)
	ic.phaseDuration.WithLabelValues(string(phase), "failure").Observe(duration.Seconds())

	return lastErr
}

// WaitForPhase waits for a phase to complete
func (ic *InitializationCoordinator) WaitForPhase(phase InitPhase, timeout time.Duration) error {
	deadline := consensus.ConsensusNow().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ic.mu.RLock()
			status, exists := ic.status[phase]
			ic.mu.RUnlock()

			if exists && !status.Completed.IsZero() {
				if status.Success {
					return nil
				}
				return fmt.Errorf("phase %s failed: %w", phase, status.Error)
			}

			if consensus.ConsensusNow().After(deadline) {
				return fmt.Errorf("timeout waiting for phase %s", phase)
			}

		case <-ic.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for phase %s", phase)
		}
	}
}

// GetStatus returns current initialization status
func (ic *InitializationCoordinator) GetStatus() map[InitPhase]InitStatus {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	status := make(map[InitPhase]InitStatus)
	for phase, s := range ic.status {
		if s != nil {
			status[phase] = *s
		}
	}

	return status
}

// GetPhaseStatus returns status for a specific phase
func (ic *InitializationCoordinator) GetPhaseStatus(phase InitPhase) (*InitStatus, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	status, exists := ic.status[phase]
	if !exists || status == nil {
		return nil, false
	}

	// Return a copy
	statusCopy := *status
	return &statusCopy, true
}

// IsInitialized returns whether initialization has completed successfully
func (ic *InitializationCoordinator) IsInitialized() bool {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	completeStatus, exists := ic.status[PhaseComplete]
	return exists && completeStatus != nil && completeStatus.Success
}

// Shutdown performs ordered shutdown
func (ic *InitializationCoordinator) Shutdown(timeout time.Duration) error {
	ic.logger.Info("Starting shutdown sequence")

	// Cancel main context to signal shutdown
	ic.cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var shutdownErrors []error

	// Shutdown in reverse order of initialization
	for _, phase := range ic.shutdownOrder {
		config, exists := ic.phaseConfigs[phase]
		if !exists || config.ShutdownHandler == nil {
			continue
		}

		ic.logger.WithField("phase", phase).Info("Shutting down phase")

		phaseCtx, phaseCancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		if err := config.ShutdownHandler(phaseCtx); err != nil {
			ic.logger.WithError(err).WithField("phase", phase).Error("Phase shutdown error")
			shutdownErrors = append(shutdownErrors, fmt.Errorf("phase %s: %w", phase, err))
		}
		phaseCancel()
	}

	if len(shutdownErrors) > 0 {
		return fmt.Errorf("shutdown completed with %d errors: %v", len(shutdownErrors), shutdownErrors)
	}

	ic.logger.Info("Shutdown sequence completed successfully")
	return nil
}

// HealthCheck performs health check on all initialized phases
func (ic *InitializationCoordinator) HealthCheck() error {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	if !ic.IsInitialized() {
		return fmt.Errorf("system not initialized")
	}

	unhealthyPhases := []string{}
	for phase, status := range ic.status {
		if phase == PhaseComplete {
			continue
		}
		if status == nil || !status.Success {
			unhealthyPhases = append(unhealthyPhases, string(phase))
		}
	}

	if len(unhealthyPhases) > 0 {
		return fmt.Errorf("unhealthy phases: %v", unhealthyPhases)
	}

	return nil
}
