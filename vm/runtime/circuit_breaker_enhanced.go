// Package runtime provides enhanced circuit breaker functionality
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// EnhancedCircuitBreaker implements enterprise-grade circuit breaker with comprehensive logging
type EnhancedCircuitBreaker struct {
	// Configuration
	name             string
	maxFailures      int
	baseResetTimeout time.Duration
	halfOpenRequests int
	logger           *logrus.Logger

	// State tracking
	mu                  sync.RWMutex
	state               CircuitBreakerState
	failures            int
	lastFailureTime     time.Time
	successCount        int
	consecutiveFailures int
	lastStateChange     time.Time

	// Callbacks
	onStateChange func(from, to CircuitBreakerState)

	// Metrics with atomic operations for thread safety
	totalRequests   uint64
	successfulCalls uint64
	failedCalls     uint64
	rejectedCalls   uint64

	// Advanced features
	adaptiveTimeout   bool
	timeoutMultiplier float64
	currentTimeout    time.Duration
	healthCheckFunc   func() error
	metricsReporter   *CircuitBreakerMetricsReporter
}

// EnhancedCircuitBreakerConfig contains configuration for enhanced circuit breaker
type EnhancedCircuitBreakerConfig struct {
	MaxFailures       int                                // Number of failures before opening
	ResetTimeout      time.Duration                      // Time to wait before attempting reset
	HalfOpenRequests  int                                // Number of requests allowed in half-open state
	OnStateChange     func(from, to CircuitBreakerState) // State change callback
	Logger            *logrus.Logger                     // Logger for comprehensive logging
	AdaptiveTimeout   bool                               // Enable adaptive timeout adjustment
	TimeoutMultiplier float64                            // Multiplier for adaptive timeout
	HealthCheckFunc   func() error                       // Custom health check function
	EnableMetrics     bool                               // Enable periodic metrics reporting
	MetricsInterval   time.Duration                      // Interval for metrics reporting
}

// CircuitBreakerMetricsReporter handles periodic metrics reporting
type CircuitBreakerMetricsReporter struct {
	breaker  *EnhancedCircuitBreaker
	interval time.Duration
	stopChan chan struct{}
	logger   *logrus.Logger
}

// NewEnhancedCircuitBreaker creates an enterprise-grade circuit breaker with logging
func NewEnhancedCircuitBreaker(name string, config EnhancedCircuitBreakerConfig) *EnhancedCircuitBreaker {
	// Set defaults
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 60 * time.Second
	}
	if config.HalfOpenRequests <= 0 {
		config.HalfOpenRequests = 3
	}
	if config.Logger == nil {
		config.Logger = logrus.New()
	}
	if config.TimeoutMultiplier <= 0 {
		config.TimeoutMultiplier = 1.5
	}
	if config.MetricsInterval <= 0 {
		config.MetricsInterval = 30 * time.Second
	}

	cb := &EnhancedCircuitBreaker{
		name:              name,
		maxFailures:       config.MaxFailures,
		baseResetTimeout:  config.ResetTimeout,
		halfOpenRequests:  config.HalfOpenRequests,
		state:             CircuitClosed,
		onStateChange:     config.OnStateChange,
		logger:            config.Logger,
		adaptiveTimeout:   config.AdaptiveTimeout,
		timeoutMultiplier: config.TimeoutMultiplier,
		currentTimeout:    config.ResetTimeout,
		healthCheckFunc:   config.HealthCheckFunc,
		lastStateChange:   consensus.ConsensusNow(),
	}

	// Log initialization
	cb.logger.WithFields(logrus.Fields{
		"name":             name,
		"maxFailures":      config.MaxFailures,
		"resetTimeout":     config.ResetTimeout,
		"halfOpenRequests": config.HalfOpenRequests,
		"adaptiveTimeout":  config.AdaptiveTimeout,
	}).Info("Circuit breaker initialized with enterprise-grade features")

	// Start metrics reporter if enabled
	if config.EnableMetrics {
		cb.metricsReporter = &CircuitBreakerMetricsReporter{
			breaker:  cb,
			interval: config.MetricsInterval,
			stopChan: make(chan struct{}),
			logger:   config.Logger,
		}
		go cb.metricsReporter.Start()
	}

	return cb
}

// Call executes the given function with circuit breaker protection
func (cb *EnhancedCircuitBreaker) Call(fn func() error) error {
	startTime := consensus.ConsensusNow()

	if err := cb.canExecute(); err != nil {
		cb.logger.WithFields(logrus.Fields{
			"name":  cb.name,
			"state": cb.GetState().String(),
			"error": err.Error(),
		}).Warn("Circuit breaker rejected call")
		return err
	}

	// Execute function
	err := fn()
	executionTime := consensus.ConsensusSince(startTime)

	// Record result
	cb.recordResult(err, executionTime)

	// Log execution
	if err != nil {
		cb.logger.WithFields(logrus.Fields{
			"name":          cb.name,
			"executionTime": executionTime,
			"error":         err.Error(),
			"failures":      cb.getFailureCount(),
		}).Error("Circuit breaker call failed")
	} else {
		cb.logger.WithFields(logrus.Fields{
			"name":          cb.name,
			"executionTime": executionTime,
			"successCount":  cb.getSuccessCount(),
		}).Debug("Circuit breaker call succeeded")
	}

	return err
}

// CallWithContext executes function with context and circuit breaker protection
func (cb *EnhancedCircuitBreaker) CallWithContext(ctx context.Context, fn func(context.Context) error) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return err
	}

	return cb.Call(func() error {
		return fn(ctx)
	})
}

// CallWithFallback executes the function with a fallback on circuit open
func (cb *EnhancedCircuitBreaker) CallWithFallback(fn func() error, fallback func() error) error {
	if err := cb.canExecute(); err != nil {
		if errors.Is(err, ErrCircuitOpen) && fallback != nil {
			cb.logger.WithFields(logrus.Fields{
				"name":  cb.name,
				"state": cb.GetState().String(),
			}).Info("Circuit breaker executing fallback")
			return fallback()
		}
		return err
	}

	err := fn()
	cb.recordResult(err, 0)
	return err
}

// GetState returns the current state of the circuit breaker
func (cb *EnhancedCircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetDetailedMetrics returns comprehensive circuit breaker metrics
func (cb *EnhancedCircuitBreaker) GetDetailedMetrics() DetailedCircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	successRate := float64(0)
	total := atomic.LoadUint64(&cb.totalRequests)
	successCalls := atomic.LoadUint64(&cb.successfulCalls)
	if total > 0 {
		successRate = float64(successCalls) / float64(total) * 100
	}

	return DetailedCircuitBreakerMetrics{
		Name:                cb.name,
		State:               cb.state.String(),
		TotalRequests:       total,
		SuccessfulCalls:     successCalls,
		FailedCalls:         atomic.LoadUint64(&cb.failedCalls),
		RejectedCalls:       atomic.LoadUint64(&cb.rejectedCalls),
		ConsecutiveFailures: cb.consecutiveFailures,
		SuccessRate:         successRate,
		CurrentTimeout:      cb.currentTimeout,
		LastStateChange:     cb.lastStateChange,
		LastFailureTime:     cb.lastFailureTime,
		Uptime:              time.Since(cb.lastStateChange),
	}
}

// Reset manually resets the circuit breaker
func (cb *EnhancedCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldState := cb.state
	cb.changeState(CircuitClosed)
	cb.failures = 0
	cb.successCount = 0
	cb.consecutiveFailures = 0
	cb.lastFailureTime = time.Time{}
	cb.currentTimeout = cb.baseResetTimeout

	cb.logger.WithFields(logrus.Fields{
		"name":     cb.name,
		"oldState": oldState.String(),
		"newState": CircuitClosed.String(),
	}).Info("Circuit breaker manually reset")
}

// ForceOpen forces the circuit breaker to open state
func (cb *EnhancedCircuitBreaker) ForceOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldState := cb.state
	cb.changeState(CircuitOpen)
	cb.lastFailureTime = consensus.ConsensusNow()

	cb.logger.WithFields(logrus.Fields{
		"name":     cb.name,
		"oldState": oldState.String(),
		"newState": CircuitOpen.String(),
	}).Warn("Circuit breaker forced open")
}

// HealthCheck performs a health check in half-open state
func (cb *EnhancedCircuitBreaker) HealthCheck() error {
	if cb.healthCheckFunc == nil {
		return nil
	}

	cb.logger.WithField("name", cb.name).Debug("Performing circuit breaker health check")

	err := cb.healthCheckFunc()
	if err != nil {
		cb.logger.WithFields(logrus.Fields{
			"name":  cb.name,
			"error": err.Error(),
		}).Warn("Circuit breaker health check failed")
	} else {
		cb.logger.WithField("name", cb.name).Debug("Circuit breaker health check passed")
	}

	return err
}

// canExecute checks if a request can be executed
func (cb *EnhancedCircuitBreaker) canExecute() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddUint64(&cb.totalRequests, 1)

	switch cb.state {
	case CircuitClosed:
		return nil

	case CircuitOpen:
		// Check if we should transition to half-open
		if consensus.ConsensusSince(cb.lastFailureTime) > cb.currentTimeout {
			// Perform health check if available
			if cb.healthCheckFunc != nil {
				if err := cb.healthCheckFunc(); err != nil {
					cb.lastFailureTime = consensus.ConsensusNow()
					cb.adjustTimeout()
					cb.logger.WithFields(logrus.Fields{
						"name":           cb.name,
						"healthCheck":    "failed",
						"currentTimeout": cb.currentTimeout,
					}).Debug("Circuit breaker health check failed, remaining open")
					return fmt.Errorf("%w: %s (health check failed)", ErrCircuitOpen, cb.name)
				}
			}

			cb.changeState(CircuitHalfOpen)
			cb.successCount = 0
			cb.logger.WithFields(logrus.Fields{
				"name":         cb.name,
				"timeoutAfter": cb.currentTimeout,
			}).Info("Circuit breaker transitioning to half-open after timeout")
			return nil
		}
		atomic.AddUint64(&cb.rejectedCalls, 1)
		return fmt.Errorf("%w: %s", ErrCircuitOpen, cb.name)

	case CircuitHalfOpen:
		if cb.successCount >= cb.halfOpenRequests {
			atomic.AddUint64(&cb.rejectedCalls, 1)
			return ErrTooManyRequests
		}
		return nil

	default:
		return fmt.Errorf("unknown circuit breaker state: %v", cb.state)
	}
}

// recordResult records the result of a call
func (cb *EnhancedCircuitBreaker) recordResult(err error, executionTime time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		cb.onSuccess()
	} else {
		cb.onFailure(err)
	}
}

// onSuccess handles successful calls
func (cb *EnhancedCircuitBreaker) onSuccess() {
	atomic.AddUint64(&cb.successfulCalls, 1)
	cb.consecutiveFailures = 0

	switch cb.state {
	case CircuitClosed:
		cb.failures = 0

	case CircuitHalfOpen:
		cb.successCount++
		cb.logger.WithFields(logrus.Fields{
			"name":         cb.name,
			"successCount": cb.successCount,
			"required":     cb.halfOpenRequests,
		}).Debug("Circuit breaker half-open success")

		if cb.successCount >= cb.halfOpenRequests {
			cb.changeState(CircuitClosed)
			cb.failures = 0
			cb.resetTimeoutToBase()
			cb.logger.WithField("name", cb.name).Info("Circuit breaker recovered and closed")
		}
	}
}

// onFailure handles failed calls
func (cb *EnhancedCircuitBreaker) onFailure(err error) {
	atomic.AddUint64(&cb.failedCalls, 1)
	cb.lastFailureTime = consensus.ConsensusNow()
	cb.consecutiveFailures++

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		cb.logger.WithFields(logrus.Fields{
			"name":        cb.name,
			"failures":    cb.failures,
			"maxFailures": cb.maxFailures,
			"error":       err.Error(),
		}).Warn("Circuit breaker failure in closed state")

		if cb.failures >= cb.maxFailures {
			cb.changeState(CircuitOpen)
			cb.logger.WithFields(logrus.Fields{
				"name":     cb.name,
				"failures": cb.failures,
				"timeout":  cb.currentTimeout,
			}).Error("Circuit breaker opened due to excessive failures")
		}

	case CircuitHalfOpen:
		cb.changeState(CircuitOpen)
		cb.failures = 0
		cb.adjustTimeout()
		cb.logger.WithFields(logrus.Fields{
			"name":           cb.name,
			"error":          err.Error(),
			"currentTimeout": cb.currentTimeout,
		}).Warn("Circuit breaker reopened from half-open state")
	}
}

// changeState transitions to a new state
func (cb *EnhancedCircuitBreaker) changeState(newState CircuitBreakerState) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState
	cb.lastStateChange = consensus.ConsensusNow()

	// Log state change with detailed context
	cb.logger.WithFields(logrus.Fields{
		"name":                cb.name,
		"oldState":            oldState.String(),
		"newState":            newState.String(),
		"failures":            cb.failures,
		"consecutiveFailures": cb.consecutiveFailures,
		"currentTimeout":      cb.currentTimeout,
		"timestamp":           cb.lastStateChange,
	}).Info("Circuit breaker state changed")

	// Notify callback
	if cb.onStateChange != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					cb.logger.WithFields(logrus.Fields{
						"name":  cb.name,
						"panic": r,
					}).Error("Panic in circuit breaker state change callback")
				}
			}()
			cb.onStateChange(oldState, newState)
		}()
	}
}

// adjustTimeout adjusts timeout based on consecutive failures (adaptive timeout)
func (cb *EnhancedCircuitBreaker) adjustTimeout() {
	if !cb.adaptiveTimeout {
		return
	}

	oldTimeout := cb.currentTimeout
	cb.currentTimeout = time.Duration(float64(cb.currentTimeout) * cb.timeoutMultiplier)

	// Cap maximum timeout at 5 minutes
	maxTimeout := 5 * time.Minute
	if cb.currentTimeout > maxTimeout {
		cb.currentTimeout = maxTimeout
	}

	cb.logger.WithFields(logrus.Fields{
		"name":       cb.name,
		"oldTimeout": oldTimeout,
		"newTimeout": cb.currentTimeout,
		"multiplier": cb.timeoutMultiplier,
	}).Info("Circuit breaker timeout adjusted")
}

// resetTimeoutToBase resets timeout to original value
func (cb *EnhancedCircuitBreaker) resetTimeoutToBase() {
	if !cb.adaptiveTimeout {
		return
	}

	if cb.currentTimeout != cb.baseResetTimeout {
		cb.logger.WithFields(logrus.Fields{
			"name":       cb.name,
			"oldTimeout": cb.currentTimeout,
			"newTimeout": cb.baseResetTimeout,
		}).Info("Circuit breaker timeout reset to original")
		cb.currentTimeout = cb.baseResetTimeout
	}
}

// Helper methods for metrics
func (cb *EnhancedCircuitBreaker) getFailureCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failures
}

func (cb *EnhancedCircuitBreaker) getSuccessCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.successCount
}

// DetailedCircuitBreakerMetrics contains comprehensive metrics
type DetailedCircuitBreakerMetrics struct {
	Name                string
	State               string
	TotalRequests       uint64
	SuccessfulCalls     uint64
	FailedCalls         uint64
	RejectedCalls       uint64
	ConsecutiveFailures int
	SuccessRate         float64
	CurrentTimeout      time.Duration
	LastStateChange     time.Time
	LastFailureTime     time.Time
	Uptime              time.Duration
}

// Start starts the metrics reporter
func (mr *CircuitBreakerMetricsReporter) Start() {
	ticker := time.NewTicker(mr.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := mr.breaker.GetDetailedMetrics()
			mr.logger.WithFields(logrus.Fields{
				"name":            metrics.Name,
				"state":           metrics.State,
				"totalRequests":   metrics.TotalRequests,
				"successfulCalls": metrics.SuccessfulCalls,
				"failedCalls":     metrics.FailedCalls,
				"rejectedCalls":   metrics.RejectedCalls,
				"successRate":     fmt.Sprintf("%.2f%%", metrics.SuccessRate),
				"currentTimeout":  metrics.CurrentTimeout,
				"uptime":          metrics.Uptime,
			}).Info("Circuit breaker metrics report")

		case <-mr.stopChan:
			mr.logger.WithField("name", mr.breaker.name).Info("Circuit breaker metrics reporter stopped")
			return
		}
	}
}

// Stop stops the metrics reporter
func (mr *CircuitBreakerMetricsReporter) Stop() {
	close(mr.stopChan)
}

// EnhancedRuntimeCircuitBreakers manages enhanced circuit breakers for all runtimes
type EnhancedRuntimeCircuitBreakers struct {
	mu       sync.RWMutex
	breakers map[RuntimeType]*EnhancedCircuitBreaker
	config   EnhancedCircuitBreakerConfig
	logger   *logrus.Logger
}

// NewEnhancedRuntimeCircuitBreakers creates enhanced circuit breaker manager
func NewEnhancedRuntimeCircuitBreakers(config EnhancedCircuitBreakerConfig, logger *logrus.Logger) *EnhancedRuntimeCircuitBreakers {
	if logger == nil {
		logger = logrus.New()
	}

	return &EnhancedRuntimeCircuitBreakers{
		breakers: make(map[RuntimeType]*EnhancedCircuitBreaker),
		config:   config,
		logger:   logger,
	}
}

// GetBreaker gets or creates an enhanced circuit breaker for a runtime
func (ercb *EnhancedRuntimeCircuitBreakers) GetBreaker(runtimeType RuntimeType) *EnhancedCircuitBreaker {
	ercb.mu.RLock()
	breaker, exists := ercb.breakers[runtimeType]
	ercb.mu.RUnlock()

	if exists {
		return breaker
	}

	// Create new breaker
	ercb.mu.Lock()
	defer ercb.mu.Unlock()

	// Double-check after acquiring write lock
	if breaker, exists := ercb.breakers[runtimeType]; exists {
		return breaker
	}

	// Configure logger for specific runtime
	// Create a copy of the config to avoid modifying the shared config
	newConfig := ercb.config
	// We can't use WithField directly as it returns *logrus.Entry, not *logrus.Logger
	// So we keep the same logger but log the runtime type in the circuit breaker name
	newConfig.Logger = ercb.logger

	// Include runtime type in the circuit breaker name
	breakerName := fmt.Sprintf("%s-runtime", string(runtimeType))
	breaker = NewEnhancedCircuitBreaker(breakerName, newConfig)
	ercb.breakers[runtimeType] = breaker

	ercb.logger.WithField("runtime", string(runtimeType)).Info("Created new enhanced circuit breaker for runtime")

	return breaker
}

// Execute executes a function with enhanced circuit breaker protection
func (ercb *EnhancedRuntimeCircuitBreakers) Execute(runtimeType RuntimeType, fn func() error) error {
	breaker := ercb.GetBreaker(runtimeType)
	return breaker.Call(fn)
}

// ExecuteWithContext executes with context and circuit breaker protection
func (ercb *EnhancedRuntimeCircuitBreakers) ExecuteWithContext(
	ctx context.Context,
	runtimeType RuntimeType,
	fn func(context.Context) error,
) error {
	breaker := ercb.GetBreaker(runtimeType)
	return breaker.CallWithContext(ctx, fn)
}

// ExecuteWithFallback executes with circuit breaker and fallback
func (ercb *EnhancedRuntimeCircuitBreakers) ExecuteWithFallback(
	runtimeType RuntimeType,
	fn func() error,
	fallback func() error,
) error {
	breaker := ercb.GetBreaker(runtimeType)
	return breaker.CallWithFallback(fn, fallback)
}

// GetAllMetrics returns detailed metrics for all circuit breakers
func (ercb *EnhancedRuntimeCircuitBreakers) GetAllMetrics() map[RuntimeType]DetailedCircuitBreakerMetrics {
	ercb.mu.RLock()
	defer ercb.mu.RUnlock()

	metrics := make(map[RuntimeType]DetailedCircuitBreakerMetrics)
	for rt, breaker := range ercb.breakers {
		metrics[rt] = breaker.GetDetailedMetrics()
	}
	return metrics
}

// ResetAll resets all circuit breakers
func (ercb *EnhancedRuntimeCircuitBreakers) ResetAll() {
	ercb.mu.RLock()
	defer ercb.mu.RUnlock()

	for _, breaker := range ercb.breakers {
		breaker.Reset()
	}

	ercb.logger.Info("All enhanced circuit breakers reset")
}

// ForceOpenAll forces all circuit breakers to open state
func (ercb *EnhancedRuntimeCircuitBreakers) ForceOpenAll() {
	ercb.mu.RLock()
	defer ercb.mu.RUnlock()

	for _, breaker := range ercb.breakers {
		breaker.ForceOpen()
	}

	ercb.logger.Warn("All enhanced circuit breakers forced open")
}

// Shutdown gracefully shuts down all circuit breakers
func (ercb *EnhancedRuntimeCircuitBreakers) Shutdown() {
	ercb.mu.Lock()
	defer ercb.mu.Unlock()

	// Stop all metrics reporters
	for rt, breaker := range ercb.breakers {
		if breaker.metricsReporter != nil {
			breaker.metricsReporter.Stop()
		}
		ercb.logger.WithField("runtime", string(rt)).Info("Circuit breaker shutdown")
	}

	// Clear breakers
	ercb.breakers = make(map[RuntimeType]*EnhancedCircuitBreaker)
	ercb.logger.Info("All enhanced circuit breakers shut down")
}
