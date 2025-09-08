// Package runtime provides circuit breaker functionality
package runtime

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	// CircuitClosed allows requests to pass through
	CircuitClosed CircuitBreakerState = iota
	// CircuitOpen blocks all requests
	CircuitOpen
	// CircuitHalfOpen allows limited requests for testing
	CircuitHalfOpen
)

var (
	// ErrCircuitOpen is returned when circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
	// ErrTooManyRequests is returned when half-open circuit reaches limit
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

// CircuitBreaker implements the circuit breaker pattern for runtime protection
type CircuitBreaker struct {
	// Configuration
	name             string
	maxFailures      int
	resetTimeout     time.Duration
	halfOpenRequests int

	// State tracking
	mu              sync.RWMutex
	state           CircuitBreakerState
	failures        int
	lastFailureTime time.Time
	successCount    int

	// Callbacks
	onStateChange func(from, to CircuitBreakerState)

	// Metrics
	metrics *CircuitBreakerMetrics
}

// CircuitBreakerConfig contains configuration for circuit breaker
type CircuitBreakerConfig struct {
	MaxFailures      int           // Number of failures before opening
	ResetTimeout     time.Duration // Time to wait before attempting reset
	HalfOpenRequests int           // Number of requests allowed in half-open state
	OnStateChange    func(from, to CircuitBreakerState)
}

// CircuitBreakerMetrics tracks circuit breaker statistics
type CircuitBreakerMetrics struct {
	TotalRequests   uint64
	SuccessfulCalls uint64
	FailedCalls     uint64
	RejectedCalls   uint64
	StateChanges    map[string]uint64 // state -> count
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string, config CircuitBreakerConfig) *CircuitBreaker {
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 60 * time.Second
	}
	if config.HalfOpenRequests <= 0 {
		config.HalfOpenRequests = 3
	}

	return &CircuitBreaker{
		name:             name,
		maxFailures:      config.MaxFailures,
		resetTimeout:     config.ResetTimeout,
		halfOpenRequests: config.HalfOpenRequests,
		state:            CircuitClosed,
		onStateChange:    config.OnStateChange,
		metrics: &CircuitBreakerMetrics{
			StateChanges: make(map[string]uint64),
		},
	}
}

// Call executes the given function with circuit breaker protection
func (cb *CircuitBreaker) Call(fn func() error) error {
	if err := cb.canExecute(); err != nil {
		return err
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// CallWithFallback executes the function with a fallback on circuit open
func (cb *CircuitBreaker) CallWithFallback(fn func() error, fallback func() error) error {
	if err := cb.canExecute(); err != nil {
		if errors.Is(err, ErrCircuitOpen) && fallback != nil {
			return fallback()
		}
		return err
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetMetrics returns circuit breaker metrics
func (cb *CircuitBreaker) GetMetrics() CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Return a copy to prevent data races
	return CircuitBreakerMetrics{
		TotalRequests:   cb.metrics.TotalRequests,
		SuccessfulCalls: cb.metrics.SuccessfulCalls,
		FailedCalls:     cb.metrics.FailedCalls,
		RejectedCalls:   cb.metrics.RejectedCalls,
		StateChanges:    copyStateChanges(cb.metrics.StateChanges),
	}
}

// Reset manually resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.changeState(CircuitClosed)
	cb.failures = 0
	cb.successCount = 0
	cb.lastFailureTime = time.Time{}
}

// canExecute checks if a request can be executed
func (cb *CircuitBreaker) canExecute() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.metrics.TotalRequests++

	switch cb.state {
	case CircuitClosed:
		return nil

	case CircuitOpen:
		// Check if we should transition to half-open
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.changeState(CircuitHalfOpen)
			cb.successCount = 0
			return nil
		}
		cb.metrics.RejectedCalls++
		return fmt.Errorf("%w: %s", ErrCircuitOpen, cb.name)

	case CircuitHalfOpen:
		if cb.successCount >= cb.halfOpenRequests {
			cb.metrics.RejectedCalls++
			return ErrTooManyRequests
		}
		return nil

	default:
		return fmt.Errorf("unknown circuit breaker state: %v", cb.state)
	}
}

// recordResult records the result of a call
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

// onSuccess handles successful calls
func (cb *CircuitBreaker) onSuccess() {
	cb.metrics.SuccessfulCalls++

	switch cb.state {
	case CircuitClosed:
		cb.failures = 0

	case CircuitHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.halfOpenRequests {
			cb.changeState(CircuitClosed)
			cb.failures = 0
		}
	}
}

// onFailure handles failed calls
func (cb *CircuitBreaker) onFailure() {
	cb.metrics.FailedCalls++
	cb.lastFailureTime = consensus.ConsensusNow()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.maxFailures {
			cb.changeState(CircuitOpen)
		}

	case CircuitHalfOpen:
		cb.changeState(CircuitOpen)
		cb.failures = 0
	}
}

// changeState transitions to a new state
func (cb *CircuitBreaker) changeState(newState CircuitBreakerState) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState

	// Track state change
	key := fmt.Sprintf("%s->%s", stateString(oldState), stateString(newState))
	cb.metrics.StateChanges[key]++

	// Notify callback
	if cb.onStateChange != nil {
		// Call in goroutine to avoid holding lock
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// Log panic but don't crash the circuit breaker
					// Since we don't have a logger here, we'll just ignore it
					// In production, this should be logged
					_ = fmt.Sprintf("panic in circuit breaker state change callback: %v", r)
				}
			}()
			cb.onStateChange(oldState, newState)
		}()
	}
}

// RuntimeCircuitBreakers manages circuit breakers for all runtimes
type RuntimeCircuitBreakers struct {
	mu       sync.RWMutex
	breakers map[RuntimeType]*CircuitBreaker
	config   CircuitBreakerConfig
}

// NewRuntimeCircuitBreakers creates circuit breaker manager for runtimes
func NewRuntimeCircuitBreakers(config CircuitBreakerConfig) *RuntimeCircuitBreakers {
	return &RuntimeCircuitBreakers{
		breakers: make(map[RuntimeType]*CircuitBreaker),
		config:   config,
	}
}

// GetBreaker gets or creates a circuit breaker for a runtime
func (rcb *RuntimeCircuitBreakers) GetBreaker(runtimeType RuntimeType) *CircuitBreaker {
	rcb.mu.RLock()
	breaker, exists := rcb.breakers[runtimeType]
	rcb.mu.RUnlock()

	if exists {
		return breaker
	}

	// Create new breaker
	rcb.mu.Lock()
	defer rcb.mu.Unlock()

	// Double-check after acquiring write lock
	if breaker, exists := rcb.breakers[runtimeType]; exists {
		return breaker
	}

	breaker = NewCircuitBreaker(string(runtimeType), rcb.config)
	rcb.breakers[runtimeType] = breaker
	return breaker
}

// Execute executes a function with circuit breaker protection
func (rcb *RuntimeCircuitBreakers) Execute(runtimeType RuntimeType, fn func() error) error {
	breaker := rcb.GetBreaker(runtimeType)
	return breaker.Call(fn)
}

// ExecuteWithFallback executes with circuit breaker and fallback
func (rcb *RuntimeCircuitBreakers) ExecuteWithFallback(
	runtimeType RuntimeType,
	fn func() error,
	fallback func() error,
) error {
	breaker := rcb.GetBreaker(runtimeType)
	return breaker.CallWithFallback(fn, fallback)
}

// GetAllMetrics returns metrics for all circuit breakers
func (rcb *RuntimeCircuitBreakers) GetAllMetrics() map[RuntimeType]CircuitBreakerMetrics {
	rcb.mu.RLock()
	defer rcb.mu.RUnlock()

	metrics := make(map[RuntimeType]CircuitBreakerMetrics)
	for rt, breaker := range rcb.breakers {
		metrics[rt] = breaker.GetMetrics()
	}
	return metrics
}

// Helper functions

func stateString(state CircuitBreakerState) string {
	switch state {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func copyStateChanges(m map[string]uint64) map[string]uint64 {
	copy := make(map[string]uint64, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

// String returns string representation of circuit breaker state
func (s CircuitBreakerState) String() string {
	return stateString(s)
}
