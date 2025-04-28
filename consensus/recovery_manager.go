// consensus/recovery_manager.go

package consensus

import (
	"fmt"
	"sync"
	"time"
)

// RecoveryStrategy defines the strategy to use for recovery
type RecoveryStrategy int

const (
	// RecoveryStrategyNone indicates no recovery should be attempted
	RecoveryStrategyNone RecoveryStrategy = iota
	// RecoveryStrategyRetry indicates the operation should be retried
	RecoveryStrategyRetry
	// RecoveryStrategyCheckpoint indicates recovery from checkpoint should be attempted
	RecoveryStrategyCheckpoint
	// RecoveryStrategyReset indicates the system should be reset to a known good state
	RecoveryStrategyReset
	// RecoveryStrategyRestart indicates the system should be restarted
	RecoveryStrategyRestart
	// RecoveryStrategyManual indicates manual intervention is required
	RecoveryStrategyManual
)

// String returns a string representation of the recovery strategy
func (s RecoveryStrategy) String() string {
	switch s {
	case RecoveryStrategyNone:
		return "None"
	case RecoveryStrategyRetry:
		return "Retry"
	case RecoveryStrategyCheckpoint:
		return "Checkpoint"
	case RecoveryStrategyReset:
		return "Reset"
	case RecoveryStrategyRestart:
		return "Restart"
	case RecoveryStrategyManual:
		return "Manual"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// RecoveryAction defines a specific recovery action to take
type RecoveryAction struct {
	Strategy      RecoveryStrategy
	Description   string
	RetryDelay    time.Duration
	CheckpointNum uint64
	MaxAttempts   int
}

// RecoveryManager handles error recovery for the consensus system
type RecoveryManager struct {
	hc *HybridConsensus

	// Recovery state

	recoveryAttempts map[ConsensusErrorCode]int
	circuitBreakers  map[ConsensusErrorCode]time.Time

	// Configuration
	maxRecoveryAttempts     int
	recoveryBackoff         time.Duration
	circuitBreakerDuration  time.Duration
	enableAutomaticRecovery bool
	recoveryStrategies      map[ConsensusErrorCode]RecoveryStrategy

	// Concurrency control
	mu sync.RWMutex
}

// NewRecoveryManager creates a new RecoveryManager
func NewRecoveryManager(hc *HybridConsensus) *RecoveryManager {
	rm := &RecoveryManager{
		hc:                      hc,
		recoveryAttempts:        make(map[ConsensusErrorCode]int),
		circuitBreakers:         make(map[ConsensusErrorCode]time.Time),
		maxRecoveryAttempts:     3,
		recoveryBackoff:         5 * time.Second,
		circuitBreakerDuration:  5 * time.Minute,
		enableAutomaticRecovery: true,
		recoveryStrategies:      make(map[ConsensusErrorCode]RecoveryStrategy),
	}

	// Set default recovery strategies
	rm.initDefaultRecoveryStrategies()

	return rm
}

// initDefaultRecoveryStrategies sets up default recovery strategies for different error codes
func (rm *RecoveryManager) initDefaultRecoveryStrategies() {
	// General errors
	rm.recoveryStrategies[ErrTimeout] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrCanceled] = RecoveryStrategyNone

	// Validator errors
	rm.recoveryStrategies[ErrInvalidValidator] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrValidatorNotFound] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrInsufficientStake] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrValidatorSetInconsistency] = RecoveryStrategyCheckpoint

	// Event errors
	rm.recoveryStrategies[ErrEventCreationFailed] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrEventValidationFailed] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrEventFinalizationFailed] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrEventDuplicate] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrEventTimeout] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrEventRejected] = RecoveryStrategyNone

	// Block errors
	rm.recoveryStrategies[ErrBlockCreationFailed] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrBlockValidationFailed] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrBlockFinalizationFailed] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrInvalidBlockNumber] = RecoveryStrategyCheckpoint
	rm.recoveryStrategies[ErrInvalidBlockProducer] = RecoveryStrategyNone

	// PoH errors
	rm.recoveryStrategies[ErrPoHVerificationFailed] = RecoveryStrategyNone
	rm.recoveryStrategies[ErrPoHDriftExceeded] = RecoveryStrategyCheckpoint
	rm.recoveryStrategies[ErrPoHSynchronizationFailed] = RecoveryStrategyCheckpoint

	// State errors
	rm.recoveryStrategies[ErrStateCorruption] = RecoveryStrategyCheckpoint
	rm.recoveryStrategies[ErrStateInconsistency] = RecoveryStrategyCheckpoint
	rm.recoveryStrategies[ErrStateSynchronizationFailed] = RecoveryStrategyCheckpoint

	// Checkpoint errors
	rm.recoveryStrategies[ErrCheckpointCreationFailed] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrCheckpointRestorationFailed] = RecoveryStrategyManual
	rm.recoveryStrategies[ErrCheckpointNotFound] = RecoveryStrategyManual
	rm.recoveryStrategies[ErrInvalidCheckpoint] = RecoveryStrategyManual

	// Network errors
	rm.recoveryStrategies[ErrNetworkPartition] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrNetworkOverload] = RecoveryStrategyRetry
	rm.recoveryStrategies[ErrMessagePropagationFailed] = RecoveryStrategyRetry

	// Configuration errors
	rm.recoveryStrategies[ErrInvalidConfiguration] = RecoveryStrategyManual
	rm.recoveryStrategies[ErrIncompatibleConfiguration] = RecoveryStrategyManual
}

// SetRecoveryStrategy sets the recovery strategy for a specific error code
func (rm *RecoveryManager) SetRecoveryStrategy(code ConsensusErrorCode, strategy RecoveryStrategy) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.recoveryStrategies[code] = strategy
}

// GetRecoveryStrategy gets the recovery strategy for a specific error code
func (rm *RecoveryManager) GetRecoveryStrategy(code ConsensusErrorCode) RecoveryStrategy {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if strategy, ok := rm.recoveryStrategies[code]; ok {
		return strategy
	}
	return RecoveryStrategyNone
}

// SetMaxRecoveryAttempts sets the maximum number of recovery attempts
func (rm *RecoveryManager) SetMaxRecoveryAttempts(max int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if max > 0 {
		rm.maxRecoveryAttempts = max
	}
}

// SetRecoveryBackoff sets the backoff duration between recovery attempts
func (rm *RecoveryManager) SetRecoveryBackoff(backoff time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if backoff > 0 {
		rm.recoveryBackoff = backoff
	}
}

// SetCircuitBreakerDuration sets the duration for which a circuit breaker remains open
func (rm *RecoveryManager) SetCircuitBreakerDuration(duration time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if duration > 0 {
		rm.circuitBreakerDuration = duration
	}
}

// EnableAutomaticRecovery enables or disables automatic recovery
func (rm *RecoveryManager) EnableAutomaticRecovery(enable bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.enableAutomaticRecovery = enable
}

// HandleError handles an error and attempts recovery if appropriate
func (rm *RecoveryManager) HandleError(err error) error {
	if err == nil {
		return nil
	}

	// Extract error information
	var cerr *ConsensusError
	var ok bool
	if cerr, ok = err.(*ConsensusError); !ok {
		// Wrap non-ConsensusError in a ConsensusError
		cerr = WrapError(err, ErrUnknown, ErrorCategoryTemporary, "Unknown error")
	}

	// Check if circuit breaker is active for this error code
	if rm.isCircuitBreakerActive(cerr.Code) {
		rm.hc.logger.Warn("Circuit breaker active, skipping recovery",
			"errorCode", cerr.Code,
			"errorCategory", cerr.Category)
		return cerr
	}

	// Get recovery strategy
	strategy := rm.GetRecoveryStrategy(cerr.Code)
	if strategy == RecoveryStrategyNone || !rm.enableAutomaticRecovery {
		return cerr
	}

	// Check if we've exceeded max recovery attempts
	attempts := rm.getRecoveryAttempts(cerr.Code)
	if attempts >= rm.maxRecoveryAttempts {
		rm.activateCircuitBreaker(cerr.Code)
		rm.hc.logger.Warn("Max recovery attempts exceeded, activating circuit breaker",
			"errorCode", cerr.Code,
			"errorCategory", cerr.Category,
			"attempts", attempts,
			"maxAttempts", rm.maxRecoveryAttempts)
		return cerr
	}

	// Increment recovery attempts
	rm.incrementRecoveryAttempts(cerr.Code)

	// Log recovery attempt
	rm.hc.logger.Info("Attempting recovery",
		"errorCode", cerr.Code,
		"errorCategory", cerr.Category,
		"strategy", strategy,
		"attempt", attempts+1,
		"maxAttempts", rm.maxRecoveryAttempts)

	// Apply recovery strategy
	var recoveryErr error
	switch strategy {
	case RecoveryStrategyRetry:
		recoveryErr = rm.applyRetryStrategy(cerr)
	case RecoveryStrategyCheckpoint:
		recoveryErr = rm.applyCheckpointStrategy(cerr)
	case RecoveryStrategyReset:
		recoveryErr = rm.applyResetStrategy(cerr)
	case RecoveryStrategyRestart:
		recoveryErr = rm.applyRestartStrategy(cerr)
	case RecoveryStrategyManual:
		recoveryErr = rm.applyManualStrategy(cerr)
	default:
		recoveryErr = fmt.Errorf("unknown recovery strategy: %v", strategy)
	}

	// If recovery failed, return the original error
	if recoveryErr != nil {
		rm.hc.logger.Error("Recovery failed",
			"errorCode", cerr.Code,
			"errorCategory", cerr.Category,
			"strategy", strategy,
			"recoveryError", recoveryErr)
		return cerr
	}

	// Recovery succeeded
	rm.hc.logger.Info("Recovery succeeded",
		"errorCode", cerr.Code,
		"errorCategory", cerr.Category,
		"strategy", strategy)
	return nil
}

// isCircuitBreakerActive checks if a circuit breaker is active for an error code
func (rm *RecoveryManager) isCircuitBreakerActive(code ConsensusErrorCode) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if openTime, ok := rm.circuitBreakers[code]; ok {
		if time.Since(openTime) < rm.circuitBreakerDuration {
			return true
		}
		// Circuit breaker has expired, remove it
		delete(rm.circuitBreakers, code)
	}
	return false
}

// activateCircuitBreaker activates a circuit breaker for an error code
func (rm *RecoveryManager) activateCircuitBreaker(code ConsensusErrorCode) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.circuitBreakers[code] = time.Now()
}

// getRecoveryAttempts gets the number of recovery attempts for an error code
func (rm *RecoveryManager) getRecoveryAttempts(code ConsensusErrorCode) int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.recoveryAttempts[code]
}

// incrementRecoveryAttempts increments the number of recovery attempts for an error code
func (rm *RecoveryManager) incrementRecoveryAttempts(code ConsensusErrorCode) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.recoveryAttempts[code]++
}

// resetRecoveryAttempts resets the number of recovery attempts for an error code
func (rm *RecoveryManager) resetRecoveryAttempts(code ConsensusErrorCode) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.recoveryAttempts, code)
}

// applyRetryStrategy applies the retry recovery strategy
func (rm *RecoveryManager) applyRetryStrategy(cerr *ConsensusError) error {
	// For retry strategy, we just wait for the backoff period
	// The actual retry will be handled by the caller
	time.Sleep(rm.recoveryBackoff)
	return nil
}

// applyCheckpointStrategy applies the checkpoint recovery strategy
func (rm *RecoveryManager) applyCheckpointStrategy(cerr *ConsensusError) error {
	// Find the latest checkpoint
	rm.mu.RLock()
	lastCheckpoint := rm.hc.lastCheckpoint
	rm.mu.RUnlock()

	if lastCheckpoint == 0 {
		return fmt.Errorf("no checkpoint available for recovery")
	}

	// Attempt to recover from the checkpoint
	return rm.hc.recoverFromError()
}

// applyResetStrategy applies the reset recovery strategy
func (rm *RecoveryManager) applyResetStrategy(cerr *ConsensusError) error {
	// Reset the consensus state
	// This is a more drastic measure than checkpoint recovery
	// It resets the consensus state to its initial state

	// Stop the consensus
	if err := rm.hc.Stop(); err != nil {
		return fmt.Errorf("failed to stop consensus: %w", err)
	}

	// Reset state
	rm.hc.lastBlockHeight = 0
	rm.hc.lastFinalizedHeight = 0

	// Restart the consensus
	if err := rm.hc.Start(); err != nil {
		return fmt.Errorf("failed to restart consensus: %w", err)
	}

	return nil
}

// applyRestartStrategy applies the restart recovery strategy
func (rm *RecoveryManager) applyRestartStrategy(cerr *ConsensusError) error {
	// Restart the consensus
	if err := rm.hc.Stop(); err != nil {
		return fmt.Errorf("failed to stop consensus: %w", err)
	}

	// Wait a bit before restarting
	time.Sleep(1 * time.Second)

	if err := rm.hc.Start(); err != nil {
		return fmt.Errorf("failed to restart consensus: %w", err)
	}

	return nil
}

// applyManualStrategy applies the manual recovery strategy
func (rm *RecoveryManager) applyManualStrategy(cerr *ConsensusError) error {
	// For manual strategy, we just log the error and return
	// Manual intervention is required
	rm.hc.logger.Error("Manual intervention required",
		"errorCode", cerr.Code,
		"errorCategory", cerr.Category,
		"message", cerr.Message,
		"blockNumber", cerr.BlockNumber)
	return fmt.Errorf("manual intervention required")
}

// ResetCircuitBreakers resets all circuit breakers
func (rm *RecoveryManager) ResetCircuitBreakers() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.circuitBreakers = make(map[ConsensusErrorCode]time.Time)
}

// ResetRecoveryAttempts resets all recovery attempts
func (rm *RecoveryManager) ResetRecoveryAttempts() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.recoveryAttempts = make(map[ConsensusErrorCode]int)
}

// GetCircuitBreakerStatus returns the status of all circuit breakers
func (rm *RecoveryManager) GetCircuitBreakerStatus() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	status := make(map[string]interface{})
	for code, openTime := range rm.circuitBreakers {
		timeRemaining := rm.circuitBreakerDuration - time.Since(openTime)
		if timeRemaining > 0 {
			status[code.String()] = map[string]interface{}{
				"active":        true,
				"openedAt":      openTime,
				"timeRemaining": timeRemaining.String(),
			}
		}
	}
	return status
}

// GetRecoveryAttemptStatus returns the status of all recovery attempts
func (rm *RecoveryManager) GetRecoveryAttemptStatus() map[string]int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	status := make(map[string]int)
	for code, attempts := range rm.recoveryAttempts {
		status[code.String()] = attempts
	}
	return status
}
